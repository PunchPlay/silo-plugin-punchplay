package provider

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// requestClass is which PunchPlay rate limit a request counts against.
type requestClass int

const (
	// classAuth requests run before a profile has a token and are neither
	// paced nor retried: the device flow has its own interval.
	classAuth requestClass = iota
	// classRead is the snapshot and changes feeds: 120 a minute each.
	classRead
	// classWrite is single writes such as unwatching: 120 a minute.
	classWrite
	// classBulk is /sync/history, /sync/ratings and /sync/watchlist: 30 a
	// minute for each.
	classBulk
	// classPlayback is live playback: 120 every five minutes.
	classPlayback
)

// pace keeps a class just under PunchPlay's limit for it, so a long first
// sync never draws a 429: at most burst requests at once, then one per every.
type pace struct {
	every time.Duration
	burst int
}

var defaultPaces = map[requestClass]pace{
	classRead:     {every: 560 * time.Millisecond, burst: 4},  // ≤ 111 a minute
	classWrite:    {every: 550 * time.Millisecond, burst: 4},  // ≤ 113 a minute
	classBulk:     {every: 2100 * time.Millisecond, burst: 1}, // ≤ 29 a minute
	classPlayback: {every: 2600 * time.Millisecond, burst: 4}, // ≤ 119 per five minutes
}

// limiterIdleTTL is how long an account's limiter may sit unused before it
// is dropped. Access tokens rotate hourly, so old keys go idle.
const limiterIdleTTL = 10 * time.Minute

type limiterKey struct {
	token [sha256.Size]byte
	class requestClass
}

type limiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// accountLimiters paces requests per account (access token) and class, so
// one profile's burst never delays another's. Keys are SHA-256 digests: the
// map never holds a token. State is in memory only; a restarted plugin starts
// with full buckets.
type accountLimiters struct {
	paces map[requestClass]pace
	now   func() time.Time

	mu        sync.Mutex
	entries   map[limiterKey]*limiterEntry
	lastSweep time.Time
}

func newAccountLimiters(paces map[requestClass]pace) *accountLimiters {
	return &accountLimiters{paces: paces, now: time.Now, entries: map[limiterKey]*limiterEntry{}}
}

// Wait blocks until the account may send a request of class. It returns an
// error without waiting when ctx is done or its deadline would pass first.
func (l *accountLimiters) Wait(ctx context.Context, token string, class requestClass) error {
	limiter := l.limiter(token, class)
	if limiter == nil {
		return nil
	}
	return limiter.Wait(ctx)
}

func (l *accountLimiters) limiter(token string, class requestClass) *rate.Limiter {
	p, ok := l.paces[class]
	if !ok || token == "" {
		return nil
	}
	key := limiterKey{token: sha256.Sum256([]byte(token)), class: class}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= limiterIdleTTL {
		l.lastSweep = now
		for k, entry := range l.entries {
			// A full idle bucket behaves exactly like a new one, so dropping
			// it never lets an account exceed its rate.
			if now.Sub(entry.lastUsed) >= limiterIdleTTL && entry.limiter.TokensAt(now) >= float64(entry.limiter.Burst()) {
				delete(l.entries, k)
			}
		}
	}
	entry, ok := l.entries[key]
	if !ok {
		entry = &limiterEntry{limiter: rate.NewLimiter(rate.Every(p.every), max(p.burst, 1))}
		l.entries[key] = entry
	}
	entry.lastUsed = now
	return entry.limiter
}

// interval is how long one request of class takes to come back into the
// bucket, the shortest useful wait after a pacing timeout.
func (l *accountLimiters) interval(class requestClass) time.Duration {
	if p, ok := l.paces[class]; ok {
		return p.every
	}
	return time.Second
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
