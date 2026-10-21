package provider

import (
	"context"
	"net/http"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// rateLimitedThen answers the first `limited` requests with a 429 carrying
// retryAfter, then hands over to ok.
func rateLimitedThen(limited int, retryAfter string, ok http.HandlerFunc) (http.HandlerFunc, *int) {
	calls := 0
	return func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls <= limited {
			w.Header().Set("Retry-After", retryAfter)
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate_limited"})
			return
		}
		ok(w, r)
	}, &calls
}

func recordSleeps(server *Server) *[]time.Duration {
	var slept []time.Duration
	server.api.sleep = func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}
	return &slept
}

func TestAShortRateLimitIsWaitedOutInPlace(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	slept := recordSleeps(server)
	handler, calls := rateLimitedThen(1, "3", jsonReply(http.StatusOK, map[string]any{"changes": []any{}, "hasMore": false, "nextCursor": "c-2"}))
	api.handle("GET /me/sync/changes", handler)

	page := list(t, server, api, "v1:c-1", "", kindRating)
	if page.GetFault() != nil || page.GetNextCursor() != "v1:c-2" {
		t.Fatalf("page = %v; a short limit must not fail the traversal", page)
	}
	if *calls != 2 || len(*slept) != 1 || (*slept)[0] != 3*time.Second {
		t.Fatalf("calls = %d, slept = %v", *calls, *slept)
	}
}

func TestALongRateLimitBecomesAFault(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	slept := recordSleeps(server)
	handler, _ := rateLimitedThen(1, "300", jsonReply(http.StatusOK, map[string]any{}))
	api.handle("GET /me/sync/changes", handler)

	page := list(t, server, api, "v1:c-1", "", kindRating)
	if page.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED ||
		page.GetFault().GetRetryAfter().AsDuration() != 300*time.Second || len(*slept) != 0 {
		t.Fatalf("fault = %v, slept = %v", page.GetFault(), *slept)
	}
}

func TestRetriesStopAfterTwo(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	slept := recordSleeps(server)
	handler, calls := rateLimitedThen(10, "1", jsonReply(http.StatusOK, map[string]any{}))
	api.handle("GET /me/sync/changes", handler)

	page := list(t, server, api, "v1:c-1", "", kindRating)
	if page.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || *calls != 3 || len(*slept) != 2 {
		t.Fatalf("fault = %v, calls = %d, slept = %v", page.GetFault(), *calls, *slept)
	}
}

func TestAWaitPastTheDeadlineIsNotTaken(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	slept := recordSleeps(server)
	handler, _ := rateLimitedThen(1, "30", jsonReply(http.StatusOK, map[string]any{}))
	api.handle("GET /me/sync/changes", handler)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	page, _ := server.ListRemoteState(ctx, &pluginv1.WatchSyncListRemoteStateRequest{
		Context: testContext(api), Cursor: "v1:c-1", PageSize: 100, StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindRating},
	})
	if page.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || len(*slept) != 0 {
		t.Fatalf("fault = %v, slept = %v", page.GetFault(), *slept)
	}
}

func TestARetriedWriteKeepsItsIdempotencyKey(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	recordSleeps(server)
	handler, _ := rateLimitedThen(1, "2", bulkEcho(func(int, map[string]any) string { return "inserted" }))
	api.handle("POST /sync/history", handler)

	response := apply(t, server, api, event("e1", opWatched, movie("603")))
	requests := api.recorded()
	if statuses(response)["e1"] != statusApplied || len(requests) != 2 ||
		requests[0].header.Get("Idempotency-Key") != requests[1].header.Get("Idempotency-Key") ||
		requests[0].rawBody != requests[1].rawBody {
		t.Fatalf("statuses = %v, requests = %d", statuses(response), len(requests))
	}
}

func TestPacingIsPerAccount(t *testing.T) {
	limits := newAccountLimiters(map[requestClass]pace{classRead: {every: 40 * time.Millisecond, burst: 1}})
	ctx := context.Background()

	start := time.Now()
	for range 3 {
		if err := limits.Wait(ctx, "token-a", classRead); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 70*time.Millisecond {
		t.Fatalf("three paced reads took %v, want at least two intervals", elapsed)
	}

	// Another account, and requests that aren't paced, don't wait.
	start = time.Now()
	_ = limits.Wait(ctx, "token-b", classRead)
	_ = limits.Wait(ctx, "token-a", classAuth)
	_ = limits.Wait(ctx, "", classRead)
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("unpaced requests waited %v", elapsed)
	}
}

func TestAPaceThatCannotFinishBeforeTheDeadlineFailsAtOnce(t *testing.T) {
	limits := newAccountLimiters(map[requestClass]pace{classBulk: {every: time.Hour, burst: 1}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := limits.Wait(ctx, "token", classBulk); err != nil {
		t.Fatalf("first request must go at once: %v", err)
	}
	start := time.Now()
	if err := limits.Wait(ctx, "token", classBulk); err == nil || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("err = %v after %v, want an immediate error", err, time.Since(start))
	}
}

// PunchPlay's limits, per account: reads and single writes 120 a minute,
// each bulk endpoint 30 a minute, playback 120 every five minutes.
func TestPacesStayUnderPunchPlaysLimits(t *testing.T) {
	limits := map[requestClass]struct {
		max    int
		window time.Duration
	}{
		classRead:     {120, time.Minute},
		classWrite:    {120, time.Minute},
		classBulk:     {30, time.Minute},
		classPlayback: {120, 5 * time.Minute},
	}
	for class, limit := range limits {
		p, ok := defaultPaces[class]
		if !ok {
			t.Fatalf("class %d has no pace", class)
		}
		// The most a bucket allows in one window: a full burst, then one per
		// interval.
		most := p.burst + int(limit.window/p.every)
		if most >= limit.max {
			t.Fatalf("class %d allows %d per %v, PunchPlay allows %d", class, most, limit.window, limit.max)
		}
	}
}
