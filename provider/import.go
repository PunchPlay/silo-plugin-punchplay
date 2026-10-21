package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultPageSize = 100
	// PunchPlay's snapshot and changes feeds return at most 500 rows a page.
	// The plugin always reads that many and carries what doesn't fit Silo's
	// smaller page in the page token, so a large library takes a fifth of the
	// requests it would at Silo's page size.
	maxPageSize = 500
	// maxFetchesPerPage bounds the reads one ListRemoteState call makes while
	// filling a page across resources.
	maxFetchesPerPage = 4
	// cursorPrefix versions the cursor Silo stores between syncs.
	cursorPrefix = "v1:"
)

// Resources in the order a first sync reads them.
const (
	resourceHistory     = "history"
	resourcePlayback    = "playback"
	resourceInteraction = "interaction"
	resourceListItem    = "list_item"
	resourceList        = "list"
)

// advertisedKinds are the state families the manifest advertises; an empty
// request means all of them.
var advertisedKinds = []pluginv1.WatchSyncRemoteStateKind{
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED,
}

// kindSet is the state families one traversal returns.
type kindSet map[pluginv1.WatchSyncRemoteStateKind]bool

func requestedKinds(requested []pluginv1.WatchSyncRemoteStateKind) (kindSet, *pluginv1.WatchSyncFault) {
	if len(requested) == 0 {
		requested = advertisedKinds
	}
	kinds := kindSet{}
	for _, kind := range requested {
		if !slices.Contains(advertisedKinds, kind) {
			return nil, invalidRequestFault("PunchPlay does not provide this state family")
		}
		kinds[kind] = true
	}
	return kinds, nil
}

// resources lists the snapshot resources the kinds need, in reading order.
func (k kindSet) resources() []string {
	var out []string
	if k[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED] {
		out = append(out, resourceHistory)
	}
	if k[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS] {
		out = append(out, resourcePlayback)
	}
	if k[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING] ||
		k[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE] ||
		k[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED] {
		out = append(out, resourceInteraction)
	}
	if k[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST] {
		out = append(out, resourceListItem)
	}
	return out
}

// pageToken carries a traversal from one page to the next. Silo never stores
// it, so its shape can change between plugin versions.
type pageToken struct {
	// Snapshot is true for a first sync, which reads full snapshots and ends
	// at Watermark; false for a later sync, which follows the changes feed.
	Snapshot  bool   `json:"s"`
	Resource  int    `json:"r,omitempty"`
	After     int64  `json:"a,omitempty"`
	Watermark string `json:"w,omitempty"`
	Changes   string `json:"c,omitempty"`
	// Done marks the changes feed read to its end; what is left is Carry.
	Done      bool    `json:"d,omitempty"`
	Watchlist []int64 `json:"l,omitempty"`
	// Carry holds rows already read but not yet handed to Silo.
	Carry []carriedRow `json:"k,omitempty"`
	// Skipped counts rows left out so far, by reason, for one warning each
	// at the end of the traversal.
	Skipped map[string]int `json:"x,omitempty"`
}

// carriedRow is one snapshot record or change, as the plugin read it.
type carriedRow struct {
	Resource  string          `json:"r"`
	Operation string          `json:"o,omitempty"`
	ID        int64           `json:"i,omitempty"`
	Data      json.RawMessage `json:"d,omitempty"`
}

func encodePageToken(token pageToken) string {
	encoded, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodePageToken(raw string) (pageToken, error) {
	var token pageToken
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return token, err
	}
	err = json.Unmarshal(decoded, &token)
	return token, err
}

// compactRecord re-encodes a record with only the fields the plugin reads,
// to keep carried rows small. Unreadable data is kept as it is, so the
// mapper still counts it as unreadable.
func compactRecord(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var r record
	if json.Unmarshal(raw, &r) != nil {
		return raw
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return raw
	}
	return encoded
}

type snapshotResponse struct {
	Items     []json.RawMessage `json:"items"`
	HasMore   bool              `json:"hasMore"`
	NextAfter *int64            `json:"nextAfter"`
}

type changesResponse struct {
	Changes []struct {
		Resource   string          `json:"resource"`
		ResourceID json.Number     `json:"resourceId"`
		Operation  string          `json:"operation"`
		ChangedAt  string          `json:"changedAt"`
		Data       json.RawMessage `json:"data"`
	} `json:"changes"`
	HasMore       bool   `json:"hasMore"`
	NextCursor    string `json:"nextCursor"`
	ResetRequired bool   `json:"resetRequired"`
}

// listState returns one page of a traversal. A sync with no cursor reads a
// full snapshot of every resource the kinds need and ends at the change
// journal's high-water mark, so the next sync continues from there.
func (s *Server) listState(ctx context.Context, acct account, req *pluginv1.WatchSyncListRemoteStateRequest) *pluginv1.WatchSyncListRemoteStateResponse {
	kinds, fault := requestedKinds(req.GetStateKinds())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	pageSize = min(pageSize, maxPageSize)

	var token pageToken
	first := strings.TrimSpace(req.GetPageToken()) == ""
	if !first {
		decoded, err := decodePageToken(strings.TrimSpace(req.GetPageToken()))
		if err != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: invalidRequestFault("PunchPlay page token is invalid")}
		}
		token = decoded
	} else {
		start, fault := s.startTraversal(ctx, acct, kinds, req.GetCursor())
		if fault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
		}
		token = start
	}
	return s.fillPage(ctx, acct, kinds, token, pageSize, first)
}

// startTraversal decides between the changes feed and a full snapshot. An
// empty or expired cursor means a snapshot.
func (s *Server) startTraversal(ctx context.Context, acct account, kinds kindSet, cursor string) (pageToken, *pluginv1.WatchSyncFault) {
	token := pageToken{}
	if kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST] {
		lists, fault := s.watchlistIDs(ctx, acct)
		if fault != nil {
			return token, fault
		}
		token.Watchlist = lists
	}
	journal := strings.TrimPrefix(strings.TrimSpace(cursor), cursorPrefix)
	if journal != "" && strings.HasPrefix(cursor, cursorPrefix) {
		token.Changes = journal
		return token, nil
	}
	// No usable cursor: ask the journal for its high-water mark, then read
	// every resource. Changes made during the snapshot are replayed by the
	// next sync, which is harmless because every state is idempotent.
	var answer changesResponse
	if _, fault := s.api.call(ctx, acct, classRead, http.MethodGet, "/me/sync/changes?limit=1", nil, nil, &answer); fault != nil {
		return token, fault
	}
	if strings.TrimSpace(answer.NextCursor) == "" {
		return token, temporaryFault("PunchPlay returned no sync cursor")
	}
	token.Snapshot = true
	token.Watermark = answer.NextCursor
	return token, nil
}

// watchlistIDs finds the user's Watchlist list. There is normally one.
func (s *Server) watchlistIDs(ctx context.Context, acct account) ([]int64, *pluginv1.WatchSyncFault) {
	var ids []int64
	var after int64
	for {
		var page snapshotResponse
		path := "/me/sync/snapshot?resource=" + resourceList + "&limit=" + strconv.Itoa(maxPageSize) + "&after=" + strconv.FormatInt(after, 10)
		if _, fault := s.api.call(ctx, acct, classRead, http.MethodGet, path, nil, nil, &page); fault != nil {
			return nil, fault
		}
		for _, raw := range page.Items {
			var list record
			if json.Unmarshal(raw, &list) == nil && list.IsWatchlist != nil && *list.IsWatchlist && num(list.ID) > 0 {
				ids = append(ids, num(list.ID))
			}
		}
		if !page.HasMore || page.NextAfter == nil || *page.NextAfter <= after {
			return ids, nil
		}
		after = *page.NextAfter
	}
}

// resyncRequired signals that the changes feed wants a full resync.
type resyncRequired struct{ watermark string }

// fillPage hands Silo up to pageSize states, first from rows carried over
// from the last page, then from fresh reads of up to 500 rows. Rows that
// don't fit are carried to the next page.
func (s *Server) fillPage(ctx context.Context, acct account, kinds kindSet, token pageToken, pageSize int, first bool) *pluginv1.WatchSyncListRemoteStateResponse {
	resources := kinds.resources()
	mapper := newStateMapper(kinds, token.Watchlist, token.Snapshot)
	next := token
	pending := token.Carry
	next.Carry = nil
	exhausted := func() bool {
		if next.Snapshot {
			return next.Resource >= len(resources)
		}
		return next.Done
	}

	for fetches := 0; ; {
		for len(pending) > 0 && len(mapper.items) < pageSize {
			mapper.addRow(pending[0])
			pending = pending[1:]
		}
		if len(pending) > 0 || len(mapper.items) >= pageSize || exhausted() || fetches >= maxFetchesPerPage {
			break
		}
		var rows []carriedRow
		var fault *pluginv1.WatchSyncFault
		if next.Snapshot {
			rows, fault = s.readSnapshot(ctx, acct, resources, &next)
		} else {
			var resync *resyncRequired
			rows, resync, fault = s.readChanges(ctx, acct, &next)
			if resync != nil {
				// The cursor expired (after 90 days) or fell behind the
				// journal's retention. A traversal keeps one kind on every
				// page, so only its first page may turn into a snapshot;
				// past that, Silo retries and the retry's first page does.
				if !first || fetches > 0 || len(token.Carry) > 0 {
					return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault("PunchPlay asked for a full resync partway through; retrying")}
				}
				return s.fillPage(ctx, acct, kinds, pageToken{Snapshot: true, Watermark: resync.watermark, Watchlist: token.Watchlist}, pageSize, first)
			}
		}
		if fault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
		}
		fetches++
		pending = rows
	}

	next.Carry = pending
	next.Watchlist = mapper.watchlist
	next.Skipped = mergeCounts(token.Skipped, mapper.skipped)
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: next.Snapshot, Items: mapper.items}
	if len(pending) == 0 && exhausted() {
		if next.Snapshot {
			response.NextCursor = cursorPrefix + next.Watermark
		} else {
			response.NextCursor = cursorPrefix + next.Changes
		}
		response.Warnings = skipWarnings(next.Skipped)
		return response
	}
	response.NextPageToken = encodePageToken(next)
	return response
}

// readSnapshot reads up to 500 rows of the current resource and moves the
// token past them.
func (s *Server) readSnapshot(ctx context.Context, acct account, resources []string, next *pageToken) ([]carriedRow, *pluginv1.WatchSyncFault) {
	resource := resources[next.Resource]
	var page snapshotResponse
	path := "/me/sync/snapshot?resource=" + resource + "&limit=" + strconv.Itoa(maxPageSize) + "&after=" + strconv.FormatInt(next.After, 10)
	if _, fault := s.api.call(ctx, acct, classRead, http.MethodGet, path, nil, nil, &page); fault != nil {
		return nil, fault
	}
	rows := make([]carriedRow, 0, len(page.Items))
	for _, raw := range page.Items {
		rows = append(rows, carriedRow{Resource: resource, Data: compactRecord(raw)})
	}
	if page.HasMore && page.NextAfter != nil && *page.NextAfter > next.After {
		next.After = *page.NextAfter
	} else {
		next.Resource++
		next.After = 0
	}
	return rows, nil
}

// readChanges reads up to 500 changes and moves the token past them.
func (s *Server) readChanges(ctx context.Context, acct account, next *pageToken) ([]carriedRow, *resyncRequired, *pluginv1.WatchSyncFault) {
	var page changesResponse
	path := "/me/sync/changes?limit=" + strconv.Itoa(maxPageSize) + "&cursor=" + url.QueryEscape(next.Changes)
	if _, fault := s.api.call(ctx, acct, classRead, http.MethodGet, path, nil, nil, &page); fault != nil {
		return nil, nil, fault
	}
	if strings.TrimSpace(page.NextCursor) == "" {
		return nil, nil, temporaryFault("PunchPlay returned no sync cursor")
	}
	if page.ResetRequired {
		return nil, &resyncRequired{watermark: page.NextCursor}, nil
	}
	rows := make([]carriedRow, 0, len(page.Changes))
	for _, change := range page.Changes {
		id, _ := change.ResourceID.Int64()
		row := carriedRow{Resource: change.Resource, Operation: change.Operation, ID: id}
		if change.Operation != "delete" {
			row.Data = compactRecord(change.Data)
		}
		rows = append(rows, row)
	}
	next.Changes = page.NextCursor
	next.Done = !page.HasMore
	return rows, nil, nil
}

// Reasons a row is left out of the import, each reported once per traversal.
const (
	skipUnreadable        = "records the plugin couldn't read"
	skipWatchNoID         = "watches without a TMDB or TVDB ID"
	skipProgressNoID      = "resume points without a TMDB or TVDB ID"
	skipInteractionNoID   = "ratings, favourites or dropped shows without a TMDB or TVDB ID"
	skipSeasonRating      = "season ratings, which Silo doesn't store"
	skipWatchlistItemNoID = "Watchlist titles without a TMDB or TVDB ID"
)

func mergeCounts(into, add map[string]int) map[string]int {
	if len(add) == 0 {
		return into
	}
	out := make(map[string]int, len(into)+len(add))
	for reason, n := range into {
		out[reason] = n
	}
	for reason, n := range add {
		out[reason] += n
	}
	return out
}

// skipWarnings turns the traversal's counts into one warning per reason.
func skipWarnings(counts map[string]int) []string {
	reasons := make([]string, 0, len(counts))
	for reason, n := range counts {
		if n > 0 {
			reasons = append(reasons, reason)
		}
	}
	slices.Sort(reasons)
	warnings := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		warnings = append(warnings, fmt.Sprintf("Skipped %d PunchPlay %s", counts[reason], reason))
	}
	return warnings
}

// stateMapper turns PunchPlay records into Silo remote states for one page.
type stateMapper struct {
	kinds     kindSet
	watchlist []int64
	complete  bool
	items     []*pluginv1.WatchSyncRemoteState
	// watched merges plays of the same title or episode on one page.
	watched map[string]*pluginv1.WatchSyncRemoteState
	skipped map[string]int
}

func newStateMapper(kinds kindSet, watchlist []int64, complete bool) *stateMapper {
	return &stateMapper{
		kinds:     kinds,
		watchlist: slices.Clone(watchlist),
		complete:  complete,
		watched:   map[string]*pluginv1.WatchSyncRemoteState{},
		skipped:   map[string]int{},
	}
}

func (m *stateMapper) skip(reason string) { m.skipped[reason]++ }

// addRow adds one carried row: a Watchlist appearing in the changes feed is
// noted, anything else is mapped.
func (m *stateMapper) addRow(row carriedRow) {
	if row.Resource == resourceList && row.Operation != "delete" {
		m.noteList(row.Data)
		return
	}
	m.add(row.Resource, row.Operation, row.ID, row.Data)
}

// noteList learns of a Watchlist created after the traversal started.
func (m *stateMapper) noteList(raw json.RawMessage) {
	var list record
	if json.Unmarshal(raw, &list) == nil && list.IsWatchlist != nil && *list.IsWatchlist {
		if id := num(list.ID); id > 0 && !slices.Contains(m.watchlist, id) {
			m.watchlist = append(m.watchlist, id)
		}
	}
}

func (m *stateMapper) add(resource, operation string, resourceID int64, raw json.RawMessage) {
	if operation == "delete" {
		m.addDeletion(resource, resourceID)
		return
	}
	var r record
	if json.Unmarshal(raw, &r) != nil {
		m.skip(skipUnreadable)
		return
	}
	switch resource {
	case resourceHistory:
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED] {
			m.addWatched(r)
		}
	case resourcePlayback:
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS] {
			m.addProgress(r)
		}
	case resourceInteraction:
		m.addInteraction(r)
	case resourceListItem:
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST] {
			m.addListItem(r)
		}
	}
}

func (m *stateMapper) addWatched(r record) {
	media := watchMedia(r)
	watchedAt := parseTime(str(r.WatchedAt))
	if media == nil {
		m.skip(skipWatchNoID)
		return
	}
	key := mediaKey(media)
	if existing := m.watched[key]; existing != nil {
		// Silo needs "watched, last on": keep the latest play.
		if watchedAt != nil && (existing.Watched.LastWatchedAt == nil || watchedAt.AsTime().After(existing.Watched.LastWatchedAt.AsTime())) {
			existing.Watched.LastWatchedAt = watchedAt
			existing.ProviderItemKey = "history:" + strconv.FormatInt(num(r.ID), 10)
		}
		return
	}
	state := &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: "history:" + strconv.FormatInt(num(r.ID), 10),
		Media:           media,
		Watched:         &pluginv1.WatchSyncRemoteWatchedState{PlayCount: 1, LastWatchedAt: watchedAt},
	}
	m.watched[key] = state
	m.items = append(m.items, state)
}

func (m *stateMapper) addProgress(r record) {
	if r.ProgressPercent == nil {
		return
	}
	percent := *r.ProgressPercent
	// Completed plays belong in history, and a resume point at 0 is none.
	if percent <= 0 || percent >= 100 || math.IsNaN(percent) {
		return
	}
	media := watchMedia(r)
	if media == nil {
		m.skip(skipProgressNoID)
		return
	}
	m.items = append(m.items, &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: "playback:" + strconv.FormatInt(num(r.ID), 10),
		Media:           media,
		Progress:        &pluginv1.WatchSyncRemoteProgressState{ProgressPercent: percent, PausedAt: parseTime(str(r.UpdatedAt))},
	})
}

// addInteraction reads a rating, favourite and dropped status from one
// interaction row. In a snapshot only set states are listed, since an absent
// item already means unset; an incremental change also reports clearing.
func (m *stateMapper) addInteraction(r record) {
	media := interactionMedia(r)
	if media == nil {
		switch {
		case str(r.Scope) != "season":
			m.skip(skipInteractionNoID)
		case rating(r.Rating) > 0 && m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING]:
			m.skip(skipSeasonRating)
		}
		return
	}
	updatedAt := parseTime(str(r.UpdatedAt))
	state := &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: "interaction:" + strconv.FormatInt(num(r.ID), 10),
		Media:           media,
	}
	if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING] {
		if rating := rating(r.Rating); rating > 0 {
			state.Rating = &pluginv1.WatchSyncRemoteRatingState{Rating: rating, RatedAt: updatedAt}
		} else if !m.complete {
			state.Rating = &pluginv1.WatchSyncRemoteRatingState{Removed: true}
		}
	}
	if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE] {
		if r.IsFavourite != nil && *r.IsFavourite {
			state.Favorite = &pluginv1.WatchSyncRemoteListState{ListedAt: updatedAt}
		} else if !m.complete {
			state.Favorite = &pluginv1.WatchSyncRemoteListState{Removed: true}
		}
	}
	if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED] &&
		media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES {
		if str(r.ShowStatus) == "DROPPED" {
			state.Dropped = &pluginv1.WatchSyncRemoteListState{ListedAt: updatedAt}
		} else if !m.complete {
			state.Dropped = &pluginv1.WatchSyncRemoteListState{Removed: true}
		}
	}
	if state.Rating != nil || state.Favorite != nil || state.Dropped != nil {
		m.items = append(m.items, state)
	}
}

func (m *stateMapper) addListItem(r record) {
	if !slices.Contains(m.watchlist, num(r.ListID)) {
		return
	}
	media := interactionMedia(record{Kind: r.Kind, MediaSource: r.MediaSource, SourceID: r.SourceID, TmdbID: r.TmdbID, Title: r.Title})
	if media == nil {
		m.skip(skipWatchlistItemNoID)
		return
	}
	m.items = append(m.items, &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: "list_item:" + strconv.FormatInt(num(r.ID), 10),
		Media:           media,
		Watchlist:       &pluginv1.WatchSyncRemoteListState{ListedAt: parseTime(str(r.AddedAt))},
	})
}

// addDeletion reports a removed favourite, rating, dropped status or
// Watchlist entry by the key it was listed under. Deleted plays are not
// reported: Silo imports watches, not their removal.
func (m *stateMapper) addDeletion(resource string, id int64) {
	if id <= 0 {
		return
	}
	key := resource + ":" + strconv.FormatInt(id, 10)
	switch resource {
	case resourceInteraction:
		state := &pluginv1.WatchSyncRemoteState{ProviderItemKey: key}
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING] {
			state.Rating = &pluginv1.WatchSyncRemoteRatingState{Removed: true}
		}
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE] {
			state.Favorite = &pluginv1.WatchSyncRemoteListState{Removed: true}
		}
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED] {
			state.Dropped = &pluginv1.WatchSyncRemoteListState{Removed: true}
		}
		if state.Rating != nil || state.Favorite != nil || state.Dropped != nil {
			m.items = append(m.items, state)
		}
	case resourceListItem:
		if m.kinds[pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST] {
			m.items = append(m.items, &pluginv1.WatchSyncRemoteState{
				ProviderItemKey: key,
				Watchlist:       &pluginv1.WatchSyncRemoteListState{Removed: true},
			})
		}
	}
}

// rating rounds PunchPlay's rating half up into Silo's whole 1 to 10.
func rating(value *float64) int32 {
	if value == nil || math.IsNaN(*value) || *value <= 0 {
		return 0
	}
	return int32(min(max(math.Floor(*value+0.5), 1), 10))
}

func parseTime(value string) *timestamppb.Timestamp {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	// PunchPlay marks "date unknown" plays with 1900 and 1970 dates; they
	// carry no real time.
	if err != nil || parsed.Year() <= 1900 || (parsed.Year() == 1970 && parsed.YearDay() == 1) {
		return nil
	}
	return timestamppb.New(parsed)
}

// mediaKey identifies a title or episode for merging plays on one page.
func mediaKey(media *pluginv1.WatchSyncMedia) string {
	external := media.GetExternalIds()
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
		external = media.GetSeriesExternalIds()
	}
	keys := make([]string, 0, len(external))
	for namespace, value := range external {
		keys = append(keys, namespace+"="+value)
	}
	slices.Sort(keys)
	return fmt.Sprintf("%d|%s|%d|%d", media.GetMediaType(), strings.Join(keys, ","), media.GetSeasonNumber(), media.GetEpisodeNumber())
}
