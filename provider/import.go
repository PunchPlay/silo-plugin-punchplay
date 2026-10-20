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
	maxPageSize = 500
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
	Snapshot  bool    `json:"s"`
	Resource  int     `json:"r,omitempty"`
	After     int64   `json:"a,omitempty"`
	Watermark string  `json:"w,omitempty"`
	Changes   string  `json:"c,omitempty"`
	Watchlist []int64 `json:"l,omitempty"`
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
		raw := strings.TrimSpace(req.GetPageToken())
		decoded, err := decodePageToken(raw)
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
	if token.Snapshot {
		return s.snapshotPage(ctx, acct, kinds, token, pageSize)
	}
	return s.changesPage(ctx, acct, kinds, token, pageSize, first)
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
	if _, fault := s.api.call(ctx, acct, http.MethodGet, "/me/sync/changes?limit=1", nil, nil, &answer); fault != nil {
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
		if _, fault := s.api.call(ctx, acct, http.MethodGet, path, nil, nil, &page); fault != nil {
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

func (s *Server) snapshotPage(ctx context.Context, acct account, kinds kindSet, token pageToken, pageSize int) *pluginv1.WatchSyncListRemoteStateResponse {
	resources := kinds.resources()
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: true}
	if token.Resource >= len(resources) {
		response.NextCursor = cursorPrefix + token.Watermark
		return response
	}
	resource := resources[token.Resource]
	var page snapshotResponse
	path := "/me/sync/snapshot?resource=" + resource + "&limit=" + strconv.Itoa(pageSize) + "&after=" + strconv.FormatInt(token.After, 10)
	if _, fault := s.api.call(ctx, acct, http.MethodGet, path, nil, nil, &page); fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
	}
	mapper := newStateMapper(kinds, token.Watchlist, true)
	for _, raw := range page.Items {
		mapper.add(resource, "", 0, raw)
	}
	response.Items, response.Warnings = mapper.result()

	next := token
	if page.HasMore && page.NextAfter != nil && *page.NextAfter > token.After {
		next.After = *page.NextAfter
	} else {
		next.Resource++
		next.After = 0
	}
	if next.Resource >= len(resources) {
		response.NextCursor = cursorPrefix + token.Watermark
	} else {
		response.NextPageToken = encodePageToken(next)
	}
	return response
}

func (s *Server) changesPage(ctx context.Context, acct account, kinds kindSet, token pageToken, pageSize int, first bool) *pluginv1.WatchSyncListRemoteStateResponse {
	var page changesResponse
	path := "/me/sync/changes?limit=" + strconv.Itoa(pageSize) + "&cursor=" + url.QueryEscape(token.Changes)
	if _, fault := s.api.call(ctx, acct, http.MethodGet, path, nil, nil, &page); fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
	}
	if page.ResetRequired {
		// The cursor expired (after 90 days) or fell behind the journal's
		// retention. Start over with a full snapshot. A traversal keeps one
		// kind on every page, so past the first page Silo retries instead,
		// and the retry's first page starts the snapshot.
		if !first {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault("PunchPlay asked for a full resync partway through; retrying")}
		}
		if strings.TrimSpace(page.NextCursor) == "" {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault("PunchPlay returned no sync cursor")}
		}
		return s.snapshotPage(ctx, acct, kinds, pageToken{Snapshot: true, Watermark: page.NextCursor, Watchlist: token.Watchlist}, pageSize)
	}
	if strings.TrimSpace(page.NextCursor) == "" {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault("PunchPlay returned no sync cursor")}
	}
	mapper := newStateMapper(kinds, token.Watchlist, false)
	for _, change := range page.Changes {
		id, _ := change.ResourceID.Int64()
		if change.Operation == "delete" {
			mapper.add(change.Resource, "delete", id, nil)
			continue
		}
		if change.Resource == resourceList {
			mapper.noteList(change.Data)
			continue
		}
		mapper.add(change.Resource, change.Operation, id, change.Data)
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{}
	response.Items, response.Warnings = mapper.result()
	if page.HasMore {
		next := token
		next.Changes = page.NextCursor
		next.Watchlist = mapper.watchlist
		response.NextPageToken = encodePageToken(next)
	} else {
		response.NextCursor = cursorPrefix + page.NextCursor
	}
	return response
}

// stateMapper turns PunchPlay records into Silo remote states for one page.
type stateMapper struct {
	kinds     kindSet
	watchlist []int64
	complete  bool
	items     []*pluginv1.WatchSyncRemoteState
	// watched merges plays of the same title or episode on one page.
	watched map[string]*pluginv1.WatchSyncRemoteState
	skipped int
}

func newStateMapper(kinds kindSet, watchlist []int64, complete bool) *stateMapper {
	return &stateMapper{kinds: kinds, watchlist: slices.Clone(watchlist), complete: complete, watched: map[string]*pluginv1.WatchSyncRemoteState{}}
}

func (m *stateMapper) result() ([]*pluginv1.WatchSyncRemoteState, []string) {
	var warnings []string
	if m.skipped > 0 {
		warnings = append(warnings, fmt.Sprintf("Skipped %d PunchPlay records without a TMDB or TVDB ID", m.skipped))
	}
	return m.items, warnings
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
		m.skipped++
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
		m.skipped++
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
		m.skipped++
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
		if str(r.Scope) != "season" {
			m.skipped++
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
		m.skipped++
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
