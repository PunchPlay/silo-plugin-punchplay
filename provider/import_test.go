package provider

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	kindWatched   = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED
	kindProgress  = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS
	kindFavorite  = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE
	kindWatchlist = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST
	kindRating    = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING
	kindDropped   = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED
)

func list(t *testing.T, server *Server, api *fakeAPI, cursor, token string, kinds ...pluginv1.WatchSyncRemoteStateKind) *pluginv1.WatchSyncListRemoteStateResponse {
	t.Helper()
	response, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: testContext(api), Cursor: cursor, PageToken: token, PageSize: 100, StateKinds: kinds,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

// snapshotRoute answers the snapshot endpoint from pages keyed by resource
// and "after".
func snapshotRoute(t *testing.T, pages map[string]map[string]map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		resource, after := query.Get("resource"), query.Get("after")
		page, ok := pages[resource][after]
		if !ok {
			t.Errorf("unexpected snapshot %s after %s", resource, after)
			page = map[string]any{"items": []any{}, "hasMore": false, "nextAfter": nil}
		}
		writeJSON(w, http.StatusOK, page)
	}
}

func TestFirstSyncReadsASnapshotAndEndsAtTheHighWaterMark(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("GET /me/sync/changes", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "" {
			t.Errorf("a first sync must not send a cursor")
		}
		writeJSON(w, http.StatusOK, map[string]any{"changes": []any{}, "hasMore": false, "nextCursor": "hw-1", "resetRequired": true})
	})
	api.handle("GET /me/sync/snapshot", snapshotRoute(t, map[string]map[string]map[string]any{
		"history": {
			"0": {"hasMore": true, "nextAfter": 20, "items": []any{
				map[string]any{"id": 10, "type": "movie", "mediaSource": "tmdb", "sourceId": 603, "tmdbId": 603, "title": "The Matrix", "year": 1999, "watchedAt": "2026-01-01T20:00:00.000Z"},
				map[string]any{"id": 11, "type": "movie", "mediaSource": "tmdb", "sourceId": 603, "tmdbId": 603, "title": "The Matrix", "watchedAt": "2026-03-01T20:00:00.000Z"},
				map[string]any{"id": 12, "type": "episode", "mediaSource": "tvdb", "sourceId": 9001, "showSourceId": 81797, "showTmdbId": 37854, "title": "One Piece", "season": 2, "episode": 1, "episodeTitle": "Arrival", "watchedAt": "1970-01-01T00:00:00.000Z"},
				map[string]any{"id": 13, "type": "movie", "mediaSource": "tmdb"},
			}},
			"20": {"hasMore": false, "nextAfter": nil, "items": []any{
				map[string]any{"id": 21, "type": "movie", "mediaSource": "tmdb", "tmdbId": 604, "watchedAt": "2026-04-01T20:00:00Z"},
			}},
		},
	}))

	first := list(t, server, api, "", "", kindWatched)
	if first.GetFault() != nil || !first.GetCompleteSnapshot() || first.GetNextCursor() != "" || first.GetNextPageToken() == "" {
		t.Fatalf("first page = %v", first)
	}
	items := first.GetItems()
	if len(items) != 2 {
		t.Fatalf("items = %v, want The Matrix merged into one and One Piece", items)
	}
	matrix := items[0]
	if matrix.GetMedia().GetExternalIds()["tmdb"] != "603" || matrix.GetWatched().GetPlayCount() != 1 ||
		matrix.GetWatched().GetLastWatchedAt().AsTime().Month() != 3 || matrix.GetProviderItemKey() != "history:11" {
		t.Fatalf("matrix = %v", matrix)
	}
	onePiece := items[1].GetMedia()
	if onePiece.GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE ||
		onePiece.GetSeriesExternalIds()["tvdb"] != "81797" || onePiece.GetSeriesExternalIds()["tmdb"] != "37854" ||
		onePiece.GetSeasonNumber() != 2 || onePiece.GetEpisodeNumber() != 1 || onePiece.GetSeriesTitle() != "One Piece" {
		t.Fatalf("one piece = %v", onePiece)
	}
	// A "date unknown" play carries no time.
	if items[1].GetWatched().GetLastWatchedAt() != nil {
		t.Fatalf("placeholder date leaked: %v", items[1].GetWatched().GetLastWatchedAt())
	}
	if len(first.GetWarnings()) != 1 {
		t.Fatalf("warnings = %v, want the identity-less row counted", first.GetWarnings())
	}

	second := list(t, server, api, "", first.GetNextPageToken(), kindWatched)
	if second.GetNextCursor() != "v1:hw-1" || second.GetNextPageToken() != "" || !second.GetCompleteSnapshot() || len(second.GetItems()) != 1 {
		t.Fatalf("second page = %v", second)
	}
}

func TestSnapshotListsOnlySetInteractionsAndTheWatchlist(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("GET /me/sync/changes", jsonReply(http.StatusOK, map[string]any{"changes": []any{}, "nextCursor": "hw", "resetRequired": true}))
	api.handle("GET /me/sync/snapshot", snapshotRoute(t, map[string]map[string]map[string]any{
		"list": {"0": {"hasMore": false, "items": []any{
			map[string]any{"id": 5, "isWatchlist": false},
			map[string]any{"id": 7, "isWatchlist": true},
		}}},
		"interaction": {"0": {"hasMore": false, "items": []any{
			map[string]any{"id": 1, "kind": "movie", "scope": "title", "mediaSource": "tmdb", "sourceId": 603, "tmdbId": 603, "rating": 7.5, "isFavourite": true, "updatedAt": "2026-05-01T00:00:00Z"},
			map[string]any{"id": 2, "kind": "show", "scope": "title", "mediaSource": "tmdb", "sourceId": 1399, "tmdbId": 1399, "showStatus": "DROPPED"},
			map[string]any{"id": 3, "kind": "show", "scope": "season", "mediaSource": "tmdb", "sourceId": 1399, "tmdbId": 1399, "rating": 9},
			map[string]any{"id": 4, "kind": "show", "scope": "episode", "mediaSource": "tmdb", "sourceId": 1399, "tmdbId": 1399, "season": 1, "episode": 9, "rating": 10},
			map[string]any{"id": 6, "kind": "movie", "scope": "title", "mediaSource": "tmdb", "tmdbId": 604},
		}}},
		"list_item": {"0": {"hasMore": false, "items": []any{
			map[string]any{"id": 40, "listId": 7, "kind": "show", "mediaSource": "tmdb", "sourceId": 95396, "tmdbId": 95396, "title": "Severance", "addedAt": "2026-02-01T00:00:00Z"},
			map[string]any{"id": 41, "listId": 5, "kind": "movie", "mediaSource": "tmdb", "tmdbId": 11},
		}}},
	}))

	var all []*pluginv1.WatchSyncRemoteState
	token := ""
	for page := 0; page < 5; page++ {
		response := list(t, server, api, "", token, kindRating, kindFavorite, kindDropped, kindWatchlist)
		if response.GetFault() != nil {
			t.Fatalf("fault = %v", response.GetFault())
		}
		all = append(all, response.GetItems()...)
		if response.GetNextPageToken() == "" {
			if response.GetNextCursor() != "v1:hw" {
				t.Fatalf("cursor = %q", response.GetNextCursor())
			}
			break
		}
		token = response.GetNextPageToken()
	}
	byKey := map[string]*pluginv1.WatchSyncRemoteState{}
	for _, item := range all {
		byKey[item.GetProviderItemKey()] = item
	}
	if len(byKey) != 4 {
		t.Fatalf("items = %v, want the movie, the dropped show, the episode rating and the Watchlist entry", byKey)
	}
	if m := byKey["interaction:1"]; m.GetRating().GetRating() != 8 || m.GetFavorite() == nil || m.GetDropped() != nil {
		t.Fatalf("movie = %v (7.5 rounds half up to 8)", m)
	}
	if s := byKey["interaction:2"]; s.GetDropped() == nil || s.GetRating() != nil ||
		s.GetMedia().GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES {
		t.Fatalf("show = %v", s)
	}
	if e := byKey["interaction:4"]; e.GetRating().GetRating() != 10 || e.GetMedia().GetEpisodeNumber() != 9 {
		t.Fatalf("episode = %v", e)
	}
	if w := byKey["list_item:40"]; w.GetWatchlist() == nil || w.GetMedia().GetTitle() != "Severance" {
		t.Fatalf("watchlist = %v", w)
	}
}

func TestLaterSyncsFollowTheChangesFeed(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("GET /me/sync/snapshot", snapshotRoute(t, map[string]map[string]map[string]any{
		"list": {"0": {"hasMore": false, "items": []any{map[string]any{"id": 7, "isWatchlist": true}}}},
	}))
	api.handle("GET /me/sync/changes", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("cursor") {
		case "c-1":
			writeJSON(w, http.StatusOK, map[string]any{"hasMore": true, "nextCursor": "c-2", "resetRequired": false, "changes": []any{
				map[string]any{"resource": "interaction", "resourceId": 1, "operation": "update", "data": map[string]any{
					"id": 1, "kind": "movie", "scope": "title", "mediaSource": "tmdb", "tmdbId": 603, "rating": nil, "isFavourite": false,
				}},
				map[string]any{"resource": "interaction", "resourceId": 2, "operation": "delete", "data": nil},
			}})
		case "c-2":
			writeJSON(w, http.StatusOK, map[string]any{"hasMore": false, "nextCursor": "c-3", "resetRequired": false, "changes": []any{
				map[string]any{"resource": "list_item", "resourceId": 40, "operation": "delete", "data": nil},
				map[string]any{"resource": "list_item", "resourceId": 41, "operation": "insert", "data": map[string]any{
					"id": 41, "listId": 7, "kind": "movie", "mediaSource": "tmdb", "tmdbId": 11,
				}},
				map[string]any{"resource": "history", "resourceId": 9, "operation": "delete", "data": nil},
			}})
		default:
			t.Errorf("cursor = %q", r.URL.Query().Get("cursor"))
		}
	})

	first := list(t, server, api, "v1:c-1", "", kindRating, kindFavorite, kindWatchlist)
	if first.GetCompleteSnapshot() || first.GetNextPageToken() == "" || first.GetNextCursor() != "" {
		t.Fatalf("first = %v", first)
	}
	if len(first.GetItems()) != 2 {
		t.Fatalf("items = %v", first.GetItems())
	}
	cleared := first.GetItems()[0]
	if !cleared.GetRating().GetRemoved() || !cleared.GetFavorite().GetRemoved() || cleared.GetMedia() == nil {
		t.Fatalf("cleared = %v, want rating and favourite tombstones", cleared)
	}
	deleted := first.GetItems()[1]
	if deleted.GetProviderItemKey() != "interaction:2" || deleted.GetMedia() != nil || !deleted.GetRating().GetRemoved() {
		t.Fatalf("deleted = %v", deleted)
	}

	second := list(t, server, api, "v1:c-1", first.GetNextPageToken(), kindRating, kindFavorite, kindWatchlist)
	if second.GetNextCursor() != "v1:c-3" || len(second.GetItems()) != 2 {
		t.Fatalf("second = %v", second)
	}
	if !second.GetItems()[0].GetWatchlist().GetRemoved() || second.GetItems()[1].GetWatchlist().GetRemoved() {
		t.Fatalf("watchlist changes = %v", second.GetItems())
	}
	// The cursor travels escaped, never re-encoded by hand.
	for _, request := range api.recorded() {
		if request.path == "/me/sync/changes" {
			if _, err := url.ParseQuery(request.query); err != nil {
				t.Fatalf("query %q: %v", request.query, err)
			}
		}
	}
}

func TestAnExpiredCursorStartsASnapshotOnlyOnTheFirstPage(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("GET /me/sync/changes", jsonReply(http.StatusOK, map[string]any{"changes": []any{}, "nextCursor": "hw-9", "resetRequired": true}))
	api.handle("GET /me/sync/snapshot", snapshotRoute(t, map[string]map[string]map[string]any{
		"playback": {"0": {"hasMore": false, "items": []any{
			map[string]any{"id": 3, "type": "movie", "mediaSource": "tmdb", "tmdbId": 603, "progressPercent": 42.5, "updatedAt": "2026-10-19T10:00:00Z"},
			map[string]any{"id": 4, "type": "movie", "mediaSource": "tmdb", "tmdbId": 604, "progressPercent": 100},
		}}},
	}))
	first := list(t, server, api, "v1:old", "", kindProgress)
	if !first.GetCompleteSnapshot() || first.GetNextCursor() != "v1:hw-9" || len(first.GetItems()) != 1 ||
		first.GetItems()[0].GetProgress().GetProgressPercent() != 42.5 {
		t.Fatalf("first = %v", first)
	}

	midway := list(t, server, api, "v1:old", encodePageToken(pageToken{Changes: "c-5"}), kindProgress)
	if midway.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("midway = %v, want a retry rather than a switch of traversal kind", midway)
	}
}

func TestUnknownStateFamiliesAreRefused(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	response := list(t, server, api, "", "", pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_UNSPECIFIED)
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("fault = %v", response.GetFault())
	}
}
