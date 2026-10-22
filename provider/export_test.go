package provider

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	opWatched    = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED
	opUnwatched  = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED
	opRate       = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING
	opUnrate     = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
	opFavourite  = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE
	opDrop       = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED
	opUndrop     = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED
	opWatchlist  = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST
	opUnwatchlst = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST
	opStart      = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START
	opPause      = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE
	opStop       = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP

	statusApplied  = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	statusNoChange = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	statusRetry    = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	statusRejected = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
)

func apply(t *testing.T, server *Server, api *fakeAPI, events ...*pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyEventsResponse {
	t.Helper()
	response, err := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{Context: testContext(api), Events: events})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestWatchesAreKeyedPerPlaySoReplaysAndReconnectsDoNotDuplicate(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/history", bulkEcho(func(index int, _ map[string]any) string {
		return []string{"inserted", "skipped", "deferred", "invalid"}[index]
	}))
	first := event("e1", opWatched, movie("603"))
	first.WatchHistoryId = "play-77"
	ep := event("e2", opWatched, episode(map[string]string{"tmdb": "95396"}, 1, 2))
	anime := event("e3", opWatched, episode(map[string]string{"tvdb": "81797"}, 0, 5))
	broken := event("e4", opWatched, movie("604"))
	response := apply(t, server, api, first, ep, anime, broken)

	got := statuses(response)
	// deferred: PunchPlay keeps and retries the match itself.
	if got["e1"] != statusApplied || got["e2"] != statusNoChange || got["e3"] != statusApplied || got["e4"] != statusRejected {
		t.Fatalf("statuses = %v", got)
	}
	requests := api.recorded()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want one bulk write", len(requests))
	}
	items := requests[0].body["items"].([]any)
	movieItem := items[0].(map[string]any)
	if movieItem["client_item_id"] != "silo-play:play-77" || movieItem["kind"] != "movie" || movieItem["tmdb_id"] != float64(603) ||
		movieItem["imdb_id"] != "tt0133093" || movieItem["watched_at"] != fixedNow.Add(-time.Minute).Format(time.RFC3339) {
		t.Fatalf("movie item = %v", movieItem)
	}
	episodeItem := items[1].(map[string]any)
	if episodeItem["client_item_id"] != "silo-event:e2" || episodeItem["kind"] != "episode" || episodeItem["tmdb_id"] != float64(95396) ||
		episodeItem["season"] != float64(1) || episodeItem["episode"] != float64(2) || episodeItem["title"] != "Severance" {
		t.Fatalf("episode item = %v", episodeItem)
	}
	// Season 0 is Specials, not "no season".
	animeItem := items[2].(map[string]any)
	if animeItem["tvdb_id"] != float64(81797) || animeItem["season"] != float64(0) {
		t.Fatalf("anime item = %v", animeItem)
	}
	key := requests[0].header.Get("Idempotency-Key")
	if !strings.HasPrefix(key, "silo-") || len(key) < 8 || len(key) > 200 {
		t.Fatalf("Idempotency-Key = %q", key)
	}

	// The same batch again carries the same key, so PunchPlay replays it.
	apply(t, server, api, first, ep, anime, broken)
	if again := api.recorded()[1].header.Get("Idempotency-Key"); again != key {
		t.Fatalf("retry key = %q, want %q", again, key)
	}
}

func TestEventsKeepTheirOrderAcrossEndpoints(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/watchlist", bulkEcho(func(int, map[string]any) string { return "updated" }))
	api.handle("POST /sync/history", bulkEcho(func(int, map[string]any) string { return "inserted" }))
	response := apply(t, server, api,
		event("add", opWatchlist, movie("603")),
		event("watch", opWatched, movie("603")),
		event("remove", opUnwatchlst, movie("603")),
	)
	var paths []string
	for _, request := range api.recorded() {
		paths = append(paths, request.path)
	}
	if strings.Join(paths, ",") != "/sync/watchlist,/sync/history,/sync/watchlist" {
		t.Fatalf("paths = %v, want the watchlist removal written last", paths)
	}
	last := api.recorded()[2].body["items"].([]any)[0].(map[string]any)
	if last["remove"] != true || last["kind"] != "movie" {
		t.Fatalf("removal = %v", last)
	}
	if got := statuses(response); got["add"] != statusApplied || got["remove"] != statusApplied {
		t.Fatalf("statuses = %v", got)
	}
}

func TestInteractionsShareOneWrite(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/ratings", bulkEcho(func(int, map[string]any) string { return "updated" }))
	rate := event("rate", opRate, movie("603"))
	rate.Rating = 8
	rateEpisode := event("rate-ep", opRate, episode(map[string]string{"tmdb": "95396"}, 1, 2))
	rateEpisode.Rating = 10
	apply(t, server, api,
		rate,
		event("unrate", opUnrate, series("95396")),
		event("fav", opFavourite, series("95396")),
		event("drop", opDrop, series("95396")),
		event("undrop", opUndrop, series("1399")),
		rateEpisode,
	)
	requests := api.recorded()
	if len(requests) != 1 {
		t.Fatalf("requests = %d", len(requests))
	}
	items := requests[0].body["items"].([]any)
	check := func(index int, want map[string]any) {
		t.Helper()
		item := items[index].(map[string]any)
		for key, value := range want {
			if item[key] != value {
				t.Fatalf("item %d %s = %v, want %v (item %v)", index, key, item[key], value, item)
			}
		}
	}
	check(0, map[string]any{"kind": "movie", "scope": "title", "rating": float64(8)})
	check(1, map[string]any{"kind": "show", "scope": "title", "rating": nil})
	check(2, map[string]any{"kind": "show", "is_favourite": true})
	check(3, map[string]any{"show_status": "DROPPED"})
	check(4, map[string]any{"show_status": nil, "tmdb_id": float64(1399)})
	check(5, map[string]any{"kind": "show", "scope": "episode", "season": float64(1), "episode": float64(2), "rating": float64(10)})
	if _, present := items[1].(map[string]any)["rating"]; !present {
		t.Fatal("a rating removal must send rating: null")
	}
}

func TestEventsWithoutUsableIDsAreRejectedWithoutARequest(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	nameless := event("e1", opWatched, &pluginv1.WatchSyncMedia{
		MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, Title: "Home video",
		ExternalIds: map[string]string{"imdb": "not-an-id", "tmdb": "-4"},
	})
	got := statuses(apply(t, server, api, nameless, &pluginv1.WatchSyncEvent{EventId: "e2", Operation: opWatched}))
	if got["e1"] != statusRejected || got["e2"] != statusRejected || len(api.recorded()) != 0 {
		t.Fatalf("statuses = %v after %d requests", got, len(api.recorded()))
	}
}

func TestConnectionFaultsFailTheWholeBatch(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/history", apiErrorReply(http.StatusUnauthorized, "invalid_token"))
	response := apply(t, server, api, event("e1", opWatched, movie("603")))
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || len(response.GetResults()) != 0 {
		t.Fatalf("response = %v", response)
	}
}

func TestRateLimitsDeferTheConnection(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/history", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "42")
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate_limited"})
	})
	response := apply(t, server, api, event("e1", opWatched, movie("603")))
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED ||
		response.GetFault().GetRetryAfter().AsDuration() != 42*time.Second {
		t.Fatalf("fault = %v", response.GetFault())
	}
}

func TestServerErrorsRetryTheEvents(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/history", apiErrorReply(http.StatusBadGateway, "bad_gateway"))
	response := apply(t, server, api, event("e1", opWatched, movie("603")))
	if response.GetFault() != nil || statuses(response)["e1"] != statusRetry {
		t.Fatalf("response = %v", response)
	}
}

func TestAnUnfinishedEarlierAttemptIsResentUnderANewKey(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	calls := 0
	api.handle("POST /sync/history", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			apiErrorReply(http.StatusConflict, "idempotency_indeterminate")(w, r)
			return
		}
		bulkEcho(func(int, map[string]any) string { return "skipped" })(w, r)
	})
	response := apply(t, server, api, event("e1", opWatched, movie("603")))
	requests := api.recorded()
	if statuses(response)["e1"] != statusNoChange || len(requests) != 2 ||
		requests[0].header.Get("Idempotency-Key") == requests[1].header.Get("Idempotency-Key") {
		t.Fatalf("statuses = %v, requests = %d", statuses(response), len(requests))
	}
}

func TestUnwatchMovesPlaysToTrash(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("DELETE /title/movie/603/history", jsonReply(http.StatusOK, map[string]any{"ok": true, "removed": 2}))
	api.handle("DELETE /title/show/95396/season/1/watch", jsonReply(http.StatusOK, map[string]any{"ok": true, "removed": 0}))
	response := apply(t, server, api,
		event("movie", opUnwatched, movie("603")),
		event("episode", opUnwatched, episode(map[string]string{"tmdb": "95396"}, 1, 3)),
		event("tvdb-only", opUnwatched, episode(map[string]string{"tvdb": "81797"}, 1, 3)),
	)
	got := statuses(response)
	if got["movie"] != statusApplied || got["episode"] != statusNoChange || got["tvdb-only"] != statusRejected {
		t.Fatalf("statuses = %v", got)
	}
	if query := api.recorded()[1].query; query != "episode=3" {
		t.Fatalf("episode query = %q", query)
	}
}

func TestScrobbleStopUsesSilosCompletion(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /playback/stop", jsonReply(http.StatusOK, map[string]any{"ok": true}))
	stop := event("stop-1", opStop, episode(map[string]string{"tmdb": "95396", "tvdb": "371980"}, 1, 2))
	stop.PlaybackSessionId = "session-9"
	stop.PositionSeconds = 2400
	stop.DurationSeconds = 3000
	stop.CompletionPercent = 80
	stop.Completed = true
	got := statuses(apply(t, server, api, stop))
	if got["stop-1"] != statusApplied {
		t.Fatalf("statuses = %v", got)
	}
	body := api.recorded()[0].body
	want := map[string]any{
		"event_id": "silo:stop-1", "playback_session_id": "silo:session-9", "media_type": "episode",
		"season": float64(1), "episode": float64(2), "tmdb_id": float64(95396), "tvdb_id": float64(371980),
		"position_seconds": float64(2400), "duration_seconds": float64(3000), "watched": true,
		"event_created_at": float64(fixedNow.Add(-time.Minute).UnixMilli()), "episode_title": "Good News About Hell",
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("%s = %v, want %v (body %v)", key, body[key], value, body)
		}
	}
}

func TestScrobbleResumeIsNotAReplayOfTheFirstStart(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /playback/start", jsonReply(http.StatusOK, map[string]any{"ok": true}))
	// Silo reuses the start's event ID when playback resumes.
	first := event("scrobble:START:session-9", opStart, movie("603"))
	first.OccurredAt = timestamppb.New(fixedNow.Add(-5 * time.Minute))
	resume := event("scrobble:START:session-9", opStart, movie("603"))
	apply(t, server, api, first)
	apply(t, server, api, resume)
	apply(t, server, api, resume)
	ids := []any{}
	for _, request := range api.recorded() {
		ids = append(ids, request.body["event_id"])
	}
	if len(ids) != 3 || ids[0] == ids[1] || ids[1] != ids[2] {
		t.Fatalf("event IDs = %v, want the resume to differ from the first start and a retry to repeat it", ids)
	}
}

func TestScrobbleReplaysAndStaleEventsChangeNothing(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /playback/stop", jsonReply(http.StatusOK, map[string]any{"ok": true, "deduped": true}))
	api.handle("POST /playback/pause", apiErrorReply(http.StatusConflict, "playback_state_conflict"))
	stale := event("old-start", opStart, movie("603"))
	stale.OccurredAt = timestamppb.New(fixedNow.Add(-3 * time.Hour))
	got := statuses(apply(t, server, api,
		event("replayed-stop", opStop, movie("603")),
		event("orphan-pause", opPause, movie("603")),
		stale,
	))
	if got["replayed-stop"] != statusNoChange || got["orphan-pause"] != statusNoChange || got["old-start"] != statusNoChange {
		t.Fatalf("statuses = %v", got)
	}
	for _, request := range api.recorded() {
		if request.path == "/playback/start" {
			t.Fatal("a three-hour-old start must not be sent as live")
		}
	}
}

func TestDuplicateEventIDsAreAppliedOnce(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /sync/history", bulkEcho(func(int, map[string]any) string { return "inserted" }))
	response := apply(t, server, api, event("same", opWatched, movie("603")), event("same", opWatched, movie("603")))
	if len(response.GetResults()) != 1 || len(api.recorded()[0].body["items"].([]any)) != 1 {
		t.Fatalf("results = %v", response.GetResults())
	}
}

func TestLongSiloIDsFitPunchPlaysKeys(t *testing.T) {
	long := strings.Repeat("x", 300)
	if key := itemKey("silo-event:", long); len(key) > maxItemKey || !strings.HasPrefix(key, "silo-event:sha256:") {
		t.Fatalf("key = %q", key)
	}
	// A retry must produce the same key, or PunchPlay would log the event twice.
	first, again := itemKey("silo-event:", long), itemKey("silo-event:", long)
	if first != again {
		t.Fatal("keys must be stable")
	}
}
