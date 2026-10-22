package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	// PunchPlay's bulk endpoints take at most 100 items, Silo's batch size.
	maxBulkItems = 100
	// maxItemKey is PunchPlay's limit on client_item_id and event_id.
	maxItemKey = 200
	// staleLiveEvent is how old a start or pause can be and still be worth
	// showing as live. Older ones arrive after a retry and describe playback
	// that has moved on; stops are always sent, because they log the watch.
	staleLiveEvent = 2 * time.Hour
)

// endpoint is where an operation's events are written. Consecutive events
// for the same bulk endpoint go out in one request.
type endpoint int

const (
	endpointNone endpoint = iota
	endpointHistory
	endpointInteractions
	endpointWatchlist
	endpointUnwatch
	endpointPlayback
)

func endpointFor(operation pluginv1.WatchSyncOperation) endpoint {
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		return endpointHistory
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED:
		return endpointUnwatch
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED:
		return endpointInteractions
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST:
		return endpointWatchlist
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		return endpointPlayback
	}
	return endpointNone
}

// applyEvents applies events in order. A run of consecutive events bound for
// the same bulk endpoint is one request, so a watchlist add followed by a
// removal of the same title still lands as a removal.
func (s *Server) applyEvents(ctx context.Context, acct account, events []*pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyEventsResponse {
	results := map[string]*pluginv1.WatchSyncApplyResult{}
	var order []string
	var run []*pluginv1.WatchSyncEvent
	runEndpoint := endpointNone

	flush := func() *pluginv1.WatchSyncFault {
		if len(run) == 0 {
			return nil
		}
		fault := s.applyRun(ctx, acct, runEndpoint, run, results)
		run = nil
		return fault
	}

	for _, event := range events {
		id := strings.TrimSpace(event.GetEventId())
		if id == "" {
			continue
		}
		if _, seen := results[id]; seen {
			// A repeated event ID is the same desired state, applied once.
			continue
		}
		order = append(order, id)
		results[id] = nil
		target := endpointFor(event.GetOperation())
		switch {
		case event.GetMedia() == nil:
			results[id] = rejected(id, invalidRequestFault("Watch event media is required"))
			continue
		case target == endpointNone:
			results[id] = rejected(id, invalidRequestFault("PunchPlay does not support this watch operation"))
			continue
		}
		if target != runEndpoint || len(run) == maxBulkItems || target == endpointUnwatch || target == endpointPlayback {
			if fault := flush(); fault != nil {
				return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}
			}
			runEndpoint = target
		}
		run = append(run, event)
	}
	if fault := flush(); fault != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}
	}

	response := &pluginv1.WatchSyncApplyEventsResponse{Results: make([]*pluginv1.WatchSyncApplyResult, 0, len(order))}
	for _, id := range order {
		result := results[id]
		if result == nil {
			result = retry(id, temporaryFault("PunchPlay did not report a result for this event"))
		}
		response.Results = append(response.Results, result)
	}
	return response
}

// applyRun writes one run and records a result per event. It returns a fault
// only when it concerns the whole connection.
func (s *Server) applyRun(ctx context.Context, acct account, target endpoint, run []*pluginv1.WatchSyncEvent, results map[string]*pluginv1.WatchSyncApplyResult) *pluginv1.WatchSyncFault {
	switch target {
	case endpointUnwatch:
		event := run[0]
		result, fault := s.unwatch(ctx, acct, event)
		if fault != nil {
			return fault
		}
		results[event.GetEventId()] = result
		return nil
	case endpointPlayback:
		event := run[0]
		result, fault := s.scrobble(ctx, acct, event)
		if fault != nil {
			return fault
		}
		results[event.GetEventId()] = result
		return nil
	}

	var path, operation string
	items := make([]map[string]any, 0, len(run))
	sent := make([]*pluginv1.WatchSyncEvent, 0, len(run))
	for _, event := range run {
		var item map[string]any
		var fault *pluginv1.WatchSyncFault
		switch target {
		case endpointHistory:
			path, operation = "/sync/history", "history"
			item, fault = historyItem(event)
		case endpointInteractions:
			path, operation = "/sync/ratings", "ratings"
			item, fault = interactionItem(event)
		case endpointWatchlist:
			path, operation = "/sync/watchlist", "watchlist"
			item, fault = watchlistItem(event)
		}
		if fault != nil {
			results[event.GetEventId()] = rejected(event.GetEventId(), fault)
			continue
		}
		items = append(items, item)
		sent = append(sent, event)
	}
	if len(items) == 0 {
		return nil
	}

	bulk, fault := s.postBulk(ctx, acct, path, operation, items)
	if fault != nil {
		if connectionWide(fault) {
			return fault
		}
		for _, event := range sent {
			results[event.GetEventId()] = resultFor(event.GetEventId(), fault)
		}
		return nil
	}
	if bulk.Cancelled {
		for _, event := range sent {
			results[event.GetEventId()] = retry(event.GetEventId(), temporaryFault("PunchPlay cancelled the write; it will be retried"))
		}
		return nil
	}
	for _, item := range bulk.Results {
		if item.Index < 0 || item.Index >= len(sent) {
			continue
		}
		id := sent[item.Index].GetEventId()
		results[id] = bulkResult(id, item.Status)
	}
	return nil
}

type bulkResponse struct {
	Cancelled bool `json:"cancelled"`
	Results   []struct {
		Index  int    `json:"index"`
		Status string `json:"status"`
	} `json:"results"`
}

func bulkResult(eventID, status string) *pluginv1.WatchSyncApplyResult {
	switch status {
	case "inserted", "updated", "removed":
		return applied(eventID)
	case "skipped":
		return noChange(eventID)
	case "deferred":
		// PunchPlay kept the item and retries the match itself. The user
		// sees it in PunchPlay's Settings until it resolves.
		return applied(eventID)
	case "invalid":
		return rejected(eventID, invalidRequestFault("PunchPlay rejected this item"))
	}
	return retry(eventID, temporaryFault("PunchPlay returned an unknown result for this item"))
}

// postBulk sends one bulk write. The Idempotency-Key is a hash of the body,
// so an identical retry replays PunchPlay's stored answer. If PunchPlay
// cannot say whether an earlier attempt finished, the write is sent again
// under a new key: every bulk write is safe to repeat, because history is
// keyed per play and the rest set a state.
func (s *Server) postBulk(ctx context.Context, acct account, path, operation string, items []map[string]any) (bulkResponse, *pluginv1.WatchSyncFault) {
	payload := map[string]any{"items": items}
	key := idempotencyKey(operation, payload)
	for attempt := 0; ; attempt++ {
		var out bulkResponse
		resp, fault := s.api.request(ctx, acct, classBulk, http.MethodPost, path, map[string]string{"Idempotency-Key": key}, payload, &out)
		switch {
		case fault != nil:
			return out, fault
		case resp.ok():
			return out, nil
		case resp.status == http.StatusConflict && resp.code == "idempotency_indeterminate" && attempt == 0:
			key = idempotencyKey(operation+":"+strconv.FormatInt(s.now().UnixNano(), 10), payload)
			continue
		case resp.status == http.StatusConflict:
			// Another attempt with this key is still running.
			return out, temporaryFault("PunchPlay is still applying an earlier attempt (" + describe(resp) + ")")
		default:
			return out, faultFor(resp)
		}
	}
}

func idempotencyKey(operation string, payload any) string {
	encoded, _ := json.Marshal(payload)
	sum := sha256.Sum256(append([]byte("silo:"+operation+":"), encoded...))
	return "silo-" + hex.EncodeToString(sum[:])
}

// itemKey fits a Silo identifier into PunchPlay's 200-character keys.
func itemKey(prefix, value string) string {
	key := prefix + value
	if len(key) <= maxItemKey {
		return key
	}
	sum := sha256.Sum256([]byte(value))
	return prefix + "sha256:" + hex.EncodeToString(sum[:])
}

// playKey identifies one play. Silo's watch-history ID survives a reconnect,
// so re-exported history does not log the same play twice; the event ID is
// the fallback.
func playKey(event *pluginv1.WatchSyncEvent) string {
	if history := strings.TrimSpace(event.GetWatchHistoryId()); history != "" {
		return itemKey("silo-play:", history)
	}
	return itemKey("silo-event:", event.GetEventId())
}

// identified adds the title's IDs, title and year to item. Every item needs
// at least one ID; a non-TMDB ID also needs a client_item_id, which callers
// always set.
func identified(item map[string]any, media *pluginv1.WatchSyncMedia) *pluginv1.WatchSyncFault {
	title := titleIDs(media)
	if title.empty() {
		return invalidRequestFault("PunchPlay needs a TMDB, IMDb or TVDB ID for this title")
	}
	for key, value := range title.fields() {
		item[key] = value
	}
	if name := titleName(media); name != "" {
		item["title"] = name
	}
	if year := titleYear(media); year > 0 {
		item["year"] = year
	}
	return nil
}

func historyItem(event *pluginv1.WatchSyncEvent) (map[string]any, *pluginv1.WatchSyncFault) {
	media := event.GetMedia()
	watchedAt := event.GetOccurredAt()
	if watchedAt == nil || !watchedAt.IsValid() || watchedAt.AsTime().IsZero() {
		return nil, invalidRequestFault("A watch needs the time it happened")
	}
	item := map[string]any{
		"client_item_id": playKey(event),
		"watched_at":     watchedAt.AsTime().UTC().Format(time.RFC3339),
	}
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		item["kind"] = "movie"
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		if media.GetSeasonNumber() < 0 || media.GetEpisodeNumber() <= 0 {
			return nil, invalidRequestFault("PunchPlay needs the season and episode number of a watched episode")
		}
		item["kind"] = "episode"
		item["season"] = media.GetSeasonNumber()
		item["episode"] = media.GetEpisodeNumber()
	default:
		return nil, invalidRequestFault("PunchPlay logs watches of movies and episodes only")
	}
	if fault := identified(item, media); fault != nil {
		return nil, fault
	}
	return item, nil
}

// interactionItem builds a /sync/ratings item: a rating, favourite or
// dropped status for a movie, series or episode.
func interactionItem(event *pluginv1.WatchSyncEvent) (map[string]any, *pluginv1.WatchSyncFault) {
	media := event.GetMedia()
	item := map[string]any{
		"client_item_id": itemKey("silo-event:", event.GetEventId()),
		"kind":           titleKind(media),
	}
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		item["scope"] = "title"
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES:
		item["scope"] = "title"
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		if media.GetSeasonNumber() < 0 || media.GetEpisodeNumber() <= 0 {
			return nil, invalidRequestFault("PunchPlay needs the season and episode number of an episode")
		}
		item["scope"] = "episode"
		item["season"] = media.GetSeasonNumber()
		item["episode"] = media.GetEpisodeNumber()
	default:
		return nil, invalidRequestFault("Unknown media type")
	}

	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING:
		rating := event.GetRating()
		if rating < 1 || rating > 10 {
			return nil, invalidRequestFault("A rating must be from 1 to 10")
		}
		item["rating"] = rating
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		item["rating"] = nil
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE:
		item["is_favourite"] = true
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE:
		item["is_favourite"] = false
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED:
		if media.GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES {
			return nil, invalidRequestFault("Only a series can be dropped")
		}
		if event.GetOperation() == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED {
			item["show_status"] = "DROPPED"
		} else {
			// Clearing the status lets PunchPlay work it out from the
			// watched episodes again.
			item["show_status"] = nil
		}
	}
	if fault := identified(item, media); fault != nil {
		return nil, fault
	}
	return item, nil
}

func watchlistItem(event *pluginv1.WatchSyncEvent) (map[string]any, *pluginv1.WatchSyncFault) {
	media := event.GetMedia()
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES:
	default:
		return nil, invalidRequestFault("PunchPlay's Watchlist holds movies and series only")
	}
	item := map[string]any{
		"client_item_id": itemKey("silo-event:", event.GetEventId()),
		"kind":           titleKind(media),
		"remove":         event.GetOperation() == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST,
	}
	if fault := identified(item, media); fault != nil {
		return nil, fault
	}
	return item, nil
}

type removedResponse struct {
	Removed *int `json:"removed"`
}

// unwatch moves every play of a movie or episode to PunchPlay's trash, where
// the user can restore it. PunchPlay addresses titles by TMDB ID here.
func (s *Server) unwatch(ctx context.Context, acct account, event *pluginv1.WatchSyncEvent) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	id := event.GetEventId()
	media := event.GetMedia()
	tmdb := titleIDs(media).tmdb
	if tmdb == 0 {
		return rejected(id, invalidRequestFault("PunchPlay can only unwatch a title with a TMDB ID")), nil
	}
	var path string
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		path = "/title/movie/" + strconv.Itoa(tmdb) + "/history"
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		if media.GetSeasonNumber() < 0 || media.GetEpisodeNumber() <= 0 {
			return rejected(id, invalidRequestFault("PunchPlay needs the season and episode number to unwatch an episode")), nil
		}
		path = "/title/show/" + strconv.Itoa(tmdb) + "/season/" + strconv.Itoa(int(media.GetSeasonNumber())) +
			"/watch?episode=" + url.QueryEscape(strconv.Itoa(int(media.GetEpisodeNumber())))
	default:
		return rejected(id, invalidRequestFault("PunchPlay unwatches movies and episodes only")), nil
	}
	var out removedResponse
	_, fault := s.api.call(ctx, acct, classWrite, http.MethodDelete, path, nil, nil, &out)
	if fault != nil {
		if connectionWide(fault) {
			return nil, fault
		}
		return resultFor(id, fault), nil
	}
	if out.Removed != nil && *out.Removed == 0 {
		return noChange(id), nil
	}
	return applied(id), nil
}

type playbackResponse struct {
	Deduped bool   `json:"deduped"`
	Ignored string `json:"ignored"`
}

// scrobble forwards live playback. PunchPlay keeps a receipt per event_id,
// so a replayed stop does not log a second watch.
func (s *Server) scrobble(ctx context.Context, acct account, event *pluginv1.WatchSyncEvent) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	id := event.GetEventId()
	media := event.GetMedia()
	var action string
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START:
		action = "start"
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		action = "pause"
	default:
		action = "stop"
	}
	occurredAt := s.now()
	if at := event.GetOccurredAt(); at != nil && at.IsValid() && !at.AsTime().IsZero() {
		occurredAt = at.AsTime()
	}
	if action != "stop" && s.now().Sub(occurredAt) > staleLiveEvent {
		return noChange(id), nil
	}
	// Silo gives a resume the same event ID as the session's first start, so
	// PunchPlay would drop it as a replay and leave the session paused. The
	// time Silo recorded the start tells them apart and stays fixed on retry.
	receiptID := id
	if action == "start" {
		receiptID = id + "@" + strconv.FormatInt(occurredAt.UnixMilli(), 10)
	}

	payload := map[string]any{
		"event_id":            itemKey("silo:", receiptID),
		"playback_session_id": playbackSession(event),
		"event_created_at":    occurredAt.UnixMilli(),
		"position_seconds":    max(event.GetPositionSeconds(), 0),
		"device_id":           "silo",
	}
	if duration := event.GetDurationSeconds(); duration > 0 {
		payload["duration_seconds"] = duration
	}
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		payload["media_type"] = "movie"
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		if media.GetSeasonNumber() < 0 || media.GetEpisodeNumber() <= 0 {
			return rejected(id, invalidRequestFault("PunchPlay needs the season and episode number of a playing episode")), nil
		}
		payload["media_type"] = "episode"
		payload["season"] = media.GetSeasonNumber()
		payload["episode"] = media.GetEpisodeNumber()
		if absolute := media.GetAbsoluteEpisodeNumber(); absolute > 0 {
			payload["absolute_episode"] = absolute
		}
		if title := strings.TrimSpace(media.GetTitle()); title != "" {
			payload["episode_title"] = title
		}
	default:
		return rejected(id, invalidRequestFault("PunchPlay scrobbles movies and episodes only")), nil
	}
	if action == "stop" {
		// Silo decides completion; the percentage alone never does.
		payload["watched"] = event.GetCompleted()
	}
	if fault := identified(payload, media); fault != nil {
		return rejected(id, fault), nil
	}

	var out playbackResponse
	resp, fault := s.api.request(ctx, acct, classPlayback, http.MethodPost, "/playback/"+action, nil, payload, &out)
	switch {
	case fault != nil:
		return resultFor(id, fault), nil
	case resp.ok() && (out.Deduped || out.Ignored != ""):
		return noChange(id), nil
	case resp.ok():
		return applied(id), nil
	case resp.status == http.StatusConflict:
		// The session already moved on, for example a pause with no
		// playback in progress.
		return noChange(id), nil
	case resp.code == "media_not_found":
		return rejected(id, invalidRequestFault("PunchPlay could not find this title ("+describe(resp)+")")), nil
	}
	fault = faultFor(resp)
	if connectionWide(fault) {
		return nil, fault
	}
	return resultFor(id, fault), nil
}

// playbackSession ties start, pause and stop of one playback together.
func playbackSession(event *pluginv1.WatchSyncEvent) string {
	if session := strings.TrimSpace(event.GetPlaybackSessionId()); session != "" {
		return itemKey("silo:", session)
	}
	if item := strings.TrimSpace(event.GetMedia().GetMediaItemId()); item != "" {
		return itemKey("silo-item:", item)
	}
	return itemKey("silo-event:", event.GetEventId())
}
