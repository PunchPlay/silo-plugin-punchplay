package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeAPI is a scripted PunchPlay Platform API. Each route answers with the
// handler registered for "METHOD /path" (the path after /api/platform/v1,
// without the query); every request is recorded.
type fakeAPI struct {
	t        *testing.T
	mu       sync.Mutex
	routes   map[string]http.HandlerFunc
	requests []recordedRequest
	server   *httptest.Server
}

type recordedRequest struct {
	method  string
	path    string
	query   string
	header  http.Header
	body    map[string]any
	rawBody string
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	api := &fakeAPI{t: t, routes: map[string]http.HandlerFunc{}}
	api.server = httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(api.server.Close)
	return api
}

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	path := strings.TrimPrefix(r.URL.Path, platformPrefix)
	recorded := recordedRequest{method: r.Method, path: path, query: r.URL.RawQuery, header: r.Header.Clone(), rawBody: string(raw)}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &recorded.body)
	}
	f.mu.Lock()
	f.requests = append(f.requests, recorded)
	handler := f.routes[r.Method+" "+path]
	f.mu.Unlock()
	if handler == nil {
		f.t.Errorf("unexpected request %s %s", r.Method, path)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	r.Body = io.NopCloser(strings.NewReader(string(raw)))
	handler(w, r)
}

func (f *fakeAPI) handle(route string, handler http.HandlerFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.routes[route] = handler
}

func (f *fakeAPI) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

func jsonReply(status int, body any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, status, body)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func apiErrorReply(status int, code string) http.HandlerFunc {
	return jsonReply(status, map[string]any{"error": code, "message": "fake " + code, "request_id": "req_test"})
}

var fixedNow = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

// testPaces lets tests run at full speed; pacing has its own tests.
var testPaces = map[requestClass]pace{
	classRead:     {every: time.Microsecond, burst: 1000},
	classWrite:    {every: time.Microsecond, burst: 1000},
	classBulk:     {every: time.Microsecond, burst: 1000},
	classPlayback: {every: time.Microsecond, burst: 1000},
}

func newTestServer(api *fakeAPI) *Server {
	server := NewServer(api.server.Client(), "test")
	server.now = func() time.Time { return fixedNow }
	server.api.limits = newAccountLimiters(testPaces)
	server.api.sleep = func(context.Context, time.Duration) error { return nil }
	server.signIn = settings{baseURL: api.server.URL, clientID: "ppc_test"}
	return server
}

// testContext is an authenticated context pointing at the fake API.
func testContext(api *fakeAPI) *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId: capabilityID,
		Credentials: &pluginv1.WatchSyncCredentials{
			AccessToken:  "access-token",
			RefreshToken: strings.Repeat("b", 64),
			SecretAttributes: map[string]string{
				attributeClientID: "ppc_test",
				attributeBaseURL:  api.server.URL,
			},
		},
	}
}

func movie(tmdb string) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
		Title:       "The Matrix",
		Year:        1999,
		ExternalIds: map[string]string{"tmdb": tmdb, "imdb": "tt0133093"},
	}
}

func series(tmdb string) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES,
		Title:       "Severance",
		Year:        2022,
		ExternalIds: map[string]string{"tmdb": tmdb},
	}
}

func episode(seriesIDs map[string]string, season, number int32) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
		Title:             "Good News About Hell",
		SeriesTitle:       "Severance",
		SeriesYear:        2022,
		SeriesExternalIds: seriesIDs,
		SeasonNumber:      season,
		EpisodeNumber:     number,
	}
}

func event(id string, operation pluginv1.WatchSyncOperation, media *pluginv1.WatchSyncMedia) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:    id,
		Operation:  operation,
		Media:      media,
		OccurredAt: timestamppb.New(fixedNow.Add(-time.Minute)),
	}
}

func statuses(response *pluginv1.WatchSyncApplyEventsResponse) map[string]pluginv1.WatchSyncApplyStatus {
	out := map[string]pluginv1.WatchSyncApplyStatus{}
	for _, result := range response.GetResults() {
		out[result.GetEventId()] = result.GetStatus()
	}
	return out
}

// bulkEcho answers a bulk write with one result per item, taking each status
// from statusFor.
func bulkEcho(statusFor func(index int, item map[string]any) string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Items []map[string]any `json:"items"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		results := make([]map[string]any, 0, len(body.Items))
		for index, item := range body.Items {
			results = append(results, map[string]any{"index": index, "client_item_id": item["client_item_id"], "status": statusFor(index, item)})
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": results})
	}
}
