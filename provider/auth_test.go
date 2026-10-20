package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

var testDeviceCode = strings.Repeat("a", 64)

func startDeviceFlow(t *testing.T, api *fakeAPI, server *Server) *pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse {
	t.Helper()
	api.handle("POST /auth/device/code", jsonReply(http.StatusOK, map[string]any{
		"user_code":                 "ABCD-1234",
		"device_code":               testDeviceCode,
		"verification_uri":          "https://punchplay.tv/link",
		"verification_uri_complete": "https://punchplay.tv/link?code=ABCD-1234&type=platform",
		"expires_in":                600,
	}))
	start, err := server.DeviceAuthorization().Start(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{
		CapabilityId:   capabilityID,
		ProviderConfig: providerConfig(api),
	})
	if err != nil || start.GetFault() != nil {
		t.Fatalf("start = %v, %v", start.GetFault(), err)
	}
	return start
}

func TestStartAsksForEveryScopeWithThePublicClient(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	start := startDeviceFlow(t, api, server)

	if start.GetUserCode() != "ABCD-1234" || start.GetVerificationUrl() != "https://punchplay.tv/link" ||
		!strings.Contains(start.GetVerificationUrlComplete(), "ABCD-1234") {
		t.Fatalf("start = %v", start)
	}
	// PunchPlay sends no interval, so the RFC 8628 default applies.
	if start.GetPollingInterval().AsDuration() != 5*time.Second {
		t.Fatalf("interval = %v", start.GetPollingInterval().AsDuration())
	}
	if !start.GetExpiresAt().AsTime().Equal(fixedNow.Add(10 * time.Minute)) {
		t.Fatalf("expires = %v", start.GetExpiresAt().AsTime())
	}
	body := api.recorded()[0].body
	if body["client_id"] != "ppc_test" || body["scope"] != strings.Join(requestedScopes, " ") {
		t.Fatalf("device code request = %v", body)
	}
	if _, hasSecret := body["client_secret"]; hasSecret {
		t.Fatal("a public client must not send a secret")
	}
	var state signInState
	if err := json.Unmarshal(start.GetProviderState(), &state); err != nil ||
		state.DeviceCode != testDeviceCode || state.ClientID != "ppc_test" || state.BaseURL != api.server.URL {
		t.Fatalf("state = %+v, %v", state, err)
	}
}

func TestStartUsesTheBuiltInClientByDefault(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /auth/device/code", apiErrorReply(http.StatusUnauthorized, "invalid_client"))
	start, _ := server.DeviceAuthorization().Start(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{
		CapabilityId: capabilityID,
		ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{
			configBaseURL: api.server.URL,
		}},
	})
	if api.recorded()[0].body["client_id"] != defaultClientID {
		t.Fatalf("client_id = %v, want the built-in one", api.recorded()[0].body["client_id"])
	}
	if start.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED {
		t.Fatalf("fault = %v, want permission denied for an unknown client", start.GetFault())
	}
}

func TestStartRefusesAPlainHTTPServer(t *testing.T) {
	server := NewServer(nil, "test")
	start, _ := server.DeviceAuthorization().Start(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{
		CapabilityId:   capabilityID,
		ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configBaseURL: "http://punchplay.example"}},
	})
	if start.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED {
		t.Fatalf("fault = %v", start.GetFault())
	}
}

func TestPollMapsEveryAnswer(t *testing.T) {
	cases := []struct {
		name   string
		reply  http.HandlerFunc
		status pluginv1.WatchSyncDeviceAuthorizationStatus
	}{
		{"pending", apiErrorReply(http.StatusBadRequest, "authorization_pending"), pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING},
		{"expired", apiErrorReply(http.StatusBadRequest, "expired"), pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{"another client", apiErrorReply(http.StatusUnauthorized, "invalid_client"), pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeAPI(t)
			server := newTestServer(api)
			start := startDeviceFlow(t, api, server)
			api.handle("POST /auth/device/token", tc.reply)
			poll, _ := server.DeviceAuthorization().Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
				CapabilityId: capabilityID, ProviderConfig: providerConfig(api), ProviderState: start.GetProviderState(),
			})
			if poll.GetStatus() != tc.status {
				t.Fatalf("status = %v, want %v (fault %v)", poll.GetStatus(), tc.status, poll.GetFault())
			}
		})
	}
}

func TestPollSlowsDownWhenRateLimited(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	start := startDeviceFlow(t, api, server)
	api.handle("POST /auth/device/token", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate_limited"})
	})
	poll, _ := server.DeviceAuthorization().Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
		CapabilityId: capabilityID, ProviderConfig: providerConfig(api), ProviderState: start.GetProviderState(),
	})
	if poll.GetStatus() != pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING ||
		poll.GetPollingInterval().AsDuration() != 30*time.Second || poll.ProviderState == nil {
		t.Fatalf("poll = %v", poll)
	}
}

func TestPollReturnsCredentialsThatRecordTheIssuer(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	start := startDeviceFlow(t, api, server)
	api.handle("POST /auth/device/token", jsonReply(http.StatusOK, map[string]any{
		"access_token": "new-access", "refresh_token": "new-refresh", "token_type": "bearer",
		"expires_in": 3600, "scope": "profile:read history:write",
	}))
	poll, _ := server.DeviceAuthorization().Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
		CapabilityId: capabilityID, ProviderConfig: providerConfig(api), ProviderState: start.GetProviderState(),
	})
	credentials := poll.GetCredentials()
	if poll.GetStatus() != pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_AUTHORIZED ||
		credentials.GetAccessToken() != "new-access" || credentials.GetRefreshToken() != "new-refresh" ||
		!credentials.GetExpiresAt().AsTime().Equal(fixedNow.Add(time.Hour)) ||
		credentials.GetSecretAttributes()[attributeClientID] != "ppc_test" ||
		credentials.GetSecretAttributes()[attributeBaseURL] != api.server.URL {
		t.Fatalf("poll = %v", poll)
	}
	body := api.recorded()[1].body
	if body["device_code"] != testDeviceCode || body["device_name"] != "Silo" || body["client_id"] != "ppc_test" {
		t.Fatalf("token request = %v", body)
	}
}

func TestPollAfterExpiryDoesNotCallPunchPlay(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	start := startDeviceFlow(t, api, server)
	server.now = func() time.Time { return fixedNow.Add(11 * time.Minute) }
	poll, _ := server.DeviceAuthorization().Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
		CapabilityId: capabilityID, ProviderConfig: providerConfig(api), ProviderState: start.GetProviderState(),
	})
	if poll.GetStatus() != pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED || len(api.recorded()) != 1 {
		t.Fatalf("poll = %v after %d requests", poll, len(api.recorded()))
	}
}

func TestRefreshRotatesTokensWithTheIssuingClient(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /auth/refresh", jsonReply(http.StatusOK, map[string]any{
		"access_token": "rotated-access", "refresh_token": "rotated-refresh", "token_type": "bearer", "expires_in": 3600,
	}))
	ctx := testContext(api)
	// An admin pointing new connections elsewhere must not move this one.
	ctx.ProviderConfig = &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: "ppc_other"}}
	ctx.Credentials.AccessToken = ""
	refreshed, _ := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: ctx})
	if refreshed.GetFault() != nil || refreshed.GetCredentials().GetRefreshToken() != "rotated-refresh" {
		t.Fatalf("refresh = %v", refreshed)
	}
	body := api.recorded()[0].body
	if body["client_id"] != "ppc_test" || body["refresh_token"] != strings.Repeat("b", 64) {
		t.Fatalf("refresh request = %v", body)
	}
}

func TestRefreshReportsARevokedConnection(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("POST /auth/refresh", apiErrorReply(http.StatusUnauthorized, "invalid_grant"))
	refreshed, _ := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: testContext(api)})
	if refreshed.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || refreshed.GetCredentials() != nil {
		t.Fatalf("refresh = %v", refreshed)
	}
	if !strings.Contains(refreshed.GetFault().GetSafeMessage(), "req_test") {
		t.Fatalf("message = %q, want the request ID", refreshed.GetFault().GetSafeMessage())
	}
}

func TestGetAccountUsesThePublicProfile(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	api.handle("GET /me", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer access-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id": "user_123", "username": "scott", "name": "Private Name",
			"profile": map[string]any{"displayName": "Scott P", "avatarUrl": "/api/avatars/user_123"},
		})
	})
	account, _ := server.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: testContext(api)})
	got := account.GetAccount()
	if got.GetExternalSubject() != "user_123" || got.GetUsername() != "scott" || got.GetDisplayName() != "Scott P" ||
		got.GetProfileUrl() != api.server.URL+"/u/scott" || got.GetAvatarUrl() != api.server.URL+"/api/avatars/user_123" {
		t.Fatalf("account = %v", got)
	}
}

func TestMissingAccessTokenAsksToReconnect(t *testing.T) {
	api := newFakeAPI(t)
	server := newTestServer(api)
	ctx := testContext(api)
	ctx.Credentials.AccessToken = ""
	account, _ := server.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: ctx})
	if account.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || len(api.recorded()) != 0 {
		t.Fatalf("fault = %v", account.GetFault())
	}
}
