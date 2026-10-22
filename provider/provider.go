// Package provider implements Silo's watch_sync_provider.v1 contract for
// PunchPlay. It holds no state of its own: Silo keeps the credentials, the
// cursors and the retry queue, and every RPC is translated into calls to the
// PunchPlay Platform API.
package provider

import (
	"context"
	"net/http"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	capabilityID = "punchplay"

	// Credentials record the app and server that issued them, because the
	// tokens only work there. A release that changes either then applies to
	// new connections without breaking existing ones.
	attributeClientID = "client_id"
	attributeBaseURL  = "base_url"

	// The host cancels every RPC after two minutes. Upstream work stops this
	// long before that, so the plugin can still answer with a fault.
	rpcDeadlineMargin = 5 * time.Second
)

// New connections sign in at punchplay.tv through PunchPlay's public device
// app for Silo, which has no secret, so it is safe to ship in the binary.
// These are variables so a development build can point at a local PunchPlay
// with -ldflags -X; see the README.
var (
	defaultBaseURL  = "https://punchplay.tv"
	defaultClientID = "ppc_2819963afd1e2e8b17132d55"
)

// requestedScopes is everything the plugin reads and writes.
var requestedScopes = []string{
	"profile:read",
	"history:read",
	"history:write",
	"ratings:read",
	"ratings:write",
	"lists:read",
	"lists:write",
	"playback:read",
	"playback:write",
}

// Server serves the WatchSyncProvider service. Register DeviceAuthorization
// alongside it: PunchPlay connects through device codes.
type Server struct {
	pluginv1.UnimplementedWatchSyncProviderServer
	api *apiClient
	now func() time.Time
	// signIn is where new connections sign in. signInFault is set instead
	// when this build's address or client ID is unusable.
	signIn      settings
	signInFault *pluginv1.WatchSyncFault
}

// NewServer returns a PunchPlay provider that sends requests with httpClient,
// or with a 20-second request timeout when httpClient is nil. version goes in
// the User-Agent.
func NewServer(httpClient *http.Client, version string) *Server {
	server := &Server{api: newAPIClient(httpClient, version), now: time.Now}
	server.signIn, server.signInFault = builtInSettings(defaultBaseURL, defaultClientID)
	return server
}

// DeviceAuthorization returns the WatchSyncDeviceAuthorizationService for
// this provider.
func (s *Server) DeviceAuthorization() pluginv1.WatchSyncDeviceAuthorizationServiceServer {
	return &deviceAuthServer{server: s}
}

// settings is the PunchPlay server and app new connections sign in through.
type settings struct {
	baseURL  string
	clientID string
}

func builtInSettings(rawBaseURL, clientID string) (settings, *pluginv1.WatchSyncFault) {
	baseURL, err := normalizeBaseURL(rawBaseURL)
	clientID = strings.TrimSpace(clientID)
	if err != nil || clientID == "" {
		return settings{}, permanentFault("This build of the PunchPlay plugin has an invalid API address or client ID")
	}
	return settings{baseURL: baseURL, clientID: clientID}, nil
}

// settingsFor returns where new connections for capability sign in.
func (s *Server) settingsFor(capability string) (settings, *pluginv1.WatchSyncFault) {
	if capability != capabilityID {
		return settings{}, invalidRequestFault("Unknown PunchPlay capability")
	}
	if s.signInFault != nil {
		return settings{}, s.signInFault
	}
	return s.signIn, nil
}

// authenticate pairs the profile's access token with the server and app that
// issued it.
func (s *Server) authenticate(auth *pluginv1.WatchSyncAuthenticatedContext) (account, *pluginv1.WatchSyncFault) {
	signIn, fault := s.settingsFor(auth.GetCapabilityId())
	if fault != nil {
		return account{}, fault
	}
	acct := issuedBy(signIn, auth.GetCredentials())
	if acct.token == "" {
		return account{}, &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			SafeMessage: "PunchPlay access token is missing; reconnect PunchPlay",
		}
	}
	return acct, nil
}

// issuedBy returns the server and app recorded with credentials, falling back
// to this build's for credentials that recorded neither.
func issuedBy(signIn settings, credentials *pluginv1.WatchSyncCredentials) account {
	acct := account{
		baseURL:  signIn.baseURL,
		clientID: signIn.clientID,
		token:    strings.TrimSpace(credentials.GetAccessToken()),
	}
	attributes := credentials.GetSecretAttributes()
	if issuer := strings.TrimSpace(attributes[attributeClientID]); issuer != "" {
		acct.clientID = issuer
	}
	if issuer := strings.TrimSpace(attributes[attributeBaseURL]); issuer != "" {
		if baseURL, err := normalizeBaseURL(issuer); err == nil {
			acct.baseURL = baseURL
		}
	}
	return acct
}

func (s *Server) RefreshCredentials(ctx context.Context, req *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	signIn, fault := s.settingsFor(req.GetContext().GetCapabilityId())
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	// An expired or missing access token is fine here: the refresh token is
	// what counts.
	acct := issuedBy(signIn, req.GetContext().GetCredentials())
	ctx, cancel := withRPCDeadline(ctx)
	defer cancel()
	credentials, fault := s.refresh(ctx, acct, req.GetContext().GetCredentials().GetRefreshToken())
	return &pluginv1.WatchSyncCredentialResponse{Credentials: credentials, Fault: fault}, nil
}

func (s *Server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	acct, fault := s.authenticate(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	ctx, cancel := withRPCDeadline(ctx)
	defer cancel()
	account, fault := s.lookupAccount(ctx, acct)
	return &pluginv1.WatchSyncGetAccountResponse{Account: account, Fault: fault}, nil
}

func (s *Server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	acct, fault := s.authenticate(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	ctx, cancel := withRPCDeadline(ctx)
	defer cancel()
	return s.listState(ctx, acct, req), nil
}

func (s *Server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	acct, fault := s.authenticate(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
	}
	ctx, cancel := withRPCDeadline(ctx)
	defer cancel()
	return s.applyEvents(ctx, acct, req.GetEvents()), nil
}

// withRPCDeadline ends upstream work rpcDeadlineMargin before the host's
// deadline for the call, unless less than that remains.
func withRPCDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= rpcDeadlineMargin {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, deadline.Add(-rpcDeadlineMargin))
}

func applied(eventID string) *pluginv1.WatchSyncApplyResult {
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED}
}

func noChange(eventID string) *pluginv1.WatchSyncApplyResult {
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE}
}

func rejected(eventID string, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED, Fault: fault}
}

func retry(eventID string, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY, Fault: fault}
}

// resultFor turns a request-level fault into one event's result: a fault
// worth retrying becomes RETRY, anything else REJECTED.
func resultFor(eventID string, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	switch fault.GetCode() {
	case pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED:
		return retry(eventID, fault)
	}
	return rejected(eventID, fault)
}
