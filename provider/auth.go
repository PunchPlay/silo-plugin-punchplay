package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// PunchPlay's device-code answer carries no interval; five seconds is the
	// RFC 8628 default.
	defaultPollInterval = 5 * time.Second
	// slowDownStep lengthens the interval after a rate-limited poll.
	slowDownStep = 5 * time.Second
	// deviceName labels the connection in PunchPlay's Connected apps.
	deviceName = "Silo"
)

var deviceCodePattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// deviceAuthServer connects a profile with a code the user enters at
// punchplay.tv/link.
type deviceAuthServer struct {
	pluginv1.UnimplementedWatchSyncDeviceAuthorizationServiceServer
	server *Server
}

// signInState is the provider_state the host keeps between polls. It records
// the server and app so a settings change mid sign-in cannot split them.
type signInState struct {
	DeviceCode      string    `json:"device_code"`
	IntervalSeconds int       `json:"interval_seconds"`
	ExpiresAt       time.Time `json:"expires_at"`
	ClientID        string    `json:"client_id"`
	BaseURL         string    `json:"base_url"`
}

type deviceCodeResponse struct {
	UserCode                string `json:"user_code"`
	DeviceCode              string `json:"device_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
}

func (d *deviceAuthServer) Start(ctx context.Context, req *pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest) (*pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse, error) {
	configured, fault := configuredSettings(req.GetCapabilityId(), req.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{Fault: fault}, nil
	}
	ctx, cancel := withRPCDeadline(ctx)
	defer cancel()
	acct := account{baseURL: configured.baseURL, clientID: configured.clientID}
	var answer deviceCodeResponse
	resp, fault := d.server.api.request(ctx, acct, http.MethodPost, "/auth/device/code", nil, map[string]any{
		"client_id": configured.clientID,
		"scope":     strings.Join(requestedScopes, " "),
	}, &answer)
	switch {
	case fault != nil:
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{Fault: fault}, nil
	case resp.status == http.StatusUnauthorized || resp.code == "invalid_client":
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{
			Fault: permissionDeniedFault("PunchPlay does not recognise the plugin's client ID; clear the Client ID in the plugin settings (" + describe(resp) + ")"),
		}, nil
	case resp.code == "invalid_scope":
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{
			Fault: permissionDeniedFault("The PunchPlay app for this client ID cannot grant the access Silo needs (" + describe(resp) + ")"),
		}, nil
	case !resp.ok():
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{Fault: faultFor(resp)}, nil
	case !deviceCodePattern.MatchString(answer.DeviceCode) || answer.UserCode == "" ||
		answer.VerificationURI == "" || answer.ExpiresIn <= 0:
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{
			Fault: permanentFault("PunchPlay returned an incomplete device code"),
		}, nil
	}
	interval := defaultPollInterval
	if answer.Interval > 0 {
		interval = time.Duration(answer.Interval) * time.Second
	}
	expiresAt := d.server.now().UTC().Add(time.Duration(answer.ExpiresIn) * time.Second)
	state, err := json.Marshal(signInState{
		DeviceCode:      answer.DeviceCode,
		IntervalSeconds: int(interval / time.Second),
		ExpiresAt:       expiresAt,
		ClientID:        configured.clientID,
		BaseURL:         configured.baseURL,
	})
	if err != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{Fault: permanentFault("PunchPlay sign-in state could not be encoded")}, nil
	}
	return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{
		UserCode:                answer.UserCode,
		VerificationUrl:         answer.VerificationURI,
		VerificationUrlComplete: answer.VerificationURIComplete,
		ProviderState:           state,
		PollingInterval:         durationpb.New(interval),
		ExpiresAt:               timestamppb.New(expiresAt),
	}, nil
}

func (d *deviceAuthServer) Poll(ctx context.Context, req *pluginv1.WatchSyncDeviceAuthorizationServicePollRequest) (*pluginv1.WatchSyncDeviceAuthorizationServicePollResponse, error) {
	if _, fault := configuredSettings(req.GetCapabilityId(), req.GetProviderConfig()); fault != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: fault}, nil
	}
	var state signInState
	if err := json.Unmarshal(req.GetProviderState(), &state); err != nil ||
		!deviceCodePattern.MatchString(state.DeviceCode) || state.ClientID == "" {
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
			Fault: invalidRequestFault("PunchPlay sign-in state is invalid; start connecting again"),
		}, nil
	}
	baseURL, err := normalizeBaseURL(state.BaseURL)
	if err != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
			Fault: invalidRequestFault("PunchPlay sign-in state is invalid; start connecting again"),
		}, nil
	}
	if !state.ExpiresAt.IsZero() && !d.server.now().Before(state.ExpiresAt) {
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED), nil
	}
	ctx, cancel := withRPCDeadline(ctx)
	defer cancel()
	acct := account{baseURL: baseURL, clientID: state.ClientID}
	var token tokenResponse
	resp, fault := d.server.api.request(ctx, acct, http.MethodPost, "/auth/device/token", nil, map[string]any{
		"client_id":   state.ClientID,
		"device_code": state.DeviceCode,
		"device_name": deviceName,
	}, &token)
	switch {
	case fault != nil:
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: fault}, nil
	case resp.ok():
		credentials := d.server.credentialsFrom(token, acct)
		if credentials == nil {
			return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: temporaryFault("PunchPlay returned no access token")}, nil
		}
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
			Status:      pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_AUTHORIZED,
			Credentials: credentials,
		}, nil
	case resp.code == "authorization_pending":
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING), nil
	case resp.status == http.StatusTooManyRequests:
		return slowDown(state, retryAfter(resp.header, d.server.now())), nil
	case resp.code == "expired":
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED), nil
	case resp.code == "access_denied":
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED), nil
	case resp.status == http.StatusUnauthorized:
		// The code belongs to another app, or the app was disabled. Either
		// way this sign-in cannot finish.
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
			Status: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED,
			Fault:  permissionDeniedFault("PunchPlay refused this sign-in (" + describe(resp) + ")"),
		}, nil
	default:
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: faultFor(resp)}, nil
	}
}

// slowDown keeps the flow pending with a longer interval, at least minimum.
// The interval is stored in the flow state so the next one grows again.
func slowDown(state signInState, minimum time.Duration) *pluginv1.WatchSyncDeviceAuthorizationServicePollResponse {
	interval := max(time.Duration(max(state.IntervalSeconds, 1))*time.Second+slowDownStep, minimum)
	state.IntervalSeconds = int((interval + time.Second - 1) / time.Second)
	interval = time.Duration(state.IntervalSeconds) * time.Second
	encoded, err := json.Marshal(state)
	if err != nil {
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING)
	}
	return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
		Status:          pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING,
		ProviderState:   encoded,
		PollingInterval: durationpb.New(interval),
	}
}

func pollStatus(status pluginv1.WatchSyncDeviceAuthorizationStatus) *pluginv1.WatchSyncDeviceAuthorizationServicePollResponse {
	return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Status: status}
}

// refresh exchanges the refresh token for a new pair. PunchPlay rotates
// refresh tokens; if the host fails to save the new pair, the old token keeps
// working for 60 seconds, so the host's retry still succeeds.
func (s *Server) refresh(ctx context.Context, acct account, refreshToken string) (*pluginv1.WatchSyncCredentials, *pluginv1.WatchSyncFault) {
	refreshToken = strings.TrimSpace(refreshToken)
	if refreshToken == "" {
		return nil, &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			SafeMessage: "PunchPlay refresh token is missing; reconnect PunchPlay",
		}
	}
	var token tokenResponse
	resp, fault := s.api.request(ctx, account{baseURL: acct.baseURL, clientID: acct.clientID}, http.MethodPost, "/auth/refresh", nil, map[string]any{
		"client_id":     acct.clientID,
		"refresh_token": refreshToken,
	}, &token)
	switch {
	case fault != nil:
		return nil, fault
	case resp.ok():
		credentials := s.credentialsFrom(token, acct)
		if credentials == nil {
			return nil, temporaryFault("PunchPlay returned no access token")
		}
		return credentials, nil
	case resp.code == "invalid_grant", resp.code == "invalid_request":
		// Revoked from PunchPlay's Connected apps, unused for a year, or
		// replayed past the grace window.
		return nil, &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			SafeMessage: "PunchPlay no longer accepts this connection's sign-in; reconnect PunchPlay (" + describe(resp) + ")",
		}
	case resp.code == "invalid_client":
		return nil, permissionDeniedFault("PunchPlay rejected the plugin's client ID (" + describe(resp) + ")")
	default:
		return nil, faultFor(resp)
	}
}

// credentialsFrom converts a token answer, or returns nil when it holds no
// access token. The credentials record the issuing server and app.
func (s *Server) credentialsFrom(token tokenResponse, acct account) *pluginv1.WatchSyncCredentials {
	accessToken := strings.TrimSpace(token.AccessToken)
	refreshToken := strings.TrimSpace(token.RefreshToken)
	if accessToken == "" || refreshToken == "" {
		return nil
	}
	credentials := &pluginv1.WatchSyncCredentials{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    token.TokenType,
		Scopes:       strings.Fields(token.Scope),
		SecretAttributes: map[string]string{
			attributeClientID: acct.clientID,
			attributeBaseURL:  acct.baseURL,
		},
	}
	if token.ExpiresIn > 0 {
		credentials.ExpiresAt = timestamppb.New(s.now().UTC().Add(time.Duration(token.ExpiresIn) * time.Second))
	}
	return credentials
}

type meResponse struct {
	ID       string  `json:"id"`
	Username *string `json:"username"`
	Profile  struct {
		AvatarURL   *string `json:"avatarUrl"`
		DisplayName *string `json:"displayName"`
	} `json:"profile"`
}

// lookupAccount reads the PunchPlay user behind the token. The subject is the
// stable user ID; usernames can change.
func (s *Server) lookupAccount(ctx context.Context, acct account) (*pluginv1.WatchSyncAccount, *pluginv1.WatchSyncFault) {
	var me meResponse
	if _, fault := s.api.call(ctx, acct, http.MethodGet, "/me", nil, nil, &me); fault != nil {
		return nil, fault
	}
	if strings.TrimSpace(me.ID) == "" {
		return nil, permanentFault("PunchPlay did not return an account for this token")
	}
	result := &pluginv1.WatchSyncAccount{ExternalSubject: me.ID}
	if me.Username != nil && *me.Username != "" {
		result.Username = *me.Username
		result.ProfileUrl = acct.baseURL + "/u/" + url.PathEscape(*me.Username)
	}
	// The account's own name stays private; Silo shows the public profile.
	result.DisplayName = result.Username
	if me.Profile.DisplayName != nil && *me.Profile.DisplayName != "" {
		result.DisplayName = *me.Profile.DisplayName
	}
	if me.Profile.AvatarURL != nil && *me.Profile.AvatarURL != "" {
		avatar := *me.Profile.AvatarURL
		if strings.HasPrefix(avatar, "/") {
			avatar = acct.baseURL + avatar
		}
		if strings.HasPrefix(avatar, "https://") || strings.HasPrefix(avatar, "http://") {
			result.AvatarUrl = avatar
		}
	}
	return result, nil
}
