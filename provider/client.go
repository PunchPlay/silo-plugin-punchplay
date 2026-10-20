package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultBaseURL        = "https://punchplay.tv"
	platformPrefix        = "/api/platform/v1"
	defaultRequestTimeout = 20 * time.Second
	// A page is at most 100 sync records; the cap only guards against an
	// unbounded body.
	maxResponseBytes  = 16 << 20
	maxErrorBodyBytes = 4 << 10

	// PunchPlay sends Retry-After with every 429. Without one, a minute
	// clears any of its per-minute buckets.
	defaultRetryAfter = time.Minute
)

// apiClient talks to the PunchPlay Platform API. One client serves every RPC.
type apiClient struct {
	http        *http.Client
	userAgent   string
	maxResponse int64
}

func newAPIClient(httpClient *http.Client, version string) *apiClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	agent := "silo-plugin-punchplay"
	if strings.TrimSpace(version) != "" {
		agent += "/" + strings.TrimSpace(version)
	}
	return &apiClient{http: httpClient, userAgent: agent, maxResponse: maxResponseBytes}
}

// account is what one RPC talks to PunchPlay with: the server, the app the
// connection signed in through, and after sign-in the profile's access token.
type account struct {
	baseURL  string
	clientID string
	token    string
}

// apiError is PunchPlay's error envelope. Only the machine-readable code and
// the request ID are ever repeated in a fault: the message can echo input.
type apiError struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id"`
}

// response is the outcome of one request that reached PunchPlay.
type response struct {
	status    int
	code      string
	requestID string
	header    http.Header
}

func (r response) ok() bool { return r.status >= 200 && r.status < 300 }

// request sends one JSON request and decodes a successful reply into out.
// It returns a fault only when PunchPlay could not be reached or answered
// unreadably; an error status is returned in response for the caller to
// interpret, because some (authorization_pending, 409) are expected answers.
func (c *apiClient) request(ctx context.Context, acct account, method, path string, headers map[string]string, payload, out any) (response, *pluginv1.WatchSyncFault) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return response{}, permanentFault("PunchPlay request could not be encoded")
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, acct.baseURL+platformPrefix+path, body)
	if err != nil {
		return response{}, permanentFault("PunchPlay request could not be created")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if acct.token != "" {
		req.Header.Set("Authorization", "Bearer "+acct.token)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			return response{}, temporaryFault("PunchPlay did not answer within the sync time limit")
		}
		return response{}, temporaryFault("PunchPlay is temporarily unreachable")
	}
	defer func() { _ = resp.Body.Close() }()
	result := response{status: resp.StatusCode, header: resp.Header, requestID: resp.Header.Get("X-PunchPlay-Request-Id")}
	if !result.ok() {
		var envelope apiError
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBodyBytes)).Decode(&envelope)
		result.code = strings.TrimSpace(envelope.Error)
		if result.requestID == "" {
			result.requestID = strings.TrimSpace(envelope.RequestID)
		}
		return result, nil
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, c.maxResponse))
		return result, nil
	}
	limited := &io.LimitedReader{R: resp.Body, N: c.maxResponse + 1}
	if err := json.NewDecoder(limited).Decode(out); err != nil {
		if limited.N <= 0 {
			return result, permanentFault("PunchPlay returned a response larger than the plugin reads")
		}
		return result, temporaryFault("PunchPlay returned an unreadable response")
	}
	return result, nil
}

// call is request for callers that treat every error status as a fault.
func (c *apiClient) call(ctx context.Context, acct account, method, path string, headers map[string]string, payload, out any) (response, *pluginv1.WatchSyncFault) {
	resp, fault := c.request(ctx, acct, method, path, headers, payload, out)
	if fault != nil {
		return resp, fault
	}
	if !resp.ok() {
		return resp, faultFor(resp)
	}
	return resp, nil
}

// faultFor maps an error status. A 401 or 403 concerns the whole connection;
// everything else concerns the request that drew it.
func faultFor(resp response) *pluginv1.WatchSyncFault {
	detail := describe(resp)
	switch {
	case resp.status == http.StatusUnauthorized:
		return &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			SafeMessage: "PunchPlay rejected the access token; reconnect PunchPlay (" + detail + ")",
		}
	case resp.status == http.StatusForbidden:
		return permissionDeniedFault("PunchPlay refused the request; reconnect PunchPlay to grant access again (" + detail + ")")
	case resp.status == http.StatusTooManyRequests:
		return rateLimitedFault(retryAfter(resp.header, time.Now()))
	case resp.status >= http.StatusInternalServerError:
		return temporaryFault("PunchPlay is temporarily unavailable (" + detail + ")")
	case resp.status >= http.StatusBadRequest:
		return invalidRequestFault("PunchPlay rejected the request (" + detail + ")")
	default:
		return permanentFault("PunchPlay request failed (" + detail + ")")
	}
}

// connectionWide reports a fault that belongs on the RPC response rather than
// on one event: the token or its scopes, or a rate limit for the account.
func connectionWide(fault *pluginv1.WatchSyncFault) bool {
	switch fault.GetCode() {
	case pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED:
		return true
	}
	return false
}

func describe(resp response) string {
	parts := []string{"HTTP " + strconv.Itoa(resp.status)}
	if resp.code != "" {
		parts = append(parts, resp.code)
	}
	if resp.requestID != "" {
		parts = append(parts, "request "+resp.requestID)
	}
	return strings.Join(parts, ", ")
}

// retryAfter reads Retry-After in seconds or as an HTTP date, then
// X-RateLimit-Reset, and falls back to a minute.
func retryAfter(header http.Header, now time.Time) time.Duration {
	if value := strings.TrimSpace(header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return max(time.Duration(seconds)*time.Second, time.Second)
		}
		if at, err := http.ParseTime(value); err == nil && at.After(now) {
			return at.Sub(now)
		}
	}
	if value := strings.TrimSpace(header.Get("X-RateLimit-Reset")); value != "" {
		if unix, err := strconv.ParseInt(value, 10, 64); err == nil {
			if at := time.Unix(unix, 0); at.After(now) {
				return at.Sub(now)
			}
		}
	}
	return defaultRetryAfter
}

// normalizeBaseURL accepts an https origin, or http for a server on this
// machine, and returns it without a trailing slash.
func normalizeBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultBaseURL, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("invalid base URL")
	}
	host := parsed.Hostname()
	local := host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".localhost")
	if parsed.Scheme != "https" && (parsed.Scheme != "http" || !local) {
		return "", fmt.Errorf("base URL must use https")
	}
	return strings.TrimRight(parsed.Scheme+"://"+parsed.Host+parsed.Path, "/"), nil
}

func rateLimitedFault(wait time.Duration) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
		SafeMessage: "PunchPlay rate limit reached",
		RetryAfter:  durationpb.New(wait),
	}
}

func temporaryFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY, SafeMessage: message}
}

func permanentFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, SafeMessage: message}
}

func invalidRequestFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, SafeMessage: message}
}

func permissionDeniedFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED, SafeMessage: message}
}
