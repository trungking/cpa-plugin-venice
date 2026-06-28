package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	ProviderKey          = "venice"
	AuthProviderKey      = "cpa-plugin-venice"
	StorageType          = "venice"
	clerkClientURL       = "https://clerk.venice.ai/v1/client?__clerk_api_version=2026-05-12&_clerk_js_version=6.22.0"
	veniceUserSessionURL = "https://outerface.venice.ai/api/user/session?bustBalanceCache=true"
	refreshLead          = 30 * time.Second
	defaultRefreshAfter  = 10 * time.Minute
	loginStateTTL        = 15 * time.Minute
)

type Provider struct{}

type loginSession struct {
	createdAt time.Time
	expiresAt time.Time
	auth      *pluginapi.AuthData
}

var loginSessions = struct {
	sync.Mutex
	byState map[string]loginSession
}{byState: make(map[string]loginSession)}

type Storage struct {
	Type                   string         `json:"type,omitempty"`
	Email                  string         `json:"email,omitempty"`
	Prefix                 string         `json:"prefix,omitempty"`
	Cookie                 string         `json:"cookie,omitempty"`
	Authorization          string         `json:"authorization,omitempty"`
	AuthorizationExpiresAt string         `json:"authorization_expires_at,omitempty"`
	UserID                 string         `json:"user_id,omitempty"`
	AccountPlan            string         `json:"account_plan,omitempty"`
	Quota                  map[string]any `json:"quota,omitempty"`
	QuotaCheckedAt         string         `json:"quota_checked_at,omitempty"`
	Raw                    map[string]any `json:"-"`
}

type clerkClientResponse struct {
	Response struct {
		LastActiveSessionID string `json:"last_active_session_id"`
		Sessions            []struct {
			ID              string `json:"id"`
			LastActiveToken struct {
				JWT string `json:"jwt"`
			} `json:"last_active_token"`
			User struct {
				ID string `json:"id"`
			} `json:"user"`
		} `json:"sessions"`
	} `json:"response"`
	Errors []struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"errors"`
}

func NewProvider() *Provider { return &Provider{} }

func (p *Provider) Identifier() string { return AuthProviderKey }

func (p *Provider) ParseAuth(_ context.Context, req pluginapi.AuthParseRequest) (pluginapi.AuthParseResponse, error) {
	storage, errParse := ParseStorage(req.RawJSON)
	if errParse != nil {
		return pluginapi.AuthParseResponse{Handled: true}, errParse
	}
	if storage == nil {
		return pluginapi.AuthParseResponse{}, nil
	}
	auth := AuthData(req.FileName, *storage)
	return pluginapi.AuthParseResponse{Handled: true, Auth: auth, Auths: []pluginapi.AuthData{auth}}, nil
}

func (p *Provider) StartLogin(_ context.Context, req pluginapi.AuthLoginStartRequest) (pluginapi.AuthLoginStartResponse, error) {
	state, errState := newLoginState()
	if errState != nil {
		return pluginapi.AuthLoginStartResponse{}, errState
	}
	expiresAt := time.Now().Add(loginStateTTL)
	loginSessions.Lock()
	loginSessions.byState[state] = loginSession{createdAt: time.Now(), expiresAt: expiresAt}
	loginSessions.Unlock()

	return pluginapi.AuthLoginStartResponse{
		Provider:  req.Provider,
		URL:       "/v0/resource/plugins/cpa-plugin-venice/login?state=" + url.QueryEscape(state),
		State:     state,
		ExpiresAt: expiresAt,
	}, nil
}

func (p *Provider) PollLogin(_ context.Context, req pluginapi.AuthLoginPollRequest) (pluginapi.AuthLoginPollResponse, error) {
	state := strings.TrimSpace(req.State)
	if state == "" {
		return pluginapi.AuthLoginPollResponse{Status: pluginapi.AuthLoginStatusError, Message: "missing Venice login state"}, nil
	}
	now := time.Now()
	loginSessions.Lock()
	session, ok := loginSessions.byState[state]
	if ok && now.After(session.expiresAt) {
		delete(loginSessions.byState, state)
		ok = false
	}
	if ok && session.auth != nil {
		auth := *session.auth
		delete(loginSessions.byState, state)
		loginSessions.Unlock()
		return pluginapi.AuthLoginPollResponse{Status: pluginapi.AuthLoginStatusSuccess, Auth: auth, Message: "Venice account saved"}, nil
	}
	loginSessions.Unlock()
	if !ok {
		return pluginapi.AuthLoginPollResponse{Status: pluginapi.AuthLoginStatusError, Message: "Venice login expired. Start login again."}, nil
	}
	return pluginapi.AuthLoginPollResponse{Status: pluginapi.AuthLoginStatusPending, Message: "Waiting for Venice cookie"}, nil
}

func (p *Provider) RefreshAuth(ctx context.Context, req pluginapi.AuthRefreshRequest) (pluginapi.AuthRefreshResponse, error) {
	storage, errParse := ParseStorage(req.StorageJSON)
	if errParse != nil {
		return pluginapi.AuthRefreshResponse{}, errParse
	}
	if storage == nil {
		return pluginapi.AuthRefreshResponse{}, fmt.Errorf("venice auth storage is missing")
	}
	if errRefresh := RefreshStorage(ctx, requireHTTPClient(req.HTTPClient), storage); errRefresh != nil {
		return pluginapi.AuthRefreshResponse{}, errRefresh
	}
	auth := AuthData(firstNonEmpty(req.AuthID, defaultFileName(*storage)), *storage)
	return pluginapi.AuthRefreshResponse{Auth: auth, NextRefreshAfter: auth.NextRefreshAfter}, nil
}

func ParseStorage(raw []byte) (*Storage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var decoded map[string]any
	if errUnmarshal := json.Unmarshal(raw, &decoded); errUnmarshal != nil {
		return nil, fmt.Errorf("decode venice auth: %w", errUnmarshal)
	}
	providerType := strings.ToLower(strings.TrimSpace(stringFromMap(decoded, "type")))
	if providerType != ProviderKey && providerType != AuthProviderKey && providerType != StorageType && providerType != "venice-web" {
		return nil, nil
	}
	storage := Storage{
		Type:                   StorageType,
		Email:                  strings.TrimSpace(stringFromMap(decoded, "email")),
		Prefix:                 strings.TrimSpace(stringFromMap(decoded, "prefix")),
		Cookie:                 NormalizeCookieInput(stringFromMap(decoded, "cookie")),
		Authorization:          strings.TrimSpace(stringFromMap(decoded, "authorization")),
		AuthorizationExpiresAt: strings.TrimSpace(stringFromMap(decoded, "authorization_expires_at")),
		UserID:                 strings.TrimSpace(stringFromMap(decoded, "user_id")),
		AccountPlan:            strings.TrimSpace(stringFromMap(decoded, "account_plan")),
		QuotaCheckedAt:         strings.TrimSpace(stringFromMap(decoded, "quota_checked_at")),
		Raw:                    cloneMap(decoded),
	}
	if quota, ok := decoded["quota"].(map[string]any); ok {
		storage.Quota = safeMetadataMap(quota)
	}
	if storage.Cookie == "" {
		storage.Cookie = NormalizeCookieInput(firstNonEmpty(
			stringFromMap(decoded, "client_cookie"),
			stringFromMap(decoded, "__client"),
		))
	}
	return &storage, nil
}

func AuthData(id string, storage Storage) pluginapi.AuthData {
	storage.Type = StorageType
	fileName := filepath.Base(strings.TrimSpace(id))
	if fileName == "" || fileName == "." {
		fileName = defaultFileName(storage)
	}
	label := storage.Email
	if label == "" {
		label = "Venice account"
	}
	metadata := cloneMap(storage.Raw)
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata["type"] = StorageType
	if storage.Email != "" {
		metadata["email"] = storage.Email
	}
	if storage.Prefix != "" {
		metadata["prefix"] = storage.Prefix
	}
	if storage.AccountPlan != "" {
		metadata["account_plan"] = storage.AccountPlan
	}
	if len(storage.Quota) > 0 {
		metadata["quota"] = storage.Quota
	}
	if storage.QuotaCheckedAt != "" {
		metadata["quota_checked_at"] = storage.QuotaCheckedAt
	}
	return pluginapi.AuthData{
		Provider:         ProviderKey,
		ID:               strings.TrimSuffix(fileName, ".json"),
		FileName:         fileName,
		Label:            label,
		Prefix:           storage.Prefix,
		StorageJSON:      storage.RawJSON(),
		Metadata:         metadata,
		Attributes:       map[string]string{"email": storage.Email},
		NextRefreshAfter: NextRefreshAfter(storage, time.Now()),
	}
}

func CompleteLogin(state string, auth pluginapi.AuthData) bool {
	state = strings.TrimSpace(state)
	if state == "" {
		return false
	}
	loginSessions.Lock()
	defer loginSessions.Unlock()
	session, ok := loginSessions.byState[state]
	if !ok || time.Now().After(session.expiresAt) {
		return false
	}
	session.auth = &auth
	loginSessions.byState[state] = session
	return true
}

func newLoginState() (string, error) {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("create Venice login state: %w", err)
	}
	return "venice-" + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func RefreshStorage(ctx context.Context, client pluginapi.HostHTTPClient, storage *Storage) error {
	if storage == nil {
		return fmt.Errorf("venice auth storage is missing")
	}
	if storage.Cookie == "" {
		return fmt.Errorf("venice __client cookie is missing")
	}
	if freshAuthorization(storage) {
		if errSession := fetchVeniceUserSession(ctx, client, storage); errSession != nil {
			return errSession
		}
		return nil
	}
	sessionID, sessionToken, errClient := clerkClient(ctx, client, storage)
	if errClient != nil {
		return errClient
	}
	jwt, errToken := clerkToken(ctx, client, storage, sessionID, sessionToken)
	if errToken != nil {
		return errToken
	}
	storage.Authorization = "Bearer " + jwt
	if exp, ok := jwtExpiry(jwt); ok {
		storage.AuthorizationExpiresAt = exp.UTC().Format(time.RFC3339)
	}
	if errSession := fetchVeniceUserSession(ctx, client, storage); errSession != nil {
		return errSession
	}
	return nil
}

func freshAuthorization(storage *Storage) bool {
	if storage == nil || strings.TrimSpace(storage.Authorization) == "" {
		return false
	}
	if expiry, ok := parseTime(storage.AuthorizationExpiresAt); ok {
		return expiry.After(time.Now().Add(refreshLead))
	}
	return false
}

func clerkClient(ctx context.Context, client pluginapi.HostHTTPClient, storage *Storage) (string, string, error) {
	resp, errDo := client.Do(ctx, pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    clerkClientURL,
		Headers: http.Header{
			"Accept":     []string{"*/*"},
			"Cookie":     []string{storage.Cookie},
			"Origin":     []string{"https://venice.ai"},
			"Referer":    []string{"https://venice.ai/"},
			"User-Agent": []string{userAgent()},
		},
	})
	if errDo != nil {
		return "", "", errDo
	}
	mergeSetCookies(storage, resp.Headers)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("clerk client lookup failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
	}
	var payload clerkClientResponse
	if errDecode := json.Unmarshal(resp.Body, &payload); errDecode != nil {
		return "", "", fmt.Errorf("decode clerk client response: %w", errDecode)
	}
	if len(payload.Errors) > 0 {
		return "", "", fmt.Errorf("clerk client error: %s", payload.Errors[0].Message)
	}
	for _, session := range payload.Response.Sessions {
		if session.ID == payload.Response.LastActiveSessionID || payload.Response.LastActiveSessionID == "" {
			if session.ID != "" && session.LastActiveToken.JWT != "" {
				return session.ID, session.LastActiveToken.JWT, nil
			}
		}
	}
	return "", "", fmt.Errorf("clerk client lookup did not return an active session")
}

func clerkToken(ctx context.Context, client pluginapi.HostHTTPClient, storage *Storage, sessionID string, sessionToken string) (string, error) {
	values := url.Values{}
	values.Set("organization_id", "")
	values.Set("token", sessionToken)
	values.Set("force_origin", "true")
	tokenURL := fmt.Sprintf("https://clerk.venice.ai/v1/client/sessions/%s/tokens?__clerk_api_version=2026-05-12&_clerk_js_version=6.22.0", url.PathEscape(sessionID))
	resp, errDo := client.Do(ctx, pluginapi.HTTPRequest{
		Method: http.MethodPost,
		URL:    tokenURL,
		Headers: http.Header{
			"Accept":       []string{"*/*"},
			"Content-Type": []string{"application/x-www-form-urlencoded"},
			"Cookie":       []string{storage.Cookie},
			"Origin":       []string{"https://venice.ai"},
			"Referer":      []string{"https://venice.ai/"},
			"User-Agent":   []string{userAgent()},
		},
		Body: []byte(values.Encode()),
	})
	if errDo != nil {
		return "", errDo
	}
	mergeSetCookies(storage, resp.Headers)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("clerk token request failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
	}
	var payload struct {
		JWT string `json:"jwt"`
	}
	if errDecode := json.Unmarshal(resp.Body, &payload); errDecode != nil {
		return "", fmt.Errorf("decode clerk token response: %w", errDecode)
	}
	if payload.JWT == "" {
		return "", fmt.Errorf("clerk token response did not include jwt")
	}
	return payload.JWT, nil
}

func fetchVeniceUserSession(ctx context.Context, client pluginapi.HostHTTPClient, storage *Storage) error {
	resp, errDo := client.Do(ctx, pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    veniceUserSessionURL,
		Headers: http.Header{
			"Accept":                        []string{"application/json"},
			"Authorization":                 []string{storage.Authorization},
			"Origin":                        []string{"https://venice.ai"},
			"Referer":                       []string{"https://venice.ai/"},
			"User-Agent":                    []string{userAgent()},
			"X-Venice-Version":              []string{"interface@20260627.111727+571b7be"},
			"x-venice-locale":               []string{"en"},
			"x-venice-request-timestamp-ms": []string{fmt.Sprintf("%d", time.Now().UnixMilli())},
		},
	})
	if errDo != nil {
		return errDo
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("venice user session failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
	}
	var payload struct {
		Token string `json:"token"`
	}
	if errDecode := json.Unmarshal(resp.Body, &payload); errDecode != nil {
		return fmt.Errorf("decode venice user session response: %w", errDecode)
	}
	var session map[string]any
	if errDecode := json.Unmarshal(resp.Body, &session); errDecode == nil {
		if quota := extractQuotaMetadata(session); len(quota) > 0 {
			storage.Quota = quota
			storage.QuotaCheckedAt = time.Now().UTC().Format(time.RFC3339)
		}
		if plan := extractPlan(session); plan != "" {
			storage.AccountPlan = plan
		}
	}
	claims := jwtClaims(payload.Token)
	if quota := extractQuotaMetadata(claims); len(quota) > 0 {
		storage.Quota = mergeQuotaMetadata(storage.Quota, quota)
		storage.QuotaCheckedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if plan := extractPlan(claims); plan != "" {
		storage.AccountPlan = plan
	}
	if email := firstNonEmpty(stringFromMap(claims, "email"), stringFromMap(claims, "userName"), stringFromMap(claims, "username")); email != "" {
		storage.Email = email
	}
	storage.UserID = firstNonEmpty(stringFromMap(claims, "clerkUserId"), stringFromMap(claims, "sub"), storage.UserID)
	return nil
}

func NextRefreshAfter(storage Storage, now time.Time) time.Time {
	if expiry, ok := parseTime(storage.AuthorizationExpiresAt); ok {
		refreshAt := expiry.Add(-refreshLead)
		if refreshAt.After(now) {
			return refreshAt
		}
		return now
	}
	return now.Add(defaultRefreshAfter)
}

func ShouldRefreshStorage(storage Storage, now time.Time) bool {
	if strings.TrimSpace(storage.Authorization) == "" {
		return true
	}
	if expiry, ok := parseTime(storage.AuthorizationExpiresAt); ok {
		return !expiry.Add(-refreshLead).After(now)
	}
	return true
}

func (s Storage) RawJSON() []byte {
	out := cloneMap(s.Raw)
	if out == nil {
		out = make(map[string]any)
	}
	out["type"] = ProviderKey
	out["cookie"] = s.Cookie
	if s.Email != "" {
		out["email"] = s.Email
	}
	if s.Prefix != "" {
		out["prefix"] = s.Prefix
	}
	if s.Authorization != "" {
		out["authorization"] = s.Authorization
	}
	if s.AuthorizationExpiresAt != "" {
		out["authorization_expires_at"] = s.AuthorizationExpiresAt
	}
	if s.UserID != "" {
		out["user_id"] = s.UserID
	}
	if s.AccountPlan != "" {
		out["account_plan"] = s.AccountPlan
	}
	if len(s.Quota) > 0 {
		out["quota"] = s.Quota
	}
	if s.QuotaCheckedAt != "" {
		out["quota_checked_at"] = s.QuotaCheckedAt
	}
	raw, _ := json.Marshal(out)
	return raw
}

func NormalizeCookieInput(value string) string {
	input := strings.TrimSpace(value)
	if input == "" {
		return ""
	}
	if jsonCookie := cookieHeaderFromJSON(input); jsonCookie != "" {
		return jsonCookie
	}
	lower := strings.ToLower(input)
	if strings.HasPrefix(lower, "cookie:") {
		return strings.TrimSpace(input[len("cookie:"):])
	}
	if line := findHeaderLine(input, "cookie"); line != "" {
		return line
	}
	if cookie := extractQuotedArg(input, "-b"); cookie != "" {
		return cookie
	}
	if cookie := extractQuotedArg(input, "--cookie"); cookie != "" {
		return cookie
	}
	if cookie := extractCookieHeaderArg(input); cookie != "" {
		return cookie
	}
	if strings.Contains(input, "__client=") && !strings.Contains(input, ";") {
		return input
	}
	return input
}

func RegisterFlags() []pluginapi.CommandLineFlag {
	return []pluginapi.CommandLineFlag{
		{Name: "venice-login", Usage: "Open Venice and prompt for the __client cookie.", Type: "bool", DefaultValue: "false"},
		{Name: "venice-cookie", Usage: "Create Venice auth from a __client cookie, Cookie header, Cookie Editor JSON, or copied cURL.", Type: "string"},
	}
}

func (p *Provider) RegisterCommandLine(context.Context, pluginapi.CommandLineRegistrationRequest) (pluginapi.CommandLineRegistrationResponse, error) {
	return pluginapi.CommandLineRegistrationResponse{Flags: RegisterFlags()}, nil
}

func (p *Provider) ExecuteCommandLine(ctx context.Context, req pluginapi.CommandLineExecutionRequest) (pluginapi.CommandLineExecutionResponse, error) {
	cookie := flagString(req.Flags, "venice-cookie")
	var stdout []byte
	if flagBool(req.TriggeredFlags, "venice-login") {
		stdout = append(stdout, []byte("Open Venice and sign in, then paste the __client cookie from clerk.venice.ai.\n\nhttps://venice.ai/chat/classic\n\n")...)
		if !flagBoolValue(req.Flags, "no-browser") {
			_ = openBrowser("https://venice.ai/chat/classic")
		}
		if cookie == "" {
			prompted, errPrompt := prompt("Paste __client cookie or Cookie header: ")
			if errPrompt != nil {
				return pluginapi.CommandLineExecutionResponse{Stdout: stdout, Stderr: []byte(errPrompt.Error() + "\n"), ExitCode: 1}, nil
			}
			cookie = prompted
		}
	}
	if cookie == "" {
		return pluginapi.CommandLineExecutionResponse{}, nil
	}
	storage := Storage{Type: ProviderKey, Cookie: NormalizeCookieInput(cookie)}
	if storage.Cookie == "" || !strings.Contains(storage.Cookie, "__client=") {
		return pluginapi.CommandLineExecutionResponse{Stdout: stdout, Stderr: []byte("venice __client cookie is required\n"), ExitCode: 1}, nil
	}
	if errRefresh := RefreshStorage(ctx, fallbackHTTPClient(req.Host.ProxyURL), &storage); errRefresh != nil {
		return pluginapi.CommandLineExecutionResponse{Stdout: stdout, Stderr: []byte(errRefresh.Error() + "\n"), ExitCode: 1}, nil
	}
	auth := AuthData(defaultFileName(storage), storage)
	stdout = append(stdout, []byte("Venice authentication successful.\n")...)
	return pluginapi.CommandLineExecutionResponse{Stdout: stdout, Auths: []pluginapi.AuthData{auth}}, nil
}

func defaultFileName(storage Storage) string {
	base := sanitizeFilePart(storage.Email)
	if base == "" {
		base = "venice"
	}
	return base + "-venice.json"
}

func sanitizeFilePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", "-", "@", "-", " ", "-", "\t", "-")
	value = strings.Trim(replacer.Replace(value), ".-")
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		}
	}
	return strings.Trim(b.String(), ".-")
}

func stringFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	switch v := m[key].(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func parseTime(raw string) (time.Time, bool) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	return ts, err == nil
}

func jwtExpiry(jwt string) (time.Time, bool) {
	claims := jwtClaims(jwt)
	exp, ok := claims["exp"].(float64)
	if !ok || exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(exp), 0), true
}

func jwtClaims(jwt string) map[string]any {
	parts := strings.Split(strings.TrimPrefix(jwt, "Bearer "), ".")
	if len(parts) < 2 {
		return nil
	}
	payload, errDecode := base64.RawURLEncoding.DecodeString(parts[1])
	if errDecode != nil {
		return nil
	}
	var out map[string]any
	if errUnmarshal := json.Unmarshal(payload, &out); errUnmarshal != nil {
		return nil
	}
	return out
}

func cookieHeaderFromJSON(input string) string {
	var decoded any
	if errUnmarshal := json.Unmarshal([]byte(input), &decoded); errUnmarshal != nil {
		return ""
	}
	var cookies []map[string]any
	switch v := decoded.(type) {
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				cookies = append(cookies, m)
			}
		}
	case map[string]any:
		if raw, ok := v["cookies"].([]any); ok {
			for _, item := range raw {
				if m, ok := item.(map[string]any); ok {
					cookies = append(cookies, m)
				}
			}
		}
	}
	parts := make([]string, 0, len(cookies))
	for _, cookie := range cookies {
		name := stringFromMap(cookie, "name")
		value := stringFromMap(cookie, "value")
		if name != "" {
			parts = append(parts, name+"="+value)
		}
	}
	return strings.Join(parts, "; ")
}

func findHeaderLine(input string, name string) string {
	prefix := strings.ToLower(name) + ":"
	for _, line := range strings.Split(input, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(trimmed), prefix) {
			return strings.TrimSpace(trimmed[len(prefix):])
		}
	}
	return ""
}

func extractQuotedArg(input string, flag string) string {
	for _, quote := range []byte{'\'', '"'} {
		marker := flag + " " + string(quote)
		if idx := strings.Index(input, marker); idx >= 0 {
			rest := input[idx+len(marker):]
			if end := strings.IndexByte(rest, quote); end >= 0 {
				return strings.TrimSpace(rest[:end])
			}
		}
	}
	return ""
}

func extractCookieHeaderArg(input string) string {
	for _, flag := range []string{"-H", "--header"} {
		if header := extractQuotedArg(input, flag); strings.HasPrefix(strings.ToLower(strings.TrimSpace(header)), "cookie:") {
			return strings.TrimSpace(header[len("cookie:"):])
		}
	}
	return ""
}

func mergeSetCookies(storage *Storage, headers http.Header) {
	if storage == nil {
		return
	}
	cookies := parseCookieHeader(storage.Cookie)
	for _, setCookie := range headers.Values("Set-Cookie") {
		pair := strings.SplitN(setCookie, ";", 2)[0]
		name, value, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(name) == "" {
			continue
		}
		cookies[strings.TrimSpace(name)] = value
	}
	storage.Cookie = serializeCookieHeader(cookies)
}

func parseCookieHeader(cookieHeader string) map[string]string {
	out := make(map[string]string)
	for _, part := range strings.Split(cookieHeader, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && name != "" {
			out[name] = value
		}
	}
	return out
}

func serializeCookieHeader(cookies map[string]string) string {
	parts := make([]string, 0, len(cookies))
	for name, value := range cookies {
		parts = append(parts, name+"="+value)
	}
	return strings.Join(parts, "; ")
}

func extractQuotaMetadata(session map[string]any) map[string]any {
	out := make(map[string]any)
	walkQuotaMetadata(out, "", session, 0)
	return out
}

func mergeQuotaMetadata(base map[string]any, overlay map[string]any) map[string]any {
	if len(base) == 0 {
		return overlay
	}
	out := make(map[string]any, len(base)+len(overlay))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range overlay {
		out[key] = value
	}
	return out
}

func walkQuotaMetadata(out map[string]any, path string, value any, depth int) {
	if depth > 4 {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			cleanKey := strings.TrimSpace(key)
			if cleanKey == "" || sensitiveMetadataKey(cleanKey) {
				continue
			}
			nextPath := cleanKey
			if path != "" {
				nextPath = path + "." + cleanKey
			}
			if quotaMetadataKey(cleanKey) {
				if safe, ok := safeMetadataValue(child, depth); ok {
					out[nextPath] = safe
				}
				continue
			}
			walkQuotaMetadata(out, nextPath, child, depth+1)
		}
	}
}

func extractPlan(session map[string]any) string {
	for _, key := range []string{"plan", "tier", "subscriptionPlan", "subscription_plan", "accountPlan", "account_plan", "userType", "user_type"} {
		if value := strings.TrimSpace(stringFromMap(session, key)); value != "" {
			return value
		}
	}
	if nested, ok := session["subscription"].(map[string]any); ok {
		return firstNonEmpty(
			strings.TrimSpace(stringFromMap(nested, "plan")),
			strings.TrimSpace(stringFromMap(nested, "tier")),
			strings.TrimSpace(stringFromMap(nested, "name")),
		)
	}
	return ""
}

func quotaMetadataKey(key string) bool {
	lower := strings.ToLower(key)
	for _, needle := range []string{"balance", "quota", "credit", "usage", "limit", "remaining", "allowance", "plan", "tier", "subscription", "point"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func sensitiveMetadataKey(key string) bool {
	lower := strings.ToLower(key)
	for _, needle := range []string{"token", "jwt", "cookie", "session", "secret", "authorization", "auth"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func safeMetadataMap(in map[string]any) map[string]any {
	out := make(map[string]any)
	for key, value := range in {
		if key == "" || sensitiveMetadataKey(key) {
			continue
		}
		if safe, ok := safeMetadataValue(value, 0); ok {
			out[key] = safe
		}
	}
	return out
}

func safeMetadataValue(value any, depth int) (any, bool) {
	if depth > 3 {
		return nil, false
	}
	switch typed := value.(type) {
	case nil, bool, float64, string:
		return typed, true
	case map[string]any:
		clean := safeMetadataMap(typed)
		return clean, len(clean) > 0
	case []any:
		if len(typed) > 20 {
			typed = typed[:20]
		}
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			if safe, ok := safeMetadataValue(item, depth+1); ok {
				out = append(out, safe)
			}
		}
		return out, len(out) > 0
	default:
		return fmt.Sprint(typed), true
	}
}

func requireHTTPClient(client pluginapi.HostHTTPClient) pluginapi.HostHTTPClient {
	if client != nil {
		return client
	}
	return fallbackHTTPClient("")
}

func fallbackHTTPClient(proxyURL string) pluginapi.HostHTTPClient {
	return fallbackClient{client: &http.Client{Timeout: 60 * time.Second}}
}

type fallbackClient struct {
	client *http.Client
}

func (c fallbackClient) Do(ctx context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	httpReq, errReq := http.NewRequestWithContext(ctx, req.Method, req.URL, strings.NewReader(string(req.Body)))
	if errReq != nil {
		return pluginapi.HTTPResponse{}, errReq
	}
	httpReq.Header = req.Headers.Clone()
	resp, errDo := c.client.Do(httpReq)
	if errDo != nil {
		return pluginapi.HTTPResponse{}, errDo
	}
	defer resp.Body.Close()
	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return pluginapi.HTTPResponse{}, errRead
	}
	return pluginapi.HTTPResponse{StatusCode: resp.StatusCode, Headers: resp.Header.Clone(), Body: body}, nil
}

func (c fallbackClient) DoStream(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	return pluginapi.HTTPStreamResponse{}, fmt.Errorf("venice fallback stream is unavailable")
}

func prompt(message string) (string, error) {
	_, _ = os.Stdout.Write([]byte(message))
	var line string
	_, err := fmt.Fscanln(os.Stdin, &line)
	return strings.TrimSpace(line), err
}

func openBrowser(target string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", target)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		cmd = exec.Command("xdg-open", target)
	}
	return cmd.Start()
}

func flagBool(flags map[string]pluginapi.CommandLineFlagValue, name string) bool {
	value, ok := flags[name]
	return ok && value.Set && strings.EqualFold(strings.TrimSpace(value.Value), "true")
}

func flagBoolValue(flags map[string]pluginapi.CommandLineFlagValue, name string) bool {
	value, ok := flags[name]
	return ok && strings.EqualFold(strings.TrimSpace(value.Value), "true")
}

func flagString(flags map[string]pluginapi.CommandLineFlagValue, name string) string {
	value, ok := flags[name]
	if !ok {
		return ""
	}
	return strings.TrimSpace(value.Value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func userAgent() string {
	return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
}
