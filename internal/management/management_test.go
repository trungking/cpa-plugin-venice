package management

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	authpkg "github.com/trungking/cpa-plugin-venice/internal/auth"
)

func TestHandleManagementReturnsVeniceAccountQuota(t *testing.T) {
	host := &fakeHost{
		entries: []pluginapi.HostAuthFileEntry{{
			ID:        "venice-user",
			AuthIndex: "auth-1",
			Name:      "venice-user.json",
			Provider:  "venice",
			Status:    "ok",
			Success:   3,
			Failed:    1,
		}, {
			ID:        "other",
			AuthIndex: "auth-2",
			Name:      "other.json",
			Provider:  "other",
		}},
		auths: map[string]json.RawMessage{
			"auth-1": []byte(`{"type":"venice","email":"user@example.com","account_plan":"pro","quota":{"balance":{"creditsRemaining":42}},"quota_checked_at":"2026-06-28T00:00:00Z","cookie":"__client=secret","authorization":"Bearer secret"}`),
		},
	}
	resp, err := New().HandleManagementWithHost(context.Background(), pluginapi.ManagementRequest{
		Method:  http.MethodGet,
		Path:    "/plugins/venice/accounts.json",
		Headers: http.Header{"Accept": []string{"application/json"}},
	}, host)
	if err != nil {
		t.Fatalf("HandleManagementWithHost error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d", resp.StatusCode)
	}
	var payload struct {
		Accounts []accountSummary `json:"accounts"`
	}
	if err := json.Unmarshal(resp.Body, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Accounts) != 1 {
		t.Fatalf("accounts len = %d", len(payload.Accounts))
	}
	account := payload.Accounts[0]
	if account.Email != "user@example.com" || account.AccountPlan != "pro" {
		t.Fatalf("account = %#v", account)
	}
	balance, _ := account.Quota["balance"].(map[string]any)
	if balance["creditsRemaining"] != float64(42) {
		t.Fatalf("quota = %#v", account.Quota)
	}
	if string(resp.Body) == "" || containsSecret(string(resp.Body)) {
		t.Fatalf("response leaked secret fields: %s", string(resp.Body))
	}
}

func TestLoginFormPreservesOAuthState(t *testing.T) {
	resp, err := New().HandleManagementWithHost(context.Background(), pluginapi.ManagementRequest{
		Method: http.MethodGet,
		Path:   "/v0/resource/plugins/cpa-plugin-venice/login",
		Query:  url.Values{"state": []string{"venice-test-state"}},
	}, &fakeHost{})
	if err != nil {
		t.Fatalf("HandleManagementWithHost error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d", resp.StatusCode)
	}
	if !strings.Contains(string(resp.Body), `name="state" value="venice-test-state"`) {
		t.Fatalf("form did not preserve state: %s", string(resp.Body))
	}
}

func TestLoginPostValidatesAndSavesAuth(t *testing.T) {
	host := &fakeHost{httpClient: fakeHTTPClient{}}
	form := url.Values{}
	form.Set("cookie", "__client=abc")
	resp, err := New().HandleManagementWithHost(context.Background(), pluginapi.ManagementRequest{
		Method:  http.MethodPost,
		Path:    "/v0/management/cpa-plugin-venice-login",
		Headers: http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}},
		Body:    []byte(form.Encode()),
	}, host)
	if err != nil {
		t.Fatalf("HandleManagementWithHost error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d", resp.StatusCode)
	}
	if host.savedName != "user-example.com-venice.json" {
		t.Fatalf("savedName = %q", host.savedName)
	}
	var saved map[string]any
	if err := json.Unmarshal(host.savedJSON, &saved); err != nil {
		t.Fatalf("decode saved auth: %v", err)
	}
	if saved["type"] != "venice" || saved["email"] != "user@example.com" {
		t.Fatalf("saved auth = %#v", saved)
	}
	if _, leaked := saved["authorization"]; !leaked {
		t.Fatalf("saved auth missing refreshed authorization")
	}
	quota, ok := saved["quota"].(map[string]any)
	if !ok || quota["bundledCredits"] != float64(95) {
		t.Fatalf("quota = %#v", saved["quota"])
	}
	if containsSecret(string(resp.Body)) {
		t.Fatalf("response leaked secret fields: %s", string(resp.Body))
	}
}

func TestLoginPostWithOAuthStateCompletesPollWithoutDirectSave(t *testing.T) {
	authProvider := authpkg.NewProvider()
	start, err := authProvider.StartLogin(context.Background(), pluginapi.AuthLoginStartRequest{Provider: "cpa-plugin-venice"})
	if err != nil {
		t.Fatalf("StartLogin error: %v", err)
	}
	host := &fakeHost{httpClient: fakeHTTPClient{}}
	form := url.Values{}
	form.Set("state", start.State)
	form.Set("cookie", "__client=abc")
	resp, err := New().HandleManagementWithHost(context.Background(), pluginapi.ManagementRequest{
		Method:  http.MethodPost,
		Path:    "/v0/management/cpa-plugin-venice-login",
		Headers: http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}},
		Body:    []byte(form.Encode()),
	}, host)
	if err != nil {
		t.Fatalf("HandleManagementWithHost error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d", resp.StatusCode)
	}
	if len(host.savedJSON) != 0 {
		t.Fatalf("OAuth form should complete poll state, not save directly")
	}
	poll, err := authProvider.PollLogin(context.Background(), pluginapi.AuthLoginPollRequest{State: start.State})
	if err != nil {
		t.Fatalf("PollLogin error: %v", err)
	}
	if poll.Status != pluginapi.AuthLoginStatusSuccess || poll.Auth.Provider != "venice" {
		t.Fatalf("poll = %#v", poll)
	}
	if !strings.Contains(string(poll.Auth.StorageJSON), `"type":"venice"`) {
		t.Fatalf("storage JSON = %s", string(poll.Auth.StorageJSON))
	}
}

func TestCreditQuotaUsesAvailableCreditsAndRefillTime(t *testing.T) {
	quota := map[string]any{
		"bundledCreditsUsage": map[string]any{
			"availableCredits": float64(95),
			"tierCap":          float64(100),
			"usedThisCycle":    float64(5),
			"nextRefillAt":     float64(1784460283868),
		},
	}
	got := progressHTML("Credits", creditAvailable(quota, quotaMetric(quota, []string{"bundledCreditsUsage", "tierCap"})), 100, epochMillisText(nestedValueOrNil(quota, []string{"bundledCreditsUsage", "nextRefillAt"})))
	if !strings.Contains(got, `95%`) || !strings.Contains(got, `07/19, 11:24`) || !strings.Contains(got, `width:95%`) {
		t.Fatalf("credit quota html = %s", got)
	}
}

type fakeHost struct {
	entries    []pluginapi.HostAuthFileEntry
	auths      map[string]json.RawMessage
	httpClient pluginapi.HostHTTPClient
	savedName  string
	savedJSON  json.RawMessage
}

func (h fakeHost) ListAuths(context.Context) ([]pluginapi.HostAuthFileEntry, error) {
	return h.entries, nil
}

func (h fakeHost) GetAuth(_ context.Context, authIndex string) (pluginapi.HostAuthGetResponse, error) {
	return pluginapi.HostAuthGetResponse{AuthIndex: authIndex, JSON: h.auths[authIndex]}, nil
}

func (h *fakeHost) SaveAuth(_ context.Context, name string, raw json.RawMessage) (pluginapi.HostAuthSaveResponse, error) {
	h.savedName = name
	h.savedJSON = append([]byte(nil), raw...)
	return pluginapi.HostAuthSaveResponse{Name: name, Path: "/auth/" + name}, nil
}

func (h fakeHost) HTTPClient() pluginapi.HostHTTPClient {
	return h.httpClient
}

type fakeHTTPClient struct{}

func (c fakeHTTPClient) Do(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	switch {
	case strings.Contains(req.URL, "/v1/client?"):
		return pluginapi.HTTPResponse{StatusCode: http.StatusOK, Headers: http.Header{}, Body: []byte(`{"response":{"last_active_session_id":"sess_1","sessions":[{"id":"sess_1","last_active_token":{"jwt":"session-token"},"user":{"id":"user_1"}}]}}`)}, nil
	case strings.Contains(req.URL, "/tokens?"):
		return pluginapi.HTTPResponse{StatusCode: http.StatusOK, Headers: http.Header{}, Body: []byte(`{"jwt":"` + testJWT(map[string]any{"exp": time.Now().Add(time.Hour).Unix()}) + `"}`)}, nil
	case strings.Contains(req.URL, "/api/user/session"):
		return pluginapi.HTTPResponse{StatusCode: http.StatusOK, Headers: http.Header{}, Body: []byte(`{"token":"` + testJWT(map[string]any{"email": "user@example.com", "accountPlan": "Plus", "bundledCredits": 95}) + `"}`)}, nil
	default:
		return pluginapi.HTTPResponse{StatusCode: http.StatusNotFound, Body: []byte(`not found`)}, nil
	}
}

func (c fakeHTTPClient) DoStream(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	return pluginapi.HTTPStreamResponse{}, nil
}

func testJWT(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, _ := json.Marshal(claims)
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func containsSecret(value string) bool {
	return strings.Contains(value, "__client=secret") || strings.Contains(value, "Bearer secret")
}
