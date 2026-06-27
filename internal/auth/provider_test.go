package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestNormalizeCookieInputAcceptsCurlCookie(t *testing.T) {
	input := `curl 'https://clerk.venice.ai/v1/client' -H 'accept: */*' -b '__client=abc; __client_uat=123'`
	got := NormalizeCookieInput(input)
	if got != "__client=abc; __client_uat=123" {
		t.Fatalf("NormalizeCookieInput = %q", got)
	}
}

func TestParseAuthAcceptsVeniceStorage(t *testing.T) {
	raw := []byte(`{"type":"venice","email":"user@example.com","prefix":"ven","cookie":"__client=abc"}`)
	resp, err := NewProvider().ParseAuth(context.Background(), pluginapi.AuthParseRequest{
		FileName: "user-venice.json",
		RawJSON:  raw,
	})
	if err != nil {
		t.Fatalf("ParseAuth error: %v", err)
	}
	if !resp.Handled || resp.Auth.Provider != ProviderKey {
		t.Fatalf("ParseAuth response = %#v", resp)
	}
	if resp.Auth.Label != "user@example.com" {
		t.Fatalf("label = %q", resp.Auth.Label)
	}
	if resp.Auth.Prefix != "ven" {
		t.Fatalf("prefix = %q", resp.Auth.Prefix)
	}
	var stored Storage
	if err := json.Unmarshal(resp.Auth.StorageJSON, &stored); err != nil {
		t.Fatalf("decode storage: %v", err)
	}
	if stored.Prefix != "ven" {
		t.Fatalf("stored prefix = %q", stored.Prefix)
	}
}

func TestRefreshAuthUsesClientCookieFlow(t *testing.T) {
	client := fakeClient{t: t}
	raw := []byte(`{"type":"venice","cookie":"__client=abc"}`)
	resp, err := NewProvider().RefreshAuth(context.Background(), pluginapi.AuthRefreshRequest{
		AuthID:      "venice-test",
		StorageJSON: raw,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("RefreshAuth error: %v", err)
	}
	var stored Storage
	if err := json.Unmarshal(resp.Auth.StorageJSON, &stored); err != nil {
		t.Fatalf("decode storage: %v", err)
	}
	if stored.Email != "user@example.com" {
		t.Fatalf("email = %q", stored.Email)
	}
	if !strings.HasPrefix(stored.Authorization, "Bearer ") {
		t.Fatalf("authorization = %q", stored.Authorization)
	}
	if !strings.Contains(stored.Cookie, "__client=rotated") {
		t.Fatalf("rotated cookie not persisted: %q", stored.Cookie)
	}
	if stored.AccountPlan != "pro" {
		t.Fatalf("account plan = %q", stored.AccountPlan)
	}
	if got := stored.Quota["bundledCredits"]; got != float64(95) {
		t.Fatalf("quota bundledCredits = %#v", got)
	}
	usage, _ := stored.Quota["bundledCreditsUsage"].(map[string]any)
	if got := usage["availableCredits"]; got != float64(95) {
		t.Fatalf("quota bundledCreditsUsage.availableCredits = %#v", got)
	}
	if got := stored.Quota["veniceCredits"]; got != float64(1089) {
		t.Fatalf("quota veniceCredits = %#v", got)
	}
	if _, ok := stored.Quota["sessionToken"]; ok {
		t.Fatal("sensitive quota field was not filtered")
	}
}

type fakeClient struct {
	t *testing.T
}

func (c fakeClient) Do(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	switch {
	case strings.HasPrefix(req.URL, "https://clerk.venice.ai/v1/client?"):
		return pluginapi.HTTPResponse{
			StatusCode: 200,
			Headers:    http.Header{"Set-Cookie": []string{"__client=rotated; Path=/; HttpOnly"}},
			Body:       []byte(`{"response":{"last_active_session_id":"sess_test","sessions":[{"id":"sess_test","last_active_token":{"jwt":"` + testJWT(map[string]any{"exp": float64(4102444800), "sub": "user_test"}) + `"},"user":{"id":"user_test"}}]}}`),
		}, nil
	case strings.Contains(req.URL, "/tokens?"):
		return pluginapi.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"jwt":"` + testJWT(map[string]any{"exp": float64(4102444800), "sub": "user_test"}) + `"}`),
		}, nil
	case strings.HasPrefix(req.URL, "https://outerface.venice.ai/api/user/session"):
		return pluginapi.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"token":"` + testJWT(map[string]any{"email": "user@example.com", "sub": "user_test", "bundledCredits": float64(95), "bundledCreditsUsage": map[string]any{"availableCredits": float64(95), "monthlyRefillCredits": float64(100)}, "rateLimits": map[string]any{"conversation": map[string]any{"remaining": float64(3600)}}, "veniceCredits": float64(1089), "sessionToken": "redacted-by-filter"}) + `","subscription":{"plan":"pro"}}`),
		}, nil
	default:
		c.t.Fatalf("unexpected URL %s", req.URL)
		return pluginapi.HTTPResponse{}, nil
	}
}

func (c fakeClient) DoStream(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	c.t.Fatal("DoStream should not be called")
	return pluginapi.HTTPStreamResponse{}, nil
}

func testJWT(payload map[string]any) string {
	raw, _ := json.Marshal(payload)
	return "header." + base64Raw(raw) + ".sig"
}

func base64Raw(raw []byte) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	var out strings.Builder
	var buffer uint
	var bits uint
	for _, b := range raw {
		buffer = (buffer << 8) | uint(b)
		bits += 8
		for bits >= 6 {
			bits -= 6
			out.WriteByte(alphabet[(buffer>>bits)&0x3f])
		}
	}
	if bits > 0 {
		out.WriteByte(alphabet[(buffer<<(6-bits))&0x3f])
	}
	return out.String()
}
