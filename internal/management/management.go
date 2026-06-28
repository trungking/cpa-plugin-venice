package management

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	authpkg "github.com/trungking/cpa-plugin-venice/internal/auth"
)

const (
	accountsPath     = "/plugins/venice/accounts"
	accountsJSONPath = "/plugins/venice/accounts.json"
	loginPath        = "/cpa-plugin-venice-login"
	resourcePath     = "/accounts"
	resourceLogin    = "/login"
)

type HostClient interface {
	ListAuths(context.Context) ([]pluginapi.HostAuthFileEntry, error)
	GetAuth(context.Context, string) (pluginapi.HostAuthGetResponse, error)
	SaveAuth(context.Context, string, json.RawMessage) (pluginapi.HostAuthSaveResponse, error)
	HTTPClient() pluginapi.HostHTTPClient
}

type Provider struct{}

type accountSummary struct {
	ID                     string         `json:"id,omitempty"`
	AuthIndex              string         `json:"auth_index,omitempty"`
	Name                   string         `json:"name,omitempty"`
	Label                  string         `json:"label,omitempty"`
	Email                  string         `json:"email,omitempty"`
	Status                 string         `json:"status,omitempty"`
	StatusMessage          string         `json:"status_message,omitempty"`
	Disabled               bool           `json:"disabled,omitempty"`
	Unavailable            bool           `json:"unavailable,omitempty"`
	RuntimeOnly            bool           `json:"runtime_only,omitempty"`
	Source                 string         `json:"source,omitempty"`
	Success                int64          `json:"success,omitempty"`
	Failed                 int64          `json:"failed,omitempty"`
	LastRefresh            string         `json:"last_refresh,omitempty"`
	NextRetryAfter         string         `json:"next_retry_after,omitempty"`
	UpdatedAt              string         `json:"updated_at,omitempty"`
	AuthorizationExpiresAt string         `json:"authorization_expires_at,omitempty"`
	AccountPlan            string         `json:"account_plan,omitempty"`
	QuotaCheckedAt         string         `json:"quota_checked_at,omitempty"`
	Quota                  map[string]any `json:"quota,omitempty"`
}

func New() *Provider { return &Provider{} }

func (p *Provider) RegisterManagement(context.Context, pluginapi.ManagementRegistrationRequest) (pluginapi.ManagementRegistrationResponse, error) {
	return pluginapi.ManagementRegistrationResponse{
		Routes: []pluginapi.ManagementRoute{{
			Method:      http.MethodGet,
			Path:        accountsPath,
			Description: "Venice account quota and status summary.",
			Handler:     p,
		}, {
			Method:      http.MethodGet,
			Path:        accountsJSONPath,
			Description: "Venice account quota and status summary as JSON.",
			Handler:     p,
		}, {
			Method:      http.MethodGet,
			Path:        loginPath,
			Description: "Venice cookie login form.",
			Handler:     p,
		}, {
			Method:      http.MethodPost,
			Path:        loginPath,
			Description: "Validate and save a Venice cookie.",
			Handler:     p,
		}},
		Resources: []pluginapi.ResourceRoute{{
			Path:        resourcePath,
			Menu:        "Venice Accounts",
			Description: "Venice account quota and status summary.",
			Handler:     p,
		}, {
			Path:        resourceLogin,
			Description: "Venice cookie login form.",
			Handler:     p,
		}},
	}, nil
}

func (p *Provider) HandleManagement(ctx context.Context, req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	return p.HandleManagementWithHost(ctx, req, nil)
}

func (p *Provider) HandleManagementWithHost(ctx context.Context, req pluginapi.ManagementRequest, host HostClient) (pluginapi.ManagementResponse, error) {
	if isLoginPath(req.Path) {
		if strings.EqualFold(req.Method, http.MethodPost) {
			return p.saveLogin(ctx, req, host), nil
		}
		return loginFormResponse("", "", strings.TrimSpace(req.Query.Get("state")), ""), nil
	}
	accounts, err := p.accounts(ctx, host)
	if err != nil {
		return jsonResponse(http.StatusBadGateway, map[string]any{"error": err.Error()}), nil
	}
	if wantsJSON(req) {
		return jsonResponse(http.StatusOK, map[string]any{"provider": authpkg.ProviderKey, "accounts": accounts}), nil
	}
	return htmlResponse(accounts), nil
}

func isLoginPath(path string) bool {
	path = strings.TrimRight(strings.TrimSpace(path), "/")
	return strings.HasSuffix(path, loginPath) || strings.HasSuffix(path, resourceLogin)
}

func (p *Provider) saveLogin(ctx context.Context, req pluginapi.ManagementRequest, host HostClient) pluginapi.ManagementResponse {
	name, cookie, state, errInput := parseLoginInput(req)
	if errInput != nil {
		return loginFormResponse(name, cookie, state, errInput.Error())
	}
	if host == nil || host.HTTPClient() == nil {
		return loginFormResponse(name, cookie, state, "CLIProxyAPI host callbacks are unavailable for saving Venice auth.")
	}
	storage := authpkg.Storage{Type: authpkg.StorageType, Cookie: authpkg.NormalizeCookieInput(cookie)}
	if storage.Cookie == "" || !strings.Contains(storage.Cookie, "__client=") {
		return loginFormResponse(name, cookie, state, "Venice __client cookie is required.")
	}
	if errRefresh := authpkg.RefreshStorage(ctx, host.HTTPClient(), &storage); errRefresh != nil {
		return loginFormResponse(name, cookie, state, "Venice account validation failed: "+errRefresh.Error())
	}
	auth := authpkg.AuthData("", storage)
	fileName := auth.FileName
	if strings.TrimSpace(name) != "" {
		fileName = loginFileName(name)
		auth.FileName = fileName
		auth.ID = strings.TrimSuffix(fileName, ".json")
	}
	if state != "" {
		if !authpkg.CompleteLogin(state, auth) {
			return loginFormResponse(name, "", state, "Venice login session expired. Start Venice Provider Login again.")
		}
		return loginSuccessResponse(storage.Email, fileName)
	}
	saved, errSave := host.SaveAuth(ctx, fileName, auth.StorageJSON)
	if errSave != nil {
		return loginFormResponse(name, "", state, "Saving Venice auth failed: "+errSave.Error())
	}
	return loginSuccessResponse(storage.Email, saved.Name)
}

func parseLoginInput(req pluginapi.ManagementRequest) (string, string, string, error) {
	contentType := strings.ToLower(req.Headers.Get("Content-Type"))
	if strings.Contains(contentType, "application/json") {
		var payload struct {
			Name   string `json:"name"`
			Cookie string `json:"cookie"`
			State  string `json:"state"`
		}
		if err := json.Unmarshal(req.Body, &payload); err != nil {
			return "", "", "", fmt.Errorf("invalid JSON body")
		}
		return strings.TrimSpace(payload.Name), strings.TrimSpace(payload.Cookie), strings.TrimSpace(firstNonEmpty(payload.State, req.Query.Get("state"))), nil
	}
	values, err := url.ParseQuery(string(req.Body))
	if err != nil {
		return "", "", "", fmt.Errorf("invalid form body")
	}
	name := strings.TrimSpace(values.Get("name"))
	cookie := strings.TrimSpace(values.Get("cookie"))
	state := strings.TrimSpace(firstNonEmpty(values.Get("state"), req.Query.Get("state")))
	if cookie == "" {
		return name, cookie, state, fmt.Errorf("paste a Venice __client cookie or Cookie header")
	}
	return name, cookie, state, nil
}

func loginFileName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", "-", "@", "-", " ", "-", "\t", "-")
	value = strings.Trim(replacer.Replace(value), ".-")
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		}
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" {
		out = "venice"
	}
	if !strings.HasSuffix(out, ".json") {
		out += ".json"
	}
	return out
}

func loginFormResponse(name, cookie, state, message string) pluginapi.ManagementResponse {
	var body strings.Builder
	body.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><title>Venice Login</title>")
	body.WriteString("<style>:root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;padding:24px;background:#101723;color:#e7eaf1;font-family:system-ui,-apple-system,Segoe UI,sans-serif}.wrap{max-width:980px;margin:0 auto}.panel{border:1px solid #293346;background:#151c2b;border-radius:10px;padding:22px}h1{font-size:22px;margin:0 0 14px}.hint{color:#a9b2c1;margin-bottom:18px;line-height:1.5}.authbox{border:1px dashed #3b465a;background:#232a39;border-radius:8px;padding:16px;margin:16px 0}.authbox .url{font-weight:900;overflow-wrap:anywhere;margin:8px 0 14px}.field{margin-top:14px}label{display:block;font-weight:800;margin-bottom:7px}input,textarea{width:100%;border:1px solid #354156;background:#0c1220;color:#f4f7fb;border-radius:8px;padding:11px;font:inherit}textarea{min-height:210px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:13px}.err{display:inline-block;border:1px solid #7f3131;background:#381b21;color:#ffd5d5;border-radius:999px;padding:7px 11px;margin-bottom:12px}.actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:18px}.btn,.linkbtn{border:1px solid #4776a8;background:#2f94ff;color:white;border-radius:11px;padding:10px 16px;font-weight:900;cursor:pointer;text-decoration:none}.linkbtn{background:#2a3345;color:#e7edf7}</style>")
	body.WriteString("</head><body><main class=\"wrap\"><section class=\"panel\"><h1>Venice Provider Login</h1>")
	body.WriteString("<p class=\"hint\">Open Clerk for Venice, sign in if needed, then paste the Clerk `__client` cookie or a full Cookie header here. The plugin validates it, fetches the account email and quota, then saves the auth file.</p>")
	body.WriteString("<div class=\"authbox\"><div>Authorization URL:</div><div class=\"url\">https://clerk.venice.ai/</div><a class=\"linkbtn\" href=\"https://clerk.venice.ai/\" target=\"_blank\" rel=\"noreferrer\">Open Clerk</a></div>")
	if strings.TrimSpace(message) != "" {
		body.WriteString("<div class=\"err\">")
		body.WriteString(html.EscapeString(message))
		body.WriteString("</div>")
	}
	body.WriteString("<form method=\"post\" action=\"/v0/management/cpa-plugin-venice-login\"><input type=\"hidden\" name=\"state\" value=\"")
	body.WriteString(html.EscapeString(state))
	body.WriteString("\"><div class=\"field\"><label for=\"name\">Name (optional)</label><input id=\"name\" name=\"name\" autocomplete=\"off\" value=\"")
	body.WriteString(html.EscapeString(name))
	body.WriteString("\"></div><div class=\"field\"><label for=\"cookie\">Cookie</label><textarea id=\"cookie\" name=\"cookie\" spellcheck=\"false\">")
	body.WriteString(html.EscapeString(cookie))
	body.WriteString("</textarea></div><div class=\"actions\"><button class=\"btn\" type=\"submit\">Save Venice Account</button></div></form></section></main></body></html>")
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       []byte(body.String()),
	}
}

func loginSuccessResponse(email, fileName string) pluginapi.ManagementResponse {
	if email == "" {
		email = "Venice account"
	}
	var body strings.Builder
	body.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><title>Venice Login Saved</title>")
	body.WriteString("<style>:root{color-scheme:dark}body{margin:0;padding:24px;background:#101723;color:#e7eaf1;font-family:system-ui,-apple-system,Segoe UI,sans-serif}.wrap{max-width:760px;margin:0 auto}.panel{border:1px solid #2d5d41;background:#142319;border-radius:10px;padding:22px}.ok{color:#8ef0a3;font-weight:900}.btn{display:inline-block;margin-top:16px;border:1px solid #4776a8;background:#213a58;color:#dbeafe;border-radius:11px;padding:10px 14px;font-weight:800;text-decoration:none}</style>")
	body.WriteString("</head><body><main class=\"wrap\"><section class=\"panel\"><h1 class=\"ok\">Venice account saved</h1><p>")
	body.WriteString(html.EscapeString(email))
	body.WriteString("</p><p>")
	body.WriteString(html.EscapeString(fileName))
	body.WriteString("</p><a class=\"btn\" href=\"/v0/resource/plugins/cpa-plugin-venice/accounts\">Open Venice Accounts</a></section></main></body></html>")
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       []byte(body.String()),
	}
}

func (p *Provider) accounts(ctx context.Context, host HostClient) ([]accountSummary, error) {
	if host == nil {
		return []accountSummary{}, nil
	}
	entries, err := host.ListAuths(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]accountSummary, 0, len(entries))
	for _, entry := range entries {
		if !looksLikeVeniceEntry(entry) {
			continue
		}
		summary := summaryFromEntry(entry)
		if entry.AuthIndex != "" {
			storage, errStorage := storageForEntry(ctx, host, entry.AuthIndex)
			if errStorage == nil && storage != nil {
				mergeStorage(&summary, *storage)
			}
		}
		if summary.Email == "" && summary.Label != "" {
			summary.Email = summary.Label
		}
		out = append(out, summary)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(out[i].Email+out[i].Name) < strings.ToLower(out[j].Email+out[j].Name)
	})
	return out, nil
}

func storageForEntry(ctx context.Context, host HostClient, authIndex string) (*authpkg.Storage, error) {
	resp, err := host.GetAuth(ctx, authIndex)
	if err != nil {
		return nil, err
	}
	return authpkg.ParseStorage(resp.JSON)
}

func looksLikeVeniceEntry(entry pluginapi.HostAuthFileEntry) bool {
	return strings.EqualFold(entry.Provider, authpkg.ProviderKey) ||
		strings.EqualFold(entry.Type, authpkg.ProviderKey) ||
		strings.Contains(strings.ToLower(entry.Name), "venice") ||
		strings.Contains(strings.ToLower(entry.ID), "venice")
}

func summaryFromEntry(entry pluginapi.HostAuthFileEntry) accountSummary {
	return accountSummary{
		ID:             entry.ID,
		AuthIndex:      entry.AuthIndex,
		Name:           entry.Name,
		Label:          entry.Label,
		Email:          entry.Email,
		Status:         entry.Status,
		StatusMessage:  entry.StatusMessage,
		Disabled:       entry.Disabled,
		Unavailable:    entry.Unavailable,
		RuntimeOnly:    entry.RuntimeOnly,
		Source:         entry.Source,
		Success:        entry.Success,
		Failed:         entry.Failed,
		LastRefresh:    timeString(entry.LastRefresh),
		NextRetryAfter: timeString(entry.NextRetryAfter),
		UpdatedAt:      timeString(entry.UpdatedAt),
	}
}

func mergeStorage(summary *accountSummary, storage authpkg.Storage) {
	if storage.Email != "" {
		summary.Email = storage.Email
	}
	if storage.AccountPlan != "" {
		summary.AccountPlan = storage.AccountPlan
	}
	if storage.AuthorizationExpiresAt != "" {
		summary.AuthorizationExpiresAt = storage.AuthorizationExpiresAt
	}
	if storage.QuotaCheckedAt != "" {
		summary.QuotaCheckedAt = storage.QuotaCheckedAt
	}
	if len(storage.Quota) > 0 {
		summary.Quota = storage.Quota
	}
}

func wantsJSON(req pluginapi.ManagementRequest) bool {
	if strings.HasSuffix(req.Path, ".json") || strings.Contains(req.Query.Get("format"), "json") {
		return true
	}
	return strings.Contains(strings.ToLower(req.Headers.Get("accept")), "application/json")
}

func jsonResponse(status int, value any) pluginapi.ManagementResponse {
	raw, _ := json.Marshal(value)
	return pluginapi.ManagementResponse{
		StatusCode: status,
		Headers:    http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		Body:       raw,
	}
}

func htmlResponse(accounts []accountSummary) pluginapi.ManagementResponse {
	var body strings.Builder
	body.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><title>Venice Accounts</title>")
	body.WriteString("<style>:root{color-scheme:dark}*{box-sizing:border-box}body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0;padding:24px;color:#e7eaf1;background:#101723;font-size:14px}.page{max-width:1680px;margin:0 auto}.bar{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:18px}.title{display:flex;align-items:center;gap:10px}h1{font-size:18px;line-height:1.2;margin:0;font-weight:800}.count{background:#0f4777;color:#9bd0ff;border-radius:999px;padding:4px 9px;font-weight:800;font-size:12px}.actions{display:flex;gap:8px;align-items:center}.seg{display:flex;border-radius:9px;overflow:hidden;background:#1e2634;border:1px solid #293346}.seg span{padding:8px 13px;color:#9da7b8;font-weight:700;font-size:12px}.seg .on{background:#12345a;color:#58a8ff}.btn{border:1px solid #4776a8;background:#213a58;color:#dbeafe;border-radius:12px;padding:8px 13px;font-weight:800;text-decoration:none;white-space:nowrap}.grid{display:grid;grid-template-columns:repeat(4,minmax(260px,1fr));gap:16px}.card{background:linear-gradient(180deg,#373b4a,#1b2130);border:1px solid #3a4252;border-radius:9px;padding:17px;min-height:224px;box-shadow:0 1px 0 rgba(255,255,255,.03) inset}.top{display:flex;align-items:center;gap:10px;border-bottom:1px dashed #454b59;padding-bottom:13px;margin-bottom:12px}.pill{background:#2732b5;color:#d9ddff;border-radius:999px;padding:5px 11px;font-weight:800;font-size:12px}.account{font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}.muted{color:#8d95a3;font-size:12px}.strong{color:#eef2f8;font-weight:800}.meta{display:flex;gap:8px;align-items:center;margin-bottom:10px}.meter{margin-top:11px}.row{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}.label{font-weight:800}.pct{font-weight:900}.track{height:8px;background:#243b5b;border-radius:999px;overflow:hidden}.fill{height:100%;border-radius:999px;background:#5ec244}.fill.warn{background:#f0a52c}.fill.bad{background:#fb7185}.buttons{display:flex;justify-content:flex-end;gap:8px;margin-top:13px}.mini{border:1px solid #4776a8;background:#243f62;color:#e6edf8;border-radius:999px;padding:8px 14px;font-weight:800;text-decoration:none}details{margin-top:12px}summary{cursor:pointer;color:#9dc9ff;font-weight:800}pre{max-height:260px;overflow:auto;white-space:pre-wrap;word-break:break-word;background:#0c1220;border:1px solid #2a3447;border-radius:7px;padding:10px;font-size:12px;line-height:1.45}.empty{color:#aeb7c6}@media(max-width:1300px){.grid{grid-template-columns:repeat(3,minmax(260px,1fr))}}@media(max-width:960px){.grid{grid-template-columns:repeat(2,minmax(240px,1fr))}.bar{align-items:flex-start;flex-direction:column}}@media(max-width:620px){body{padding:14px}.grid{grid-template-columns:1fr}.actions{flex-wrap:wrap}}</style>")
	body.WriteString("</head><body><main class=\"page\"><div class=\"bar\"><div class=\"title\"><h1>Venice Quota</h1><span class=\"count\">")
	body.WriteString(fmt.Sprint(len(accounts)))
	body.WriteString("</span></div><div class=\"actions\"><div class=\"seg\"><span class=\"on\">Full</span><span>Paged</span><span class=\"on\">Show all</span></div><a class=\"btn\" href=\"?refresh=1\">Refresh all credentials</a></div></div>")
	if len(accounts) == 0 {
		body.WriteString("<p class=\"empty\">No Venice accounts found.</p></main></body></html>")
		return pluginapi.ManagementResponse{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
			Body:       []byte(body.String()),
		}
	}
	body.WriteString("<div class=\"grid\">")
	for _, account := range accounts {
		quota, _ := json.MarshalIndent(account.Quota, "", "  ")
		creditCap := quotaMetric(account.Quota, []string{"bundledCreditsUsage", "tierCap"}, []string{"bundledCreditsUsage", "monthlyRefillCredits"})
		credits := creditAvailable(account.Quota, creditCap)
		creditReset := epochMillisText(nestedValueOrNil(account.Quota, []string{"bundledCreditsUsage", "nextRefillAt"}))
		conversation := quotaMetric(account.Quota, []string{"rateLimits", "conversation", "remaining"})
		conversationReset := epochMillisText(nestedValueOrNil(account.Quota, []string{"rateLimits", "conversation", "resetAt"}))
		image := quotaMetric(account.Quota, []string{"rateLimits", "image", "remaining"})
		imageReset := epochMillisText(nestedValueOrNil(account.Quota, []string{"rateLimits", "image", "resetAt"}))
		body.WriteString("<section class=\"card\"><div class=\"top\"><span class=\"pill\">Venice</span><div class=\"account\" title=\"")
		body.WriteString(html.EscapeString(firstNonEmpty(account.Email, account.Label, account.Name, account.ID)))
		body.WriteString("\">")
		body.WriteString(html.EscapeString(firstNonEmpty(account.Email, account.Label, account.Name, account.ID)))
		body.WriteString("</div></div><div class=\"meta\"><span class=\"muted\">Plan</span><span class=\"strong\">")
		body.WriteString(html.EscapeString(firstNonEmpty(account.AccountPlan, "-")))
		body.WriteString("</span><span class=\"muted\">| Reset credits</span><span class=\"strong\">")
		body.WriteString(html.EscapeString(resetCreditsText(account)))
		body.WriteString("</span></div>")
		body.WriteString(progressHTML("Credits", credits, creditCap, creditReset))
		body.WriteString(progressHTML("Conversation limit", conversation, 3600, conversationReset))
		body.WriteString(progressHTML("Image limit", image, 1000, imageReset))
		body.WriteString("<div class=\"buttons\"><a class=\"mini\" href=\"?refresh=1\">Refresh Quota</a></div><details><summary>Details</summary><pre>")
		body.WriteString(html.EscapeString(string(quota)))
		body.WriteString("</pre></details></section>")
	}
	body.WriteString("</div></main></body></html>")
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       []byte(body.String()),
	}
}

func nestedValue(value any, path []string) (any, bool) {
	if len(path) == 0 {
		return value, true
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	child, ok := m[path[0]]
	if !ok {
		return nil, false
	}
	return nestedValue(child, path[1:])
}

func nestedValueOrNil(value any, path []string) any {
	if out, ok := nestedValue(value, path); ok {
		return out
	}
	return nil
}

func quotaMetric(quota map[string]any, paths ...[]string) float64 {
	for _, path := range paths {
		if value, ok := nestedValue(quota, path); ok {
			switch typed := value.(type) {
			case float64:
				return typed
			case int:
				return float64(typed)
			case int64:
				return float64(typed)
			case json.Number:
				if parsed, err := typed.Float64(); err == nil {
					return parsed
				}
			case string:
				var parsed json.Number = json.Number(strings.TrimSpace(typed))
				if value, err := parsed.Float64(); err == nil {
					return value
				}
			}
		}
	}
	return 0
}

func creditAvailable(quota map[string]any, cap float64) float64 {
	available := quotaMetric(quota,
		[]string{"bundledCreditsUsage", "availableCredits"},
		[]string{"veniceCredits"},
		[]string{"bundledCredits"},
	)
	if available > 0 {
		return available
	}
	used := quotaMetric(quota, []string{"bundledCreditsUsage", "usedThisCycle"})
	if cap > 0 && used > 0 {
		remaining := cap - used
		if remaining > 0 {
			return remaining
		}
	}
	return 0
}

func progressHTML(label string, value float64, cap float64, resetText string) string {
	percent := 0.0
	if cap > 0 {
		percent = value / cap * 100
	}
	if percent > 100 {
		percent = 100
	}
	class := "fill"
	if percent < 20 {
		class += " bad"
	} else if percent < 55 {
		class += " warn"
	}
	return fmt.Sprintf("<div class=\"meter\"><div class=\"row\"><span class=\"label\">%s</span><span><span class=\"pct\">%.0f%%</span> <span class=\"muted\">%s</span></span></div><div class=\"track\"><div class=\"%s\" style=\"width:%.0f%%\"></div></div></div>",
		html.EscapeString(label),
		percent,
		html.EscapeString(resetText),
		class,
		percent,
	)
}

func resetCreditsText(account accountSummary) string {
	if value, ok := nestedValue(account.Quota, []string{"bundledCreditsUsage", "nextRefillAt"}); ok {
		return epochMillisText(value)
	}
	return "Not Recorded"
}

func shortTime(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 16 {
		return value[5:10] + ", " + value[11:16]
	}
	return value
}

func epochMillisText(value any) string {
	var millis float64
	switch typed := value.(type) {
	case float64:
		millis = typed
	case int64:
		millis = float64(typed)
	case int:
		millis = float64(typed)
	default:
		return "Not Recorded"
	}
	if millis <= 0 {
		return "Not Recorded"
	}
	return time.UnixMilli(int64(millis)).UTC().Format("01/02, 15:04")
}

func accountRequestText(account accountSummary) string {
	return "success " + intText(account.Success) + " / failed " + intText(account.Failed)
}

func intText(value int64) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func timeString(value interface {
	IsZero() bool
	Format(string) string
}) string {
	if value.IsZero() {
		return ""
	}
	return value.Format("2006-01-02T15:04:05Z07:00")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
