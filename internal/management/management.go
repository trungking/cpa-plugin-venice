package management

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	authpkg "github.com/trungking/cpa-plugin-venice/internal/auth"
	"github.com/trungking/cpa-plugin-venice/internal/monitor"
	settingspkg "github.com/trungking/cpa-plugin-venice/internal/settings"
)

const (
	accountsPath     = "/plugins/venice/accounts"
	accountsJSONPath = "/plugins/venice/accounts.json"
	realtimePath     = "/plugins/venice/realtime"
	realtimeJSONPath = "/plugins/venice/realtime.json"
	loginPath        = "/cpa-plugin-venice-login"
	resourcePath     = "/accounts"
	resourceRealtime = "/realtime"
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
			Path:        realtimePath,
			Description: "Venice realtime request monitor.",
			Handler:     p,
		}, {
			Method:      http.MethodGet,
			Path:        realtimeJSONPath,
			Description: "Venice realtime request monitor as JSON.",
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
			Path:        resourceRealtime,
			Description: "Venice realtime request monitor.",
			Handler:     p,
		}, {
			Path:        resourceRealtime + ".json",
			Description: "Venice realtime request monitor as JSON.",
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
	if isRealtimePath(req.Path) {
		snapshot := monitor.SnapshotPage(queryInt(req.Query, "page_size", queryInt(req.Query, "rows", 50)), queryInt(req.Query, "page", 1), queryBool(req.Query, "failed"), req.Query.Get("masked") != "0")
		if wantsJSON(req) {
			return jsonResponse(http.StatusOK, snapshot), nil
		}
		return realtimeHTMLResponse(), nil
	}
	pageSettings := loadSettings(ctx, host)
	if rawSetting := strings.TrimSpace(req.Query.Get("tool_repair")); rawSetting != "" {
		pageSettings.ToolRepairEnabled = queryBool(url.Values{"tool_repair": []string{rawSetting}}, "tool_repair")
		if err := saveSettings(ctx, host, pageSettings); err != nil {
			return jsonResponse(http.StatusBadGateway, map[string]any{"error": err.Error()}), nil
		}
	}
	accounts, err := p.accounts(ctx, host, req.Query.Get("refresh") == "1")
	if err != nil {
		return jsonResponse(http.StatusBadGateway, map[string]any{"error": err.Error()}), nil
	}
	stats := settingspkg.SnapshotStats()
	if wantsJSON(req) {
		return jsonResponse(http.StatusOK, map[string]any{"provider": authpkg.ProviderKey, "settings": pageSettings, "stats": stats, "accounts": accounts}), nil
	}
	return htmlResponse(accounts, pageSettings, stats), nil
}

func isLoginPath(path string) bool {
	path = strings.TrimRight(strings.TrimSpace(path), "/")
	return strings.HasSuffix(path, loginPath) || strings.HasSuffix(path, resourceLogin)
}

func isRealtimePath(path string) bool {
	path = strings.TrimRight(strings.TrimSpace(path), "/")
	return strings.HasSuffix(path, realtimePath) ||
		strings.HasSuffix(path, realtimeJSONPath) ||
		strings.HasSuffix(path, resourceRealtime) ||
		strings.HasSuffix(path, resourceRealtime+".json")
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
	body.WriteString("<style>:root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;padding:24px;background:#101723;color:#e7eaf1;font-family:system-ui,-apple-system,Segoe UI,sans-serif}.wrap{max-width:980px;margin:0 auto}.panel{border:1px solid #293346;background:#151c2b;border-radius:10px;padding:22px}h1{font-size:22px;margin:0 0 14px}.hint{color:#a9b2c1;margin-bottom:18px;line-height:1.5}.authbox{border:1px dashed #3b465a;background:#232a39;border-radius:8px;padding:16px;margin:16px 0}.authbox .url{font-weight:900;overflow-wrap:anywhere;margin:8px 0 14px}.field{margin-top:14px}label{display:block;font-weight:800;margin-bottom:7px}input,textarea{width:100%;border:1px solid #354156;background:#0c1220;color:#f4f7fb;border-radius:8px;padding:11px;font:inherit}textarea{min-height:210px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:13px}.err{display:inline-block;border:1px solid #7f3131;background:#381b21;color:#ffd5d5;border-radius:999px;padding:7px 11px;margin-bottom:12px}.actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:18px}.btn,.linkbtn{border:1px solid #4776a8;background:#2f94ff;color:white;border-radius:11px;padding:10px 16px;font-weight:900;cursor:pointer;text-decoration:none}.linkbtn{background:#2a3345;color:#e7edf7}.status{margin-top:14px;color:#b8c6d8}.hidden{display:none}</style>")
	body.WriteString("</head><body><main class=\"wrap\"><section class=\"panel\"><h1>Venice Provider Login</h1>")
	body.WriteString("<p class=\"hint\">Open Clerk for Venice, sign in if needed, then paste the Clerk `__client` cookie or a full Cookie header here. The plugin validates it, fetches the account email and quota, then saves the auth file.</p>")
	body.WriteString("<div class=\"authbox\"><div>Authorization URL:</div><div class=\"url\">https://clerk.venice.ai/</div><a class=\"linkbtn\" href=\"https://clerk.venice.ai/\" target=\"_blank\" rel=\"noreferrer\">Open Clerk</a></div>")
	if strings.TrimSpace(message) != "" {
		body.WriteString("<div class=\"err\">")
		body.WriteString(html.EscapeString(message))
		body.WriteString("</div>")
	}
	body.WriteString("<form id=\"venice-login-form\" method=\"post\" action=\"/v0/management/cpa-plugin-venice-login\"><input type=\"hidden\" name=\"state\" value=\"")
	body.WriteString(html.EscapeString(state))
	body.WriteString("\"><div class=\"field\"><label for=\"name\">Name (optional)</label><input id=\"name\" name=\"name\" autocomplete=\"off\" value=\"")
	body.WriteString(html.EscapeString(name))
	body.WriteString("\"></div><div class=\"field\"><label for=\"cookie\">Cookie</label><textarea id=\"cookie\" name=\"cookie\" spellcheck=\"false\">")
	body.WriteString(html.EscapeString(cookie))
	body.WriteString("</textarea></div><div id=\"management-key-field\" class=\"field hidden\"><label for=\"management-key\">Management key</label><input id=\"management-key\" name=\"management_key\" autocomplete=\"off\" type=\"password\"></div><div id=\"form-status\" class=\"status\"></div><div class=\"actions\"><button class=\"btn\" type=\"submit\">Save Venice Account</button></div></form>")
	body.WriteString("<script>")
	body.WriteString(`(function(){const form=document.getElementById("venice-login-form"),status=document.getElementById("form-status"),keyField=document.getElementById("management-key-field"),keyInput=document.getElementById("management-key");function text(v){return (v||"").trim()}function storageKeys(){return["managementKey","management-key","management_key","managementPassword","management-password","management_password","MANAGEMENT_PASSWORD","remoteManagementKey","remote-management-key","remote_management_key","secretKey","secret-key","secret_key","cpa_management_key","cliproxy_management_key","authToken","token"]}function readStorage(){const out=[];for(const store of [localStorage,sessionStorage]){for(const k of storageKeys()){try{const v=text(store.getItem(k));if(v)out.push(v)}catch(e){}}try{for(let i=0;i<store.length;i++){const k=store.key(i)||"";if(/management|secret|admin|token/i.test(k)){const v=text(store.getItem(k));if(v)out.push(v)}}}catch(e){}}return out}function readURL(){const out=[];for(const raw of [location.search,location.hash.replace(/^#/,"?")]){try{const p=new URLSearchParams(raw);for(const k of ["management_key","managementKey","key","token"]){const v=text(p.get(k));if(v)out.push(v)}}catch(e){}}return out}function candidates(){return Array.from(new Set([text(keyInput.value),...readStorage(),...readURL()].filter(Boolean)))}async function submitWithKey(key){const data=new URLSearchParams(new FormData(form));const headers={"Content-Type":"application/x-www-form-urlencoded"};if(key){headers["X-Management-Key"]=key;headers["Authorization"]="Bearer "+key}const res=await fetch("/v0/management/cpa-plugin-venice-login",{method:"POST",headers,body:data.toString(),credentials:"same-origin"});const body=await res.text();if(res.ok&&/<!doctype html/i.test(body)){document.open();document.write(body);document.close();return true}if(res.status===401||/missing management key|invalid management key/i.test(body)){return false}throw new Error(body||("HTTP "+res.status))}form.addEventListener("submit",async function(ev){ev.preventDefault();status.textContent="Validating Venice account...";let tried=false;try{for(const key of candidates()){tried=true;if(await submitWithKey(key))return}keyField.classList.remove("hidden");status.textContent=tried?"Management key was rejected. Enter the current CPAMP management key and submit again.":"Enter the CPAMP management key and submit again.";keyInput.focus()}catch(e){status.textContent=String(e&&e.message?e.message:e)}});})();`)
	body.WriteString("</script></section></main></body></html>")
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

func (p *Provider) accounts(ctx context.Context, host HostClient, refresh bool) ([]accountSummary, error) {
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
				if refresh && host.HTTPClient() != nil && storage.Cookie != "" {
					if errRefresh := authpkg.RefreshStorage(ctx, host.HTTPClient(), storage); errRefresh != nil {
						summary.Status = "error"
						summary.StatusMessage = errRefresh.Error()
					} else {
						auth := authpkg.AuthData(firstNonEmpty(entry.Name, entry.ID), *storage)
						saveName := firstNonEmpty(entry.Name, auth.FileName)
						if _, errSave := host.SaveAuth(ctx, saveName, auth.StorageJSON); errSave != nil {
							summary.Status = "error"
							summary.StatusMessage = errSave.Error()
						}
					}
				}
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

func loadSettings(ctx context.Context, host HostClient) settingspkg.Config {
	config := settingspkg.Get()
	if host == nil {
		return config
	}
	entries, err := host.ListAuths(ctx)
	if err != nil {
		return config
	}
	for _, entry := range entries {
		if !looksLikeSettingsEntry(entry) || entry.AuthIndex == "" {
			continue
		}
		resp, errGet := host.GetAuth(ctx, entry.AuthIndex)
		if errGet != nil {
			continue
		}
		if parsed, ok := settingspkg.Parse(resp.JSON); ok {
			settingspkg.Set(parsed)
			return parsed
		}
	}
	return config
}

func saveSettings(ctx context.Context, host HostClient, config settingspkg.Config) error {
	settingspkg.Set(config)
	if host == nil {
		return fmt.Errorf("CLIProxyAPI host callbacks are unavailable for saving Venice settings")
	}
	_, err := host.SaveAuth(ctx, settingspkg.FileName, settingspkg.Marshal(config))
	return err
}

func storageForEntry(ctx context.Context, host HostClient, authIndex string) (*authpkg.Storage, error) {
	resp, err := host.GetAuth(ctx, authIndex)
	if err != nil {
		return nil, err
	}
	return authpkg.ParseStorage(resp.JSON)
}

func looksLikeVeniceEntry(entry pluginapi.HostAuthFileEntry) bool {
	if looksLikeSettingsEntry(entry) {
		return false
	}
	return strings.EqualFold(entry.Provider, authpkg.ProviderKey) ||
		strings.EqualFold(entry.Type, authpkg.ProviderKey) ||
		strings.Contains(strings.ToLower(entry.Name), "venice") ||
		strings.Contains(strings.ToLower(entry.ID), "venice")
}

func looksLikeSettingsEntry(entry pluginapi.HostAuthFileEntry) bool {
	return strings.EqualFold(entry.Provider, settingspkg.Type) ||
		strings.EqualFold(entry.Type, settingspkg.Type) ||
		strings.EqualFold(entry.Name, settingspkg.FileName) ||
		strings.EqualFold(entry.ID, strings.TrimSuffix(settingspkg.FileName, ".json"))
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

func realtimeHTMLResponse() pluginapi.ManagementResponse {
	var body strings.Builder
	body.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><title>Venice Realtime</title>")
	body.WriteString("<style>:root{color-scheme:dark}*{box-sizing:border-box}body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0;padding:24px;color:#e7eaf1;background:#101723;font-size:14px}.page{max-width:1680px;margin:0 auto}.bar{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:18px}.title{display:flex;align-items:center;gap:10px}h1{font-size:18px;line-height:1.2;margin:0;font-weight:800}.count{background:#0f4777;color:#9bd0ff;border-radius:999px;padding:4px 9px;font-weight:800;font-size:12px}.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.seg{display:flex;border-radius:9px;overflow:hidden;background:#1e2634;border:1px solid #293346}.seg a,.seg span{display:flex;align-items:center;gap:8px;padding:8px 13px;color:#9da7b8;font-weight:800;font-size:12px;text-decoration:none}.seg .on{background:#12345a;color:#58a8ff}.seg .count{background:#3a4250;color:#f1f4f8;padding:2px 7px}.seg .on .count{background:#873d45;color:#ffb1b7}.btn,.select{border:1px solid #4776a8;background:#213a58;color:#dbeafe;border-radius:12px;padding:8px 13px;font-weight:800;text-decoration:none;white-space:nowrap;cursor:pointer}.btn:disabled{opacity:.45;cursor:not-allowed}.btn.off{border-color:#3a4659;background:#1e2634;color:#d9e1ed}.btn.fail{border-color:#8e3b4b;color:#ff9faa}.select{background:#1e2634}.pageinfo{color:#aeb6c3;font-weight:800;font-size:12px;padding:0 4px}.table{width:100%;border-collapse:separate;border-spacing:0 8px}.head th{background:#1e2634;border-top:1px solid #293346;border-bottom:1px solid #293346;padding:10px;text-align:left;color:#aeb6c3;font-size:12px;text-transform:uppercase}.head th:first-child{border-left:1px solid #293346;border-radius:9px 0 0 9px}.head th:last-child{border-right:1px solid #293346;border-radius:0 9px 9px 0}.row td{background:#151b2a;border-top:1px solid #293346;border-bottom:1px solid #293346;padding:17px 10px;vertical-align:middle}.row td:first-child{border-left:1px solid #293346;border-radius:9px 0 0 9px}.row td:last-child{border-right:1px solid #293346;border-radius:0 9px 9px 0}.source{font-size:16px;font-weight:900}.sub{color:#9da7b8;font-size:12px;margin-top:6px}.model{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:15px;font-weight:900}.effort{color:#48a6ff;font-weight:900}.ok{color:#5ee24b;font-weight:900}.badtext{color:#ff7c8f;font-weight:900}.num{font-size:16px}.usage{font-size:16px}.small{color:#9da7b8;font-size:12px}.metric{color:#61dc51;font-weight:900}.empty{padding:40px;text-align:center;color:#aeb7c6;border:1px dashed #344054;border-radius:9px;background:#151b2a}.details td{padding:0 10px 12px}.details pre{margin:0;max-height:260px;overflow:auto;white-space:pre-wrap;word-break:break-word;background:#0c1220;border:1px solid #2a3447;border-radius:7px;padding:12px;font-size:12px;line-height:1.45}.toggle{color:#91c9ff;cursor:pointer;text-decoration:underline;text-underline-offset:3px}@media(max-width:1100px){body{padding:14px}.bar{align-items:flex-start;flex-direction:column}.table{display:block;overflow:auto}.head th,.row td{white-space:nowrap}}</style>")
	body.WriteString("</head><body><main class=\"page\"><div class=\"bar\"><div class=\"title\"><h1>Venice Realtime</h1><span id=\"row-count-title\" class=\"count\">0</span></div><div class=\"actions\"><div class=\"seg\"><a href=\"/v0/resource/plugins/cpa-plugin-venice/accounts\">Accounts <span id=\"account-count\" class=\"count\">0</span></a><span class=\"on\">Realtime <span id=\"row-count\" class=\"count\">0</span></span></div><select id=\"page-size\" class=\"select\"><option>25</option><option selected>50</option><option>100</option><option>250</option><option>500</option></select><button id=\"prev\" class=\"btn\" type=\"button\">Prev</button><span id=\"page-info\" class=\"pageinfo\">Page 1 / 1</span><button id=\"next\" class=\"btn\" type=\"button\">Next</button><button id=\"mask\" class=\"btn\" type=\"button\">Masked</button><button id=\"failed\" class=\"btn fail off\" type=\"button\">Failed</button><button id=\"refresh\" class=\"btn\" type=\"button\">Refresh</button></div></div><div id=\"content\" class=\"empty\">Waiting for Venice requests...</div></main>")
	body.WriteString("<script>")
	body.WriteString(`(function(){let masked=true,failed=false,openRow="",page=1,pageSize=50,aliasMap={},aliasNext=0;const content=document.getElementById("content"),rowsBadge=document.getElementById("row-count"),titleBadge=document.getElementById("row-count-title"),acctBadge=document.getElementById("account-count"),maskBtn=document.getElementById("mask"),failedBtn=document.getElementById("failed"),refreshBtn=document.getElementById("refresh"),prevBtn=document.getElementById("prev"),nextBtn=document.getElementById("next"),pageInfo=document.getElementById("page-info"),sizeSel=document.getElementById("page-size");function esc(v){return String(v==null?"":v).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]))}function fmtMS(v){v=Number(v||0);return v>=1000?(v/1000).toFixed(v>=10000?1:2)+" s":Math.round(v)+" ms"}function usage(u){u=u||{};return {i:Number(u.input_tokens||0),o:Number(u.output_tokens||0),t:Number(u.total_tokens||0)}}function time(v){if(!v)return "";const d=new Date(v);return d.toLocaleDateString()+"<br>"+d.toLocaleTimeString()}function keyHash(row){return String(row.client_key_hash||"").toLowerCase()}function keyText(row){return aliasMap[keyHash(row)]||row.client_key||"unknown"}function keySub(row){return aliasMap[keyHash(row)]?"Alias":"API key"}function detail(row,id){return '<tr class="details" id="d-'+esc(id)+'"><td colspan="11"><pre>'+esc(JSON.stringify(row.details||[],null,2))+'</pre></td></tr>'}function rowHTML(row,i){const u=usage(row.usage),id=String(row.id||("row-"+i)),status=row.status==="Success"?'<span class="ok">Success</span>':'<span class="badtext">Failed</span>',err=row.error?'<div class="sub badtext">'+esc(row.error)+'</div>':"";return '<tr class="row"><td>'+time(row.time)+'</td><td><div class="source">'+esc(row.source)+'</div><div class="sub">Provider: venice</div>'+err+'</td><td><div class="source">'+esc(keyText(row))+'</div><div class="sub">'+esc(keySub(row))+'</div></td><td><span class="model">'+esc(row.model)+'</span></td><td><div class="effort">'+esc(row.effort||"medium")+'</div><div class="small">Tier: '+esc(row.tier||"default")+'</div></td><td>'+status+'</td><td><span class="metric">'+fmtMS(row.ttft_ms)+'</span></td><td><span class="metric">'+fmtMS(row.elapsed_ms)+'</span></td><td class="num">'+Number(row.tps||0).toFixed(0)+'</td><td><div class="usage">'+(u.t>=1000?(u.t/1000).toFixed(1)+"K":u.t)+'</div><div class="small">I '+u.i+' / O '+u.o+'</div></td><td><span class="toggle" data-id="'+esc(id)+'">Details</span></td></tr>'+(openRow===id?detail(row,id):"")}function render(data){const s=data.summary||{},total=Number(s.total||s.rows||0),pages=Number(s.pages||1)||1;page=Number(s.page||page)||1;rowsBadge.textContent=total;titleBadge.textContent=total;acctBadge.textContent=Number(s.accounts||0);pageInfo.textContent="Page "+page+" / "+pages;prevBtn.disabled=page<=1;nextBtn.disabled=page>=pages;if(!data.rows||!data.rows.length){content.className="empty";content.textContent=total?"No rows on this page.":"Waiting for Venice requests...";return}content.className="";content.innerHTML='<table class="table"><thead class="head"><tr><th>Time</th><th>Source</th><th>Key</th><th>Model</th><th>Effort</th><th>Status</th><th>TTFT</th><th>Elapsed</th><th>TPS</th><th>Usage</th><th>Detail</th></tr></thead><tbody>'+data.rows.map(rowHTML).join("")+'</tbody></table>';content.querySelectorAll(".toggle").forEach(el=>el.onclick=()=>{openRow=openRow===el.dataset.id?"":el.dataset.id;load()})}async function loadAliases(){if(Date.now()<aliasNext)return;aliasNext=Date.now()+30000;try{const res=await fetch("/v0/management/api-key-aliases",{cache:"no-store"});if(!res.ok)return;const data=await res.json(),next={};(data.items||[]).forEach(item=>{const h=String(item.apiKeyHash||"").toLowerCase(),a=String(item.alias||"").trim();if(h&&a)next[h]=a});aliasMap=next}catch(e){}}async function load(){const url="realtime.json?page="+page+"&page_size="+pageSize+"&masked="+(masked?1:0)+"&failed="+(failed?1:0)+"&_="+Date.now();try{await loadAliases();const res=await fetch(url,{cache:"no-store"});render(await res.json())}catch(e){content.className="empty";content.textContent=String(e&&e.message?e.message:e)}}maskBtn.onclick=()=>{masked=!masked;maskBtn.classList.toggle("off",!masked);maskBtn.textContent=masked?"Masked":"Unmasked";load()};failedBtn.onclick=()=>{failed=!failed;page=1;failedBtn.classList.toggle("off",!failed);load()};refreshBtn.onclick=()=>{aliasNext=0;load()};prevBtn.onclick=()=>{if(page>1){page--;load()}};nextBtn.onclick=()=>{page++;load()};sizeSel.onchange=()=>{pageSize=Number(sizeSel.value||50);page=1;load()};load();setInterval(load,1000);})();`)
	body.WriteString("</script></body></html>")
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       []byte(body.String()),
	}
}

func queryBool(values url.Values, key string) bool {
	switch strings.ToLower(strings.TrimSpace(values.Get(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func queryInt(values url.Values, key string, fallback int) int {
	raw := strings.TrimSpace(values.Get(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func htmlResponse(accounts []accountSummary, settings settingspkg.Config, stats settingspkg.Stats) pluginapi.ManagementResponse {
	var body strings.Builder
	body.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><title>Venice Accounts</title>")
	body.WriteString("<style>:root{color-scheme:dark}*{box-sizing:border-box}body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0;padding:24px;color:#e7eaf1;background:#101723;font-size:14px}.page{max-width:1680px;margin:0 auto}.bar{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:18px}.title{display:flex;align-items:center;gap:10px}h1{font-size:18px;line-height:1.2;margin:0;font-weight:800}.count{background:#0f4777;color:#9bd0ff;border-radius:999px;padding:4px 9px;font-weight:800;font-size:12px}.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.seg{display:flex;border-radius:9px;overflow:hidden;background:#1e2634;border:1px solid #293346}.seg a,.seg span{display:flex;align-items:center;gap:8px;padding:8px 13px;color:#9da7b8;font-weight:800;font-size:12px;text-decoration:none}.seg .on{background:#12345a;color:#58a8ff}.btn{border:1px solid #4776a8;background:#213a58;color:#dbeafe;border-radius:12px;padding:8px 13px;font-weight:800;text-decoration:none;white-space:nowrap}.btn.off{border-color:#3a4659;background:#1e2634;color:#d9e1ed}.settings{display:flex;align-items:center;justify-content:space-between;gap:16px;border:1px solid #293346;background:#151b2a;border-radius:9px;padding:14px 16px;margin-bottom:16px}.settings-title{font-weight:900}.settings-note{color:#9da7b8;font-size:12px;margin-top:4px}.stats{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.stat{border:1px solid #293346;background:#101723;border-radius:8px;padding:7px 10px}.stat b{display:block;color:#eef2f8}.stat span{color:#9da7b8;font-size:12px}.grid{display:grid;grid-template-columns:repeat(4,minmax(260px,1fr));gap:16px}.card{background:#151b2a;border:1px solid #293346;border-radius:9px;padding:17px;min-height:224px;box-shadow:0 1px 0 rgba(255,255,255,.03) inset}.top{display:flex;align-items:center;gap:10px;border-bottom:1px dashed #454b59;padding-bottom:13px;margin-bottom:12px}.pill{background:#2732b5;color:#d9ddff;border-radius:999px;padding:5px 11px;font-weight:800;font-size:12px}.account{font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}.muted{color:#8d95a3;font-size:12px}.strong{color:#eef2f8;font-weight:800}.meta{display:flex;gap:8px;align-items:center;margin-bottom:10px}.meter{margin-top:11px}.row{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}.label{font-weight:800}.pct{font-weight:900}.track{height:8px;background:#243b5b;border-radius:999px;overflow:hidden}.fill{height:100%;border-radius:999px;background:#5ec244}.fill.warn{background:#f0a52c}.fill.bad{background:#fb7185}.buttons{display:flex;justify-content:flex-end;gap:8px;margin-top:13px}.mini{border:1px solid #4776a8;background:#243f62;color:#e6edf8;border-radius:999px;padding:8px 14px;font-weight:800;text-decoration:none}details{margin-top:12px}summary{cursor:pointer;color:#9dc9ff;font-weight:800}pre{max-height:260px;overflow:auto;white-space:pre-wrap;word-break:break-word;background:#0c1220;border:1px solid #2a3447;border-radius:7px;padding:10px;font-size:12px;line-height:1.45}.empty{color:#aeb7c6}@media(max-width:1300px){.grid{grid-template-columns:repeat(3,minmax(260px,1fr))}}@media(max-width:960px){.grid{grid-template-columns:repeat(2,minmax(240px,1fr))}.bar,.settings{align-items:flex-start;flex-direction:column}}@media(max-width:620px){body{padding:14px}.grid{grid-template-columns:1fr}}</style>")
	body.WriteString("</head><body><main class=\"page\"><div class=\"bar\"><div class=\"title\"><h1>Venice Quota</h1><span class=\"count\">")
	body.WriteString(fmt.Sprint(len(accounts)))
	body.WriteString("</span></div><div class=\"actions\"><div class=\"seg\"><span class=\"on\">Accounts</span><a href=\"/v0/resource/plugins/cpa-plugin-venice/realtime\">Realtime</a></div><a class=\"btn\" href=\"?refresh=1\">Refresh all credentials</a></div></div>")
	body.WriteString("<section class=\"settings\"><div><div class=\"settings-title\">Tool-call repair</div><div class=\"settings-note\">Plugin-wide guardrails for Venice tool calls; useful when a model writes pending action text instead of calling a tool.</div><div class=\"stats\"><div class=\"stat\"><b>")
	body.WriteString(fmt.Sprint(stats.ToolRepairApplied))
	body.WriteString("</b><span>guardrail applied</span></div><div class=\"stat\"><b>")
	body.WriteString(fmt.Sprint(stats.ToolCallConversions))
	body.WriteString("</b><span>tool JSON converted</span></div><div class=\"stat\"><b>")
	body.WriteString(fmt.Sprint(stats.ToolCallsEmitted))
	body.WriteString("</b><span>tool calls emitted</span></div></div></div><a class=\"btn")
	if !settings.ToolRepairEnabled {
		body.WriteString(" off")
	}
	body.WriteString("\" href=\"?tool_repair=")
	if settings.ToolRepairEnabled {
		body.WriteString("0\">Disable")
	} else {
		body.WriteString("1\">Enable")
	}
	body.WriteString("</a></section>")
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
