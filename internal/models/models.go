package models

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const catalogURL = "https://api.venice.ai/api/v1/models"

type Provider struct{}

type catalogResponse struct {
	Data []catalogModel `json:"data"`
}

type catalogModel struct {
	ID               string `json:"id"`
	Object           string `json:"object"`
	Created          int64  `json:"created"`
	OwnedBy          string `json:"owned_by"`
	Type             string `json:"type"`
	DisplayName      string `json:"display_name"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	ContextLength    int64  `json:"context_length"`
	InputTokenLimit  int64  `json:"input_token_limit"`
	OutputTokenLimit int64  `json:"output_token_limit"`
	ModelSpec        struct {
		Capabilities map[string]any `json:"capabilities"`
	} `json:"model_spec"`
}

func NewProvider() *Provider { return &Provider{} }

func (p *Provider) StaticModels(context.Context, pluginapi.StaticModelRequest) (pluginapi.ModelResponse, error) {
	return pluginapi.ModelResponse{Provider: "venice", Models: fallbackModels()}, nil
}

func (p *Provider) ModelsForAuth(ctx context.Context, req pluginapi.AuthModelRequest) (pluginapi.ModelResponse, error) {
	if req.HTTPClient == nil {
		return pluginapi.ModelResponse{Provider: "venice", Models: fallbackModels()}, nil
	}
	models, err := fetchCatalog(ctx, req.HTTPClient)
	if err != nil || len(models) == 0 {
		return pluginapi.ModelResponse{Provider: "venice", Models: fallbackModels()}, nil
	}
	return pluginapi.ModelResponse{Provider: "venice", Models: models}, nil
}

func fetchCatalog(ctx context.Context, client pluginapi.HostHTTPClient) ([]pluginapi.ModelInfo, error) {
	resp, err := client.Do(ctx, pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    catalogURL,
		Headers: http.Header{
			"Accept":     []string{"application/json"},
			"User-Agent": []string{"Mozilla/5.0"},
		},
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil
	}
	var payload catalogResponse
	if err := json.Unmarshal(resp.Body, &payload); err != nil {
		var list []catalogModel
		if errList := json.Unmarshal(resp.Body, &list); errList != nil {
			return nil, err
		}
		payload.Data = list
	}
	out := make([]pluginapi.ModelInfo, 0, len(payload.Data))
	seen := make(map[string]bool)
	for _, model := range payload.Data {
		info := modelInfo(model.ID, firstNonEmpty(model.DisplayName, model.Name, model.ID), firstPositive(model.ContextLength, model.InputTokenLimit, defaultContext(model.ID)))
		info.Created = model.Created
		info.Object = firstNonEmpty(model.Object, "model")
		info.OwnedBy = firstNonEmpty(model.OwnedBy, "venice")
		info.Description = firstNonEmpty(model.Description, info.Description)
		info.Type = firstNonEmpty(model.Type, "chat")
		info.OutputTokenLimit = firstPositive(model.OutputTokenLimit, 65_536)
		addModel(&out, seen, info)
	}
	return out, nil
}

func fallbackModels() []pluginapi.ModelInfo {
	ids := []string{
		"zai-org-glm-5-2",
		"zai-org-glm-5-1",
		"zai-org-glm-5",
		"z-ai-glm-5-turbo",
		"z-ai-glm-5v-turbo",
		"zai-org-glm-4.7",
		"zai-org-glm-4.7-flash",
		"zai-org-glm-4.6",
		"gemini-3-5-flash",
		"gemini-3-1-pro-preview",
		"gemini-3-flash-preview",
		"claude-sonnet-4-6",
		"claude-opus-4-8",
		"openai-gpt-55",
		"openai-gpt-54",
		"qwen3-235b-a22b-instruct-2507",
		"qwen3-235b-a22b-thinking-2507",
		"kimi-k2-6",
		"deepseek-v4-pro",
	}
	out := make([]pluginapi.ModelInfo, 0, len(ids))
	seen := make(map[string]bool)
	for _, id := range ids {
		addModel(&out, seen, modelInfo(id, displayName(id), defaultContext(id)))
	}
	return out
}

func modelInfo(id string, display string, context int64) pluginapi.ModelInfo {
	return pluginapi.ModelInfo{
		ID:                         id,
		Object:                     "model",
		OwnedBy:                    "venice",
		Type:                       "chat",
		DisplayName:                display,
		Name:                       id,
		Description:                display + " via Venice web chat",
		InputTokenLimit:            context,
		OutputTokenLimit:           65_536,
		ContextLength:              context,
		MaxCompletionTokens:        65_536,
		SupportedGenerationMethods: []string{"chat.completions"},
		SupportedInputModalities:   []string{"text"},
		SupportedOutputModalities:  []string{"text"},
		SupportedParameters:        []string{"temperature", "top_p", "stream", "response_format"},
		Thinking: &pluginapi.ThinkingSupport{
			ZeroAllowed:    true,
			DynamicAllowed: true,
			Levels:         []string{"none", "auto"},
		},
	}
}

func addModel(out *[]pluginapi.ModelInfo, seen map[string]bool, model pluginapi.ModelInfo) {
	model.ID = strings.TrimSpace(model.ID)
	if model.ID == "" || seen[model.ID] {
		return
	}
	seen[model.ID] = true
	*out = append(*out, model)
}

func displayName(id string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(id, "-", " "), ".", "."))
}

func defaultContext(id string) int64 {
	if strings.Contains(id, "glm-5") || strings.Contains(id, "gemini") || strings.Contains(id, "claude") {
		return 1_000_000
	}
	return 200_000
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstPositive(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
