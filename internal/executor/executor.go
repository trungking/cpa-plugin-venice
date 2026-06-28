package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	authpkg "github.com/trungking/cpa-plugin-venice/internal/auth"
	"github.com/trungking/cpa-plugin-venice/internal/monitor"
	settingspkg "github.com/trungking/cpa-plugin-venice/internal/settings"
)

const chatURL = "https://outerface.venice.ai/api/inference/chat"

type Executor struct{}

type openAIRequest struct {
	Model             string            `json:"model"`
	Messages          []openAIMessage   `json:"messages"`
	Stream            bool              `json:"stream"`
	Temperature       any               `json:"temperature,omitempty"`
	TopP              any               `json:"top_p,omitempty"`
	ReasoningEffort   string            `json:"reasoning_effort,omitempty"`
	ServiceTier       string            `json:"service_tier,omitempty"`
	Tools             []json.RawMessage `json:"tools,omitempty"`
	ToolChoice        any               `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool             `json:"parallel_tool_calls,omitempty"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    any              `json:"content"`
	Name       string           `json:"name,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolCall struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function openAIFunctionCall `json:"function"`
}

type openAIFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type veniceLine struct {
	Kind             string `json:"kind"`
	Content          string `json:"content"`
	ReasoningContent string `json:"reasoning_content"`
	CompletionID     string `json:"completion_id"`
}

func NewExecutor() *Executor { return &Executor{} }

func (e *Executor) Identifier() string { return authpkg.ProviderKey }

func (e *Executor) Execute(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	storage, errStorage := refreshedStorage(ctx, req)
	if errStorage != nil {
		return pluginapi.ExecutorResponse{}, errStorage
	}
	openReq, veniceBody, model, errBuild := buildVeniceRequest(req)
	if errBuild != nil {
		return pluginapi.ExecutorResponse{}, errBuild
	}
	span := monitor.Start(monitor.RequestInfo{
		Source:      firstNonEmpty(storage.Email, storage.UserID, req.AuthID),
		ClientKey:   clientKeyLabel(req),
		ClientHash:  clientKeyHash(req),
		Model:       model,
		Effort:      firstNonEmpty(metadataString(req.Metadata, "reasoning_effort"), openReq.ReasoningEffort),
		Tier:        firstNonEmpty(metadataString(req.Metadata, "service_tier"), openReq.ServiceTier),
		InputTokens: int64(estimateRequestTokens(openReq)),
	})
	resp, errDo := requireClient(req.HTTPClient).Do(ctx, pluginapi.HTTPRequest{
		Method:  http.MethodPost,
		URL:     chatURL,
		Headers: veniceHeaders(*storage, false),
		Body:    veniceBody,
	})
	if errDo != nil {
		span.Finish(monitor.Result{Success: false, Error: errDo.Error()})
		return pluginapi.ExecutorResponse{}, errDo
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errStatus := fmt.Errorf("venice chat failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
		span.Finish(monitor.Result{Success: false, Error: errStatus.Error()})
		return pluginapi.ExecutorResponse{}, errStatus
	}
	payload := aggregateOpenAIResponse(resp.Body, model, openReq)
	usage := usageFromOpenAIResponse(payload)
	span.Finish(monitor.Result{Success: true, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens})
	return pluginapi.ExecutorResponse{
		Payload: payload,
		Headers: http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

func (e *Executor) ExecuteStream(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
	storage, errStorage := refreshedStorage(ctx, req)
	if errStorage != nil {
		return pluginapi.ExecutorStreamResponse{}, errStorage
	}
	openReq, veniceBody, model, errBuild := buildVeniceRequest(req)
	if errBuild != nil {
		return pluginapi.ExecutorStreamResponse{}, errBuild
	}
	span := monitor.Start(monitor.RequestInfo{
		Source:      firstNonEmpty(storage.Email, storage.UserID, req.AuthID),
		ClientKey:   clientKeyLabel(req),
		ClientHash:  clientKeyHash(req),
		Model:       model,
		Effort:      firstNonEmpty(metadataString(req.Metadata, "reasoning_effort"), openReq.ReasoningEffort),
		Tier:        firstNonEmpty(metadataString(req.Metadata, "service_tier"), openReq.ServiceTier),
		InputTokens: int64(estimateRequestTokens(openReq)),
	})
	resp, errDo := requireClient(req.HTTPClient).DoStream(ctx, pluginapi.HTTPRequest{
		Method:  http.MethodPost,
		URL:     chatURL,
		Headers: veniceHeaders(*storage, true),
		Body:    veniceBody,
	})
	if errDo != nil {
		span.Finish(monitor.Result{Success: false, Error: errDo.Error()})
		return pluginapi.ExecutorStreamResponse{}, errDo
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errStatus := fmt.Errorf("venice chat stream failed: status %d", resp.StatusCode)
		span.Finish(monitor.Result{Success: false, Error: errStatus.Error()})
		return pluginapi.ExecutorStreamResponse{}, errStatus
	}
	return pluginapi.ExecutorStreamResponse{
		Headers: http.Header{"Content-Type": []string{"text/event-stream"}},
		Chunks:  openAIStreamChunksWithMonitor(ctx, resp.Chunks, model, openReq, span),
	}, nil
}

func (e *Executor) CountTokens(context.Context, pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	return pluginapi.ExecutorResponse{
		Payload: []byte(`{"total_tokens":0}`),
		Headers: http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

func (e *Executor) HttpRequest(ctx context.Context, req pluginapi.ExecutorHTTPRequest) (pluginapi.ExecutorHTTPResponse, error) {
	storage, errParse := authpkg.ParseStorage(req.StorageJSON)
	if errParse != nil {
		return pluginapi.ExecutorHTTPResponse{}, errParse
	}
	if storage == nil {
		return pluginapi.ExecutorHTTPResponse{}, fmt.Errorf("venice auth storage is missing")
	}
	if errRefresh := authpkg.RefreshStorage(ctx, requireClient(req.HTTPClient), storage); errRefresh != nil {
		return pluginapi.ExecutorHTTPResponse{}, errRefresh
	}
	headers := req.Headers.Clone()
	headers.Set("Authorization", storage.Authorization)
	resp, errDo := requireClient(req.HTTPClient).Do(ctx, pluginapi.HTTPRequest{
		Method:  firstNonEmpty(req.Method, http.MethodPost),
		URL:     req.URL,
		Headers: headers,
		Body:    req.Body,
	})
	if errDo != nil {
		return pluginapi.ExecutorHTTPResponse{}, errDo
	}
	return pluginapi.ExecutorHTTPResponse{StatusCode: resp.StatusCode, Headers: resp.Headers, Body: resp.Body}, nil
}

func refreshedStorage(ctx context.Context, req pluginapi.ExecutorRequest) (*authpkg.Storage, error) {
	storage, errParse := authpkg.ParseStorage(req.StorageJSON)
	if errParse != nil {
		return nil, errParse
	}
	if storage == nil {
		return nil, fmt.Errorf("venice auth storage is missing")
	}
	if authpkg.ShouldRefreshStorage(*storage, time.Now()) {
		if errRefresh := authpkg.RefreshStorage(ctx, requireClient(req.HTTPClient), storage); errRefresh != nil {
			return nil, errRefresh
		}
	}
	return storage, nil
}

func buildVeniceRequest(req pluginapi.ExecutorRequest) (openAIRequest, []byte, string, error) {
	body := req.Payload
	if len(body) == 0 {
		body = req.OriginalRequest
	}
	var openReq openAIRequest
	if errDecode := json.Unmarshal(body, &openReq); errDecode != nil {
		return openAIRequest{}, nil, "", fmt.Errorf("decode OpenAI request: %w", errDecode)
	}
	if len(openReq.Messages) == 0 {
		return openAIRequest{}, nil, "", fmt.Errorf("messages must be an array")
	}
	model := firstNonEmpty(req.Model, openReq.Model, "zai-org-glm-5.2")
	veniceModel := toVeniceWebModelID(model)
	systemPrompt, prompt := splitMessages(openReq)
	temperature := openReq.Temperature
	if temperature == nil {
		temperature = 0.6
	}
	topP := openReq.TopP
	if topP == nil {
		topP = 0.95
	}
	payload := map[string]any{
		"clientProcessingTime":         1,
		"conversationType":             "text",
		"enableLargeContextChat":       true,
		"enableStructuredSystemPrompt": true,
		"includeVeniceSystemPrompt":    false,
		"isCharacter":                  false,
		"modelId":                      veniceModel,
		"prompt":                       prompt,
		"reasoning":                    len(openReq.Tools) == 0,
		"requestId":                    randomID(),
		"simpleMode":                   false,
		"systemPrompt":                 systemPrompt,
		"temperature":                  temperature,
		"topP":                         topP,
		"webEnabled":                   true,
		"webScrapeEnabled":             false,
		"xSearchEnabled":               false,
	}
	raw, errMarshal := json.Marshal(payload)
	return openReq, raw, veniceModel, errMarshal
}

func splitMessages(req openAIRequest) (string, []map[string]string) {
	systemParts := make([]string, 0)
	prompt := make([]map[string]string, 0, len(req.Messages))
	for _, message := range req.Messages {
		content := normalizeContent(message.Content)
		if message.Role == "system" {
			if content != "" {
				systemParts = append(systemParts, content)
			}
			continue
		}
		if message.Role == "tool" {
			content = formatToolResultMessage(message)
		}
		if len(message.ToolCalls) > 0 {
			content = strings.TrimSpace(content + "\n\n" + formatAssistantToolCalls(message.ToolCalls))
		}
		if content == "" {
			continue
		}
		role := message.Role
		if role == "tool" {
			role = "user"
		}
		prompt = append(prompt, map[string]string{"role": role, "content": content})
	}
	if instructions := toolInstructions(req); instructions != "" {
		systemParts = append(systemParts, instructions)
		prompt = appendToolInstructionsToLastUser(prompt, instructions)
	}
	return strings.Join(systemParts, "\n\n"), maybePrefixReasoning(prompt, len(req.Tools) == 0)
}

func appendToolInstructionsToLastUser(prompt []map[string]string, instructions string) []map[string]string {
	instructions = strings.TrimSpace(instructions)
	if instructions == "" {
		return prompt
	}
	for i := len(prompt) - 1; i >= 0; i-- {
		if prompt[i]["role"] == "user" {
			prompt[i]["content"] = strings.TrimSpace(prompt[i]["content"] + "\n\n" + instructions)
			return prompt
		}
	}
	return append(prompt, map[string]string{"role": "user", "content": instructions})
}

func maybePrefixReasoning(prompt []map[string]string, enabled bool) []map[string]string {
	if !enabled {
		return prompt
	}
	for i := len(prompt) - 1; i >= 0; i-- {
		if prompt[i]["role"] == "user" && !strings.HasPrefix(prompt[i]["content"], "/think ") {
			prompt[i]["content"] = "/think " + prompt[i]["content"]
			break
		}
	}
	return prompt
}

func normalizeContent(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				if text, okText := m["text"].(string); okText {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		return fmt.Sprint(v)
	}
}

func formatToolResultMessage(message openAIMessage) string {
	content := normalizeContent(message.Content)
	name := strings.TrimSpace(message.Name)
	if name != "" {
		name = " (" + name + ")"
	}
	id := strings.TrimSpace(message.ToolCallID)
	if id == "" {
		id = "unknown"
	}
	return fmt.Sprintf("Tool result for %s%s:\n%s", id, name, content)
}

func formatAssistantToolCalls(calls []openAIToolCall) string {
	raw, _ := json.Marshal(calls)
	return "Assistant requested these tool calls:\n" + string(raw)
}

func toolInstructions(req openAIRequest) string {
	if len(req.Tools) == 0 {
		return ""
	}
	toolsRaw, _ := json.Marshal(req.Tools)
	choiceRaw, _ := json.Marshal(req.ToolChoice)
	repairMode := ""
	if settingspkg.Get().ToolRepairEnabled {
		repairMode = "\nPlugin tool-call repair is enabled. If your draft response is a promise to act next, convert that pending action into the matching tool call JSON instead of sending the promise as text."
	}
	return strings.TrimSpace(fmt.Sprintf(`Tool calling is available.
If a tool is needed, respond with exactly one JSON object and no markdown, no prose, no thinking text, and no surrounding text:
{"tool_calls":[{"name":"tool_name","arguments":{}}]}
The "name" must exactly match one available tool name. The "arguments" value must be a JSON object matching that tool schema.
Do not answer in natural language when the next step requires a tool. Do not describe the tool; call it with JSON.
Never say you will inspect, check, search, read, grep, list, or explore something. If that is needed, call a tool in this response.
After tool results are provided in later messages, answer normally or request another tool with the same JSON format.
%s

Available tools:
%s

tool_choice:
%s`, repairMode, string(toolsRaw), string(choiceRaw)))
}

func aggregateOpenAIResponse(body []byte, model string, req openAIRequest) []byte {
	content := strings.Builder{}
	reasoning := strings.Builder{}
	upstreamID := "chatcmpl-" + randomID()
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		event, ok := parseVeniceLine(line)
		if !ok {
			continue
		}
		if event.CompletionID != "" {
			upstreamID = event.CompletionID
		}
		if !event.hasAssistantText() {
			continue
		}
		if event.isInitialProcessingStatus(content.Len() == 0 && reasoning.Len() == 0) {
			continue
		}
		content.WriteString(event.Content)
		reasoning.WriteString(event.ReasoningContent)
	}
	message := map[string]any{"role": "assistant", "content": content.String()}
	finishReason := "stop"
	if toolCalls, ok := parseToolCallsFromText(content.String(), reasoning.String()); ok && len(req.Tools) > 0 {
		message["content"] = nil
		message["tool_calls"] = toolCalls
		finishReason = "tool_calls"
	} else if reasoning.Len() > 0 {
		message["reasoning_content"] = reasoning.String()
	}
	promptTokens := estimateRequestTokens(req)
	completionTokens := estimateTokens(content.String()) + estimateTokens(reasoning.String())
	out := map[string]any{
		"id":      upstreamID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": finishReason,
		}},
		"usage": openAIUsage(promptTokens, completionTokens),
	}
	raw, _ := json.Marshal(out)
	return raw
}

func openAIStreamChunks(ctx context.Context, in <-chan pluginapi.HTTPStreamChunk, model string, req openAIRequest) <-chan pluginapi.ExecutorStreamChunk {
	return openAIStreamChunksWithMonitor(ctx, in, model, req, nil)
}

func openAIStreamChunksWithMonitor(ctx context.Context, in <-chan pluginapi.HTTPStreamChunk, model string, req openAIRequest, span *monitor.Span) <-chan pluginapi.ExecutorStreamChunk {
	out := make(chan pluginapi.ExecutorStreamChunk)
	go func() {
		defer close(out)
		streamID := "chatcmpl-" + randomID()
		created := time.Now().Unix()
		content := strings.Builder{}
		reasoning := strings.Builder{}
		emit := func(payload map[string]any) bool {
			raw, _ := json.Marshal(payload)
			select {
			case out <- pluginapi.ExecutorStreamChunk{Payload: raw}:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !emit(openAIStreamPayload(streamID, created, model, map[string]any{"role": "assistant"}, nil)) {
			return
		}
		for {
			select {
			case <-ctx.Done():
				if span != nil {
					span.Finish(monitor.Result{Success: false, Error: ctx.Err().Error()})
				}
				out <- pluginapi.ExecutorStreamChunk{Err: ctx.Err()}
				return
			case chunk, ok := <-in:
				if !ok {
					promptTokens := estimateRequestTokens(req)
					completionTokens := estimateTokens(content.String()) + estimateTokens(reasoning.String())
					if len(req.Tools) > 0 && !emitBufferedToolAwareStream(emit, streamID, created, model, req, content.String(), reasoning.String()) {
						return
					}
					if len(req.Tools) == 0 && !emit(openAIStreamPayload(streamID, created, model, map[string]any{}, "stop")) {
						return
					}
					if !emit(openAIStreamUsagePayload(streamID, created, model, openAIUsage(promptTokens, completionTokens))) {
						return
					}
					if span != nil {
						span.Finish(monitor.Result{Success: true, OutputTokens: int64(completionTokens), TotalTokens: int64(promptTokens + completionTokens)})
					}
					return
				}
				if chunk.Err != nil {
					if span != nil {
						span.Finish(monitor.Result{Success: false, Error: chunk.Err.Error()})
					}
					out <- pluginapi.ExecutorStreamChunk{Err: chunk.Err}
					continue
				}
				for _, line := range strings.Split(string(chunk.Payload), "\n") {
					event, ok := parseVeniceLine(line)
					if !ok {
						continue
					}
					if event.CompletionID != "" {
						streamID = event.CompletionID
					}
					if !event.hasAssistantText() {
						continue
					}
					if event.isInitialProcessingStatus(content.Len() == 0 && reasoning.Len() == 0) {
						continue
					}
					if len(req.Tools) > 0 {
						if event.Content != "" || event.ReasoningContent != "" {
							if span != nil {
								span.MarkTTFT()
							}
							content.WriteString(event.Content)
							reasoning.WriteString(event.ReasoningContent)
							if toolCalls, ok := parseToolCallsFromText(content.String(), reasoning.String()); ok {
								promptTokens := estimateRequestTokens(req)
								completionTokens := estimateTokens(content.String()) + estimateTokens(reasoning.String())
								if !emitToolCallStream(emit, streamID, created, model, toolCalls) {
									return
								}
								if !emit(openAIStreamUsagePayload(streamID, created, model, openAIUsage(promptTokens, completionTokens))) {
									return
								}
								if span != nil {
									span.Finish(monitor.Result{Success: true, OutputTokens: int64(completionTokens), TotalTokens: int64(promptTokens + completionTokens)})
								}
								return
							}
						}
						continue
					}
					delta := make(map[string]any)
					if event.Content != "" {
						if span != nil {
							span.MarkTTFT()
						}
						delta["content"] = event.Content
						content.WriteString(event.Content)
					}
					if event.ReasoningContent != "" {
						if span != nil {
							span.MarkTTFT()
						}
						delta["reasoning_content"] = event.ReasoningContent
						reasoning.WriteString(event.ReasoningContent)
					}
					if len(delta) == 0 {
						continue
					}
					if !emit(openAIStreamPayload(streamID, created, model, delta, nil)) {
						return
					}
				}
			}
		}
	}()
	return out
}

func emitBufferedToolAwareStream(emit func(map[string]any) bool, streamID string, created int64, model string, req openAIRequest, content, reasoning string) bool {
	if toolCalls, ok := parseToolCallsFromText(content, reasoning); ok && len(req.Tools) > 0 {
		return emitToolCallStream(emit, streamID, created, model, toolCalls)
	}
	delta := make(map[string]any)
	if content != "" {
		delta["content"] = content
	}
	if reasoning != "" {
		delta["reasoning_content"] = reasoning
	}
	if len(delta) > 0 {
		if !emit(openAIStreamPayload(streamID, created, model, delta, nil)) {
			return false
		}
	}
	return emit(openAIStreamPayload(streamID, created, model, map[string]any{}, "stop"))
}

func emitToolCallStream(emit func(map[string]any) bool, streamID string, created int64, model string, toolCalls []openAIToolCall) bool {
	for i, call := range toolCalls {
		delta := map[string]any{
			"tool_calls": []map[string]any{{
				"index": i,
				"id":    call.ID,
				"type":  firstNonEmpty(call.Type, "function"),
				"function": map[string]any{
					"name":      call.Function.Name,
					"arguments": call.Function.Arguments,
				},
			}},
		}
		if !emit(openAIStreamPayload(streamID, created, model, delta, nil)) {
			return false
		}
	}
	return emit(openAIStreamPayload(streamID, created, model, map[string]any{}, "tool_calls"))
}

func parseToolCallsFromText(content, reasoning string) ([]openAIToolCall, bool) {
	for _, text := range []string{content, reasoning, strings.TrimSpace(content + "\n" + reasoning)} {
		if calls, ok := parseToolCalls(text); ok {
			return calls, true
		}
	}
	return nil, false
}

func usageFromOpenAIResponse(payload []byte) monitor.Usage {
	var body struct {
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return monitor.Usage{}
	}
	return monitor.Usage{
		InputTokens:  body.Usage.PromptTokens,
		OutputTokens: body.Usage.CompletionTokens,
		TotalTokens:  body.Usage.TotalTokens,
	}
}

func metadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, ok := metadata[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func clientKeyLabel(req pluginapi.ExecutorRequest) string {
	for _, key := range []string{
		"alias", "key_alias", "keyAlias", "api_key_alias", "apiKeyAlias",
		"api-key-alias", "key-alias", "cpamp_key_alias", "cpampKeyAlias",
		"api_key_name", "apiKeyName", "key_name", "keyName",
		"api_key_label", "apiKeyLabel", "key_label", "keyLabel",
		"api_key_id", "apiKeyID", "key_id", "keyID",
		"client_key", "clientKey", "consumer", "consumer_name", "user",
	} {
		if value := metadataString(req.Metadata, key); value != "" {
			return value
		}
	}
	for _, key := range []string{
		"alias", "key_alias", "keyAlias", "api_key_alias", "apiKeyAlias",
		"api-key-alias", "key-alias", "cpamp_key_alias", "cpampKeyAlias",
		"api_key_name", "apiKeyName", "key_name", "keyName",
		"api_key_label", "apiKeyLabel", "key_label", "keyLabel",
		"api_key_id", "apiKeyID", "key_id", "keyID",
		"client_key", "clientKey", "consumer", "consumer_name", "user",
	} {
		if value := metadataString(req.AuthMetadata, key); value != "" {
			return value
		}
	}
	for _, key := range []string{"alias", "key_alias", "keyAlias", "api_key_alias", "apiKeyAlias", "api-key-alias", "key-alias", "api_key_name", "apiKeyName", "key_name", "keyName", "api_key_id", "key_id", "name"} {
		if value := strings.TrimSpace(req.AuthAttributes[key]); value != "" {
			return value
		}
	}
	for _, header := range []string{"X-API-Key-Alias", "X-Api-Key-Alias", "X-Key-Alias", "X-CPAMP-Key-Alias", "X-API-Key-Name", "X-Api-Key-Name", "X-API-Key-ID", "X-Api-Key-Id", "X-Consumer-Name", "X-Request-Key"} {
		if value := strings.TrimSpace(req.Headers.Get(header)); value != "" {
			return value
		}
	}
	if auth := strings.TrimSpace(req.Headers.Get("Authorization")); auth != "" {
		return auth
	}
	if key := strings.TrimSpace(req.Headers.Get("X-API-Key")); key != "" {
		return key
	}
	return ""
}

func clientKeyHash(req pluginapi.ExecutorRequest) string {
	for _, key := range []string{
		"apiKeyHash", "api_key_hash", "api-key-hash", "keyHash", "key_hash", "key-hash",
		"clientKeyHash", "client_key_hash", "cpampApiKeyHash", "cpamp_api_key_hash",
	} {
		if value := metadataString(req.Metadata, key); isSHA256Hex(value) {
			return strings.ToLower(value)
		}
	}
	for _, key := range []string{
		"apiKeyHash", "api_key_hash", "api-key-hash", "keyHash", "key_hash", "key-hash",
		"clientKeyHash", "client_key_hash", "cpampApiKeyHash", "cpamp_api_key_hash",
	} {
		if value := metadataString(req.AuthMetadata, key); isSHA256Hex(value) {
			return strings.ToLower(value)
		}
	}
	for _, key := range []string{"apiKeyHash", "api_key_hash", "api-key-hash", "keyHash", "key_hash", "key-hash"} {
		if value := strings.TrimSpace(req.AuthAttributes[key]); isSHA256Hex(value) {
			return strings.ToLower(value)
		}
	}
	for _, header := range []string{"X-API-Key-Hash", "X-Api-Key-Hash", "X-Key-Hash", "X-CPAMP-Key-Hash"} {
		if value := strings.TrimSpace(req.Headers.Get(header)); isSHA256Hex(value) {
			return strings.ToLower(value)
		}
	}
	if key := bearerToken(req.Headers.Get("Authorization")); key != "" {
		return sha256Hex(key)
	}
	if key := strings.TrimSpace(req.Headers.Get("X-API-Key")); key != "" {
		return sha256Hex(key)
	}
	return ""
}

func bearerToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[7:])
	}
	return ""
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])
}

func isSHA256Hex(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func openAIStreamUsagePayload(id string, created int64, model string, usage map[string]int) map[string]any {
	return map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []map[string]any{},
		"usage":   usage,
	}
}

func openAIStreamPayload(id string, created int64, model string, delta map[string]any, finishReason any) map[string]any {
	return map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []map[string]any{{
			"index":         0,
			"delta":         delta,
			"finish_reason": finishReason,
		}},
	}
}

func parseToolCalls(text string) ([]openAIToolCall, bool) {
	for _, candidate := range toolJSONCandidates(text) {
		if calls, ok := parseToolCallsJSON(candidate); ok {
			return calls, true
		}
	}
	return nil, false
}

func toolJSONCandidates(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	out := []string{text}
	if fenced := extractFencedJSON(text); fenced != "" {
		out = append(out, fenced)
	}
	if obj := extractBalancedJSON(text, '{', '}'); obj != "" && obj != text {
		out = append(out, obj)
	}
	if arr := extractBalancedJSON(text, '[', ']'); arr != "" && arr != text {
		out = append(out, arr)
	}
	return out
}

func extractFencedJSON(text string) string {
	start := strings.Index(text, "```")
	if start < 0 {
		return ""
	}
	rest := text[start+3:]
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		rest = rest[nl+1:]
	}
	end := strings.Index(rest, "```")
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:end])
}

func extractBalancedJSON(text string, open, close rune) string {
	start := strings.IndexRune(text, open)
	if start < 0 {
		return ""
	}
	depth := 0
	inString := false
	escaped := false
	for i, r := range text[start:] {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch r {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch r {
		case '"':
			inString = true
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return strings.TrimSpace(text[start : start+i+1])
			}
		}
	}
	return ""
}

func parseToolCallsJSON(raw string) ([]openAIToolCall, bool) {
	var value any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &value); err != nil {
		return nil, false
	}
	items := toolCallItems(value)
	if len(items) == 0 {
		return nil, false
	}
	calls := make([]openAIToolCall, 0, len(items))
	for i, item := range items {
		call, ok := normalizeToolCall(item, i)
		if !ok {
			return nil, false
		}
		calls = append(calls, call)
	}
	return calls, true
}

func toolCallItems(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case map[string]any:
		if raw, ok := typed["tool_calls"].([]any); ok {
			return raw
		}
		if raw, ok := typed["toolCalls"].([]any); ok {
			return raw
		}
		if _, hasName := typed["name"]; hasName {
			return []any{typed}
		}
		if _, hasFunction := typed["function"]; hasFunction {
			return []any{typed}
		}
	}
	return nil
}

func normalizeToolCall(value any, _ int) (openAIToolCall, bool) {
	item, ok := value.(map[string]any)
	if !ok {
		return openAIToolCall{}, false
	}
	id := firstNonEmpty(stringFromAny(item["id"]), "call_"+randomID())
	callType := firstNonEmpty(stringFromAny(item["type"]), "function")
	name := firstNonEmpty(stringFromAny(item["name"]), stringFromAny(item["tool"]))
	args := item["arguments"]
	if args == nil {
		args = item["args"]
	}
	if args == nil {
		args = item["parameters"]
	}
	if fn, ok := item["function"].(map[string]any); ok {
		name = firstNonEmpty(stringFromAny(fn["name"]), name)
		if fn["arguments"] != nil {
			args = fn["arguments"]
		}
		if args == nil {
			args = fn["parameters"]
		}
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return openAIToolCall{}, false
	}
	return openAIToolCall{
		ID:   id,
		Type: callType,
		Function: openAIFunctionCall{
			Name:      name,
			Arguments: argumentsString(args),
		},
	}, true
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return strings.TrimSpace(typed)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func argumentsString(value any) string {
	if value == nil {
		return "{}"
	}
	if text, ok := value.(string); ok {
		text = strings.TrimSpace(text)
		if text == "" {
			return "{}"
		}
		if json.Valid([]byte(text)) {
			return text
		}
		if repaired := repairJSONBackslashes(text); repaired != "" && json.Valid([]byte(repaired)) {
			return repaired
		}
		raw, _ := json.Marshal(map[string]string{"value": text})
		return string(raw)
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) == 0 || string(raw) == "null" {
		return "{}"
	}
	return string(raw)
}

func repairJSONBackslashes(text string) string {
	if !strings.HasPrefix(strings.TrimSpace(text), "{") && !strings.HasPrefix(strings.TrimSpace(text), "[") {
		return ""
	}
	var out strings.Builder
	out.Grow(len(text) + 8)
	for i := 0; i < len(text); {
		ch := text[i]
		if ch != '\\' {
			out.WriteByte(ch)
			i++
			continue
		}
		if i+1 >= len(text) {
			out.WriteString(`\\`)
			i++
			continue
		}
		next := text[i+1]
		if strings.ContainsRune(`"\/bfnrt`, rune(next)) {
			out.WriteByte('\\')
			out.WriteByte(next)
			i += 2
			continue
		}
		if next == 'u' && i+5 < len(text) && isHex4(text[i+2:i+6]) {
			out.WriteString(text[i : i+6])
			i += 6
			continue
		}
		out.WriteString(`\\`)
		i++
	}
	return out.String()
}

func isHex4(text string) bool {
	if len(text) != 4 {
		return false
	}
	for _, ch := range text {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
			return false
		}
	}
	return true
}

func openAIUsage(promptTokens, completionTokens int) map[string]int {
	if promptTokens < 0 {
		promptTokens = 0
	}
	if completionTokens < 0 {
		completionTokens = 0
	}
	return map[string]int{
		"prompt_tokens":     promptTokens,
		"completion_tokens": completionTokens,
		"total_tokens":      promptTokens + completionTokens,
	}
}

func estimateRequestTokens(req openAIRequest) int {
	total := estimateTokens(req.Model)
	for _, message := range req.Messages {
		total += estimateTokens(message.Role)
		total += estimateTokens(normalizeContent(message.Content))
	}
	return total
}

func estimateTokens(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	return max(1, (len([]rune(text))+3)/4)
}

func parseVeniceLine(line string) (veniceLine, bool) {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "data:")
	line = strings.TrimSpace(line)
	if line == "" || line == "[DONE]" {
		return veniceLine{}, false
	}
	var event veniceLine
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		return veniceLine{}, false
	}
	return event, true
}

func (line veniceLine) hasAssistantText() bool {
	kind := strings.ToLower(strings.TrimSpace(line.Kind))
	if kind != "" && kind != "content" {
		return false
	}
	return line.Content != "" || line.ReasoningContent != ""
}

func (line veniceLine) isInitialProcessingStatus(noAssistantTextYet bool) bool {
	return noAssistantTextYet &&
		line.ReasoningContent == "" &&
		strings.EqualFold(strings.TrimSpace(line.Content), "processing")
}

func veniceHeaders(storage authpkg.Storage, stream bool) http.Header {
	accept := "application/json"
	if stream {
		accept = "text/event-stream"
	}
	return http.Header{
		"Accept":                        []string{accept},
		"Content-Type":                  []string{"application/json"},
		"Authorization":                 []string{storage.Authorization},
		"Origin":                        []string{"https://venice.ai"},
		"Referer":                       []string{"https://venice.ai/"},
		"User-Agent":                    []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"},
		"X-Venice-Version":              []string{"interface@20260627.111727+571b7be"},
		"x-venice-locale":               []string{"en"},
		"x-venice-middleface-version":   []string{"0.1.828"},
		"x-venice-request-timestamp-ms": []string{fmt.Sprintf("%d", time.Now().UnixMilli())},
	}
}

func toVeniceWebModelID(model string) string {
	model = strings.TrimSpace(model)
	model = strings.Replace(model, "zai-org-glm-5-2", "zai-org-glm-5.2", 1)
	return model
}

func randomID() string {
	buf := make([]byte, 10)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func requireClient(client pluginapi.HostHTTPClient) pluginapi.HostHTTPClient {
	if client != nil {
		return client
	}
	return missingClient{}
}

type missingClient struct{}

func (missingClient) Do(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	return pluginapi.HTTPResponse{}, fmt.Errorf("host HTTP client is required")
}

func (missingClient) DoStream(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	return pluginapi.HTTPStreamResponse{}, fmt.Errorf("host HTTP client is required")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
