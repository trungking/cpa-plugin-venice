package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	authpkg "github.com/trungking/cpa-plugin-venice/internal/auth"
)

const chatURL = "https://outerface.venice.ai/api/inference/chat"

type Executor struct{}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	Stream      bool            `json:"stream"`
	Temperature any             `json:"temperature,omitempty"`
	TopP        any             `json:"top_p,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
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
	resp, errDo := requireClient(req.HTTPClient).Do(ctx, pluginapi.HTTPRequest{
		Method:  http.MethodPost,
		URL:     chatURL,
		Headers: veniceHeaders(*storage, false),
		Body:    veniceBody,
	})
	if errDo != nil {
		return pluginapi.ExecutorResponse{}, errDo
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return pluginapi.ExecutorResponse{}, fmt.Errorf("venice chat failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
	}
	payload := aggregateOpenAIResponse(resp.Body, model, openReq.Stream)
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
	_, veniceBody, model, errBuild := buildVeniceRequest(req)
	if errBuild != nil {
		return pluginapi.ExecutorStreamResponse{}, errBuild
	}
	resp, errDo := requireClient(req.HTTPClient).DoStream(ctx, pluginapi.HTTPRequest{
		Method:  http.MethodPost,
		URL:     chatURL,
		Headers: veniceHeaders(*storage, true),
		Body:    veniceBody,
	})
	if errDo != nil {
		return pluginapi.ExecutorStreamResponse{}, errDo
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return pluginapi.ExecutorStreamResponse{}, fmt.Errorf("venice chat stream failed: status %d", resp.StatusCode)
	}
	return pluginapi.ExecutorStreamResponse{
		Headers: http.Header{"Content-Type": []string{"text/event-stream"}},
		Chunks:  openAIStreamChunks(ctx, resp.Chunks, model),
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
	systemPrompt, prompt := splitMessages(openReq.Messages)
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
		"reasoning":                    true,
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

func splitMessages(messages []openAIMessage) (string, []map[string]string) {
	systemParts := make([]string, 0)
	prompt := make([]map[string]string, 0, len(messages))
	for _, message := range messages {
		content := normalizeContent(message.Content)
		if content == "" {
			continue
		}
		if message.Role == "system" {
			systemParts = append(systemParts, content)
			continue
		}
		prompt = append(prompt, map[string]string{"role": message.Role, "content": content})
	}
	return strings.Join(systemParts, "\n\n"), maybePrefixReasoning(prompt)
}

func maybePrefixReasoning(prompt []map[string]string) []map[string]string {
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

func aggregateOpenAIResponse(body []byte, model string, stream bool) []byte {
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
		content.WriteString(event.Content)
		reasoning.WriteString(event.ReasoningContent)
	}
	message := map[string]any{"role": "assistant", "content": content.String()}
	if reasoning.Len() > 0 {
		message["reasoning_content"] = reasoning.String()
	}
	out := map[string]any{
		"id":      upstreamID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": "stop",
		}},
		"usage": map[string]int{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
	}
	raw, _ := json.Marshal(out)
	return raw
}

func openAIStreamChunks(ctx context.Context, in <-chan pluginapi.HTTPStreamChunk, model string) <-chan pluginapi.ExecutorStreamChunk {
	out := make(chan pluginapi.ExecutorStreamChunk)
	go func() {
		defer close(out)
		streamID := "chatcmpl-" + randomID()
		created := time.Now().Unix()
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
				out <- pluginapi.ExecutorStreamChunk{Err: ctx.Err()}
				return
			case chunk, ok := <-in:
				if !ok {
					if !emit(openAIStreamPayload(streamID, created, model, map[string]any{}, "stop")) {
						return
					}
					out <- pluginapi.ExecutorStreamChunk{Payload: []byte("[DONE]")}
					return
				}
				if chunk.Err != nil {
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
					delta := make(map[string]any)
					if event.Content != "" {
						delta["content"] = event.Content
					}
					if event.ReasoningContent != "" {
						delta["reasoning_content"] = event.ReasoningContent
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
