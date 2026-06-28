package executor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestBuildVeniceRequestFromOpenAIChat(t *testing.T) {
	req := pluginapi.ExecutorRequest{
		Model: "zai-org-glm-5-2",
		Payload: []byte(`{
			"model":"zai-org-glm-5-2",
			"messages":[
				{"role":"system","content":"Be terse."},
				{"role":"user","content":"Say hi"}
			],
			"temperature":0.2,
			"top_p":0.8
		}`),
	}
	_, raw, model, err := buildVeniceRequest(req)
	if err != nil {
		t.Fatalf("buildVeniceRequest error: %v", err)
	}
	if model != "zai-org-glm-5.2" {
		t.Fatalf("model = %q", model)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["systemPrompt"] != "Be terse." {
		t.Fatalf("systemPrompt = %#v", body["systemPrompt"])
	}
	prompt := body["prompt"].([]any)
	last := prompt[len(prompt)-1].(map[string]any)
	if last["content"] != "/think Say hi" {
		t.Fatalf("last content = %#v", last["content"])
	}
	if body["includeVeniceSystemPrompt"] != false {
		t.Fatalf("includeVeniceSystemPrompt = %#v", body["includeVeniceSystemPrompt"])
	}
}

func TestBuildVeniceRequestAddsToolInstructions(t *testing.T) {
	req := pluginapi.ExecutorRequest{
		Model: "zai-org-glm-5-2",
		Payload: []byte(`{
			"model":"zai-org-glm-5-2",
			"messages":[
				{"role":"system","content":"You are an agent."},
				{"role":"user","content":"Inspect files"}
			],
			"tools":[{
				"type":"function",
				"function":{
					"name":"list_files",
					"description":"List files",
					"parameters":{"type":"object","properties":{"path":{"type":"string"}}}
				}
			}],
			"tool_choice":"auto"
		}`),
	}
	_, raw, _, err := buildVeniceRequest(req)
	if err != nil {
		t.Fatalf("buildVeniceRequest error: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	systemPrompt := body["systemPrompt"].(string)
	if !strings.Contains(systemPrompt, "Tool calling is available") || !strings.Contains(systemPrompt, "list_files") {
		t.Fatalf("systemPrompt missing tool instructions: %s", systemPrompt)
	}
	prompt := body["prompt"].([]any)
	last := prompt[len(prompt)-1].(map[string]any)
	if strings.HasPrefix(last["content"].(string), "/think ") {
		t.Fatalf("tool-enabled request should not force /think: %#v", last["content"])
	}
}

func TestOpenAIStreamChunksConvertsVeniceStream(t *testing.T) {
	in := make(chan pluginapi.HTTPStreamChunk, 1)
	in <- pluginapi.HTTPStreamChunk{Payload: []byte(
		`data: {"kind":"meta","completion_id":"upstream-id"}` + "\n" +
			`{"kind":"status","content":"processing"}` + "\n" +
			`{"kind":"content","content":"processing"}` + "\n" +
			`{"kind":"content","reasoning_content":"thinking"}` + "\n" +
			`{"kind":"content","content":"hel"}` + "\n" +
			`data: {"kind":"content","content":"lo"}` + "\n",
	)}
	close(in)

	var frames []string
	req := openAIRequest{Model: "zai-org-glm-5.2", Messages: []openAIMessage{{Role: "user", Content: "hello"}}}
	for chunk := range openAIStreamChunks(context.Background(), in, "zai-org-glm-5.2", req) {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if strings.HasPrefix(string(chunk.Payload), "data:") {
			t.Fatalf("plugin stream chunk should not be SSE-framed before host writes it: %q", string(chunk.Payload))
		}
		frames = append(frames, string(chunk.Payload))
	}
	joined := strings.Join(frames, "")
	if !strings.Contains(joined, `"object":"chat.completion.chunk"`) {
		t.Fatalf("stream did not contain OpenAI chunks: %s", joined)
	}
	if !strings.Contains(joined, `"reasoning_content":"thinking"`) {
		t.Fatalf("stream did not contain reasoning delta: %s", joined)
	}
	if !strings.Contains(joined, `"content":"hel"`) || !strings.Contains(joined, `"content":"lo"`) {
		t.Fatalf("stream did not contain content deltas: %s", joined)
	}
	if strings.Contains(joined, "processing") {
		t.Fatalf("stream leaked Venice status text: %s", joined)
	}
	if !strings.Contains(joined, `"usage"`) || !strings.Contains(joined, `"total_tokens"`) {
		t.Fatalf("stream did not contain usage chunk: %s", joined)
	}
	if strings.Contains(joined, `[DONE]`) {
		t.Fatalf("plugin stream should leave the terminal DONE marker to the host: %s", joined)
	}
}

func TestOpenAIStreamChunksConvertsToolCall(t *testing.T) {
	in := make(chan pluginapi.HTTPStreamChunk, 1)
	in <- pluginapi.HTTPStreamChunk{Payload: []byte(
		`data: {"kind":"content","content":"{\"tool_calls\":[{\"name\":\"list_files\",\"arguments\":{\"path\":\".\"}}]}"}` + "\n",
	)}
	close(in)

	var frames []string
	req := openAIRequest{
		Model:    "zai-org-glm-5.2",
		Messages: []openAIMessage{{Role: "user", Content: "inspect"}},
		Tools: []json.RawMessage{
			json.RawMessage(`{"type":"function","function":{"name":"list_files"}}`),
		},
	}
	for chunk := range openAIStreamChunks(context.Background(), in, "zai-org-glm-5.2", req) {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		frames = append(frames, string(chunk.Payload))
	}
	joined := strings.Join(frames, "")
	if !strings.Contains(joined, `"tool_calls"`) || !strings.Contains(joined, `"name":"list_files"`) {
		t.Fatalf("stream did not contain tool call delta: %s", joined)
	}
	if !strings.Contains(joined, `"finish_reason":"tool_calls"`) {
		t.Fatalf("stream did not finish with tool_calls: %s", joined)
	}
	if strings.Contains(joined, `"content":"{\\"tool_calls\\"`) {
		t.Fatalf("stream leaked tool JSON as content: %s", joined)
	}
}

func TestAggregateOpenAIResponse(t *testing.T) {
	raw := []byte(`{"kind":"meta","completion_id":"upstream-id"}` + "\n" +
		`{"kind":"status","content":"processing"}` + "\n" +
		`{"kind":"content","content":"processing"}` + "\n" +
		`{"kind":"content","content":"hel","reasoning_content":"r1"}` + "\n" +
		`{"kind":"content","content":"lo","reasoning_content":"r2"}` + "\n")
	req := openAIRequest{Model: "zai-org-glm-5.2", Messages: []openAIMessage{{Role: "user", Content: "hello"}}}
	out := aggregateOpenAIResponse(raw, "zai-org-glm-5.2", req)
	var body map[string]any
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if body["id"] != "upstream-id" {
		t.Fatalf("id = %#v", body["id"])
	}
	choice := body["choices"].([]any)[0].(map[string]any)
	message := choice["message"].(map[string]any)
	if message["content"] != "hello" {
		t.Fatalf("content = %#v", message["content"])
	}
	if strings.Contains(message["content"].(string), "processing") {
		t.Fatalf("content leaked status text: %#v", message["content"])
	}
	if message["reasoning_content"] != "r1r2" {
		t.Fatalf("reasoning = %#v", message["reasoning_content"])
	}
	usage := body["usage"].(map[string]any)
	if usage["total_tokens"].(float64) <= 0 {
		t.Fatalf("usage = %#v, want estimated tokens", usage)
	}
}

func TestAggregateOpenAIResponseWithToolCall(t *testing.T) {
	raw := []byte(`{"kind":"content","content":"{\"tool_calls\":[{\"name\":\"read_file\",\"arguments\":{\"path\":\"README.md\"}}]}"}` + "\n")
	req := openAIRequest{
		Model:    "zai-org-glm-5.2",
		Messages: []openAIMessage{{Role: "user", Content: "read readme"}},
		Tools: []json.RawMessage{
			json.RawMessage(`{"type":"function","function":{"name":"read_file"}}`),
		},
	}
	out := aggregateOpenAIResponse(raw, "zai-org-glm-5.2", req)
	var body map[string]any
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	choice := body["choices"].([]any)[0].(map[string]any)
	if choice["finish_reason"] != "tool_calls" {
		t.Fatalf("finish_reason = %#v", choice["finish_reason"])
	}
	message := choice["message"].(map[string]any)
	calls := message["tool_calls"].([]any)
	call := calls[0].(map[string]any)
	fn := call["function"].(map[string]any)
	if fn["name"] != "read_file" || !strings.Contains(fn["arguments"].(string), "README.md") {
		t.Fatalf("tool call = %#v", call)
	}
	if message["content"] != nil {
		t.Fatalf("content = %#v, want nil for tool call", message["content"])
	}
}

func TestClientKeyLabelPrefersAlias(t *testing.T) {
	req := pluginapi.ExecutorRequest{
		Metadata: map[string]any{
			"api_key_id": "raw-key-id",
			"alias":      "team-build",
		},
		Headers: map[string][]string{
			"Authorization": {"Bearer secret-token"},
		},
	}
	if got := clientKeyLabel(req); got != "team-build" {
		t.Fatalf("clientKeyLabel = %q", got)
	}
	if got := clientKeyHash(req); got != sha256Hex("secret-token") {
		t.Fatalf("clientKeyHash = %q", got)
	}
}
