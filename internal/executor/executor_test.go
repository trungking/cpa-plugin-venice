package executor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	settingspkg "github.com/trungking/cpa-plugin-venice/internal/settings"
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
	if body["reasoning"] != false {
		t.Fatalf("tool-enabled request should disable Venice reasoning: %#v", body["reasoning"])
	}
	prompt := body["prompt"].([]any)
	last := prompt[len(prompt)-1].(map[string]any)
	if strings.HasPrefix(last["content"].(string), "/think ") {
		t.Fatalf("tool-enabled request should not force /think: %#v", last["content"])
	}
	if !strings.Contains(last["content"].(string), "no thinking text") || !strings.Contains(last["content"].(string), "list_files") {
		t.Fatalf("last user prompt missing tool instructions: %#v", last["content"])
	}
}

func TestToolRepairSettingAddsStrictInstruction(t *testing.T) {
	settingspkg.Set(settingspkg.Config{ToolRepairEnabled: true})
	settingspkg.ResetStatsForTest()
	t.Cleanup(func() {
		settingspkg.Set(settingspkg.Config{})
		settingspkg.ResetStatsForTest()
	})
	req := pluginapi.ExecutorRequest{
		Model: "zai-org-glm-5-2",
		Payload: []byte(`{
			"model":"zai-org-glm-5-2",
			"messages":[{"role":"user","content":"Inspect files"}],
			"tools":[{"type":"function","function":{"name":"list_files"}}],
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
	if !strings.Contains(systemPrompt, "Plugin tool-call repair is enabled") {
		t.Fatalf("systemPrompt missing repair mode: %s", systemPrompt)
	}
	if got := settingspkg.SnapshotStats().ToolRepairApplied; got != 1 {
		t.Fatalf("ToolRepairApplied = %d", got)
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
	settingspkg.ResetStatsForTest()
	t.Cleanup(settingspkg.ResetStatsForTest)
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
	stats := settingspkg.SnapshotStats()
	if stats.ToolCallConversions != 1 || stats.ToolCallsEmitted != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestOpenAIStreamChunksFlushesToolCallBeforeUpstreamClose(t *testing.T) {
	in := make(chan pluginapi.HTTPStreamChunk, 1)
	in <- pluginapi.HTTPStreamChunk{Payload: []byte(
		`data: {"kind":"content","content":"{\"tool_calls\":[{\"name\":\"list_files\",\"arguments\":{\"path\":\".\"}}]}"}` + "\n",
	)}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := openAIRequest{
		Model:    "zai-org-glm-5.2",
		Messages: []openAIMessage{{Role: "user", Content: "inspect"}},
		Tools: []json.RawMessage{
			json.RawMessage(`{"type":"function","function":{"name":"list_files"}}`),
		},
	}
	out := openAIStreamChunks(ctx, in, "zai-org-glm-5.2", req)
	var joined strings.Builder
	for chunk := range out {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		joined.Write(chunk.Payload)
	}
	if !strings.Contains(joined.String(), `"finish_reason":"tool_calls"`) {
		t.Fatalf("stream did not flush tool call before upstream close: %s", joined.String())
	}
}

func TestOpenAIStreamChunksConvertsToolCallFromReasoning(t *testing.T) {
	in := make(chan pluginapi.HTTPStreamChunk, 1)
	in <- pluginapi.HTTPStreamChunk{Payload: []byte(
		`data: {"kind":"content","reasoning_content":"{\"tool_calls\":[{\"name\":\"list_files\",\"parameters\":{\"path\":\".\"}}]}"}` + "\n",
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
		t.Fatalf("stream did not contain reasoning tool call delta: %s", joined)
	}
	if strings.Contains(joined, `"reasoning_content"`) {
		t.Fatalf("stream leaked reasoning tool JSON: %s", joined)
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

func TestArgumentsStringRepairsWindowsPathJSON(t *testing.T) {
	raw := `{"filePath":"C:\Users\vhctr\Documents\Codex\go.mod"}`
	repaired := argumentsString(raw)
	var decoded map[string]string
	if err := json.Unmarshal([]byte(repaired), &decoded); err != nil {
		t.Fatalf("repaired arguments are invalid JSON: %s: %v", repaired, err)
	}
	if decoded["filePath"] != `C:\Users\vhctr\Documents\Codex\go.mod` {
		t.Fatalf("filePath = %q", decoded["filePath"])
	}
	if _, hasValue := decoded["value"]; hasValue {
		t.Fatalf("arguments were wrapped as value: %s", repaired)
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
