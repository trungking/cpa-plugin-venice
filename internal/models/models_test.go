package models

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestModelsForAuthFetchesCatalogIDsOnly(t *testing.T) {
	resp, err := NewProvider().ModelsForAuth(context.Background(), pluginapi.AuthModelRequest{
		HTTPClient: fakeCatalogClient{},
	})
	if err != nil {
		t.Fatalf("ModelsForAuth error: %v", err)
	}
	if !hasModel(resp.Models, "gemini-3-5-flash") {
		t.Fatalf("catalog model missing: %#v", resp.Models)
	}
	if hasModel(resp.Models, "gemini-3.5-flash") {
		t.Fatalf("unexpected non-catalog alias present: %#v", resp.Models)
	}
}

type fakeCatalogClient struct{}

func (fakeCatalogClient) Do(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	return pluginapi.HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"data":[{"id":"gemini-3-5-flash","owned_by":"venice.ai","context_length":1000000},{"id":"zai-org-glm-5-2","owned_by":"venice.ai"}]}`),
	}, nil
}

func (fakeCatalogClient) DoStream(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	return pluginapi.HTTPStreamResponse{}, nil
}

func hasModel(models []pluginapi.ModelInfo, id string) bool {
	for _, model := range models {
		if model.ID == id {
			return true
		}
	}
	return false
}
