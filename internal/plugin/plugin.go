package plugin

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	authpkg "github.com/trungking/cpa-plugin-venice/internal/auth"
	"github.com/trungking/cpa-plugin-venice/internal/executor"
	"github.com/trungking/cpa-plugin-venice/internal/management"
	"github.com/trungking/cpa-plugin-venice/internal/models"
)

const Provider = "venice"
const executorFormat = "openai"

type VenicePlugin struct {
	auth     *authpkg.Provider
	models   *models.Provider
	executor *executor.Executor
	mgmt     *management.Provider
}

func New() *VenicePlugin {
	return &VenicePlugin{
		auth:     authpkg.NewProvider(),
		models:   models.NewProvider(),
		executor: executor.NewExecutor(),
		mgmt:     management.New(),
	}
}

func Build(configYAML []byte) pluginapi.Plugin {
	p := New()
	return pluginapi.Plugin{
		Metadata: pluginapi.Metadata{
			Name:             "Venice Provider",
			Version:          "1.0.5",
			Author:           "router-for-me",
			GitHubRepository: "https://github.com/trungking/cpa-plugin-venice",
		},
		Capabilities: pluginapi.Capabilities{
			AuthProvider:          p,
			ModelProvider:         p,
			Executor:              p,
			ExecutorModelScope:    pluginapi.ExecutorModelScopeOAuth,
			ExecutorInputFormats:  []string{executorFormat},
			ExecutorOutputFormats: []string{executorFormat},
			CommandLinePlugin:     p,
			ManagementAPI:         p,
		},
	}
}

func (p *VenicePlugin) Identifier() string { return Provider }

func (p *VenicePlugin) ParseAuth(ctx context.Context, req pluginapi.AuthParseRequest) (pluginapi.AuthParseResponse, error) {
	return p.auth.ParseAuth(ctx, req)
}

func (p *VenicePlugin) StartLogin(ctx context.Context, req pluginapi.AuthLoginStartRequest) (pluginapi.AuthLoginStartResponse, error) {
	return p.auth.StartLogin(ctx, req)
}

func (p *VenicePlugin) PollLogin(ctx context.Context, req pluginapi.AuthLoginPollRequest) (pluginapi.AuthLoginPollResponse, error) {
	return p.auth.PollLogin(ctx, req)
}

func (p *VenicePlugin) RefreshAuth(ctx context.Context, req pluginapi.AuthRefreshRequest) (pluginapi.AuthRefreshResponse, error) {
	return p.auth.RefreshAuth(ctx, req)
}

func (p *VenicePlugin) StaticModels(ctx context.Context, req pluginapi.StaticModelRequest) (pluginapi.ModelResponse, error) {
	return p.models.StaticModels(ctx, req)
}

func (p *VenicePlugin) ModelsForAuth(ctx context.Context, req pluginapi.AuthModelRequest) (pluginapi.ModelResponse, error) {
	return p.models.ModelsForAuth(ctx, req)
}

func (p *VenicePlugin) TranslateRequest(ctx context.Context, req pluginapi.RequestTransformRequest) (pluginapi.PayloadResponse, error) {
	return pluginapi.PayloadResponse{Body: req.Body}, nil
}

func (p *VenicePlugin) TranslateResponse(ctx context.Context, req pluginapi.ResponseTransformRequest) (pluginapi.PayloadResponse, error) {
	return pluginapi.PayloadResponse{Body: req.Body}, nil
}

func (p *VenicePlugin) ApplyThinking(ctx context.Context, req pluginapi.ThinkingApplyRequest) (pluginapi.PayloadResponse, error) {
	return pluginapi.PayloadResponse{Body: req.Body}, nil
}

func (p *VenicePlugin) Execute(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	return p.executor.Execute(ctx, req)
}

func (p *VenicePlugin) ExecuteStream(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
	return p.executor.ExecuteStream(ctx, req)
}

func (p *VenicePlugin) CountTokens(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	return p.executor.CountTokens(ctx, req)
}

func (p *VenicePlugin) HttpRequest(ctx context.Context, req pluginapi.ExecutorHTTPRequest) (pluginapi.ExecutorHTTPResponse, error) {
	return p.executor.HttpRequest(ctx, req)
}

func (p *VenicePlugin) RegisterCommandLine(ctx context.Context, req pluginapi.CommandLineRegistrationRequest) (pluginapi.CommandLineRegistrationResponse, error) {
	return p.auth.RegisterCommandLine(ctx, req)
}

func (p *VenicePlugin) ExecuteCommandLine(ctx context.Context, req pluginapi.CommandLineExecutionRequest) (pluginapi.CommandLineExecutionResponse, error) {
	return p.auth.ExecuteCommandLine(ctx, req)
}

func (p *VenicePlugin) RegisterManagement(ctx context.Context, req pluginapi.ManagementRegistrationRequest) (pluginapi.ManagementRegistrationResponse, error) {
	return p.mgmt.RegisterManagement(ctx, req)
}

func (p *VenicePlugin) HandleManagement(ctx context.Context, req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	return p.mgmt.HandleManagement(ctx, req)
}

func (p *VenicePlugin) HandleManagementWithHost(ctx context.Context, req pluginapi.ManagementRequest, host management.HostClient) (pluginapi.ManagementResponse, error) {
	return p.mgmt.HandleManagementWithHost(ctx, req, host)
}

var _ pluginapi.AuthProvider = (*VenicePlugin)(nil)
var _ pluginapi.ModelProvider = (*VenicePlugin)(nil)
var _ pluginapi.ProviderExecutor = (*VenicePlugin)(nil)
var _ pluginapi.CommandLinePlugin = (*VenicePlugin)(nil)
var _ pluginapi.ManagementAPI = (*VenicePlugin)(nil)
var _ pluginapi.ManagementHandler = (*VenicePlugin)(nil)
