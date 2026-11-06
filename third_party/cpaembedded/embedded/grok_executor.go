package embedded

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	internalexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const grokCPAProvider = "xai"

type GrokExecutionError struct {
	status     int
	code       string
	summary    string
	retryAfter time.Duration
}

func (err *GrokExecutionError) Error() string {
	if err == nil || err.summary == "" {
		return "Grok upstream request failed"
	}
	return err.summary
}

func (err *GrokExecutionError) StatusCode() int {
	if err == nil {
		return 0
	}
	return err.status
}

func (err *GrokExecutionError) ErrorCode() string {
	if err == nil {
		return ""
	}
	return err.code
}

func (err *GrokExecutionError) RetryAfter() *time.Duration {
	if err == nil || err.retryAfter <= 0 {
		return nil
	}
	value := err.retryAfter
	return &value
}

type GrokHTTPExecutor interface {
	ExecuteCanonical(context.Context, string, GrokCredential, ExecuteRequest) (ExecuteResponse, error)
	CountTokensCanonical(context.Context, ExecuteRequest) (ExecuteResponse, error)
	ExecuteStreamCanonical(context.Context, string, GrokCredential, ExecuteRequest) (*ExecuteStreamResponse, error)
}

type grokHTTPExecutor struct {
	cfg     *internalconfig.Config
	inner   *internalexecutor.XAIExecutor
	baseURL string
}

func NewGrokHTTPExecutor() GrokHTTPExecutor {
	cfg := &internalconfig.Config{}
	return &grokHTTPExecutor{cfg: cfg, inner: internalexecutor.NewXAIExecutor(cfg)}
}

func (executor *grokHTTPExecutor) executionBaseURL(apiRoot string) (string, error) {
	endpoints, err := ResolveGrokAPIEndpoints(apiRoot)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(apiRoot) == "" && executor.baseURL != "" {
		return executor.baseURL, nil
	}
	return endpoints.ExecutionBase, nil
}

func NewGrokAuth(id string, credential GrokCredential, baseURL string) *cliproxyauth.Auth {
	metadata := map[string]any{
		"type": ProviderGrok, "access_token": credential.AccessToken,
		"auth_kind": "oauth", "using_api": false,
	}
	attributes := map[string]string{
		"auth_kind": "oauth", "using_api": "false",
		// CPA 默认仅向官方域附加这些头，API 代理仍需保持相同订阅协议。
		"header:X-XAI-Token-Auth":         "xai-grok-cli",
		"header:x-grok-client-version":    GrokClientVersion,
		"header:User-Agent":               "xai-grok-workspace/" + GrokClientVersion,
		"header:x-grok-client-identifier": "grok-shell",
		"header:x-authenticateresponse":   "authenticate-response",
	}
	if value := strings.TrimSpace(baseURL); value != "" {
		attributes["base_url"] = value
	}
	return &cliproxyauth.Auth{
		ID: strings.TrimSpace(id), Provider: grokCPAProvider, Attributes: attributes, Metadata: metadata,
	}
}

func (executor *grokHTTPExecutor) ExecuteCanonical(
	ctx context.Context,
	credentialID string,
	credential GrokCredential,
	request ExecuteRequest,
) (ExecuteResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateGrokCredential(credential); err != nil {
		return ExecuteResponse{}, err
	}
	baseURL, err := executor.executionBaseURL(request.BaseURL)
	if err != nil {
		return ExecuteResponse{}, err
	}
	request = scopeGrokExecutionRequest(request, baseURL)
	format := sdktranslator.FromString(request.Format)
	auth := NewGrokAuth(credentialID, credential, baseURL)
	auth.ProxyURL = request.ProxyURL
	observation := newProviderExecutionObservation(request, grokCPAProvider)
	executionCtx := executor.executionContext(ctx, auth, observation)
	response, err := executor.inner.Execute(executionCtx, authWithoutProxyURL(auth), cliproxyexecutor.Request{
		Model: request.Model, Payload: append([]byte(nil), request.Payload...), Format: format,
	}, grokExecutorOptions(request, format, false))
	if err != nil {
		return ExecuteResponse{Headers: observation.responseHeaders(), AppliedReasoningEffort: observation.reasoningEffort(),
			AppliedReasoningMode:         observation.reasoningMode(),
			AppliedReasoningBudgetTokens: observation.reasoningBudgetTokens()}, normalizeGrokExecutionError(err)
	}
	return ExecuteResponse{
		Payload: append([]byte(nil), response.Payload...), Headers: response.Headers.Clone(),
		AppliedReasoningEffort:       observation.reasoningEffort(),
		AppliedReasoningMode:         observation.reasoningMode(),
		AppliedReasoningBudgetTokens: observation.reasoningBudgetTokens(),
	}, nil
}

func (executor *grokHTTPExecutor) CountTokensCanonical(
	ctx context.Context,
	request ExecuteRequest,
) (ExecuteResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	baseURL, err := executor.executionBaseURL(request.BaseURL)
	if err != nil {
		return ExecuteResponse{}, err
	}
	request = prepareGrokExecutionRequest(request)
	request.ContinuityKey = targetContinuityScope(request.ContinuityKey, baseURL)
	format := sdktranslator.FromString(request.Format)
	response, err := executor.inner.CountTokens(ctx, NewGrokAuth("local-token-count", GrokCredential{}, baseURL), cliproxyexecutor.Request{
		Model: request.Model, Payload: append([]byte(nil), request.Payload...), Format: format,
	}, grokExecutorOptions(request, format, false))
	if err != nil {
		return ExecuteResponse{}, normalizeGrokExecutionError(err)
	}
	payload := append([]byte(nil), response.Payload...)
	if format == sdktranslator.FormatOpenAIResponse {
		payload, err = normalizeCodexResponsesTokenCount(payload)
		if err != nil {
			return ExecuteResponse{}, err
		}
	}
	return ExecuteResponse{Payload: payload, Headers: response.Headers.Clone()}, nil
}

func (executor *grokHTTPExecutor) ExecuteStreamCanonical(
	ctx context.Context,
	credentialID string,
	credential GrokCredential,
	request ExecuteRequest,
) (*ExecuteStreamResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateGrokCredential(credential); err != nil {
		return nil, err
	}
	baseURL, err := executor.executionBaseURL(request.BaseURL)
	if err != nil {
		return nil, err
	}
	request = scopeGrokExecutionRequest(request, baseURL)
	format := sdktranslator.FromString(request.Format)
	auth := NewGrokAuth(credentialID, credential, baseURL)
	auth.ProxyURL = request.ProxyURL
	observation := newProviderExecutionObservation(request, grokCPAProvider)
	executionCtx := executor.executionContext(ctx, auth, observation)
	response, err := executor.inner.ExecuteStream(executionCtx, authWithoutProxyURL(auth), cliproxyexecutor.Request{
		Model: request.Model, Payload: append([]byte(nil), request.Payload...), Format: format,
	}, grokExecutorOptions(request, format, true))
	if err != nil {
		return &ExecuteStreamResponse{Headers: observation.responseHeaders(), AppliedReasoningEffort: observation.reasoningEffort(),
			AppliedReasoningMode:         observation.reasoningMode(),
			AppliedReasoningBudgetTokens: observation.reasoningBudgetTokens()}, normalizeGrokExecutionError(err)
	}
	chunks := make(chan ExecuteStreamChunk)
	go func() {
		defer close(chunks)
		for chunk := range response.Chunks {
			converted := ExecuteStreamChunk{Payload: append([]byte(nil), chunk.Payload...)}
			if chunk.Err != nil {
				converted.Err = normalizeGrokExecutionError(chunk.Err)
			}
			select {
			case chunks <- converted:
			case <-ctx.Done():
				return
			}
		}
	}()
	return &ExecuteStreamResponse{
		Headers: response.Headers.Clone(), Chunks: chunks,
		AppliedReasoningEffort:       observation.reasoningEffort(),
		AppliedReasoningMode:         observation.reasoningMode(),
		AppliedReasoningBudgetTokens: observation.reasoningBudgetTokens(),
	}, nil
}

func grokExecutorOptions(request ExecuteRequest, format sdktranslator.Format, stream bool) cliproxyexecutor.Options {
	options := cliproxyexecutor.Options{
		Stream: stream, Headers: request.Headers.Clone(),
		OriginalRequest: append([]byte(nil), request.OriginalRequest...),
		SourceFormat:    format, ResponseFormat: format,
	}
	// Responses/Chat 把网关 UUID 写在请求体 prompt_cache_key 上，CPA 用它同时填 x-grok-conv-id。
	// 不要放进 ExecutionSessionMetadataKey：那会被当成可信的 reasoning replay 会话，
	// 下一轮把推理块插到用户输入前面，提示词缓存就只剩固定前缀。
	if scope := strings.TrimSpace(request.ContinuityKey); scope != "" && !grokCarriesPromptCacheKey(string(format)) && !grokIsMediaFormat(string(format)) {
		options.Metadata = map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: grokConversationID(scope),
		}
	}
	if requestPath := strings.TrimSpace(request.RequestPath); requestPath != "" {
		if options.Metadata == nil {
			options.Metadata = make(map[string]any, 1)
		}
		options.Metadata[cliproxyexecutor.RequestPathMetadataKey] = requestPath
	}
	return options
}

func grokIsMediaFormat(format string) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "openai-image", "openai-video":
		return true
	default:
		return false
	}
}

func grokConversationID(scope string) string {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return ""
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("gpt-load-grok\x00"+scope)).String()
}

// scopeGrokExecutionRequest 先丢掉调用方会话字段，再把网关自己的缓存键写回 Responses/Chat 请求体。
// cli-chat-proxy 只按 prompt_cache_key 复用提示词缓存，不认 x-grok-conv-id。
func scopeGrokExecutionRequest(request ExecuteRequest, baseURL string) ExecuteRequest {
	request = prepareGrokExecutionRequest(request)
	if strings.EqualFold(strings.TrimSpace(request.Format), "openai-image") {
		request = normalizeGrokImageExecutionRequest(request)
	}
	request.ContinuityKey = targetContinuityScope(request.ContinuityKey, baseURL)
	return applyGrokPromptCacheKey(request)
}

func normalizeGrokImageExecutionRequest(request ExecuteRequest) ExecuteRequest {
	contentType := ""
	if request.Headers != nil {
		contentType = request.Headers.Get("Content-Type")
	}
	mediaType, params, _ := mime.ParseMediaType(contentType)
	if strings.EqualFold(mediaType, "multipart/form-data") {
		if converted, err := convertGrokMultipartImageToJSON(request.Payload, params["boundary"], request.Model); err == nil {
			request.Payload = converted
			request.OriginalRequest = append([]byte(nil), converted...)
			if request.Headers == nil {
				request.Headers = make(http.Header)
			} else {
				request.Headers = request.Headers.Clone()
			}
			request.Headers.Set("Content-Type", "application/json")
			request.RequestPath = "/v1/images/edits"
			return request
		}
	}
	if !gjson.ValidBytes(request.Payload) {
		return request
	}
	rawJSON := request.Payload
	model := strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String())
	if model == "" {
		model = strings.TrimSpace(request.Model)
	}
	if model == "" {
		model = "grok-imagine-image"
	}
	prompt := strings.TrimSpace(gjson.GetBytes(rawJSON, "prompt").String())
	rawResponseFormat := strings.TrimSpace(gjson.GetBytes(rawJSON, "response_format").String())
	responseFormat := "b64_json"
	if strings.EqualFold(rawResponseFormat, "url") {
		responseFormat = "url"
	}
	size := strings.TrimSpace(gjson.GetBytes(rawJSON, "size").String())
	aspectRatio := grokImageAspectRatio(gjson.GetBytes(rawJSON, "aspect_ratio").String(), "")
	aspectRatio = grokImageAspectRatioFromSize(size, aspectRatio)
	images := collectGrokImagesFromJSON(rawJSON)
	isEdit := strings.HasSuffix(strings.TrimSpace(request.RequestPath), "/images/edits") || len(images) > 0
	if !isEdit && aspectRatio == "" {
		aspectRatio = "1:1"
	}
	defaultRes := ""
	if !isEdit {
		defaultRes = "1k"
	}
	resolution := grokImageResolution(gjson.GetBytes(rawJSON, "resolution").String(), size, defaultRes)
	quality := strings.TrimSpace(gjson.GetBytes(rawJSON, "quality").String())
	n := int64(0)
	if v := gjson.GetBytes(rawJSON, "n"); v.Exists() && v.Type == gjson.Number {
		n = v.Int()
	}

	built := buildGrokImageJSONPayload(model, prompt, responseFormat, aspectRatio, resolution, quality, n, images)
	origBuilt := buildGrokImageJSONPayload(model, prompt, rawResponseFormat, aspectRatio, resolution, quality, n, images)
	request.Payload = built
	request.OriginalRequest = origBuilt
	if isEdit {
		request.RequestPath = "/v1/images/edits"
	} else if strings.TrimSpace(request.RequestPath) == "" {
		request.RequestPath = "/v1/images/generations"
	}
	return request
}

func convertGrokMultipartImageToJSON(body []byte, boundary string, fallbackModel string) ([]byte, error) {
	if strings.TrimSpace(boundary) == "" {
		return nil, errors.New("missing multipart boundary")
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	form, err := reader.ReadForm(64 << 20)
	if err != nil {
		return nil, err
	}
	defer func() { _ = form.RemoveAll() }()

	firstVal := func(key string) string {
		if vals := form.Value[key]; len(vals) > 0 {
			return strings.TrimSpace(vals[0])
		}
		return ""
	}
	model := firstVal("model")
	if model == "" {
		model = strings.TrimSpace(fallbackModel)
	}
	if model == "" {
		model = "grok-imagine-image"
	}
	prompt := firstVal("prompt")
	rawResponseFormat := firstVal("response_format")
	responseFormat := "b64_json"
	if strings.EqualFold(rawResponseFormat, "url") {
		responseFormat = "url"
	}
	size := firstVal("size")
	aspectRatio := grokImageAspectRatio(firstVal("aspect_ratio"), "")
	aspectRatio = grokImageAspectRatioFromSize(size, aspectRatio)
	resolution := grokImageResolution(firstVal("resolution"), size, "")
	quality := firstVal("quality")
	n := int64(0)
	if nStr := firstVal("n"); nStr != "" {
		if parsed, errParse := strconv.ParseInt(nStr, 10, 64); errParse == nil {
			n = parsed
		}
	}
	var fileHeaders []*multipart.FileHeader
	if files := form.File["image[]"]; len(files) > 0 {
		fileHeaders = files
	} else if files := form.File["image"]; len(files) > 0 {
		fileHeaders = files
	}
	var images []string
	for _, fh := range fileHeaders {
		if fh == nil {
			continue
		}
		f, errOpen := fh.Open()
		if errOpen != nil {
			continue
		}
		data, errRead := io.ReadAll(f)
		_ = f.Close()
		if errRead != nil || len(data) == 0 {
			continue
		}
		mt := strings.TrimSpace(fh.Header.Get("Content-Type"))
		if mt == "" || mt == "application/octet-stream" {
			mt = http.DetectContentType(data)
		}
		images = append(images, "data:"+mt+";base64,"+base64.StdEncoding.EncodeToString(data))
	}
	return buildGrokImageJSONPayload(model, prompt, responseFormat, aspectRatio, resolution, quality, n, images), nil
}

func buildGrokImageJSONPayload(model, prompt, responseFormat, aspectRatio, resolution, quality string, n int64, images []string) []byte {
	req := []byte(`{}`)
	req, _ = sjson.SetBytes(req, "model", strings.TrimSpace(model))
	req, _ = sjson.SetBytes(req, "prompt", strings.TrimSpace(prompt))
	if rf := strings.TrimSpace(responseFormat); rf != "" {
		if strings.EqualFold(rf, "url") {
			req, _ = sjson.SetBytes(req, "response_format", "url")
		} else {
			req, _ = sjson.SetBytes(req, "response_format", "b64_json")
		}
	}
	if aspectRatio != "" {
		req, _ = sjson.SetBytes(req, "aspect_ratio", aspectRatio)
	}
	if resolution != "" {
		req, _ = sjson.SetBytes(req, "resolution", resolution)
	}
	if q := strings.TrimSpace(quality); q != "" {
		req, _ = sjson.SetBytes(req, "quality", q)
	}
	if n > 0 {
		req, _ = sjson.SetBytes(req, "n", n)
	}
	if len(images) == 1 {
		ref := []byte(`{"type":"image_url","url":""}`)
		ref, _ = sjson.SetBytes(ref, "url", images[0])
		req, _ = sjson.SetRawBytes(req, "image", ref)
	} else if len(images) > 1 {
		for _, img := range images {
			ref := []byte(`{"type":"image_url","url":""}`)
			ref, _ = sjson.SetBytes(ref, "url", img)
			req, _ = sjson.SetRawBytes(req, "images.-1", ref)
		}
	}
	return req
}

func collectGrokImagesFromJSON(rawJSON []byte) []string {
	var images []string
	appendImage := func(url string) {
		url = strings.TrimSpace(url)
		if url != "" {
			images = append(images, url)
		}
	}
	for _, field := range []string{"image", "image2", "image3"} {
		if img := gjson.GetBytes(rawJSON, field); img.Exists() {
			if img.Type == gjson.String {
				appendImage(img.String())
			} else if img.Type == gjson.JSON {
				appendImage(img.Get("image_url.url").String())
				if imageURL := img.Get("image_url"); imageURL.Type == gjson.String {
					appendImage(imageURL.String())
				}
				appendImage(img.Get("url").String())
			}
		}
	}
	if imagesResult := gjson.GetBytes(rawJSON, "images"); imagesResult.IsArray() {
		for _, img := range imagesResult.Array() {
			if img.Type == gjson.String {
				appendImage(img.String())
				continue
			}
			appendImage(img.Get("image_url.url").String())
			if imageURL := img.Get("image_url"); imageURL.Type == gjson.String {
				appendImage(imageURL.String())
			}
			appendImage(img.Get("url").String())
		}
	}
	return images
}

func grokImageAspectRatio(raw string, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1:1", "square":
		return "1:1"
	case "16:9", "landscape":
		return "16:9"
	case "9:16", "portrait":
		return "9:16"
	case "9:20":
		return "9:20"
	case "20:9":
		return "20:9"
	case "4:3":
		return "4:3"
	case "3:4":
		return "3:4"
	case "3:2":
		return "3:2"
	case "2:3":
		return "2:3"
	default:
		return fallback
	}
}

func grokImageAspectRatioFromSize(size string, fallback string) string {
	size = strings.ToLower(strings.TrimSpace(size))
	switch size {
	case "1024x1024", "2048x2048", "1:1":
		return "1:1"
	case "1792x1024", "16:9":
		return "16:9"
	case "1024x1792", "9:16":
		return "9:16"
	case "9:20":
		return "9:20"
	case "20:9":
		return "20:9"
	case "1536x1024", "3:2":
		return "3:2"
	case "1024x1536", "2:3":
		return "2:3"
	default:
		return fallback
	}
}

func grokImageResolution(raw string, size string, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1k", "2k":
		return strings.ToLower(strings.TrimSpace(raw))
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(size)), "2048") {
		return "2k"
	}
	return fallback
}

func applyGrokPromptCacheKey(request ExecuteRequest) ExecuteRequest {
	if !grokCarriesPromptCacheKey(request.Format) {
		return request
	}
	cacheKey := grokConversationID(request.ContinuityKey)
	if cacheKey == "" {
		return request
	}
	request.Payload = writeGrokPromptCacheKey(request.Payload, cacheKey)
	request.OriginalRequest = writeGrokPromptCacheKey(request.OriginalRequest, cacheKey)
	return request
}

func grokCarriesPromptCacheKey(format string) bool {
	switch sdktranslator.FromString(format) {
	case sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse:
		return true
	default:
		return false
	}
}

func writeGrokPromptCacheKey(raw []byte, cacheKey string) []byte {
	updated, err := sjson.SetBytes(raw, "prompt_cache_key", cacheKey)
	if err != nil {
		return raw
	}
	return updated
}

func prepareGrokExecutionRequest(request ExecuteRequest) ExecuteRequest {
	if strings.TrimSpace(request.ContinuityKey) == "" && strings.TrimSpace(request.AttemptID) != "" {
		request.ContinuityKey = "gpt-load-grok-attempt\x00" + strings.TrimSpace(request.AttemptID)
	}
	request.Payload = stripGrokCallerContinuity(request.Payload)
	request.OriginalRequest = stripGrokCallerContinuity(request.OriginalRequest)
	request.Headers = request.Headers.Clone()
	for name := range request.Headers {
		if strings.EqualFold(name, "Session-Id") || strings.EqualFold(name, "X-Grok-Conv-Id") {
			delete(request.Headers, name)
		}
	}
	return request
}

func stripGrokCallerContinuity(raw []byte) []byte {
	result := append([]byte(nil), raw...)
	for _, path := range []string{
		"session_id", "sessionId", "prompt_cache_key", "metadata.session_id", "metadata.sessionId",
	} {
		updated, err := sjson.DeleteBytes(result, path)
		if err == nil {
			result = updated
		}
	}
	return result
}

func (executor *grokHTTPExecutor) executionContext(
	ctx context.Context,
	auth *cliproxyauth.Auth,
	observation *executionObservation,
) context.Context {
	baseClient := helps.NewProxyAwareHTTPClient(ctx, executor.cfg, auth, 0)
	transport := baseClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return context.WithValue(ctx, "cliproxy.roundtripper", noRedirectRoundTripper{
		base: transport, observation: observation,
	})
}

func normalizeGrokExecutionError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var statusError interface {
		error
		StatusCode() int
	}
	if !errors.As(err, &statusError) || statusError == nil || statusError.StatusCode() == 0 {
		return err
	}
	status := statusError.StatusCode()
	retryAfter := time.Duration(0)
	var retry interface{ RetryAfter() *time.Duration }
	if errors.As(err, &retry) && retry != nil {
		if value := retry.RetryAfter(); value != nil && *value > 0 {
			retryAfter = *value
		}
	}
	code := boundedGrokExecutionCode([]byte(statusError.Error()))
	return &GrokExecutionError{
		status: status, code: code, retryAfter: retryAfter,
		summary: grokExecutionSummary(status, code),
	}
}

func boundedGrokExecutionCode(raw []byte) string {
	var payload struct {
		Code  string `json:"code"`
		Error any    `json:"error"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	code := strings.TrimSpace(payload.Code)
	if code == "" {
		if object, ok := payload.Error.(map[string]any); ok {
			code, _ = object["code"].(string)
		}
	}
	code = strings.TrimSpace(code)
	if code == "" || len(code) > 128 || strings.ContainsAny(code, "\r\n\x00") {
		return ""
	}
	return code
}

func grokExecutionSummary(status int, code string) string {
	lower := strings.ToLower(code)
	if strings.Contains(lower, "free-usage-exhausted") {
		return "Grok included free usage is exhausted."
	}
	if strings.Contains(lower, "content-moderated") || strings.Contains(lower, "moderation") {
		return "Generated video rejected by content moderation (" + code + ")."
	}
	if code != "" {
		return fmt.Sprintf("Grok upstream returned status %d (%s)", status, code)
	}
	return fmt.Sprintf("Grok upstream returned status %d", status)
}

var _ GrokHTTPExecutor = (*grokHTTPExecutor)(nil)
