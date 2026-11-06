package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/outboundproxy"
	"gpt-load/internal/protocol"
	"gpt-load/internal/scheduler"
	"gpt-load/internal/state"
	"gpt-load/internal/subscription/providers/grok"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	defaultGrokVideoModel      = "grok-imagine-video-1.5"
	videoBindingTTL            = 3 * time.Hour
	videoCreateTimeout         = 60 * time.Second
	videoRetrieveSingleTimeout = 30 * time.Second
)

type videoBinding struct {
	credentialID uint
	groupID      uint
	model        string
	expiresAt    time.Time
}

var (
	videoBindingsMu sync.RWMutex
	videoBindings   = make(map[string]videoBinding)
)

func bindVideoTask(requestID string, credentialID uint, groupID uint, model string) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" || credentialID == 0 {
		return
	}
	now := time.Now()
	videoBindingsMu.Lock()
	for id, entry := range videoBindings {
		if now.After(entry.expiresAt) {
			delete(videoBindings, id)
		}
	}
	videoBindings[requestID] = videoBinding{
		credentialID: credentialID,
		groupID:      groupID,
		model:        strings.TrimSpace(model),
		expiresAt:    now.Add(videoBindingTTL),
	}
	videoBindingsMu.Unlock()
}

func lookupVideoTaskBinding(requestID string) (videoBinding, bool) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return videoBinding{}, false
	}
	now := time.Now()
	videoBindingsMu.RLock()
	entry, ok := videoBindings[requestID]
	videoBindingsMu.RUnlock()
	if !ok || now.After(entry.expiresAt) {
		return videoBinding{}, false
	}
	return entry, true
}

func (handler *Handler) handleVideos(c *gin.Context, request *dataPlaneRequestContext) {
	release, failure := handler.acquireRequestConcurrency(request.accessKey.ID)
	if failure != nil {
		_ = handler.writeReason(c, *failure)
		return
	}
	defer release()

	if c.Request.Method == http.MethodGet {
		handler.handleVideosRetrieve(c, request)
		return
	}
	handler.handleVideosCreate(c, request)
}

func (handler *Handler) handleVideosCreate(c *gin.Context, request *dataPlaneRequestContext) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, handler.RequestBodyLimit()+1))
	if err != nil || int64(len(body)) > handler.RequestBodyLimit() {
		_ = handler.writeReason(c, reasonInvalidProtocolRequest)
		return
	}
	if !json.Valid(body) {
		_ = handler.writeReason(c, reasonInvalidProtocolRequest)
		return
	}

	model := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if model == "" {
		model = defaultGrokVideoModel
	}
	canonicalModel := canonicalGrokVideoModel(model)
	cleanedBody := normalizeGrokVideoCreatePayload(body)
	payload, _ := sjson.SetBytes(cleanedBody, "model", canonicalModel)

	query := scheduler.Query{
		ClientProtocol:        protocol.OpenAIResponses,
		Operation:             execution.OperationResponsesCreate,
		RouteRequirement:      execution.RouteRequirementAny,
		ExternalModel:         &model,
		AccessKey:             request.accessKey,
		AllowedCredentialRefs: make(map[uint]state.CredentialRef),
	}
	groups := scheduler.CandidateGroupIDsForQuery(request.snapshot, query)
	for _, ref := range handler.registry.CaptureActiveCredentialRefs(groups) {
		query.AllowedCredentialRefs[ref.ID] = ref
	}
	iterator := scheduler.New(request.snapshot, handler.registry, query)

	exec := grok.NewExecutor()
	ctx, cancel := context.WithTimeout(c.Request.Context(), videoCreateTimeout)
	defer cancel()

	var lastErr error
	var lastStatus int
	for sequence := 1; sequence <= retryAttemptLimit(request.snapshot.Settings.RetryCount); sequence++ {
		selection, nextErr := iterator.Next()
		if nextErr != nil {
			break
		}
		if selection.ChannelID != channel.Grok {
			continue
		}
		ref := query.AllowedCredentialRefs[selection.CredentialID]
		cred, baseURL, proxyURL, prepErr := handler.prepareGrokVideoCredential(ctx, selection.Group, selection.ResolvedTarget.TargetConfig, ref)
		if prepErr != nil {
			lastErr = prepErr
			continue
		}
		upstreamModel := canonicalModel
		if selection.UpstreamModelID != nil && strings.TrimSpace(*selection.UpstreamModelID) != "" {
			upstreamModel = canonicalGrokVideoModel(*selection.UpstreamModelID)
		}
		attemptPayload, _ := sjson.SetBytes(payload, "model", upstreamModel)
		resp, execErr := exec.Execute(ctx, strconv.FormatUint(uint64(ref.ID), 10), cred, grok.ExecuteRequest{
			AttemptID:       "video-create-" + strconv.Itoa(sequence),
			Model:           upstreamModel,
			Payload:         attemptPayload,
			Format:          "openai-video",
			RequestPath:     c.Request.URL.Path,
			Headers:         c.Request.Header.Clone(),
			OriginalRequest: attemptPayload,
			BaseURL:         baseURL,
			ProxyURL:        proxyURL,
		})
		if execErr != nil {
			lastErr = execErr
			var statusErr interface{ StatusCode() int }
			if errors.As(execErr, &statusErr) && statusErr != nil {
				lastStatus = statusErr.StatusCode()
			}
			continue
		}
		handler.recordCredentialSuccess(ref, time.Now())
		requestID := strings.TrimSpace(gjson.GetBytes(resp.Payload, "request_id").String())
		if requestID == "" {
			requestID = strings.TrimSpace(gjson.GetBytes(resp.Payload, "id").String())
		}
		if requestID != "" {
			bindVideoTask(requestID, ref.ID, selection.GroupID, upstreamModel)
		}
		outPayload := normalizeGrokVideoResponse(resp.Payload, requestID)
		c.Data(http.StatusOK, "application/json", outPayload)
		return
	}

	if lastErr != nil {
		status := lastStatus
		if status < 400 || status >= 600 {
			status = http.StatusBadGateway
		}
		c.JSON(status, gin.H{
			"error": gin.H{
				"message": lastErr.Error(),
				"type":    "upstream_error",
			},
		})
		return
	}
	_ = handler.writeReason(c, reasonNoCandidate)
}

func (handler *Handler) handleVideosRetrieve(c *gin.Context, request *dataPlaneRequestContext) {
	requestID := strings.TrimSpace(c.Param("request_id"))
	if requestID == "" {
		_ = handler.writeReason(c, reasonInvalidProtocolRequest)
		return
	}

	payload, _ := sjson.SetBytes([]byte(`{}`), "request_id", requestID)
	exec := grok.NewExecutor()

	// Collect candidate Grok credential refs: bound credential first, then all active Grok credentials.
	var candidates []state.CredentialRef
	var boundCredentialID uint
	seen := make(map[uint]bool)
	if binding, ok := lookupVideoTaskBinding(requestID); ok {
		if ref, exists := handler.registry.CredentialRef(binding.credentialID); exists {
			candidates = append(candidates, ref)
			seen[ref.ID] = true
			boundCredentialID = ref.ID
		}
	}
	var grokGroupIDs []uint
	for groupID, group := range request.snapshot.Groups {
		if group.ChannelID == channel.Grok {
			grokGroupIDs = append(grokGroupIDs, groupID)
		}
	}
	for _, ref := range handler.registry.CaptureActiveCredentialRefs(grokGroupIDs) {
		if !seen[ref.ID] {
			candidates = append(candidates, ref)
			seen[ref.ID] = true
		}
	}

	if len(candidates) == 0 {
		_ = handler.writeReason(c, reasonNoCandidate)
		return
	}

	var lastErr error
	var lastStatus int
	for _, ref := range candidates {
		group, ok := request.snapshot.Groups[ref.GroupID]
		if !ok {
			continue
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), videoRetrieveSingleTimeout)
		cred, baseURL, proxyURL, prepErr := handler.prepareGrokVideoCredential(ctx, group, group.ResolvedTarget.TargetConfig, ref)
		if prepErr != nil {
			cancel()
			if lastErr == nil || ref.ID == boundCredentialID {
				lastErr = prepErr
			}
			continue
		}
		resp, execErr := exec.Execute(ctx, strconv.FormatUint(uint64(ref.ID), 10), cred, grok.ExecuteRequest{
			AttemptID:       "video-retrieve-" + requestID,
			Model:           "grok-imagine-video",
			Payload:         payload,
			Format:          "openai-video",
			RequestPath:     "/v1/videos/" + requestID,
			Headers:         c.Request.Header.Clone(),
			OriginalRequest: payload,
			BaseURL:         baseURL,
			ProxyURL:        proxyURL,
		})
		cancel()
		if execErr != nil {
			var codeErr interface{ ErrorCode() string }
			errCode := ""
			if errors.As(execErr, &codeErr) && codeErr != nil {
				errCode = strings.TrimSpace(codeErr.ErrorCode())
			}
			if strings.Contains(strings.ToLower(errCode), "content-moderated") || strings.Contains(strings.ToLower(execErr.Error()), "content-moderated") {
				c.JSON(http.StatusOK, gin.H{
					"id":         requestID,
					"request_id": requestID,
					"status":     "REJECTED",
					"video":      gin.H{"respect_moderation": false},
					"error": gin.H{
						"code":    errCode,
						"message": "Generated video rejected by content moderation.",
					},
				})
				return
			}
			st := 0
			var statusErr interface{ StatusCode() int }
			if errors.As(execErr, &statusErr) && statusErr != nil {
				st = statusErr.StatusCode()
			}
			if ref.ID == boundCredentialID && st != http.StatusNotFound {
				lastErr = execErr
				lastStatus = st
				break
			}
			if lastErr == nil {
				lastErr = execErr
				lastStatus = st
			}
			continue
		}
		bindVideoTask(requestID, ref.ID, ref.GroupID, "grok-imagine-video")
		outPayload := normalizeGrokVideoResponse(resp.Payload, requestID)
		c.Data(http.StatusOK, "application/json", outPayload)
		return
	}

	status := lastStatus
	if status < 400 || status >= 600 {
		status = http.StatusBadGateway
	}
	errMsg := "Grok video retrieve failed"
	if lastErr != nil {
		errMsg = lastErr.Error()
	}
	c.JSON(status, gin.H{
		"error": gin.H{
			"message": errMsg,
			"type":    "upstream_error",
		},
	})
}

func (handler *Handler) prepareGrokVideoCredential(
	ctx context.Context,
	group state.GroupView,
	targetConfig json.RawMessage,
	ref state.CredentialRef,
) (grok.Credential, string, string, error) {
	if latestRef, ok := handler.registry.CredentialRef(ref.ID); ok {
		ref = latestRef
	}
	encrypted, exists := handler.registry.ActiveEncryptedCredentialDataIfMatch(ref)
	if !exists {
		return grok.Credential{}, "", "", errors.New("credential not active")
	}
	plain, err := handler.encryption.Decrypt(encrypted)
	if err != nil {
		return grok.Credential{}, "", "", err
	}
	cred, err := grok.ParseCredentialJSON([]byte(plain))
	if err != nil {
		return grok.Credential{}, "", "", err
	}
	baseURL, _ := (subscriptionruntime.Target{Config: targetConfig}).BaseURL()
	proxy, _, err := resolveAttemptProxy(handler.encryption, group.Proxy, ref)
	if err != nil {
		return grok.Credential{}, "", "", err
	}
	if exp, ok := grok.CredentialExpiresAt(cred); ok && time.Now().After(exp.Add(-90*time.Second)) {
		rctx := subscriptionruntime.WithNetworkContext(ctx, subscriptionruntime.NetworkContext{Proxy: proxy})
		if refreshed, rErr := grok.RefreshCredentialOnce(rctx, cred); rErr == nil {
			cred = refreshed
		}
	}
	proxyURL := ""
	if proxy.Config.Mode == outboundproxy.ModeCustom {
		proxyURL = strings.TrimSpace(proxy.Config.URL)
	}
	return cred, baseURL, proxyURL, nil
}

func canonicalGrokVideoModel(model string) string {
	lower := strings.ToLower(strings.TrimSpace(model))
	switch lower {
	case "grok-imagine-video-1.5", "grok-imagine-video-1.5-preview":
		return "grok-imagine-video-1.5"
	case "grok-imagine-video-1.5-lite":
		return "grok-imagine-video-1.5-lite"
	default:
		return "grok-imagine-video"
	}
}

func normalizeGrokVideoResponse(payload []byte, requestID string) []byte {
	if !gjson.ValidBytes(payload) {
		return payload
	}
	out := append([]byte(nil), payload...)
	if requestID != "" {
		if !gjson.GetBytes(out, "request_id").Exists() {
			out, _ = sjson.SetBytes(out, "request_id", requestID)
		}
		if !gjson.GetBytes(out, "id").Exists() {
			out, _ = sjson.SetBytes(out, "id", requestID)
		}
	}
	videoURL := strings.TrimSpace(gjson.GetBytes(out, "video.url").String())
	status := strings.TrimSpace(gjson.GetBytes(out, "status").String())
	if videoURL != "" && status == "" {
		out, _ = sjson.SetBytes(out, "status", "SUCCESS")
	}
	return out
}

func normalizeGrokVideoCreatePayload(body []byte) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}
	out := append([]byte(nil), body...)
	rawSize := strings.ToLower(strings.TrimSpace(gjson.GetBytes(out, "size").String()))
	if gjson.GetBytes(out, "size").Exists() {
		out, _ = sjson.DeleteBytes(out, "size")
	}
	hasAR := strings.TrimSpace(gjson.GetBytes(out, "aspect_ratio").String()) != ""
	hasRes := strings.TrimSpace(gjson.GetBytes(out, "resolution").String()) != ""
	if rawSize != "" {
		inferredAR, inferredRes := inferGrokVideoParamsFromSize(rawSize)
		if !hasAR && inferredAR != "" {
			out, _ = sjson.SetBytes(out, "aspect_ratio", inferredAR)
		}
		if !hasRes && inferredRes != "" {
			out, _ = sjson.SetBytes(out, "resolution", inferredRes)
		}
	}
	if curRes := strings.ToLower(strings.TrimSpace(gjson.GetBytes(out, "resolution").String())); curRes != "" && curRes != "480p" && curRes != "720p" {
		_, mappedRes := inferGrokVideoParamsFromSize(curRes)
		if mappedRes != "" {
			out, _ = sjson.SetBytes(out, "resolution", mappedRes)
		} else {
			out, _ = sjson.DeleteBytes(out, "resolution")
		}
	}
	return out
}

func inferGrokVideoParamsFromSize(s string) (string, string) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "480p":
		return "", "480p"
	case "720p", "1080p", "2k", "4k", "hd":
		return "", "720p"
	}
	sep := -1
	for i, r := range s {
		if r == 'x' || r == '×' || r == '*' || r == ':' {
			sep = i
			break
		}
	}
	if sep > 0 && sep < len(s)-1 {
		w, errW := strconv.Atoi(strings.TrimSpace(s[:sep]))
		h, errH := strconv.Atoi(strings.TrimSpace(s[sep+1:]))
		if errW == nil && errH == nil && w > 0 && h > 0 {
			shortSide := w
			if h < shortSide {
				shortSide = h
			}
			res := "720p"
			if shortSide <= 540 {
				res = "480p"
			}
			ar := "16:9"
			diff := w - h
			if diff < 0 {
				diff = -diff
			}
			maxSide := w
			if h > maxSide {
				maxSide = h
			}
			if float64(diff) <= float64(maxSide)*0.05 {
				ar = "1:1"
			} else if w > h {
				ratio := float64(w) / float64(h)
				if ratio > 1.21 && ratio < 1.45 {
					ar = "4:3"
				} else if ratio >= 1.45 && ratio < 1.62 {
					ar = "3:2"
				} else {
					ar = "16:9"
				}
			} else {
				ratio := float64(h) / float64(w)
				if ratio > 1.21 && ratio < 1.45 {
					ar = "3:4"
				} else if ratio >= 1.45 && ratio < 1.62 {
					ar = "2:3"
				} else {
					ar = "9:16"
				}
			}
			return ar, res
		}
	}
	if strings.Contains(s, "480") {
		return "", "480p"
	}
	if strings.Contains(s, "720") || strings.Contains(s, "1080") || strings.Contains(s, "1280") || strings.Contains(s, "1920") {
		return "", "720p"
	}
	return "", ""
}
