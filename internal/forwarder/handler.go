package forwarder

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"sr-forwarder/internal/config"
)

type Handler struct {
	routes    RouteLookup
	publisher Publisher
	cfg       config.ServerConfig
	logger    *log.Logger
	metrics   *Metrics
	breaker   *CircuitBreaker
}

type eventRequest struct {
	DataSet string            `json:"dataSet"`
	Data    []json.RawMessage `json:"data"`
}

type itemMeta struct {
	TenantID string `json:"tenantId"`
	UUID     string `json:"uuId"`
}

const unknownDataSetLabel = "unknown"

type responseBody struct {
	OK        bool   `json:"ok"`
	Success   bool   `json:"success"`
	Code      int    `json:"code"`
	Error     string `json:"error,omitempty"`
	Accepted  int    `json:"accepted,omitempty"`
	DataSet   string `json:"dataSet,omitempty"`
	Topic     string `json:"topic,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

func NewHandler(routes RouteLookup, publisher Publisher, cfg config.ServerConfig, logger *log.Logger) *Handler {
	if logger == nil {
		logger = log.Default()
	}
	cfg = cfg.WithDefaults()
	return &Handler{
		routes:    routes,
		publisher: publisher,
		cfg:       cfg,
		logger:    logger,
		metrics:   NewMetrics(),
		breaker:   NewCircuitBreaker(),
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/events", h.handleEvents)
	mux.HandleFunc("/gop/gop-data-service/api/v1/mss/web/alert/outer/add", h.handleEvents)
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/readyz", h.handleReady)
	mux.HandleFunc("/metrics", h.handleMetrics)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, responseBody{OK: true})
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	cfg := h.serverConfig()
	ctx, cancel := context.WithTimeout(r.Context(), cfg.ReadinessTimeoutDuration())
	defer cancel()

	topics := h.topics()
	if len(topics) > 0 {
		if err := h.publisher.Ready(ctx, topics); err != nil {
			h.logger.Printf("readiness failed err=%v", err)
			writeJSON(w, http.StatusServiceUnavailable, responseBody{OK: false, Error: "pulsar not ready"})
			return
		}
		for _, topic := range topics {
			if h.breaker.IsOpen(topic, cfg.CircuitBreaker, time.Now()) {
				writeJSON(w, http.StatusServiceUnavailable, responseBody{OK: false, Error: "publish circuit open", Topic: topic})
				return
			}
		}
	}
	writeJSON(w, http.StatusOK, responseBody{OK: true})
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	h.metrics.WritePrometheus(w)
}

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.writeEventJSON(w, http.StatusMethodNotAllowed, responseBody{OK: false, Error: "method not allowed"}, "", "method_not_allowed")
		return
	}

	cfg := h.serverConfig()
	ctx := r.Context()
	if timeout := cfg.RequestTimeoutDuration(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	req, bodyBytes, decodeErr := decodeEventRequest(w, r, cfg.MaxBodyBytes)
	if req != nil {
		req.DataSet = strings.TrimSpace(req.DataSet)
	}

	var route config.RouteEntry
	var routeFound bool
	if decodeErr == nil && req != nil && req.DataSet != "" {
		route, routeFound = h.lookupRoute(req.DataSet)
	}

	tokenAuth := cfg.Auth.BearerTokenConfig
	if routeFound && authHasToken(route.Auth) {
		tokenAuth = route.Auth
	}
	if !h.authorized(r, cfg.Auth.Enabled, tokenAuth) {
		h.logger.Printf("request rejected reason=unauthorized remote=%s", r.RemoteAddr)
		w.Header().Set("WWW-Authenticate", "Bearer")
		h.writeEventJSON(w, http.StatusUnauthorized, responseBody{OK: false, Error: "unauthorized"}, unknownDataSetLabel, "unauthorized")
		return
	}

	if decodeErr != nil {
		status := http.StatusBadRequest
		reason := "invalid_json"
		if errors.Is(decodeErr, errBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
			reason = "body_too_large"
		}
		h.writeEventJSON(w, status, responseBody{OK: false, Error: decodeErr.Error()}, "", reason)
		return
	}

	if req.DataSet == "" {
		h.writeEventJSON(w, http.StatusBadRequest, responseBody{OK: false, Error: "dataSet is required"}, "", "missing_data_set")
		return
	}
	if !routeFound {
		h.logger.Printf("request rejected dataSet=%s reason=route_not_found", req.DataSet)
		h.writeEventJSON(w, http.StatusUnprocessableEntity, responseBody{OK: false, Error: "dataSet route not found", DataSet: req.DataSet}, unknownDataSetLabel, "route_not_found")
		return
	}
	topic := route.Topic

	if len(req.Data) == 0 {
		h.writeEventJSON(w, http.StatusBadRequest, responseBody{OK: false, Error: "data must not be empty"}, req.DataSet, "empty_data")
		return
	}
	if len(req.Data) > cfg.MaxBatchItems {
		h.logger.Printf("request rejected dataSet=%s reason=max_batch_items limit=%d items=%d", req.DataSet, cfg.MaxBatchItems, len(req.Data))
		h.writeEventJSON(w, http.StatusRequestEntityTooLarge, responseBody{OK: false, Error: "data item count exceeds limit"}, req.DataSet, "max_batch_items")
		return
	}

	if status, reason, message := validateRouteRequest(route, bodyBytes, req); status != 0 {
		h.logger.Printf("request rejected dataSet=%s topic=%s reason=%s bodyBytes=%d items=%d error=%s", req.DataSet, topic, reason, bodyBytes, len(req.Data), message)
		h.writeEventJSON(w, status, responseBody{OK: false, Error: message, DataSet: req.DataSet, Topic: topic}, req.DataSet, reason)
		return
	}
	metas, badIndex, err := parseItemMetas(req.Data)
	if err != nil {
		h.logger.Printf("request rejected dataSet=%s topic=%s reason=invalid_data_object index=%d", req.DataSet, topic, badIndex)
		h.writeEventJSON(w, http.StatusBadRequest, responseBody{OK: false, Error: fmt.Sprintf("data[%d] is not a valid object", badIndex), DataSet: req.DataSet, Topic: topic}, req.DataSet, "invalid_data_object")
		return
	}

	if h.breaker.IsOpen(topic, cfg.CircuitBreaker, time.Now()) {
		h.logger.Printf("publish circuit open dataSet=%s topic=%s", req.DataSet, topic)
		h.metrics.Inc("sr_forwarder_publish_circuit_rejected_total", publishLabels(req.DataSet, topic))
		h.metrics.SetGauge("sr_forwarder_publish_circuit_open", publishLabels(req.DataSet, topic), 1)
		h.writeEventJSON(w, http.StatusServiceUnavailable, responseBody{
			OK:      false,
			Error:   "publish temporarily unavailable",
			DataSet: req.DataSet,
			Topic:   topic,
		}, req.DataSet, "circuit_open")
		return
	}

	accepted := 0
	for idx, item := range req.Data {
		meta := metas[idx]
		props := map[string]string{
			"dataSet": req.DataSet,
		}
		if meta.TenantID != "" {
			props["tenantId"] = meta.TenantID
		}

		start := time.Now()
		if err := h.publishWithRetry(ctx, cfg.PublishRetry, req.DataSet, topic, meta, item, props); err != nil {
			opened := false
			if !isContextDone(err) {
				opened = h.breaker.RecordFailure(topic, cfg.CircuitBreaker, time.Now())
			}
			if opened {
				h.metrics.Inc("sr_forwarder_publish_circuit_open_total", publishLabels(req.DataSet, topic))
				h.metrics.SetGauge("sr_forwarder_publish_circuit_open", publishLabels(req.DataSet, topic), 1)
			}
			h.metrics.Inc("sr_forwarder_publish_failure_total", publishLabels(req.DataSet, topic))
			h.metrics.ObserveDuration("sr_forwarder_publish_latency_seconds", publishLabels(req.DataSet, topic), time.Since(start))
			h.logger.Printf("publish failed dataSet=%s topic=%s tenantId=%s uuId=%s accepted=%d attemptMax=%d circuitOpened=%t err=%v", req.DataSet, topic, redactID(meta.TenantID), redactID(meta.UUID), accepted, cfg.PublishRetry.MaxAttempts, opened, err)
			h.writeEventJSON(w, http.StatusServiceUnavailable, responseBody{
				OK:       false,
				Error:    "publish to pulsar failed",
				Accepted: accepted,
				DataSet:  req.DataSet,
				Topic:    topic,
			}, req.DataSet, "publish_failed")
			return
		}
		h.breaker.RecordSuccess(topic, cfg.CircuitBreaker)
		h.metrics.SetGauge("sr_forwarder_publish_circuit_open", publishLabels(req.DataSet, topic), 0)
		h.metrics.Inc("sr_forwarder_publish_success_total", publishLabels(req.DataSet, topic))
		h.metrics.ObserveDuration("sr_forwarder_publish_latency_seconds", publishLabels(req.DataSet, topic), time.Since(start))
		h.metrics.Inc("sr_forwarder_accepted_items_total", publishLabels(req.DataSet, topic))
		accepted++
	}

	h.writeEventJSON(w, http.StatusOK, responseBody{
		OK:       true,
		Accepted: accepted,
		DataSet:  req.DataSet,
		Topic:    topic,
	}, req.DataSet, "")
}

func (h *Handler) serverConfig() config.ServerConfig {
	if lookup, ok := h.routes.(ServerConfigLookup); ok {
		return lookup.ServerConfig()
	}
	return h.cfg
}

func (h *Handler) topics() []string {
	if lister, ok := h.routes.(TopicLister); ok {
		return lister.Topics()
	}
	return nil
}

func (h *Handler) authorized(r *http.Request, enabled bool, tokenAuth config.BearerTokenConfig) bool {
	if !enabled {
		return true
	}

	expected, err := bearerToken(tokenAuth)
	if err != nil {
		h.logger.Printf("auth token load failed err=%v", err)
		return false
	}
	if expected == "" {
		h.logger.Print("auth token is empty")
		return false
	}
	actual := strings.TrimSpace(r.Header.Get("Authorization"))
	const prefix = "Bearer "
	if len(actual) <= len(prefix) || !strings.EqualFold(actual[:len(prefix)], prefix) {
		return false
	}

	actualToken := strings.TrimSpace(actual[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(actualToken), []byte(expected)) == 1
}

func authHasToken(cfg config.BearerTokenConfig) bool {
	return strings.TrimSpace(cfg.BearerToken) != "" || strings.TrimSpace(cfg.BearerTokenFile) != ""
}

func bearerToken(cfg config.BearerTokenConfig) (string, error) {
	if strings.TrimSpace(cfg.BearerTokenFile) != "" {
		data, err := os.ReadFile(cfg.BearerTokenFile)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(data)), nil
	}
	return strings.TrimSpace(cfg.BearerToken), nil
}

func (h *Handler) lookupRoute(dataSet string) (config.RouteEntry, bool) {
	if lookup, ok := h.routes.(RouteEntryLookup); ok {
		return lookup.LookupRoute(dataSet)
	}
	topic, ok := h.routes.LookupTopic(dataSet)
	return config.RouteEntry{Topic: topic}, ok
}

func (h *Handler) publishWithRetry(ctx context.Context, retry config.RetryConfig, dataSet string, topic string, meta itemMeta, payload []byte, properties map[string]string) error {
	initialBackoff := retry.InitialBackoffDuration()
	maxBackoff := retry.MaxBackoffDuration()

	var err error
	for attempt := 1; attempt <= retry.MaxAttempts; attempt++ {
		err = h.publisher.Publish(ctx, topic, meta.UUID, payload, properties)
		if err == nil {
			return nil
		}
		if attempt == retry.MaxAttempts {
			return err
		}

		backoff := retryBackoff(initialBackoff, maxBackoff, attempt)
		h.metrics.Inc("sr_forwarder_publish_retry_total", publishLabels(dataSet, topic))
		h.logger.Printf("publish retry scheduled dataSet=%s topic=%s tenantId=%s uuId=%s attempt=%d maxAttempts=%d backoff=%s err=%v", dataSet, topic, redactID(meta.TenantID), redactID(meta.UUID), attempt+1, retry.MaxAttempts, backoff, err)
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
	}
	return err
}

func redactID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 6 {
		return "***"
	}
	return value[:3] + "***" + value[len(value)-3:]
}

func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func retryBackoff(initialBackoff time.Duration, maxBackoff time.Duration, failedAttempts int) time.Duration {
	backoff := initialBackoff
	for i := 1; i < failedAttempts; i++ {
		if backoff >= maxBackoff/2 {
			return maxBackoff
		}
		backoff *= 2
	}
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateRouteRequest(route config.RouteEntry, bodyBytes int64, req *eventRequest) (int, string, string) {
	validation := route.Validation
	if validation.MaxBodyBytes > 0 && bodyBytes > validation.MaxBodyBytes {
		return http.StatusRequestEntityTooLarge, "route_max_body_bytes", "request body exceeds dataSet limit"
	}
	if validation.MaxBatchItems > 0 && len(req.Data) > validation.MaxBatchItems {
		return http.StatusRequestEntityTooLarge, "route_max_batch_items", "data item count exceeds dataSet limit"
	}
	if len(validation.RequiredFields) == 0 {
		return 0, "", ""
	}
	for idx, item := range req.Data {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(item, &obj); err != nil || obj == nil {
			return http.StatusBadRequest, "invalid_data_object", fmt.Sprintf("data[%d] is not a valid object", idx)
		}
		for _, field := range validation.RequiredFields {
			if !hasRequiredField(obj, field) {
				return http.StatusBadRequest, "missing_required_field", fmt.Sprintf("data[%d].%s is required", idx, field)
			}
		}
	}
	return 0, "", ""
}

func hasRequiredField(obj map[string]json.RawMessage, field string) bool {
	raw, ok := obj[field]
	if !ok || string(raw) == "null" {
		return false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return strings.TrimSpace(value) != ""
	}
	return len(raw) > 0
}

func (h *Handler) writeEventJSON(w http.ResponseWriter, status int, body responseBody, dataSet string, rejectReason string) {
	h.metrics.Inc("sr_forwarder_http_requests_total", map[string]string{
		"status": strconv.Itoa(status),
	})
	if rejectReason != "" {
		h.metrics.Inc("sr_forwarder_rejected_requests_total", map[string]string{
			"dataSet": dataSet,
			"reason":  rejectReason,
		})
	}
	writeJSON(w, status, body)
}

func publishLabels(dataSet string, topic string) map[string]string {
	return map[string]string{
		"dataSet": dataSet,
		"topic":   topic,
	}
}

var errBodyTooLarge = errors.New("request body too large")

func decodeEventRequest(w http.ResponseWriter, r *http.Request, maxBodyBytes int64) (*eventRequest, int64, error) {
	counter := &countingReader{reader: http.MaxBytesReader(w, r.Body, maxBodyBytes)}
	r.Body = counter
	defer r.Body.Close()

	var req eventRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return nil, counter.n, errBodyTooLarge
		}
		return nil, counter.n, fmt.Errorf("invalid json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, counter.n, errors.New("request body must contain one json object")
	}
	return &req, counter.n, nil
}

type countingReader struct {
	reader io.ReadCloser
	n      int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) Close() error {
	return c.reader.Close()
}

func parseItemMeta(item json.RawMessage) (itemMeta, error) {
	var meta itemMeta
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(item, &obj); err != nil {
		return meta, err
	}
	if obj == nil {
		return meta, errors.New("item must be object")
	}
	if err := json.Unmarshal(item, &meta); err != nil {
		return meta, err
	}
	meta.UUID = strings.TrimSpace(meta.UUID)
	meta.TenantID = strings.TrimSpace(meta.TenantID)
	return meta, nil
}

func parseItemMetas(items []json.RawMessage) ([]itemMeta, int, error) {
	metas := make([]itemMeta, len(items))
	for idx, item := range items {
		meta, err := parseItemMeta(item)
		if err != nil {
			return nil, idx, err
		}
		metas[idx] = meta
	}
	return metas, -1, nil
}

func writeJSON(w http.ResponseWriter, status int, body responseBody) {
	if status >= http.StatusOK && status < http.StatusMultipleChoices && body.OK {
		body.Success = true
		body.Code = 0
	} else {
		body.Success = false
		body.Code = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
