package forwarder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"sr-forwarder/internal/config"
)

type Handler struct {
	routes    RouteLookup
	publisher Publisher
	cfg       config.ServerConfig
	logger    *log.Logger
}

type eventRequest struct {
	DataSet string            `json:"dataSet"`
	Data    []json.RawMessage `json:"data"`
}

type itemMeta struct {
	TenantID string `json:"tenantId"`
	UUID     string `json:"uuId"`
}

type responseBody struct {
	OK        bool   `json:"ok"`
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
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/events", h.handleEvents)
	mux.HandleFunc("/gop/gop-data-service/api/v1/mss/web/alert/outer/add", h.handleEvents)
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/readyz", h.handleReady)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, responseBody{OK: true})
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, responseBody{OK: true})
}

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, responseBody{OK: false, Error: "method not allowed"})
		return
	}

	cfg := h.serverConfig()
	req, err := decodeEventRequest(w, r, cfg.MaxBodyBytes)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSON(w, status, responseBody{OK: false, Error: err.Error()})
		return
	}

	req.DataSet = strings.TrimSpace(req.DataSet)
	if req.DataSet == "" {
		writeJSON(w, http.StatusBadRequest, responseBody{OK: false, Error: "dataSet is required"})
		return
	}
	if len(req.Data) == 0 {
		writeJSON(w, http.StatusBadRequest, responseBody{OK: false, Error: "data must not be empty"})
		return
	}
	if len(req.Data) > cfg.MaxBatchItems {
		writeJSON(w, http.StatusRequestEntityTooLarge, responseBody{OK: false, Error: "data item count exceeds limit"})
		return
	}

	topic, ok := h.routes.LookupTopic(req.DataSet)
	if !ok {
		writeJSON(w, http.StatusUnprocessableEntity, responseBody{OK: false, Error: "dataSet route not found", DataSet: req.DataSet})
		return
	}

	accepted := 0
	for idx, item := range req.Data {
		meta, err := parseItemMeta(item)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, responseBody{OK: false, Error: fmt.Sprintf("data[%d] is not a valid object", idx)})
			return
		}
		props := map[string]string{
			"dataSet": req.DataSet,
		}
		if meta.TenantID != "" {
			props["tenantId"] = meta.TenantID
		}

		if err := h.publishWithRetry(r.Context(), cfg.PublishRetry, topic, meta.UUID, item, props); err != nil {
			h.logger.Printf("publish failed dataSet=%s topic=%s accepted=%d err=%v", req.DataSet, topic, accepted, err)
			writeJSON(w, http.StatusServiceUnavailable, responseBody{
				OK:       false,
				Error:    "publish to pulsar failed",
				Accepted: accepted,
				DataSet:  req.DataSet,
				Topic:    topic,
			})
			return
		}
		accepted++
	}

	writeJSON(w, http.StatusAccepted, responseBody{
		OK:       true,
		Accepted: accepted,
		DataSet:  req.DataSet,
		Topic:    topic,
	})
}

func (h *Handler) serverConfig() config.ServerConfig {
	if lookup, ok := h.routes.(ServerConfigLookup); ok {
		return lookup.ServerConfig()
	}
	return h.cfg
}

func (h *Handler) publishWithRetry(ctx context.Context, retry config.RetryConfig, topic string, key string, payload []byte, properties map[string]string) error {
	initialBackoff := retry.InitialBackoffDuration()
	maxBackoff := retry.MaxBackoffDuration()

	var err error
	for attempt := 1; attempt <= retry.MaxAttempts; attempt++ {
		err = h.publisher.Publish(ctx, topic, key, payload, properties)
		if err == nil {
			return nil
		}
		if attempt == retry.MaxAttempts {
			return err
		}

		backoff := retryBackoff(initialBackoff, maxBackoff, attempt)
		h.logger.Printf("publish retry scheduled topic=%s attempt=%d maxAttempts=%d backoff=%s err=%v", topic, attempt+1, retry.MaxAttempts, backoff, err)
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
	}
	return err
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

var errBodyTooLarge = errors.New("request body too large")

func decodeEventRequest(w http.ResponseWriter, r *http.Request, maxBodyBytes int64) (*eventRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()

	var req eventRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return nil, errBodyTooLarge
		}
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("request body must contain one json object")
	}
	return &req, nil
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

func writeJSON(w http.ResponseWriter, status int, body responseBody) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
