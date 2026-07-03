package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"sr-forwarder/internal/config"
)

type staticRoutes map[string]string

func (s staticRoutes) LookupTopic(dataSet string) (string, bool) {
	topic, ok := s[dataSet]
	return topic, ok
}

func (s staticRoutes) Topics() []string {
	topics := make([]string, 0, len(s))
	for _, topic := range s {
		topics = append(topics, topic)
	}
	return topics
}

type staticRouteEntries map[string]config.RouteEntry

func (s staticRouteEntries) LookupTopic(dataSet string) (string, bool) {
	route, ok := s[dataSet]
	return route.Topic, ok
}

func (s staticRouteEntries) LookupRoute(dataSet string) (config.RouteEntry, bool) {
	route, ok := s[dataSet]
	return route, ok
}

func (s staticRouteEntries) Topics() []string {
	topics := make([]string, 0, len(s))
	for _, route := range s {
		topics = append(topics, route.Topic)
	}
	return topics
}

type publishCall struct {
	topic      string
	key        string
	payload    string
	properties map[string]string
}

type fakePublisher struct {
	mu                   sync.Mutex
	err                  error
	readyErr             error
	failRemaining        int
	recoverAfterFailures bool
	attempts             int
	calls                []publishCall
}

func (f *fakePublisher) Publish(_ context.Context, topic string, key string, payload []byte, properties map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.err != nil {
		if !f.recoverAfterFailures {
			return f.err
		}
		if f.failRemaining > 0 {
			f.failRemaining--
			return f.err
		}
		f.err = nil
	}
	copiedProps := make(map[string]string, len(properties))
	for k, v := range properties {
		copiedProps[k] = v
	}
	f.calls = append(f.calls, publishCall{
		topic:      topic,
		key:        key,
		payload:    string(payload),
		properties: copiedProps,
	})
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func (f *fakePublisher) Ready(_ context.Context, _ []string) error {
	return f.readyErr
}

func testServerConfig() config.ServerConfig {
	return config.ServerConfig{
		MaxBodyBytes:  4096,
		MaxBatchItems: 10,
		PublishRetry: config.RetryConfig{
			MaxAttempts:    1,
			InitialBackoff: "1ms",
			MaxBackoff:     "1ms",
		},
	}
}

func TestHandleEventsPublishesEachItem(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRoutes{"mss_tag_push_event_test": "persistent://public/default/mss_tag_push_event_test"},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)

	body := []byte(`{
		"dataSet": "mss_tag_push_event_test",
		"data": [
			{"tenantId":"95842832","uuId":"incident-1","alertTag":"{}"},
			{"tenantId":"95842832","uuId":"incident-2","alertTag":"{}"}
		]
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response responseBody
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || !response.Success || response.Code != 0 {
		t.Fatalf("response = %+v", response)
	}
	if len(publisher.calls) != 2 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
	if publisher.calls[0].topic != "persistent://public/default/mss_tag_push_event_test" {
		t.Fatalf("topic = %q", publisher.calls[0].topic)
	}
	if publisher.calls[0].key != "incident-1" || publisher.calls[1].key != "incident-2" {
		t.Fatalf("keys = %q, %q", publisher.calls[0].key, publisher.calls[1].key)
	}
	if publisher.calls[0].properties["dataSet"] != "mss_tag_push_event_test" {
		t.Fatalf("dataSet property = %q", publisher.calls[0].properties["dataSet"])
	}
	if publisher.calls[0].properties["tenantId"] != "95842832" {
		t.Fatalf("tenantId property = %q", publisher.calls[0].properties["tenantId"])
	}
}

func TestRegisterSupportsOuterAlertAddPath(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRoutes{"mss_tag_push_event_test": "persistent://public/default/mss_tag_push_event_test"},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	mux := http.NewServeMux()
	handler.Register(mux)

	body := []byte(`{"dataSet":"mss_tag_push_event_test","data":[{"tenantId":"95842832","uuId":"incident-1"}]}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/gop/gop-data-service/api/v1/mss/web/alert/outer/add", bytes.NewReader(body))

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
	if publisher.calls[0].key != "incident-1" {
		t.Fatalf("key = %q", publisher.calls[0].key)
	}
}

func TestHandleEventsRetriesPublishFailure(t *testing.T) {
	publisher := &fakePublisher{
		err:                  errors.New("temporary pulsar error"),
		failRemaining:        2,
		recoverAfterFailures: true,
	}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		config.ServerConfig{
			MaxBodyBytes:  4096,
			MaxBatchItems: 10,
			PublishRetry: config.RetryConfig{
				MaxAttempts:    3,
				InitialBackoff: "1ms",
				MaxBackoff:     "2ms",
			},
		},
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"uuId":"u1"}]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if publisher.attempts != 3 {
		t.Fatalf("attempts = %d", publisher.attempts)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
}

func TestHandleEventsRequiresBearerTokenWhenAuthEnabled(t *testing.T) {
	publisher := &fakePublisher{}
	cfg := testServerConfig()
	cfg.Auth = config.AuthConfig{
		Enabled: true,
		BearerTokenConfig: config.BearerTokenConfig{
			BearerToken: "secret-token",
		},
	}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		cfg,
		log.New(io.Discard, "", 0),
	)
	body := []byte(`{"dataSet":"ds","data":[{"uuId":"u1"}]}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("www-authenticate = %q", rec.Header().Get("WWW-Authenticate"))
	}
	if strings.Contains(rec.Body.String(), "ds") || strings.Contains(rec.Body.String(), "topic") {
		t.Fatalf("unauthorized body leaked route data: %s", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer wrong-token")
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
}

func TestHandleEventsRejectsRouteBearerTokenWhenGlobalTokenMissing(t *testing.T) {
	publisher := &fakePublisher{}
	cfg := testServerConfig()
	cfg.Auth = config.AuthConfig{Enabled: true}
	handler := NewHandler(
		staticRouteEntries{
			"ds-a": {
				Topic: "topic-a",
				Auth:  config.BearerTokenConfig{BearerToken: "token-a"},
			},
			"ds-b": {
				Topic: "topic-b",
				Auth:  config.BearerTokenConfig{BearerToken: "token-b"},
			},
		},
		publisher,
		cfg,
		log.New(io.Discard, "", 0),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds-a","data":[{"uuId":"u1"}]}`)))
	req.Header.Set("Authorization", "Bearer token-b")
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong route token status = %d, body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds-a","data":[{"uuId":"u1"}]}`)))
	req.Header.Set("Authorization", "Bearer token-a")
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("route token status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(publisher.calls) != 0 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
}

func TestHandleEventsSkipsFieldValidationWhenRouteValidationMissing(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRouteEntries{"ds": {Topic: "topic"}},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"alertTag":"{}"}]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleEventsAppliesRouteValidation(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRouteEntries{
			"ds": {
				Topic: "topic",
				Validation: config.RouteValidation{
					MaxBatchItems:  1,
					RequiredFields: []string{"tenantId", "uuId"},
				},
			},
		},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"tenantId":"95842832"}]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "uuId is required") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if len(publisher.calls) != 0 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
}

func TestHandleEventsAppliesRouteMaxBodyBytes(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRouteEntries{
			"ds": {
				Topic: "topic",
				Validation: config.RouteValidation{
					MaxBodyBytes: 10,
				},
			},
		},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{}]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleEventsCircuitBreakerFailsFast(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("pulsar down")}
	cfg := testServerConfig()
	cfg.CircuitBreaker = config.CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 1,
		OpenDuration:     "1m",
	}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		cfg,
		log.New(io.Discard, "", 0),
	)
	reqBody := []byte(`{"dataSet":"ds","data":[{"uuId":"u1"}]}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d, body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	handler.handleEvents(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("second status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if publisher.attempts != 1 {
		t.Fatalf("attempts = %d", publisher.attempts)
	}
	if !strings.Contains(rec.Body.String(), "temporarily unavailable") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestMetricsEndpointReportsPublishMetrics(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	mux := http.NewServeMux()
	handler.Register(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"uuId":"u1"}]}`)))
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "sr_forwarder_publish_success_total") {
		t.Fatalf("metrics body = %s", body)
	}
	if !strings.Contains(body, "sr_forwarder_accepted_items_total") {
		t.Fatalf("metrics body = %s", body)
	}
}

func TestHandleEventsRejectsUnknownDataSet(t *testing.T) {
	handler := NewHandler(staticRoutes{}, &fakePublisher{}, testServerConfig(), log.New(io.Discard, "", 0))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"missing","data":[{}]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	key := `sr_forwarder_rejected_requests_total{dataSet="unknown",reason="route_not_found"}`
	if handler.metrics.counters[key] != 1 {
		t.Fatalf("unknown dataSet metric = %d", handler.metrics.counters[key])
	}
	badKey := `sr_forwarder_rejected_requests_total{dataSet="missing",reason="route_not_found"}`
	if handler.metrics.counters[badKey] != 0 {
		t.Fatalf("raw dataSet metric = %d", handler.metrics.counters[badKey])
	}
}

func TestHandleEventsRejectsMalformedJSON(t *testing.T) {
	handler := NewHandler(staticRoutes{}, &fakePublisher{}, testServerConfig(), log.New(io.Discard, "", 0))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleEventsStopsOnPublishError(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("pulsar down")}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"uuId":"u1"}]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var body responseBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Accepted != 0 {
		t.Fatalf("accepted = %d", body.Accepted)
	}
	if body.OK || body.Success || body.Code != 1 {
		t.Fatalf("body = %+v", body)
	}
}

func TestHandleEventsRedactsTenantAndUUIDInPublishLogs(t *testing.T) {
	var logs bytes.Buffer
	publisher := &fakePublisher{err: errors.New("pulsar down")}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		testServerConfig(),
		log.New(&logs, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"tenantId":"95842832","uuId":"incident-7e4ab624-cf12-4878-bf4d-4091e62d1f51"}]}`)))

	handler.handleEvents(rec, req)

	body := logs.String()
	if strings.Contains(body, "95842832") || strings.Contains(body, "incident-7e4ab624-cf12-4878-bf4d-4091e62d1f51") {
		t.Fatalf("log leaked sensitive ids: %s", body)
	}
	if !strings.Contains(body, "958***832") || !strings.Contains(body, "inc***f51") {
		t.Fatalf("log did not include redacted ids: %s", body)
	}
}

func TestHandleEventsPrevalidatesAllItemsBeforePublishing(t *testing.T) {
	publisher := &fakePublisher{}
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		publisher,
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{"dataSet":"ds","data":[{"uuId":"u1"},1]}`)))

	handler.handleEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(publisher.calls) != 0 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
	}
	if !strings.Contains(rec.Body.String(), "data[1] is not a valid object") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestReadyzChecksPublisher(t *testing.T) {
	handler := NewHandler(
		staticRoutes{"ds": "topic"},
		&fakePublisher{readyErr: errors.New("pulsar down")},
		testServerConfig(),
		log.New(io.Discard, "", 0),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	handler.handleReady(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}
