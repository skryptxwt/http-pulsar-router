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
	"sync"
	"testing"

	"sr-forwarder/internal/config"
)

type staticRoutes map[string]string

func (s staticRoutes) LookupTopic(dataSet string) (string, bool) {
	topic, ok := s[dataSet]
	return topic, ok
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

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
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

	if rec.Code != http.StatusAccepted {
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

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if publisher.attempts != 3 {
		t.Fatalf("attempts = %d", publisher.attempts)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("publish calls = %d", len(publisher.calls))
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
}
