package pulsarpub

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
)

type fakeClient struct {
	mu            sync.Mutex
	producer      pulsar.Producer
	createCount   int
	partitionsErr error
}

func (f *fakeClient) CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount++
	return f.producer, nil
}

func (f *fakeClient) TopicPartitions(string) ([]string, error) {
	return []string{"topic"}, f.partitionsErr
}

func (f *fakeClient) Close() {}

type fakeProducer struct {
	sendErr error
	closed  bool
}

func (f *fakeProducer) Topic() string { return "topic" }
func (f *fakeProducer) Name() string  { return "fake" }
func (f *fakeProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, f.sendErr
}
func (f *fakeProducer) SendAsync(ctx context.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	callback(nil, msg, f.sendErr)
}
func (f *fakeProducer) LastSequenceID() int64              { return -1 }
func (f *fakeProducer) Flush() error                       { return nil }
func (f *fakeProducer) FlushWithCtx(context.Context) error { return nil }
func (f *fakeProducer) Close()                             { f.closed = true }

func TestPublisherRejectsPublishAfterClose(t *testing.T) {
	p := &Publisher{
		producers: make(map[string]*producerSlot),
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	err := p.Publish(context.Background(), "topic", "key", []byte("payload"), nil)
	if !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("publish after close error = %v", err)
	}
}

func TestPublishFailureEvictsProducerForRecreation(t *testing.T) {
	failed := &fakeProducer{sendErr: errors.New("broker unavailable")}
	client := &fakeClient{producer: failed}
	p := &Publisher{client: client, producers: make(map[string]*producerSlot)}

	if err := p.Publish(context.Background(), "topic", "key", []byte("payload"), nil); err == nil {
		t.Fatal("expected publish error")
	}
	if !failed.closed {
		t.Fatal("failed producer was not closed")
	}
	slot := p.producers["topic"]
	if slot == nil || slot.producer != nil {
		t.Fatalf("producer slot after failure = %+v", slot)
	}

	replacement := &fakeProducer{}
	client.producer = replacement
	if err := p.Publish(context.Background(), "topic", "key", []byte("payload"), nil); err != nil {
		t.Fatalf("publish with replacement: %v", err)
	}
	if client.createCount != 2 {
		t.Fatalf("create count = %d", client.createCount)
	}
}

func TestReadyPerformsMetadataLookupAndEvictsCachedProducerOnFailure(t *testing.T) {
	cached := &fakeProducer{}
	client := &fakeClient{partitionsErr: errors.New("lookup failed")}
	p := &Publisher{
		client: client,
		producers: map[string]*producerSlot{
			"topic": {producer: cached},
		},
	}

	if err := p.Ready(context.Background(), []string{"topic"}); err == nil {
		t.Fatal("expected readiness error")
	}
	if !cached.closed {
		t.Fatal("cached producer was not closed")
	}
	if p.producers["topic"].producer != nil {
		t.Fatal("cached producer was not evicted")
	}
}
