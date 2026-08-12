package pulsarpub

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"

	"sr-forwarder/internal/config"
)

var ErrPublisherClosed = errors.New("publisher is closed")

type Publisher struct {
	client    pulsarClient
	mu        sync.Mutex
	closed    bool
	producers map[string]*producerSlot
}

type pulsarClient interface {
	CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error)
	TopicPartitions(string) ([]string, error)
	Close()
}

type producerSlot struct {
	mu       sync.Mutex
	producer pulsar.Producer
}

func New(cfg config.PulsarConfig) (*Publisher, error) {
	options := pulsar.ClientOptions{
		URL:               cfg.URL,
		OperationTimeout:  cfg.OperationTimeoutDuration(),
		ConnectionTimeout: cfg.ConnectionTimeoutDuration(),
	}
	if token, err := authToken(cfg); err != nil {
		return nil, err
	} else if token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(token)
	}

	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client:    client,
		producers: make(map[string]*producerSlot),
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, key string, payload []byte, properties map[string]string) error {
	producer, err := p.producer(topic)
	if err != nil {
		return err
	}
	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Key:        key,
		Payload:    payload,
		Properties: properties,
	})
	if err != nil {
		p.invalidateProducer(topic, producer)
	}
	return err
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	slots := make([]*producerSlot, 0, len(p.producers))
	for _, producer := range p.producers {
		slots = append(slots, producer)
	}
	p.mu.Unlock()

	for _, slot := range slots {
		slot.mu.Lock()
		if slot.producer != nil {
			slot.producer.Close()
		}
		slot.mu.Unlock()
	}
	if p.client != nil {
		p.client.Close()
	}
	return nil
}

func (p *Publisher) Ready(ctx context.Context, topics []string) error {
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		if err := p.lookupTopic(ctx, topic); err != nil {
			p.invalidateProducer(topic, nil)
			return err
		}
	}
	return nil
}

func (p *Publisher) lookupTopic(ctx context.Context, topic string) error {
	result := make(chan error, 1)
	go func() {
		_, err := p.client.TopicPartitions(topic)
		result <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

// invalidateProducer removes a failed producer so the next publish attempt
// creates a fresh producer. If failed is non-nil, a concurrently replaced
// producer is preserved.
func (p *Publisher) invalidateProducer(topic string, failed pulsar.Producer) {
	slot, err := p.producerSlot(topic)
	if err != nil {
		return
	}
	slot.mu.Lock()
	if slot.producer == nil || (failed != nil && slot.producer != failed) {
		slot.mu.Unlock()
		return
	}
	stale := slot.producer
	slot.producer = nil
	slot.mu.Unlock()
	stale.Close()
}

func (p *Publisher) producer(topic string) (pulsar.Producer, error) {
	slot, err := p.producerSlot(topic)
	if err != nil {
		return nil, err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()

	if slot.producer != nil {
		return slot.producer, nil
	}
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, err
	}
	slot.producer = producer
	return producer, nil
}

func (p *Publisher) producerSlot(topic string) (*producerSlot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPublisherClosed
	}
	if p.producers == nil {
		p.producers = make(map[string]*producerSlot)
	}
	slot, ok := p.producers[topic]
	if !ok {
		slot = &producerSlot{}
		p.producers[topic] = slot
	}
	return slot, nil
}

func authToken(cfg config.PulsarConfig) (string, error) {
	if strings.TrimSpace(cfg.AuthTokenFile) != "" {
		data, err := os.ReadFile(cfg.AuthTokenFile)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(data)), nil
	}
	return strings.TrimSpace(cfg.AuthToken), nil
}
