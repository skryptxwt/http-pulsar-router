package pulsarpub

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"

	"sr-forwarder/internal/config"
)

type Publisher struct {
	client    pulsar.Client
	mu        sync.Mutex
	producers map[string]*producerSlot
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
	return err
}

func (p *Publisher) Close() error {
	p.mu.Lock()
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
	p.client.Close()
	return nil
}

func (p *Publisher) Ready(ctx context.Context, topics []string) error {
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		producer, err := p.producer(topic)
		if err != nil {
			return err
		}
		if err := producer.FlushWithCtx(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) producer(topic string) (pulsar.Producer, error) {
	slot := p.producerSlot(topic)
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

func (p *Publisher) producerSlot(topic string) *producerSlot {
	p.mu.Lock()
	defer p.mu.Unlock()

	slot, ok := p.producers[topic]
	if !ok {
		slot = &producerSlot{}
		p.producers[topic] = slot
	}
	return slot
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
