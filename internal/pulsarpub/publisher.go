package pulsarpub

import (
	"context"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"

	"sr-forwarder/internal/config"
)

type Publisher struct {
	client    pulsar.Client
	mu        sync.Mutex
	producers map[string]pulsar.Producer
}

func New(cfg config.PulsarConfig) (*Publisher, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               cfg.URL,
		OperationTimeout:  cfg.OperationTimeoutDuration(),
		ConnectionTimeout: cfg.ConnectionTimeoutDuration(),
	})
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client:    client,
		producers: make(map[string]pulsar.Producer),
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
	defer p.mu.Unlock()

	for _, producer := range p.producers {
		producer.Close()
	}
	p.client.Close()
	return nil
}

func (p *Publisher) producer(topic string) (pulsar.Producer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if producer, ok := p.producers[topic]; ok {
		return producer, nil
	}
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, err
	}
	p.producers[topic] = producer
	return producer, nil
}
