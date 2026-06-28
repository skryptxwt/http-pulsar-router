package forwarder

import "context"

type Publisher interface {
	Publish(ctx context.Context, topic string, key string, payload []byte, properties map[string]string) error
	Close() error
}

type RouteLookup interface {
	LookupTopic(dataSet string) (string, bool)
}
