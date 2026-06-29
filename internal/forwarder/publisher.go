package forwarder

import (
	"context"

	"sr-forwarder/internal/config"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, key string, payload []byte, properties map[string]string) error
	Close() error
}

type RouteLookup interface {
	LookupTopic(dataSet string) (string, bool)
}

type ServerConfigLookup interface {
	ServerConfig() config.ServerConfig
}
