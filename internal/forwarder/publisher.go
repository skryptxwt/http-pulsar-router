package forwarder

import (
	"context"

	"sr-forwarder/internal/config"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, key string, payload []byte, properties map[string]string) error
	Ready(ctx context.Context, topics []string) error
	Close() error
}

type RouteLookup interface {
	LookupTopic(dataSet string) (string, bool)
}

type RouteEntryLookup interface {
	LookupRoute(dataSet string) (config.RouteEntry, bool)
}

type ServerConfigLookup interface {
	ServerConfig() config.ServerConfig
}

type TopicLister interface {
	Topics() []string
}
