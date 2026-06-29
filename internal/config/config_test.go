package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndRoutes(t *testing.T) {
	path := writeConfig(t, `{
		"pulsar": {"url":"pulsar://127.0.0.1:6650"},
		"routes": {"ds": {"topic":"persistent://public/default/ds"}}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Addr != defaultAddr {
		t.Fatalf("addr = %q", cfg.Server.Addr)
	}
	if cfg.Server.MaxBodyBytes != defaultMaxBodyBytes {
		t.Fatalf("max body bytes = %d", cfg.Server.MaxBodyBytes)
	}
	if cfg.Server.PublishRetry.MaxAttempts != defaultRetryAttempts {
		t.Fatalf("retry attempts = %d", cfg.Server.PublishRetry.MaxAttempts)
	}
	if cfg.Server.PublishRetry.InitialBackoff != defaultInitialBackoff {
		t.Fatalf("initial backoff = %q", cfg.Server.PublishRetry.InitialBackoff)
	}
	if cfg.Server.PublishRetry.MaxBackoff != defaultMaxBackoff {
		t.Fatalf("max backoff = %q", cfg.Server.PublishRetry.MaxBackoff)
	}
	if cfg.Routes["ds"].Topic != "persistent://public/default/ds" {
		t.Fatalf("topic = %q", cfg.Routes["ds"].Topic)
	}
}

func TestLoadRejectsInvalidPublishRetry(t *testing.T) {
	path := writeConfig(t, `{
		"server": {
			"publishRetry": {
				"maxAttempts": 3,
				"initialBackoff": "2s",
				"maxBackoff": "1s"
			}
		},
		"pulsar": {"url":"pulsar://127.0.0.1:6650"},
		"routes": {"ds": {"topic":"persistent://public/default/ds"}}
	}`)

	if _, err := Load(path); err == nil {
		t.Fatal("expected invalid publish retry config")
	}
}

func TestStoreReloadIfChanged(t *testing.T) {
	path := writeConfig(t, `{
		"pulsar": {"url":"pulsar://127.0.0.1:6650"},
		"routes": {"ds": {"topic":"topic-a"}}
	}`)

	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if topic, ok := store.LookupTopic("ds"); !ok || topic != "topic-a" {
		t.Fatalf("initial topic = %q, ok = %v", topic, ok)
	}

	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(path, []byte(`{
		"server": {"publishRetry": {"maxAttempts": 5, "initialBackoff": "10ms", "maxBackoff": "50ms"}},
		"pulsar": {"url":"pulsar://127.0.0.1:6650"},
		"routes": {"ds": {"topic":"topic-b"}}
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	changed, err := store.ReloadIfChanged()
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected changed config")
	}
	if topic, ok := store.LookupTopic("ds"); !ok || topic != "topic-b" {
		t.Fatalf("reloaded topic = %q, ok = %v", topic, ok)
	}
	if retry := store.ServerConfig().PublishRetry; retry.MaxAttempts != 5 || retry.InitialBackoff != "10ms" || retry.MaxBackoff != "50ms" {
		t.Fatalf("reloaded retry = %+v", retry)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
