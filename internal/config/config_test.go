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
	if cfg.Routes["ds"].Topic != "persistent://public/default/ds" {
		t.Fatalf("topic = %q", cfg.Routes["ds"].Topic)
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
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
