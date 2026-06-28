package config

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Store struct {
	path    string
	modTime time.Time
	mu      sync.Mutex
	value   atomic.Value
}

func NewStore(path string) (*Store, error) {
	cfg, stat, err := loadWithStat(path)
	if err != nil {
		return nil, err
	}

	s := &Store{
		path:    path,
		modTime: stat.ModTime(),
	}
	s.value.Store(cfg)
	return s, nil
}

func (s *Store) Snapshot() *Config {
	cfg, _ := s.value.Load().(*Config)
	return cfg
}

func (s *Store) LookupTopic(dataSet string) (string, bool) {
	cfg := s.Snapshot()
	if cfg == nil {
		return "", false
	}
	route, ok := cfg.Routes[dataSet]
	if !ok {
		return "", false
	}
	return route.Topic, true
}

func (s *Store) ReloadIfChanged() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stat, err := os.Stat(s.path)
	if err != nil {
		return false, err
	}
	if !stat.ModTime().After(s.modTime) {
		return false, nil
	}

	cfg, stat, err := loadWithStat(s.path)
	if err != nil {
		return false, err
	}
	s.value.Store(cfg)
	s.modTime = stat.ModTime()
	return true, nil
}

func (s *Store) Watch(ctx context.Context, interval time.Duration, logger *log.Logger) {
	if interval <= 0 {
		interval = 2 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := s.ReloadIfChanged()
			if err != nil {
				logger.Printf("config reload failed: %v", err)
				continue
			}
			if changed {
				logger.Printf("config reloaded from %s", s.path)
			}
		}
	}
}

func loadWithStat(path string) (*Config, os.FileInfo, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, nil, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	return cfg, stat, nil
}
