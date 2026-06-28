package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultAddr            = ":8080"
	defaultReadTimeout     = "5s"
	defaultWriteTimeout    = "10s"
	defaultShutdownTimeout = "15s"
	defaultOperationTO     = "5s"
	defaultConnectionTO    = "5s"
	defaultMaxBodyBytes    = int64(1 << 20)
	defaultMaxBatchItems   = 1000
)

type Config struct {
	Server ServerConfig          `json:"server"`
	Pulsar PulsarConfig          `json:"pulsar"`
	Routes map[string]RouteEntry `json:"routes"`
}

type ServerConfig struct {
	Addr            string `json:"addr"`
	ReadTimeout     string `json:"readTimeout"`
	WriteTimeout    string `json:"writeTimeout"`
	ShutdownTimeout string `json:"shutdownTimeout"`
	MaxBodyBytes    int64  `json:"maxBodyBytes"`
	MaxBatchItems   int    `json:"maxBatchItems"`
}

type PulsarConfig struct {
	URL               string `json:"url"`
	OperationTimeout  string `json:"operationTimeout"`
	ConnectionTimeout string `json:"connectionTimeout"`
	AuthToken         string `json:"authToken,omitempty"`
	AuthTokenFile     string `json:"authTokenFile,omitempty"`
}

type RouteEntry struct {
	Topic string `json:"topic"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if strings.TrimSpace(c.Server.Addr) == "" {
		return errors.New("server.addr is required")
	}
	if c.Server.MaxBodyBytes <= 0 {
		return errors.New("server.maxBodyBytes must be positive")
	}
	if c.Server.MaxBatchItems <= 0 {
		return errors.New("server.maxBatchItems must be positive")
	}
	if _, err := time.ParseDuration(c.Server.ReadTimeout); err != nil {
		return fmt.Errorf("server.readTimeout: %w", err)
	}
	if _, err := time.ParseDuration(c.Server.WriteTimeout); err != nil {
		return fmt.Errorf("server.writeTimeout: %w", err)
	}
	if _, err := time.ParseDuration(c.Server.ShutdownTimeout); err != nil {
		return fmt.Errorf("server.shutdownTimeout: %w", err)
	}
	if strings.TrimSpace(c.Pulsar.URL) == "" {
		return errors.New("pulsar.url is required")
	}
	if _, err := time.ParseDuration(c.Pulsar.OperationTimeout); err != nil {
		return fmt.Errorf("pulsar.operationTimeout: %w", err)
	}
	if _, err := time.ParseDuration(c.Pulsar.ConnectionTimeout); err != nil {
		return fmt.Errorf("pulsar.connectionTimeout: %w", err)
	}
	if len(c.Routes) == 0 {
		return errors.New("routes must not be empty")
	}
	for dataSet, route := range c.Routes {
		if strings.TrimSpace(dataSet) == "" {
			return errors.New("routes contains empty dataSet")
		}
		if strings.TrimSpace(route.Topic) == "" {
			return fmt.Errorf("routes.%s.topic is required", dataSet)
		}
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = defaultAddr
	}
	if c.Server.ReadTimeout == "" {
		c.Server.ReadTimeout = defaultReadTimeout
	}
	if c.Server.WriteTimeout == "" {
		c.Server.WriteTimeout = defaultWriteTimeout
	}
	if c.Server.ShutdownTimeout == "" {
		c.Server.ShutdownTimeout = defaultShutdownTimeout
	}
	if c.Server.MaxBodyBytes == 0 {
		c.Server.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.Server.MaxBatchItems == 0 {
		c.Server.MaxBatchItems = defaultMaxBatchItems
	}
	if c.Pulsar.OperationTimeout == "" {
		c.Pulsar.OperationTimeout = defaultOperationTO
	}
	if c.Pulsar.ConnectionTimeout == "" {
		c.Pulsar.ConnectionTimeout = defaultConnectionTO
	}
}

func (s ServerConfig) ReadTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(s.ReadTimeout)
	return d
}

func (s ServerConfig) WriteTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(s.WriteTimeout)
	return d
}

func (s ServerConfig) ShutdownTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(s.ShutdownTimeout)
	return d
}

func (p PulsarConfig) OperationTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(p.OperationTimeout)
	return d
}

func (p PulsarConfig) ConnectionTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(p.ConnectionTimeout)
	return d
}
