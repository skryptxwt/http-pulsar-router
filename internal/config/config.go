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
	defaultRetryAttempts   = 3
	defaultInitialBackoff  = "100ms"
	defaultMaxBackoff      = "2s"
	defaultCBThreshold     = 20
	defaultCBOpenDuration  = "10s"
)

type Config struct {
	Server ServerConfig          `json:"server"`
	Pulsar PulsarConfig          `json:"pulsar"`
	Routes map[string]RouteEntry `json:"routes"`
}

type ServerConfig struct {
	Addr            string               `json:"addr"`
	ReadTimeout     string               `json:"readTimeout"`
	WriteTimeout    string               `json:"writeTimeout"`
	ShutdownTimeout string               `json:"shutdownTimeout"`
	MaxBodyBytes    int64                `json:"maxBodyBytes"`
	MaxBatchItems   int                  `json:"maxBatchItems"`
	Auth            AuthConfig           `json:"auth"`
	PublishRetry    RetryConfig          `json:"publishRetry"`
	CircuitBreaker  CircuitBreakerConfig `json:"circuitBreaker"`
}

type AuthConfig struct {
	Enabled         bool   `json:"enabled"`
	BearerToken     string `json:"bearerToken,omitempty"`
	BearerTokenFile string `json:"bearerTokenFile,omitempty"`
}

type RetryConfig struct {
	MaxAttempts    int    `json:"maxAttempts"`
	InitialBackoff string `json:"initialBackoff"`
	MaxBackoff     string `json:"maxBackoff"`
}

type CircuitBreakerConfig struct {
	Enabled          bool   `json:"enabled"`
	FailureThreshold int    `json:"failureThreshold"`
	OpenDuration     string `json:"openDuration"`
}

type PulsarConfig struct {
	URL               string `json:"url"`
	OperationTimeout  string `json:"operationTimeout"`
	ConnectionTimeout string `json:"connectionTimeout"`
	AuthToken         string `json:"authToken,omitempty"`
	AuthTokenFile     string `json:"authTokenFile,omitempty"`
}

type RouteEntry struct {
	Topic      string          `json:"topic"`
	Validation RouteValidation `json:"validation,omitempty"`
}

type RouteValidation struct {
	MaxBodyBytes   int64    `json:"maxBodyBytes,omitempty"`
	MaxBatchItems  int      `json:"maxBatchItems,omitempty"`
	RequiredFields []string `json:"requiredFields,omitempty"`
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
	if c.Server.Auth.Enabled && strings.TrimSpace(c.Server.Auth.BearerToken) == "" && strings.TrimSpace(c.Server.Auth.BearerTokenFile) == "" {
		return errors.New("server.auth.bearerToken or server.auth.bearerTokenFile is required when auth is enabled")
	}
	if c.Server.PublishRetry.MaxAttempts <= 0 {
		return errors.New("server.publishRetry.maxAttempts must be positive")
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
	initialBackoff, err := time.ParseDuration(c.Server.PublishRetry.InitialBackoff)
	if err != nil {
		return fmt.Errorf("server.publishRetry.initialBackoff: %w", err)
	}
	if initialBackoff <= 0 {
		return errors.New("server.publishRetry.initialBackoff must be positive")
	}
	maxBackoff, err := time.ParseDuration(c.Server.PublishRetry.MaxBackoff)
	if err != nil {
		return fmt.Errorf("server.publishRetry.maxBackoff: %w", err)
	}
	if maxBackoff <= 0 {
		return errors.New("server.publishRetry.maxBackoff must be positive")
	}
	if maxBackoff < initialBackoff {
		return errors.New("server.publishRetry.maxBackoff must be greater than or equal to initialBackoff")
	}
	if c.Server.CircuitBreaker.Enabled {
		if c.Server.CircuitBreaker.FailureThreshold <= 0 {
			return errors.New("server.circuitBreaker.failureThreshold must be positive when enabled")
		}
		openDuration, err := time.ParseDuration(c.Server.CircuitBreaker.OpenDuration)
		if err != nil {
			return fmt.Errorf("server.circuitBreaker.openDuration: %w", err)
		}
		if openDuration <= 0 {
			return errors.New("server.circuitBreaker.openDuration must be positive when enabled")
		}
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
		if route.Validation.MaxBodyBytes < 0 {
			return fmt.Errorf("routes.%s.validation.maxBodyBytes must not be negative", dataSet)
		}
		if route.Validation.MaxBatchItems < 0 {
			return fmt.Errorf("routes.%s.validation.maxBatchItems must not be negative", dataSet)
		}
		for idx, field := range route.Validation.RequiredFields {
			if strings.TrimSpace(field) == "" {
				return fmt.Errorf("routes.%s.validation.requiredFields[%d] must not be empty", dataSet, idx)
			}
		}
	}
	return nil
}

func (c *Config) applyDefaults() {
	c.Server = c.Server.WithDefaults()
	if c.Pulsar.OperationTimeout == "" {
		c.Pulsar.OperationTimeout = defaultOperationTO
	}
	if c.Pulsar.ConnectionTimeout == "" {
		c.Pulsar.ConnectionTimeout = defaultConnectionTO
	}
	for dataSet, route := range c.Routes {
		for idx, field := range route.Validation.RequiredFields {
			route.Validation.RequiredFields[idx] = strings.TrimSpace(field)
		}
		c.Routes[dataSet] = route
	}
}

func (s ServerConfig) WithDefaults() ServerConfig {
	if s.Addr == "" {
		s.Addr = defaultAddr
	}
	if s.ReadTimeout == "" {
		s.ReadTimeout = defaultReadTimeout
	}
	if s.WriteTimeout == "" {
		s.WriteTimeout = defaultWriteTimeout
	}
	if s.ShutdownTimeout == "" {
		s.ShutdownTimeout = defaultShutdownTimeout
	}
	if s.MaxBodyBytes == 0 {
		s.MaxBodyBytes = defaultMaxBodyBytes
	}
	if s.MaxBatchItems == 0 {
		s.MaxBatchItems = defaultMaxBatchItems
	}
	if s.PublishRetry.MaxAttempts == 0 {
		s.PublishRetry.MaxAttempts = defaultRetryAttempts
	}
	if s.PublishRetry.InitialBackoff == "" {
		s.PublishRetry.InitialBackoff = defaultInitialBackoff
	}
	if s.PublishRetry.MaxBackoff == "" {
		s.PublishRetry.MaxBackoff = defaultMaxBackoff
	}
	if s.CircuitBreaker.Enabled {
		if s.CircuitBreaker.FailureThreshold == 0 {
			s.CircuitBreaker.FailureThreshold = defaultCBThreshold
		}
		if s.CircuitBreaker.OpenDuration == "" {
			s.CircuitBreaker.OpenDuration = defaultCBOpenDuration
		}
	}
	return s
}

func (r RetryConfig) InitialBackoffDuration() time.Duration {
	d, _ := time.ParseDuration(r.InitialBackoff)
	return d
}

func (r RetryConfig) MaxBackoffDuration() time.Duration {
	d, _ := time.ParseDuration(r.MaxBackoff)
	return d
}

func (c CircuitBreakerConfig) OpenDurationValue() time.Duration {
	d, _ := time.ParseDuration(c.OpenDuration)
	return d
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
