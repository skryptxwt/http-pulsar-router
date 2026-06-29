package forwarder

import (
	"sync"
	"time"

	"sr-forwarder/internal/config"
)

type CircuitBreaker struct {
	mu     sync.Mutex
	states map[string]*circuitState
}

type circuitState struct {
	failures  int
	openUntil time.Time
}

func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{states: make(map[string]*circuitState)}
}

func (c *CircuitBreaker) Allow(key string, cfg config.CircuitBreakerConfig, now time.Time) bool {
	if c == nil || !cfg.Enabled {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.state(key)
	if state.openUntil.IsZero() || !now.Before(state.openUntil) {
		return true
	}
	return false
}

func (c *CircuitBreaker) RecordSuccess(key string, cfg config.CircuitBreakerConfig) {
	if c == nil || !cfg.Enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.state(key)
	state.failures = 0
	state.openUntil = time.Time{}
}

func (c *CircuitBreaker) RecordFailure(key string, cfg config.CircuitBreakerConfig, now time.Time) bool {
	if c == nil || !cfg.Enabled {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.state(key)
	state.failures++
	if state.failures < cfg.FailureThreshold {
		return false
	}

	state.failures = 0
	state.openUntil = now.Add(cfg.OpenDurationValue())
	return true
}

func (c *CircuitBreaker) IsOpen(key string, cfg config.CircuitBreakerConfig, now time.Time) bool {
	return !c.Allow(key, cfg, now)
}

func (c *CircuitBreaker) state(key string) *circuitState {
	state, ok := c.states[key]
	if !ok {
		state = &circuitState{}
		c.states[key] = state
	}
	return state
}
