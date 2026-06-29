package forwarder

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	mu       sync.Mutex
	counters map[string]uint64
	gauges   map[string]float64
	sums     map[string]float64
	counts   map[string]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		counters: make(map[string]uint64),
		gauges:   make(map[string]float64),
		sums:     make(map[string]float64),
		counts:   make(map[string]uint64),
	}
}

func (m *Metrics) Inc(name string, labels map[string]string) {
	m.Add(name, labels, 1)
}

func (m *Metrics) Add(name string, labels map[string]string, value uint64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[metricKey(name, labels)] += value
}

func (m *Metrics) SetGauge(name string, labels map[string]string, value float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[metricKey(name, labels)] = value
}

func (m *Metrics) ObserveDuration(name string, labels map[string]string, duration time.Duration) {
	if m == nil {
		return
	}
	key := metricKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sums[key] += duration.Seconds()
	m.counts[key]++
}

func (m *Metrics) WritePrometheus(w io.Writer) {
	m.mu.Lock()
	counters := copyUintMap(m.counters)
	gauges := copyFloatMap(m.gauges)
	sums := copyFloatMap(m.sums)
	counts := copyUintMap(m.counts)
	m.mu.Unlock()

	writeUintMetrics(w, counters)
	writeFloatMetrics(w, gauges, "")
	writeFloatMetrics(w, sums, "_sum")
	writeUintMetricsWithSuffix(w, counts, "_count")
}

func writeUintMetrics(w io.Writer, values map[string]uint64) {
	writeUintMetricsWithSuffix(w, values, "")
}

func writeUintMetricsWithSuffix(w io.Writer, values map[string]uint64, suffix string) {
	keys := sortedKeys(values)
	for _, key := range keys {
		name, labels := splitMetricKey(key)
		fmt.Fprintf(w, "%s%s%s %d\n", name, suffix, labels, values[key])
	}
}

func writeFloatMetrics(w io.Writer, values map[string]float64, suffix string) {
	keys := sortedKeys(values)
	for _, key := range keys {
		name, labels := splitMetricKey(key)
		fmt.Fprintf(w, "%s%s%s %.6f\n", name, suffix, labels, values[key])
	}
}

func metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+escapeLabelValue(labels[key]))
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func splitMetricKey(key string) (string, string) {
	idx := strings.IndexByte(key, '{')
	if idx < 0 {
		return key, ""
	}
	return key[:idx], key[idx:]
}

func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")
	return "\"" + replacer.Replace(value) + "\""
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func copyUintMap(values map[string]uint64) map[string]uint64 {
	copied := make(map[string]uint64, len(values))
	for key, value := range values {
		copied[key] = value
	}
	return copied
}

func copyFloatMap(values map[string]float64) map[string]float64 {
	copied := make(map[string]float64, len(values))
	for key, value := range values {
		copied[key] = value
	}
	return copied
}
