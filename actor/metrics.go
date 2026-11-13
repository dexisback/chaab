package actor

import (
	"sync/atomic"
	"time"
)

// MetricsCollector defines telemetry hooks invoked by the actor runtime.
// Third-party integrations (e.g. Prometheus, OpenTelemetry, Datadog)
// implement this interface to observe runtime performance and health.
type MetricsCollector interface {
	// MessageProcessed records message execution duration and completion status.
	MessageProcessed(actorID string, duration time.Duration, err error)

	// ActorRestarted records when an actor fails and is restarted by supervision.
	ActorRestarted(actorID string)

	// TaskStolen records when an idle worker steals a task from another worker's queue.
	TaskStolen(thiefWorkerID int, victimWorkerID int)
}

// SystemStats represents a snapshot of runtime metrics.
type SystemStats struct {
	ActiveActors      int
	TotalProcessed    uint64
	TotalRestarts     uint64
	TotalTaskSteals   uint64
	TotalDeadLetters  uint64
}

// DefaultMetricsCollector provides a lock-free, atomic in-memory telemetry collector.
type DefaultMetricsCollector struct {
	totalProcessed  atomic.Uint64
	totalRestarts   atomic.Uint64
	totalTaskSteals atomic.Uint64
}

// NewDefaultMetricsCollector creates a new atomic in-memory metrics collector.
func NewDefaultMetricsCollector() *DefaultMetricsCollector {
	return &DefaultMetricsCollector{}
}

func (m *DefaultMetricsCollector) MessageProcessed(actorID string, duration time.Duration, err error) {
	m.totalProcessed.Add(1)
}

func (m *DefaultMetricsCollector) ActorRestarted(actorID string) {
	m.totalRestarts.Add(1)
}

func (m *DefaultMetricsCollector) TaskStolen(thiefWorkerID int, victimWorkerID int) {
	m.totalTaskSteals.Add(1)
}

// TotalProcessed returns the cumulative count of processed messages.
func (m *DefaultMetricsCollector) TotalProcessed() uint64 {
	return m.totalProcessed.Load()
}

// TotalRestarts returns the cumulative count of supervised actor restarts.
func (m *DefaultMetricsCollector) TotalRestarts() uint64 {
	return m.totalRestarts.Load()
}

// TotalTaskSteals returns the cumulative count of work-stealing occurrences.
func (m *DefaultMetricsCollector) TotalTaskSteals() uint64 {
	return m.totalTaskSteals.Load()
}

// noopMetricsCollector discards all metrics events.
type noopMetricsCollector struct{}

func (n noopMetricsCollector) MessageProcessed(actorID string, duration time.Duration, err error) {}
func (n noopMetricsCollector) ActorRestarted(actorID string)                                      {}
func (n noopMetricsCollector) TaskStolen(thiefWorkerID int, victimWorkerID int)                   {}
