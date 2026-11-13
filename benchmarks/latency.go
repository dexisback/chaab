package benchmarks

import (
	"sort"
	"sync"
	"time"
)

type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

func NewLatencyRecorder(capacity int) *LatencyRecorder {
	return &LatencyRecorder{
		samples: make([]time.Duration, 0, capacity),
	}
}

func (r *LatencyRecorder) Record(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d) // append duration sample
	r.mu.Unlock()
}

type LatencySummary struct {
	Count int
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Min   time.Duration
	Max   time.Duration
}

func (r *LatencyRecorder) Summary() LatencySummary {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(r.samples)
	if n == 0 {
		return LatencySummary{}
	}

	sorted := make([]time.Duration, n)
	copy(sorted, r.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] }) // sort samples for percentiles

	return LatencySummary{
		Count: n,
		Min:   sorted[0],
		P50:   sorted[n*50/100],
		P95:   sorted[n*95/100],
		P99:   sorted[n*99/100],
		Max:   sorted[n-1],
	}
}
