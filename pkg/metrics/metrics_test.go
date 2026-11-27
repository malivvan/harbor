package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// recorder is a test Observer that captures every call.
type recorder struct {
	counters   map[string]int64
	histograms map[string][]float64
	gauges     map[string]float64
	spans      []string
}

func newRecorder() *recorder {
	return &recorder{
		counters:   map[string]int64{},
		histograms: map[string][]float64{},
		gauges:     map[string]float64{},
	}
}

func (r *recorder) Counter(name string, delta int64, _ Labels) {
	r.counters[name] += delta
}

func (r *recorder) Histogram(name string, value float64, _ Labels) {
	r.histograms[name] = append(r.histograms[name], value)
}

func (r *recorder) Gauge(name string, value float64, _ Labels) {
	r.gauges[name] = value
}

func (r *recorder) StartSpan(ctx context.Context, name string, _ Labels) (context.Context, func(error)) {
	r.spans = append(r.spans, name)
	return ctx, func(error) {}
}

// TestNilObserverIsSafe pins the contract that every helper tolerates a nil
// observer, so an uninstrumented store never needs a guard at the call site.
func TestNilObserverIsSafe(t *testing.T) {
	var obs Observer

	require.NotPanics(t, func() {
		Counter(obs, MetricTxTotal, 1, nil)
		Histogram(obs, MetricTxDuration, 1, nil)
		Gauge(obs, MetricCacheSize, 1, nil)

		ctx, end := StartSpan(context.Background(), obs, "op", nil)
		require.NotNil(t, ctx)
		end(nil)
	})
}

// TestHelpersForwardToObserver checks the helpers reach the observer and that
// the Nop observer satisfies the interface.
func TestHelpersForwardToObserver(t *testing.T) {
	rec := newRecorder()

	Counter(rec, MetricTxTotal, 3, Labels{LabelStore: "db"})
	Histogram(rec, MetricTxDuration, 42, Labels{LabelStore: "db"})
	Gauge(rec, MetricCacheSize, 7, Labels{LabelStore: "fs"})
	_, end := StartSpan(context.Background(), rec, "eidos.op", nil)
	end(nil)

	require.Equal(t, int64(3), rec.counters[MetricTxTotal])
	require.Equal(t, []float64{42}, rec.histograms[MetricTxDuration])
	require.Equal(t, 7.0, rec.gauges[MetricCacheSize])
	require.Equal(t, []string{"eidos.op"}, rec.spans)
}

// TestNopObserver verifies the discard implementation is usable as a default.
func TestNopObserver(t *testing.T) {
	var obs Observer = Nop{}

	require.NotPanics(t, func() {
		Counter(obs, MetricTxTotal, 1, nil)
		Histogram(obs, MetricTxDuration, 1, nil)
		Gauge(obs, MetricCacheSize, 1, nil)
		_, end := StartSpan(context.Background(), obs, "op", nil)
		end(nil)
	})
}
