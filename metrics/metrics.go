// Package metrics defines the optional observability hooks tempest uses to
// report operation counters, latency histograms and tracing spans.
//
// tempest does not depend on any metrics or tracing library. Instead it calls
// the small Observer interface defined here, and the application supplies an
// implementation that forwards to Prometheus, OpenTelemetry, expvar, or
// anything else. A nil Observer is valid everywhere and means "no
// instrumentation", so an uninstrumented store pays only the cost of a nil
// check.
//
// Observers must be safe for concurrent use: tempest calls them from many
// goroutines, including the raft FSM goroutine.
package metrics

import "context"

// Observer receives tempest's operation metrics and spans.
//
// Every method must tolerate being called concurrently. Implementations should
// not block: a slow observer stalls the operation that reported to it.
type Observer interface {
	// Counter adds delta to the named monotonic counter. Implementations
	// should treat a negative delta as a decrement rather than panicking.
	Counter(name string, delta int64, labels Labels)

	// Histogram records one observation of the named distribution, in the
	// unit implied by the metric name (tempest uses nanoseconds for the
	// duration metrics and bytes for the size metrics).
	Histogram(name string, value float64, labels Labels)

	// Gauge reports the current value of the named gauge.
	Gauge(name string, value float64, labels Labels)

	// StartSpan begins a traced operation and returns a context carrying the
	// span plus a function that ends it. The returned function must be safe
	// to call exactly once. Implementations that do not trace may return the
	// context unchanged and a no-op end function.
	StartSpan(ctx context.Context, name string, labels Labels) (context.Context, func(err error))
}

// Labels is a small, allocation-friendly set of key/value pairs attached to a
// metric. tempest uses a handful of well-known keys (see the Label* constants)
// and keeps cardinality bounded by never using record keys or file names as
// label values.
type Labels map[string]string

// Well-known label keys. Using constants keeps the cardinality of the metric
// series predictable for an exporter.
const (
	// LabelStore is the sub-store that produced the metric, named by the
	// internal/bucket registry ("record", "fstree", "series", "vector", "search",
	// "graphs", "object") or "cluster".
	LabelStore = "store"

	// LabelOp is the operation name, for example "insert" or "range".
	LabelOp = "op"

	// LabelResult is the outcome: ResultOK or ResultError.
	LabelResult = "result"

	// LabelKind is a sub-classification, for example the cache or index kind.
	LabelKind = "kind"
)

// Well-known label values for LabelResult.
const (
	ResultOK    = "ok"
	ResultError = "error"
)

// Metric names tempest reports. They are exported so an application can
// pre-register them with its exporter.
const (
	// MetricTxDuration is the wall-clock duration of a bbolt transaction, in
	// nanoseconds, labelled by store and read/write kind.
	MetricTxDuration = "tempest.tx.duration"

	// MetricTxTotal counts bbolt transactions, labelled by store, kind and
	// result.
	MetricTxTotal = "tempest.tx.total"

	// MetricChunkBytes is the number of content bytes read or written by the
	// fs chunk layer, labelled by op.
	MetricChunkBytes = "tempest.fs.chunk.bytes"

	// MetricChunkOps counts fs chunk read/write operations, labelled by op
	// and result.
	MetricChunkOps = "tempest.fs.chunk.ops"

	// MetricCacheHits counts fstree inode-cache lookups, labelled by result
	// (hit or miss).
	MetricCacheHits = "tempest.fs.cache.lookups"

	// MetricCacheSize is the current number of entries in the fstree inode cache.
	MetricCacheSize = "tempest.fs.cache.size"

	// MetricSeriesDecodedPoints counts time-series points decoded from blocks.
	MetricSeriesDecodedPoints = "tempest.ts.points.decoded"

	// MetricSeriesAggregateDuration is the duration of a time-series aggregate
	// call, in nanoseconds.
	MetricSeriesAggregateDuration = "tempest.ts.aggregate.duration"

	// MetricVectorCandidates counts vectors examined during a vector search.
	MetricVectorCandidates = "tempest.vec.search.candidates"

	// MetricVectorFallbacks counts vector searches that fell back to an exact
	// scan because the index could not prove enough matches.
	MetricVectorFallbacks = "tempest.vec.search.fallbacks"

	// MetricVectorScanSize is the number of points scanned by an exact fallback,
	// which signals how sparse a filter is.
	MetricVectorScanSize = "tempest.vec.search.scan.size"

	// MetricVectorSearchDuration is the duration of a vector search, in
	// nanoseconds.
	MetricVectorSearchDuration = "tempest.vec.search.duration"

	// MetricVectorFlushDuration is the duration of a vector flush to disk, in
	// nanoseconds.
	MetricVectorFlushDuration = "tempest.vec.flush.duration"

	// MetricVectorFlushTotal counts vector flushes, labelled by result.
	MetricVectorFlushTotal = "tempest.vec.flush.total"

	// MetricRotateDuration is the duration of a key rotation, in
	// nanoseconds.
	MetricRotateDuration = "tempest.rotate.duration"

	// MetricClusterApplyLag is the delay between a raft log entry being
	// committed and applied locally, in nanoseconds.
	MetricClusterApplyLag = "tempest.cluster.apply.lag"

	// MetricClusterApplyTotal counts applied raft log entries, labelled by
	// result.
	MetricClusterApplyTotal = "tempest.cluster.apply.total"

	// MetricWalkCallbackDuration is the duration of one FastWalk callback, in
	// nanoseconds.
	MetricWalkCallbackDuration = "tempest.fs.walk.callback.duration"

	// MetricWalkQueueDepth is the number of directories buffered for traversal
	// by FastWalk.
	MetricWalkQueueDepth = "tempest.fs.walk.queue.depth"
)

// Nop is an Observer that discards everything. It is useful as an explicit
// default in tests and as an embedded base for partial implementations.
type Nop struct{}

// Counter implements Observer.
func (Nop) Counter(string, int64, Labels) {}

// Histogram implements Observer.
func (Nop) Histogram(string, float64, Labels) {}

// Gauge implements Observer.
func (Nop) Gauge(string, float64, Labels) {}

// StartSpan implements Observer.
func (Nop) StartSpan(ctx context.Context, _ string, _ Labels) (context.Context, func(error)) {
	return ctx, func(error) {}
}

// Counter adds delta to the named counter on o. A nil observer is a no-op.
func Counter(o Observer, name string, delta int64, labels Labels) {
	if o != nil {
		o.Counter(name, delta, labels)
	}
}

// Histogram records value on o. A nil observer is a no-op.
func Histogram(o Observer, name string, value float64, labels Labels) {
	if o != nil {
		o.Histogram(name, value, labels)
	}
}

// Gauge reports value on o. A nil observer is a no-op.
func Gauge(o Observer, name string, value float64, labels Labels) {
	if o != nil {
		o.Gauge(name, value, labels)
	}
}

// StartSpan begins a span on o. A nil observer returns ctx unchanged and a
// no-op end function, so callers can always use the result directly.
func StartSpan(ctx context.Context, o Observer, name string, labels Labels) (context.Context, func(error)) {
	if o == nil {
		return ctx, func(error) {}
	}
	return o.StartSpan(ctx, name, labels)
}
