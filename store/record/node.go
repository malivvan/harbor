package record

import (
	"context"
	"time"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/metrics"
	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

// observeTx runs one top-level bbolt transaction, reporting its latency and
// outcome to the configured observer. It is a no-op wrapper when the store has
// no observer.
func (s *Store) observeTx(kind string, run func() error) error {
	if s == nil || s.obs == nil {
		return run()
	}
	start := time.Now()
	err := run()
	labels := metrics.Labels{
		metrics.LabelStore:  bucket.RECORD,
		metrics.LabelKind:   kind,
		metrics.LabelResult: resultLabel(err),
	}
	metrics.Histogram(s.obs, metrics.MetricTxDuration, float64(time.Since(start)), labels)
	metrics.Counter(s.obs, metrics.MetricTxTotal, 1, labels)
	return err
}

// resultLabel maps an error to the ResultOK/ResultError label value.
func resultLabel(err error) string {
	if err != nil {
		return metrics.ResultError
	}
	return metrics.ResultOK
}

// Node exposes tempest operations relative to a bbolt bucket path.
type Node interface {
	TypeStore
	KeyValueStore
	BucketScanner

	// From returns a new tempest node with a new bucket root below the current.
	// All Store operations on the new node are executed relative to this bucket.
	From(addend ...string) Node

	// Bucket returns the bucket name as a slice from the root.
	// In the normal, simple case this will be empty.
	Bucket() []string

	// Codec returns the codec selected for this node.
	Codec() Codec

	// WithCodec returns a copy of the node configured to use the given codec.
	WithCodec(codec Codec) Node

	// Aggregate computes grouped measures over the records of this bucket.
	Aggregate(ctx context.Context, spec AggregateSpec) (AggregateResult, error)

	// ValidateSchema checks that the reference fields of the sample records
	// name buckets that exist.
	ValidateSchema(ctx context.Context, samples ...any) error
}

// Compile-time assertion that *node implements the final Node interface.
var _ Node = (*node)(nil)

// node implements Node for a specific bucket path and optional transaction.
type node struct {
	s *Store

	// The root bucket path. An empty path is the database root; the tempest root
	// package configures the `record` root bucket explicitly.
	root [][]byte

	// Transaction object. Nil if not in transaction
	tx *bolt.Tx

	// Codec of this node
	codec Codec

	// trace, when non-nil, replaces the store's sink for writes performed
	// through this node. The root package uses it to stage trace events for a
	// cross-store transaction instead of recording them inside the transaction.
	trace ledger.Sink

	// project, when non-empty, limits decode to these top-level fields. It is
	// set on a query's private node copy by Query.Project.
	project []string

	// skipRevision suppresses the per-record revision bump for maintenance
	// rewrites (ReIndex) that do not represent a caller-visible write.
	skipRevision bool
}

// cloneBucketPath returns a defensive copy of the given bucket path.
// nil is preserved as nil; an empty non-nil slice is preserved.
func cloneBucketPath(path [][]byte) [][]byte {
	if path == nil {
		return nil
	}

	cloned := make([][]byte, len(path))
	for i, part := range path {
		cloned[i] = append([]byte(nil), part...)
	}
	return cloned
}

// From returns a new tempest Node with a new bucket root below the current.
// All Store operations on the new node will be executed relative to this bucket.
func (n node) From(addend ...string) Node {
	path := make([][]byte, 0, len(n.root)+len(addend))
	// Bucket path elements are immutable once created, so the parent path can
	// be shared: only the outer slice is copied for the new node.
	path = append(path, n.root...)
	for _, part := range addend {
		path = append(path, []byte(part))
	}
	n.root = path
	return &n
}

// withTransaction returns a new tempest Node that will use the given transaction.
func (n node) withTransaction(tx *bolt.Tx) *node {
	n.tx = tx
	return &n
}

// WithCodec returns a new tempest Node that will use the given Codec.
func (n node) WithCodec(codec Codec) Node {
	n.codec = codec
	return &n
}

// Bucket returns the bucket name as a slice from the root.
// In the normal, simple case this will be empty.
func (n *node) Bucket() []string {
	if n.root == nil {
		return nil
	}
	parts := make([]string, len(n.root))
	for i, part := range n.root {
		parts[i] = string(part)
	}
	return parts
}

// Codec returns the EncodeDecoder used by this instance of tempest
func (n *node) Codec() Codec {
	return n.codec
}

// Detects if already in transaction or runs a read write transaction.
func (n *node) readWriteTx(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	run := func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		if err := fn(tx); err != nil {
			return err
		}

		return checkContext(ctx)
	}

	if n.tx != nil {
		return run(n.tx)
	}

	// Serialize with rotation: the whole top-level write transaction runs under
	// the read side of rotMu, so a rotation cannot commit between the key load
	// and the write.
	n.s.rotMuOf().RLock()
	defer n.s.rotMuOf().RUnlock()
	return n.s.observeTx("write", func() error { return n.s.bolt.Update(run) })
}

// Detects if already in transaction or runs a read transaction.
func (n *node) readTx(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	run := func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return checkContext(ctx)
	}

	if n.tx != nil {
		return run(n.tx)
	}

	n.s.rotMuOf().RLock()
	defer n.s.rotMuOf().RUnlock()
	return n.s.observeTx("read", func() error { return n.s.bolt.View(run) })
}
