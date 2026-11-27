package series

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/malivvan/eidos-db/pkg/metrics"
	"github.com/malivvan/eidos-db/store"
	"github.com/malivvan/eidos-db/store/ledger"
	bolt "go.etcd.io/bbolt"
)

const metaBucketName = "__ts_meta__"

// ErrInTransaction reports an operation that cannot run on a store bound to a
// caller-owned transaction (for example key rotation).
var ErrInTransaction = errors.New("series: operation is not available inside an explicit transaction")

// FormatVersion is the on-disk time-series format revision this build writes
// and accepts. The first release has a single version 1; any other value fails
// the open with ErrIncompatibleVersion.
const FormatVersion uint64 = 1

// nowProvider wraps the pluggable clock so it can be stored in an atomic
// pointer.
type nowProvider struct {
	fn func() int64
}

// Store manages named time series backed by a bbolt database.
type Store struct {
	b *bolt.DB
	// tx, when non-nil, binds every operation to a caller-owned transaction.
	// See WithTransaction.
	tx *bolt.Tx
	// base points at the store this value is a rooted view of, or nil for a
	// store returned by Open. Views share the handle, clock and encryption
	// state with their base.
	base   *Store
	root   [][]byte
	now    atomic.Pointer[nowProvider]
	mu     sync.RWMutex
	series map[string]*Series
	order  []string

	// enc is the optional encryption state. It is an atomic pointer so a
	// rotation can swap the keys while readers are serving queries.
	enc atomic.Pointer[encState]

	// audit receives an entry for every committed write, or nil when the store
	// was opened without audit. It is set once at Open and never mutated.
	audit ledger.Sink

	// obs receives operation metrics and spans, or nil when the store was
	// opened without instrumentation. It is set once at Open and never
	// mutated.
	obs metrics.Observer
}

// view runs fn against the bound transaction, or a fresh read transaction.
func (s *Store) view(fn func(*bolt.Tx) error) error {
	if s == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.b.View(fn)
}

// update runs fn against the bound transaction, or a fresh write transaction.
// A bound Store never commits; the transaction owner does.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	if s == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return fn(s.tx)
	}
	return s.b.Update(fn)
}

// SetAuditSink replaces the sink that receives committed-write events and
// returns the receiver. It is intended for a transaction-bound view created by
// WithTransaction: the view stages events and the transaction owner records
// them after it commits. It must not be called concurrently with writes on the
// same Store.
func (s *Store) SetAuditSink(sink ledger.Sink) *Store {
	if s != nil {
		s.audit = sink
	}
	return s
}

// WithTransaction returns a Store whose operations run inside tx instead of
// opening their own bbolt transaction. The bound store never commits, rolls
// back or closes tx. It is used by the root package for a cross-store
// transaction; rotation is rejected on a bound store.
func (s *Store) WithTransaction(tx *bolt.Tx) *Store {
	if s == nil || tx == nil {
		return nil
	}
	bound := &Store{
		b:      s.b,
		tx:     tx,
		base:   s.rootStore(),
		root:   cloneRoot(s.root),
		series: make(map[string]*Series),
		audit:  s.audit,
		obs:    s.obs,
	}
	return bound
}

// auditEntry records one committed write in the store's audit trail. It is a
// no-op when the store has no audit sink. Digest is the SHA-256 of the written
// payload, or nil for deletes.
func (s *Store) auditEntry(op, series string, digest []byte) error {
	if s == nil || s.audit == nil {
		return nil
	}
	return s.audit.Record(ledger.Entry{
		Store:  ledger.StoreSeries,
		Op:     op,
		Entity: series,
		Digest: digest,
	})
}

// digestOf returns the SHA-256 of data, or nil when data is empty.
func digestOf(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	sum := sha256.Sum256(data)
	return sum[:]
}

// observeAggregate reports one aggregate call to the configured observer: its
// latency and the number of samples it produced.
func (s *Store) observeAggregate(d time.Duration, samples int, err error) {
	if s == nil || s.obs == nil {
		return
	}
	labels := metrics.Labels{
		metrics.LabelStore:  store.SERIES,
		metrics.LabelOp:     "aggregate",
		metrics.LabelResult: resultLabel(err),
	}
	metrics.Histogram(s.obs, metrics.MetricSeriesAggregateDuration, float64(d), labels)
	metrics.Counter(s.obs, metrics.MetricSeriesDecodedPoints, int64(samples), labels)
}

// resultLabel maps an error to the ResultOK/ResultError label value.
func resultLabel(err error) string {
	if err != nil {
		return metrics.ResultError
	}
	return metrics.ResultOK
}

// encState returns the current encryption state, or nil for a plaintext store.
func (s *Store) rootStore() *Store {
	if s != nil && s.base != nil {
		return s.base
	}
	return s
}

func (s *Store) encState() *encState {
	base := s.rootStore()
	if base == nil {
		return nil
	}
	return base.enc.Load()
}

// nowOf returns the clock shared with the base store.
func (s *Store) nowOf() *nowProvider {
	base := s.rootStore()
	if base == nil {
		return nil
	}
	return base.now.Load()
}

// WithRoot returns a view of the store rooted at the given bucket path. The
// view shares the handle, clock and encryption state with the receiver. No
// segments means the database root; pass the conventional root explicitly
// (for example store.SERIES) to keep it.
func (s *Store) WithRoot(parts ...string) *Store {
	if s == nil {
		return nil
	}
	root := make([][]byte, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		root = append(root, []byte(part))
	}
	return &Store{
		b:      s.b,
		tx:     s.tx,
		base:   s.rootStore(),
		root:   root,
		series: make(map[string]*Series),
		audit:  s.audit,
		obs:    s.obs,
	}
}

// encryptsValues reports whether values and series metadata are sealed.
func (s *Store) encryptsValues() bool {
	if s == nil {
		return false
	}
	enc := s.encState()
	return enc != nil && enc.encValues
}

// blockDuration returns the configured encryption time-block width, or zero
// when block mode is off.
func (s *Store) blockDuration() int64 {
	if s == nil {
		return 0
	}
	enc := s.encState()
	if enc == nil {
		return 0
	}
	return enc.blockDur
}

// Root returns a copy of the store's root bucket path.
func (s *Store) Root() [][]byte {
	if s == nil {
		return nil
	}
	return cloneRoot(s.root)
}

// Close releases the Store wrapper. The underlying bbolt database remains open
// and must be closed by its owner.
func (s *Store) Close() error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	return nil
}

// Series returns the named series, creating and caching it on demand.
func (s *Store) Series(name string) *Series {
	if s == nil || s.b == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if series, ok := s.series[name]; ok {
		return series
	}

	series := &Series{store: s, name: name}
	s.series[name] = series
	s.order = append(s.order, name)
	return series
}

// SeriesNames returns the names of series created through this Store, in
// creation order.
func (s *Store) SeriesNames() []string {
	if s == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, len(s.order))
	copy(names, s.order)
	return names
}

// SeriesInfo describes one persisted series.
type SeriesInfo struct {
	// Name is the logical series name.
	Name string
	// Labels are the series labels, when a metadata record exists.
	Labels map[string]string
	// Retention is the persisted retention policy, when a metadata record
	// exists.
	Retention Retention
	// Count is the live point count recorded in metadata; zero when no
	// metadata record exists.
	Count int64
}

// ListSeries returns every persisted series in the store, sorted by name. It
// scans the metadata bucket and, when the store has a configured Root, the
// series buckets under it, so a series created by a plain Insert (which writes
// no metadata record) is still found. Labels, retention and count are filled
// in when a metadata record exists.
//
// At the database root (no configured Root) only series with a metadata record
// are returned, because unrelated top-level buckets cannot be distinguished
// from series buckets.
func (s *Store) ListSeries(ctx context.Context) ([]SeriesInfo, error) {
	if s == nil || s.b == nil {
		return nil, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	seen := make(map[string]SeriesInfo)
	err := s.view(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		meta, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if meta != nil {
			if err := meta.ForEach(func(physical, raw []byte) error {
				if err := checkContext(ctx); err != nil {
					return err
				}
				if string(physical) == markerKey || string(physical) == versionKey {
					return nil
				}
				name, err := s.logicalName(physical)
				if err != nil {
					return err
				}
				info, err := s.decodeMeta(raw)
				if err != nil {
					return err
				}
				seen[name] = SeriesInfo{
					Name:      name,
					Labels:    info.Labels,
					Retention: info.Retention,
					Count:     info.Count,
				}
				return nil
			}); err != nil {
				return err
			}
		}

		// A series created by a plain Insert has a data bucket but no metadata
		// record. Include those buckets when the root is configured, where every
		// child bucket except the metadata bucket is a series.
		if len(s.root) == 0 {
			return nil
		}
		root := s.rootBucket(tx)
		if root == nil {
			return nil
		}
		return root.ForEach(func(physical, _ []byte) error {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if string(physical) == metaBucketName || root.Bucket(physical) == nil {
				return nil
			}
			name, err := s.logicalName(physical)
			if err != nil {
				return err
			}
			if _, ok := seen[name]; !ok {
				seen[name] = SeriesInfo{Name: name}
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	out := make([]SeriesInfo, 0, len(seen))
	for _, info := range seen {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ListSeriesNames returns the names of every persisted series, sorted.
func (s *Store) ListSeriesNames(ctx context.Context) ([]string, error) {
	infos, err := s.ListSeries(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name
	}
	return names, nil
}

// DeleteSeries drops a series bucket and its metadata.
func (s *Store) DeleteSeries(ctx context.Context, name string) error {
	if s == nil || s.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if name == "" {
		return ErrNilParam
	}

	err := s.update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		found := false
		var parent *bolt.Bucket
		if len(s.root) > 0 {
			parent = s.rootBucket(tx)
			// A configured-but-missing root must never fall back to the
			// database root: that could delete an unrelated top-level bucket
			// that happens to share the series name.
			if parent == nil {
				return ErrNotFound
			}
		}

		key, err := s.bucketKey(name)
		if err != nil {
			return err
		}
		if parent == nil {
			if tx.Bucket(key) != nil {
				if err := tx.DeleteBucket(key); err != nil {
					return err
				}
				found = true
			}
		} else if parent.Bucket(key) != nil {
			if err := parent.DeleteBucket(key); err != nil {
				return err
			}
			found = true
		}

		meta, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		metaKey, err := s.bucketKey(name)
		if err != nil {
			return err
		}
		if meta != nil && meta.Get(metaKey) != nil {
			if err := meta.Delete(metaKey); err != nil {
				return err
			}
			found = true
		}

		if !found {
			return ErrNotFound
		}
		return checkContext(ctx)
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.series, name)
	for i, cached := range s.order {
		if cached == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	if err := s.auditEntry("delete_series", name, nil); err != nil {
		return err
	}
	return nil
}

// bucketKey returns the physical key for a series bucket and its metadata.
// Names are encrypted with deterministic AES-SIV when EncryptSeriesNames is on.
func (s *Store) bucketKey(name string) ([]byte, error) {
	if s == nil {
		return []byte(name), nil
	}
	enc := s.encState()
	if enc == nil || !enc.encNames || enc.siv == nil {
		return []byte(name), nil
	}
	return enc.siv.SealName([]byte(name))
}

// logicalName maps a physical bucket/meta key back to the series name.
func (s *Store) logicalName(physical []byte) (string, error) {
	if s == nil {
		return string(physical), nil
	}
	enc := s.encState()
	if enc == nil || !enc.encNames || enc.siv == nil {
		return string(physical), nil
	}
	name, err := enc.siv.OpenName(physical)
	if err != nil {
		return "", err
	}
	return string(name), nil
}

// rootBucket traverses the configured root path, or returns nil when the path
// is empty or any element is missing.
func (s *Store) rootBucket(tx *bolt.Tx) *bolt.Bucket {
	var b *bolt.Bucket
	for _, name := range s.root {
		if len(name) == 0 {
			continue
		}
		if b == nil {
			b = tx.Bucket(name)
		} else {
			b = b.Bucket(name)
		}
		if b == nil {
			return nil
		}
	}
	return b
}

// createRootBucket creates the configured root path and returns its innermost
// bucket. An empty path returns nil.
func (s *Store) createRootBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	var b *bolt.Bucket
	for _, name := range s.root {
		if len(name) == 0 {
			continue
		}
		var err error
		if b == nil {
			b, err = tx.CreateBucketIfNotExists(name)
		} else {
			b, err = b.CreateBucketIfNotExists(name)
		}
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// cloneRoot returns a deep copy of a bucket path.
func cloneRoot(root [][]byte) [][]byte {
	if root == nil {
		return nil
	}
	out := make([][]byte, len(root))
	for i, part := range root {
		out[i] = append([]byte(nil), part...)
	}
	return out
}

func (s *Store) seriesBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, err
	}
	if len(s.root) == 0 {
		return tx.Bucket(key), nil
	}

	root := s.rootBucket(tx)
	if root == nil {
		return nil, nil
	}
	return root.Bucket(key), nil
}

// seriesCount returns the number of points currently stored in a series.
// Non-block series use the bucket key count; block series decrypt their blocks.
func (s *Store) seriesCount(ctx context.Context, tx *bolt.Tx, name string) (int64, error) {
	bucket, err := s.seriesBucket(tx, name)
	if err != nil {
		return 0, err
	}
	if bucket == nil {
		return 0, nil
	}
	if s.blockDuration() > 0 {
		count, err := s.countBlocks(ctx, bucket)
		return int64(count), err
	}
	return int64(bucket.Stats().KeyN), nil
}

func (s *Store) createSeriesBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, err
	}
	if len(s.root) == 0 {
		return tx.CreateBucketIfNotExists(key)
	}

	root, err := s.createRootBucket(tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("%w: root path is empty", ErrInvalidConfig)
	}
	return root.CreateBucketIfNotExists(key)
}

func (s *Store) metaBucket(tx *bolt.Tx, create bool) (*bolt.Bucket, error) {
	var parent *bolt.Bucket
	var err error

	if len(s.root) > 0 {
		if create {
			parent, err = s.createRootBucket(tx)
		} else {
			parent = s.rootBucket(tx)
		}
		if err != nil {
			return nil, err
		}
		if parent == nil {
			return nil, nil
		}
		if create {
			return parent.CreateBucketIfNotExists([]byte(metaBucketName))
		}
		return parent.Bucket([]byte(metaBucketName)), nil
	}

	if create {
		return tx.CreateBucketIfNotExists([]byte(metaBucketName))
	}
	return tx.Bucket([]byte(metaBucketName)), nil
}
