package vec

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/malivvan/harbor/metrics"
)

// Collection represents a single logical group of vectors, similar to a table in SQL databases.
// It provides thread-safe operations for inserting, searching, and deleting vectors.
// Each collection has a fixed vector dimension and uses a specific distance metric.
//
// Writes are buffered: Upsert updates the in-memory index immediately (so
// searches see the new points) and appends them to writeBuf. Once
// flushThreshold points are buffered, they are written to bbolt in one batch,
// turning N fsyncs into one.
type Collection struct {
	Name      string   // Unique name of the collection
	VectorLen int      // Dimension of vectors in this collection
	Metric    Distance // Distance metric used for similarity search

	mu      sync.RWMutex
	index   VectorIndex // Transparent index engine (Flat or HNSW)
	storage *Store      // Embedded Persistence

	writeBuf       []PointStruct // points buffered for the next batch write
	flushThreshold int           // flush when writeBuf reaches this size
	lastVersion    uint64        // last store-managed Version handed out

	// tombMu guards tombstones: IDs deleted while a flush batch was detached.
	// A failed flush re-buffers the detached batch; filtering it against these
	// tombstones prevents a later Flush from resurrecting deleted points.
	tombMu     sync.Mutex
	tombstones map[string]struct{}
}

// DefaultFlushThreshold is the default write-buffer threshold in points. Once
// this many points are buffered they are written to bbolt together, merging
// several fsyncs into one.
const DefaultFlushThreshold = 64

// CollectionOptions configures NewCollectionWithOptions. It collects the
// collection's identity, index choice and buffering into one validated struct.
//
// Quantization is a store-level setting (Config.UseQuant and Config.Quantizer)
// inherited by every collection, so it is not part of the per-collection
// options.
type CollectionOptions struct {
	// Name is the collection name. It is required.
	Name string
	// VectorLen is the fixed vector dimension. It must be positive.
	VectorLen int
	// Metric is the distance metric: Cosine, Euclid or Dot.
	Metric Distance
	// UseHNSW selects the approximate HNSW graph index instead of the exact
	// flat index.
	UseHNSW bool
	// HNSWParams tunes the HNSW index when UseHNSW is set. Zero fields take
	// their defaults.
	HNSWParams HNSWParams
	// FlushThreshold is the number of buffered points that triggers a flush.
	// Zero selects DefaultFlushThreshold.
	FlushThreshold int
}

// resolve validates the options and fills in defaults.
func (o CollectionOptions) resolve() (CollectionOptions, error) {
	if o.Name == "" {
		return o, fmt.Errorf("%w: collection name is required", ErrInvalidConfig)
	}
	if o.VectorLen <= 0 {
		return o, fmt.Errorf("%w: vector length must be positive, got %d", ErrInvalidConfig, o.VectorLen)
	}
	switch o.Metric {
	case Cosine, Euclid, Dot:
	default:
		return o, fmt.Errorf("%w: unknown distance metric %q", ErrInvalidConfig, o.Metric)
	}
	if o.UseHNSW {
		o.HNSWParams = normalizeHNSWParams(o.HNSWParams)
	}
	if o.FlushThreshold <= 0 {
		o.FlushThreshold = DefaultFlushThreshold
	}
	return o, nil
}

// NewCollection initializes a new vector collection, optionally loading from storage.
// If useHNSW is true, it uses the optimized HNSW graph search; otherwise uses flat memory search.
// When the receiver is non-nil, existing points are automatically loaded into memory.
// Returns an error if the collection cannot be created or if loaded points have invalid dimensions.
func (s *Store) NewCollection(name string, vectorLen int, metric Distance, useHNSW bool) (*Collection, error) {
	return s.NewCollectionWithOptions(CollectionOptions{
		Name:      name,
		VectorLen: vectorLen,
		Metric:    metric,
		UseHNSW:   useHNSW,
	})
}

// NewCollectionWithParams initializes a new vector collection with custom HNSW parameters.
// If useHNSW is true, it uses the optimized HNSW graph search with the provided parameters;
// otherwise uses flat memory search.
// When the receiver is non-nil, existing points are automatically loaded into memory.
// Returns an error if the collection cannot be created or if loaded points have invalid dimensions.
func (s *Store) NewCollectionWithParams(name string, vectorLen int, metric Distance, useHNSW bool, hnswParams HNSWParams) (*Collection, error) {
	return s.NewCollectionWithOptions(CollectionOptions{
		Name:       name,
		VectorLen:  vectorLen,
		Metric:     metric,
		UseHNSW:    useHNSW,
		HNSWParams: hnswParams,
	})
}

// NewCollectionWithOptions initializes a collection from validated options. It
// is the single constructor behind NewCollection and NewCollectionWithParams.
func (s *Store) NewCollectionWithOptions(opts CollectionOptions) (*Collection, error) {
	opts, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	return s.newCollection(opts)
}

func (s *Store) newCollection(opts CollectionOptions) (*Collection, error) {
	name, vectorLen, metric, useHNSW, hnswParams := opts.Name, opts.VectorLen, opts.Metric, opts.UseHNSW, opts.HNSWParams

	var index VectorIndex
	if useHNSW {
		index = NewHNSWIndexWithParams(metric, hnswParams)
	} else {
		index = NewFlatIndex(metric)
	}

	col := &Collection{
		Name:      name,
		VectorLen: vectorLen,
		Metric:    metric,
		index:     index,
		storage:   s,

		writeBuf:       make([]PointStruct, 0),
		flushThreshold: opts.FlushThreshold,
	}

	if s != nil {
		if err := s.EnsureCollection(name); err != nil {
			return nil, fmt.Errorf("failed to ensure storage collection %s: %w", name, err)
		}

		// An existing collection must keep the configuration it was created
		// with. Silently overwriting it would reinterpret old vectors with a
		// different metric or index type.
		existing, metaErr := s.LoadCollectionMeta(name)
		switch {
		case metaErr == nil:
			if existing.VectorLen != vectorLen || existing.Metric != metric || existing.UseHNSW != useHNSW || existing.UseQuant != s.useQuant {
				return nil, fmt.Errorf("%w: collection %q already exists with a different configuration", ErrInvalidConfig, name)
			}
			if useHNSW && existing.HNSWParams != hnswParams {
				return nil, fmt.Errorf("%w: collection %q already exists with different HNSW parameters", ErrInvalidConfig, name)
			}
			if existing.SchemaFingerprint != "" && existing.SchemaFingerprint != StoredPointSchemaFingerprint {
				return nil, fmt.Errorf("%w: collection %q uses an incompatible point schema", ErrInvalidConfig, name)
			}
		case errors.Is(metaErr, ErrMetadataNotFound):
			// New collection.
		default:
			return nil, fmt.Errorf("failed to read metadata for %s: %w", name, metaErr)
		}

		// Load and validate existing points before persisting the metadata:
		// opening an existing collection with the wrong configuration must not
		// overwrite the configuration already stored on disk.
		loadedPoints, err := s.LoadCollection(name)
		if err != nil {
			return nil, fmt.Errorf("failed to load points for %s: %w", name, err)
		}

		// Validate and prep array for Upsert
		var batch []PointStruct
		for _, p := range loadedPoints {
			if len(p.Vector) != vectorLen {
				return nil, fmt.Errorf("loaded point %s has invalid dimension %d (expected %d)", p.ID, len(p.Vector), vectorLen)
			}
			batch = append(batch, *p)
		}

		if len(batch) > 0 {
			if err := col.index.Upsert(batch); err != nil {
				return nil, fmt.Errorf("failed to load initial points into index: %w", err)
			}
		}

		// Save collection metadata last, once the configuration is known to be
		// consistent with what is stored.
		meta := CollectionMeta{
			Name:              name,
			VectorLen:         vectorLen,
			Metric:            metric,
			UseHNSW:           useHNSW,
			HNSWParams:        hnswParams,
			UseQuant:          s.useQuant,
			FormatVersion:     collectionFormatVersion,
			SchemaFingerprint: StoredPointSchemaFingerprint,
		}
		if err := s.SaveCollectionMeta(name, meta); err != nil {
			return nil, fmt.Errorf("failed to save metadata for %s: %w", name, err)
		}
		s.trackCollection(col)
	}

	return col, nil
}

// Upsert adds or updates points in the collection.
//
// Write path:
//  1. Update the in-memory index so the points are immediately searchable.
//  2. Append the points to writeBuf.
//  3. When the buffer reaches flushThreshold, write the batch to bbolt in one
//     transaction, turning N fsyncs into one.
//
// Points are first written to the memory index, then buffered for bbolt persistence.
// Returns an error if any point has an invalid vector length or if persistence fails.
func (c *Collection) Upsert(points []PointStruct) error {
	if err := validatePayloadKeys(points); err != nil {
		return err
	}
	c.mu.Lock()

	// 1. Validate and deep-copy: the caller may reuse its vectors/payloads
	// after Upsert, and Version is store-managed.
	version := uint64(time.Now().UnixNano())
	if version <= c.lastVersion {
		version = c.lastVersion + 1
	}
	c.lastVersion = version

	batch := make([]PointStruct, len(points))
	for i := range points {
		p := &points[i]
		if len(p.Vector) != c.VectorLen {
			c.mu.Unlock()
			return fmt.Errorf("point %s has invalid vector length %d, expected %d", p.ID, len(p.Vector), c.VectorLen)
		}
		batch[i] = clonePoint(p)
		batch[i].Version = version
	}

	// 2. Update the memory index immediately so searches see the new points. The in-tree
	// indexes accept ownership of the already-cloned batch, avoiding a second
	// deep copy per point.
	if owned, ok := c.index.(interface{ upsertOwned([]PointStruct) error }); ok {
		if err := owned.upsertOwned(batch); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("failed to update memory index: %w", err)
		}
	} else if err := c.index.Upsert(batch); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to update memory index: %w", err)
	}

	// 3. Buffer for bbolt persistence, batching to reduce fsyncs.
	c.writeBuf = append(c.writeBuf, batch...)
	if c.storage == nil || len(c.writeBuf) < c.flushThreshold {
		c.mu.Unlock()
		return nil
	}

	// Take the batch out of the collection, then commit it outside c.mu so
	// searches and counts are not blocked by the bbolt fsync. flushMu is
	// acquired before releasing c.mu so a concurrent Delete (which takes the
	// same order) cannot delete the batch's points between the two steps.
	flush := c.writeBuf
	c.writeBuf = nil
	c.storage.flushMu.Lock()
	c.mu.Unlock()

	err := c.storage.UpsertPoints(c.Name, flush)
	c.storage.flushMu.Unlock()
	if err != nil {
		c.mu.Lock()
		c.rebuffer(flush)
		c.mu.Unlock()
		return fmt.Errorf("failed to flush buffer to bbolt: %w", err)
	}
	c.clearTombstones()
	return nil
}

// Flush writes every buffered point in writeBuf to bbolt.
func (c *Collection) Flush() error {
	c.mu.Lock()
	flush := c.writeBuf
	c.writeBuf = nil
	if c.storage == nil || len(flush) == 0 {
		c.mu.Unlock()
		return nil
	}

	c.storage.flushMu.Lock()
	c.mu.Unlock()

	started := time.Now()
	err := c.storage.UpsertPoints(c.Name, flush)
	c.storage.flushMu.Unlock()
	c.observeFlush(time.Since(started), len(flush), err)
	if err != nil {
		c.mu.Lock()
		c.rebuffer(flush)
		c.mu.Unlock()
		return fmt.Errorf("failed to flush buffer to bbolt: %w", err)
	}
	c.clearTombstones()
	return nil
}

// observeFlush reports one vector flush to the store's observer.
func (c *Collection) observeFlush(d time.Duration, points int, err error) {
	if c == nil || c.storage == nil || c.storage.obs == nil {
		return
	}
	labels := metrics.Labels{
		metrics.LabelStore:  "vec",
		metrics.LabelOp:     "flush",
		metrics.LabelResult: resultLabel(err),
	}
	metrics.Histogram(c.storage.obs, metrics.MetricVecFlushDuration, float64(d), labels)
	metrics.Counter(c.storage.obs, metrics.MetricVecFlushTotal, 1, labels)
}

// Close flushes the collection's buffered points. It deliberately does NOT
// close the Store's bbolt handle: that handle is shared with every sibling
// collection (and, in the harbor root package, with the other sub-stores) and
// is owned by whoever created it. Call Store.Flush/Store.Close to finish the
// whole store.
func (c *Collection) Close() error {
	return c.Flush()
}

// Search performs a similarity search using the underlying VectorIndex.
// It finds the topK nearest neighbors to the query vector, optionally filtered by payload.
// Returns an error if the query vector dimension doesn't match the collection.
func (c *Collection) Search(queryVector []float32, filter *Filter, topK int) ([]ScoredPoint, error) {
	if len(queryVector) != c.VectorLen {
		return nil, fmt.Errorf("query vector length mismatch: expected %d, got %d", c.VectorLen, len(queryVector))
	}
	if topK <= 0 {
		// Reject before dispatching: a negative topK used to panic in the
		// indexes and a zero topK still returned a point from HNSW.
		return nil, fmt.Errorf("topK must be greater than 0, got %d", topK)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	started := time.Now()
	results, err := c.index.Search(queryVector, filter, topK)
	c.observeSearch(time.Since(started), len(results), err)
	return results, err
}

// observeSearch reports one vector search to the store's observer.
func (c *Collection) observeSearch(d time.Duration, results int, err error) {
	if c == nil || c.storage == nil || c.storage.obs == nil {
		return
	}
	labels := metrics.Labels{
		metrics.LabelStore:  "vec",
		metrics.LabelOp:     "search",
		metrics.LabelResult: resultLabel(err),
	}
	metrics.Histogram(c.storage.obs, metrics.MetricVecSearchDuration, float64(d), labels)
	metrics.Counter(c.storage.obs, metrics.MetricVecCandidates, int64(results), labels)
}

// Count returns the number of points currently stored in the collection.
func (c *Collection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.index.Count()
}

// GetPointsByFilter returns all points with full payload and vectors that match the given filter.
func (c *Collection) GetPointsByFilter(filter *Filter) ([]PointStruct, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// A nil filter matches every point, consistent with Search.
	return c.index.GetPointsByFilter(filter), nil
}

// Get returns the point with the given ID, or ErrPointNotFound. It reads the
// in-memory index, so it also sees upserts that are buffered but not yet
// flushed to disk.
func (c *Collection) Get(id string) (*PointStruct, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	point, err := c.index.Get(id)
	if err != nil {
		return nil, err
	}
	if point == nil {
		return nil, ErrPointNotFound
	}
	return point, nil
}

// ListOptions controls ListPoints and, when applied by the caller, pagination
// of ForEachPoint.
type ListOptions struct {
	// Offset skips this many matching points, in ascending ID order.
	Offset int
	// Limit caps the number of points returned. Zero means unlimited.
	Limit int
}

func (o ListOptions) validate() error {
	if o.Offset < 0 || o.Limit < 0 {
		return ErrInvalidOptions
	}
	return nil
}

// sortedFilterIDs returns the IDs matching filter in ascending order. Sorting by
// ID gives pagination a stable order independent of the index's map iteration.
func (c *Collection) sortedFilterIDs(filter *Filter) []string {
	ids := c.index.GetIDsByFilter(filter)
	sort.Strings(ids)
	return ids
}

// ListPoints returns points matching filter in ascending ID order, honoring
// Offset and Limit. Only the selected page is materialized; the matching IDs
// are sorted to make pagination stable.
func (c *Collection) ListPoints(filter *Filter, opts ListOptions) ([]PointStruct, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	c.mu.RLock()
	ids := c.sortedFilterIDs(filter)
	c.mu.RUnlock()

	start := opts.Offset
	if start > len(ids) {
		start = len(ids)
	}
	end := len(ids)
	if opts.Limit > 0 && start+opts.Limit < end {
		end = start + opts.Limit
	}

	out := make([]PointStruct, 0, end-start)
	for _, id := range ids[start:end] {
		point, err := c.index.Get(id)
		if err != nil {
			return nil, err
		}
		if point == nil {
			continue // deleted concurrently
		}
		out = append(out, *point)
	}
	return out, nil
}

// ForEachPoint streams points matching filter in ascending ID order, calling fn
// for each. The matching IDs are materialized and sorted so the order is
// stable; only one point is held at a time. Returning ErrStopIteration from fn
// stops the iteration without an error. The collection lock is not held while
// fn runs, so fn may call back into the collection.
func (c *Collection) ForEachPoint(filter *Filter, fn func(PointStruct) error) error {
	if fn == nil {
		return ErrInvalidOptions
	}

	c.mu.RLock()
	ids := c.sortedFilterIDs(filter)
	c.mu.RUnlock()

	for _, id := range ids {
		point, err := c.index.Get(id)
		if err != nil {
			return err
		}
		if point == nil {
			continue // deleted concurrently
		}
		if err := fn(*point); err != nil {
			if errors.Is(err, ErrStopIteration) {
				return nil
			}
			return err
		}
	}
	return nil
}

// Delete removes points either by explicit IDs or by a filter match.
// If points slice is provided, those specific points are deleted.
// If filter is provided, all points matching the filter are deleted.
// Returns the number of points deleted and any error encountered.
// Returns an error if neither points nor filter is provided.
// Ensures data consistency between storage and memory index.
func (c *Collection) Delete(points []string, filter *Filter) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First, determine which IDs to delete
	var targetIDs []string

	useFilter := false
	if len(points) > 0 {
		targetIDs = points
	} else if filter != nil {
		targetIDs = c.index.GetIDsByFilter(filter)
		useFilter = true
	} else {
		return 0, fmt.Errorf("must provide points or filter")
	}

	if len(targetIDs) == 0 {
		return 0, nil
	}

	// Build a set for O(1) lookup
	targetSet := make(map[string]struct{}, len(targetIDs))
	for _, id := range targetIDs {
		targetSet[id] = struct{}{}
	}

	// 1. Delete from storage first (if configured). flushMu serializes with an
	// in-flight flush so a detached batch cannot re-insert deleted points.
	if c.storage != nil {
		c.storage.flushMu.Lock()
		err := c.storage.DeletePoints(c.Name, targetIDs)
		if err == nil {
			// Record the IDs before releasing flushMu: a failed in-flight
			// flush may re-buffer its batch afterwards, and must filter these.
			c.tombMu.Lock()
			if c.tombstones == nil {
				c.tombstones = make(map[string]struct{}, len(targetIDs))
			}
			for _, id := range targetIDs {
				c.tombstones[id] = struct{}{}
			}
			c.tombMu.Unlock()
		}
		c.storage.flushMu.Unlock()
		if err != nil {
			return 0, fmt.Errorf("failed to delete from storage: %w", err)
		}
	}

	// 2. Delete from memory index
	successCount := 0
	if useFilter {
		deleted, err := c.index.DeleteByFilter(filter)
		if err != nil {
			return 0, fmt.Errorf("failed to delete from memory index: %w", err)
		}
		successCount = len(deleted)
	} else {
		for _, id := range targetIDs {
			if err := c.index.Delete(id); err == nil {
				successCount++
			}
		}
	}

	// 3. Only now drop the IDs from the write buffer, so a flush cannot
	// resurrect points deleted in the meantime:
	// dropping them first would lose the points still present in the index when
	// the storage delete fails.
	if len(c.writeBuf) > 0 {
		filtered := make([]PointStruct, 0, len(c.writeBuf))
		for _, p := range c.writeBuf {
			if _, ok := targetSet[p.ID]; !ok {
				filtered = append(filtered, p)
			}
		}
		c.writeBuf = filtered
	}

	return successCount, nil
}

// validatePayloadKeys rejects payload keys that the storage layer owns.
func validatePayloadKeys(points []PointStruct) error {
	for _, p := range points {
		if p.Payload == nil {
			continue
		}
		if _, ok := p.Payload[payloadJSONKey]; ok {
			return fmt.Errorf("vec: payload key %q is reserved", payloadJSONKey)
		}
		if _, ok := p.Payload["__quantized_vector"]; ok {
			return fmt.Errorf("vec: payload key %q is reserved", "__quantized_vector")
		}
	}
	return nil
}

// rebuffer prepends a batch detached by a failed flush to writeBuf, dropping
// points deleted while the batch was in flight. Must be called with c.mu held.
func (c *Collection) rebuffer(flush []PointStruct) {
	c.tombMu.Lock()
	if len(c.tombstones) > 0 {
		kept := flush[:0]
		for _, p := range flush {
			if _, dead := c.tombstones[p.ID]; !dead {
				kept = append(kept, p)
			}
		}
		flush = kept
		c.tombstones = nil
	}
	c.tombMu.Unlock()
	c.writeBuf = append(flush, c.writeBuf...)
}

// clearTombstones drops the delete tombstones after a successful commit. Any
// batch they applied to has already been persisted or filtered.
func (c *Collection) clearTombstones() {
	c.tombMu.Lock()
	c.tombstones = nil
	c.tombMu.Unlock()
}
