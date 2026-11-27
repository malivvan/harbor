package fstree

import (
	"sync"
	"sync/atomic"

	"github.com/malivvan/eidos-db/pkg/metrics"
	"github.com/malivvan/eidos-db/store"
)

// inodeCache provides a thread-safe in-memory cache for inodes to improve
// performance by reducing BoltDB read operations. The cache uses a simple
// LRU-like eviction policy based on access time and size limits.
type inodeCache struct {
	mu      sync.RWMutex
	entries map[uint64]*cacheEntry
	maxSize int
	hits    atomic.Uint64
	misses  atomic.Uint64
	// clock hands out logical access ticks for LRU ordering. A counter is
	// cheaper than time.Now() and orders accesses exactly.
	clock   atomic.Uint64
	enabled bool

	// obs receives cache lookup counters and the current size, or nil when
	// the owning store was opened without instrumentation. It is set once
	// before the cache is shared and never mutated.
	obs metrics.Observer
}

// observeLookup reports one cache lookup outcome to the configured observer.
func (c *inodeCache) observeLookup(hit bool) {
	if c == nil || c.obs == nil {
		return
	}
	result := metrics.ResultOK
	if !hit {
		result = metrics.ResultError
	}
	metrics.Counter(c.obs, metrics.MetricCacheHits, 1, metrics.Labels{
		metrics.LabelStore:  store.FSTREE,
		metrics.LabelResult: result,
	})
}

// cacheEntry represents a cached inode with metadata for cache management.
type cacheEntry struct {
	node *iNode
	// lastUsed is a logical access tick used to order LRU eviction. It is
	// updated atomically so a cache hit does not need the write lock.
	lastUsed atomic.Uint64
	dirty    bool // true if the node has been modified but not persisted
}

// newInodeCache creates a new inode cache with the specified maximum size.
// A maxSize of 0 or negative disables the cache.
func newInodeCache(maxSize int) *inodeCache {
	return &inodeCache{
		entries: make(map[uint64]*cacheEntry),
		maxSize: maxSize,
		enabled: maxSize > 0,
	}
}

// Get retrieves an inode from the cache if it exists.
// Returns nil if the inode is not in the cache.
func (c *inodeCache) Get(ino uint64) *iNode {
	if !c.enabled {
		return nil
	}

	c.mu.RLock()
	entry, ok := c.entries[ino]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		c.observeLookup(false)
		return nil
	}

	// A hit only touches atomics: recency is a logical tick, so the read path
	// never takes the write lock or calls time.Now().
	entry.lastUsed.Store(c.clock.Add(1))
	c.hits.Add(1)
	c.observeLookup(true)

	// Return a copy to prevent external modifications
	return copyInode(entry.node)
}

// GetShared retrieves an inode without copying it. Callers must treat the
// returned node as immutable: mutation paths must use Get (which copies) or
// clone the parts they change.
func (c *inodeCache) GetShared(ino uint64) *iNode {
	if !c.enabled {
		return nil
	}

	c.mu.RLock()
	entry, ok := c.entries[ino]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		c.observeLookup(false)
		return nil
	}

	entry.lastUsed.Store(c.clock.Add(1))
	c.hits.Add(1)
	c.observeLookup(true)

	return entry.node
}

// Put adds or updates an inode in the cache.
//
// The cache never exceeds maxSize: when it is full the least recently used
// entry is dropped, dirty or not. Dropping a dirty entry is safe because the
// store persists inodes synchronously (see fsBucket.PutInode) and a later
// lookup simply re-reads the node from disk; keeping the entry would let the
// cache grow without bound once every entry is dirty.
func (c *inodeCache) Put(ino uint64, node *iNode, dirty bool) {
	if !c.enabled || node == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict old entries if the cache is full. A re-put of an existing inode
	// replaces its entry and does not need room.
	if _, exists := c.entries[ino]; !exists && len(c.entries) >= c.maxSize {
		c.evictOldest()
	}

	// Store a copy to prevent external modifications
	entry := &cacheEntry{
		node:  copyInode(node),
		dirty: dirty,
	}
	entry.lastUsed.Store(c.clock.Add(1))
	c.entries[ino] = entry
}

// Invalidate removes an inode from the cache.
func (c *inodeCache) Invalidate(ino uint64) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	delete(c.entries, ino)
	c.mu.Unlock()
}

// The dirty-tracking helpers below are retained for cache tests; the store
// persists inodes synchronously and does not rely on them.

// MarkDirty marks a cached inode as dirty (modified but not persisted).
func (c *inodeCache) MarkDirty(ino uint64) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[ino]; ok {
		entry.dirty = true
		entry.lastUsed.Store(c.clock.Add(1))
	}
}

// MarkClean marks a cached inode as clean (persisted to disk).
func (c *inodeCache) MarkClean(ino uint64) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[ino]; ok {
		entry.dirty = false
	}
}

// IsDirty checks if a cached inode has unsaved modifications.
func (c *inodeCache) IsDirty(ino uint64) bool {
	if !c.enabled {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if entry, ok := c.entries[ino]; ok {
		return entry.dirty
	}
	return false
}

// GetDirty returns all dirty (unsaved) inodes in the cache.
func (c *inodeCache) GetDirty() map[uint64]*iNode {
	if !c.enabled {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	dirty := make(map[uint64]*iNode)
	for ino, entry := range c.entries {
		if entry.dirty {
			dirty[ino] = copyInode(entry.node)
		}
	}
	return dirty
}

// Flush removes all entries from the cache.
func (c *inodeCache) Flush() {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	c.entries = make(map[uint64]*cacheEntry)
	c.mu.Unlock()
}

// Stats returns cache statistics.
func (c *inodeCache) Stats() CacheStats {
	c.mu.RLock()
	size := len(c.entries)
	maxSize := c.maxSize
	enabled := c.enabled
	c.mu.RUnlock()

	stats := CacheStats{
		Size:    size,
		MaxSize: maxSize,
		Hits:    c.hits.Load(),
		Misses:  c.misses.Load(),
		Enabled: enabled,
	}

	if c.obs != nil {
		metrics.Gauge(c.obs, metrics.MetricCacheSize, float64(stats.Size), metrics.Labels{
			metrics.LabelStore: store.FSTREE,
		})
	}
	return stats
}

// CacheStats contains statistics about cache performance.
type CacheStats struct {
	Size    int    // Current number of cached entries
	MaxSize int    // Maximum cache size
	Hits    uint64 // Number of cache hits
	Misses  uint64 // Number of cache misses
	Enabled bool   // Whether the cache is enabled
}

// HitRate returns the cache hit rate as a percentage.
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total) * 100
}

// evictOldest removes the least recently used entry from the cache. Dirty
// entries are eligible: the store persists inodes synchronously, so dropping
// one only costs a re-read, whereas skipping them would let the cache grow
// without bound once every entry is dirty.
// Must be called with c.mu locked.
func (c *inodeCache) evictOldest() {
	var oldestIno uint64
	var oldestTick uint64
	first := true

	for ino, entry := range c.entries {
		tick := entry.lastUsed.Load()
		if first || tick < oldestTick {
			oldestIno = ino
			oldestTick = tick
			first = false
		}
	}

	if !first {
		delete(c.entries, oldestIno)
	}
}

// Enable enables the cache with the specified maximum size.
func (c *inodeCache) Enable(maxSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxSize = maxSize
	c.enabled = maxSize > 0
	if !c.enabled {
		c.entries = make(map[uint64]*cacheEntry)
	}
}

// Disable disables the cache and flushes all entries.
func (c *inodeCache) Disable() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.enabled = false
	c.entries = make(map[uint64]*cacheEntry)
}
