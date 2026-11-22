package index

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/malivvan/storm/internal"
	bolt "go.etcd.io/bbolt"
)

// NewListIndex loads a ListIndex
func NewListIndex(parent *bolt.Bucket, indexName []byte) (*ListIndex, error) {
	if parent == nil || len(indexName) == 0 {
		return nil, wrapError("index new list", ErrNilParam)
	}
	var err error
	b := parent.Bucket(indexName)
	if b == nil {
		if !parent.Writable() {
			return nil, wrapError("index new list", ErrNotFound)
		}
		b, err = parent.CreateBucket(indexName)
		if err != nil {
			return nil, wrapError("index new list", err)
		}
	}

	idx := &ListIndex{
		IndexBucket: b,
		Parent:      parent,
	}

	// The ID bucket is only needed to mutate the index. In a read-only
	// transaction skip the extra bucket open and resolve it lazily.
	if parent.Writable() {
		ids, err := NewUniqueIndex(b, []byte("storm__ids"))
		if err != nil {
			return nil, wrapError("index new list", err)
		}
		idx.IDs = ids
	}

	return idx, nil
}

// NewListIndexRead opens a ListIndex for reading without opening its IDs
// bucket. It is the read-only counterpart of NewListIndex.
func NewListIndexRead(parent *bolt.Bucket, indexName []byte) (*ListIndex, error) {
	if parent == nil || len(indexName) == 0 {
		return nil, wrapError("index new list", ErrNilParam)
	}
	b := parent.Bucket(indexName)
	if b == nil {
		return nil, wrapError("index new list", ErrNotFound)
	}
	return &ListIndex{IndexBucket: b, Parent: parent}, nil
}

// ids returns the ID bucket, opening it on first use.
func (idx *ListIndex) ids() (*UniqueIndex, error) {
	if idx.IDs != nil {
		return idx.IDs, nil
	}
	ids, err := NewUniqueIndex(idx.IndexBucket, []byte("storm__ids"))
	if err != nil {
		return nil, err
	}
	idx.IDs = ids
	return ids, nil
}

// ListIndex is an index that references values and the corresponding IDs.
type ListIndex struct {
	// Parent contains the record bucket and its index buckets.
	Parent *bolt.Bucket
	// IndexBucket stores value-to-ID list entries.
	IndexBucket *bolt.Bucket
	// IDs tracks indexed IDs for cleanup and full-record traversal.
	IDs *UniqueIndex
}

// Add a value to the list index
func (idx *ListIndex) Add(ctx context.Context, newValue []byte, targetID []byte) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("index add", err)
	}
	if len(newValue) == 0 {
		return wrapError("index add", ErrNilParam)
	}
	if len(targetID) == 0 {
		return wrapError("index add", ErrNilParam)
	}

	ids, err := idx.ids()
	if err != nil {
		return wrapError("index add", err)
	}

	key, err := ids.Get(ctx, targetID)
	if err != nil {
		return wrapError("index add", err)
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("index add", err)
	}

	if key != nil {
		err := idx.IndexBucket.Delete(key)
		if err != nil {
			return wrapError("index add", err)
		}

		if err := checkContext(ctx); err != nil {
			return wrapError("index add", err)
		}

		err = ids.Remove(ctx, targetID)
		if err != nil {
			return wrapError("index add", err)
		}

		if err := checkContext(ctx); err != nil {
			return wrapError("index add", err)
		}

		// Never reuse the slice returned by bbolt: it may alias database
		// memory. Build the new key in a fresh buffer.
		key = nil
	}

	key = EncodeListKey(newValue, targetID)

	err = ids.Add(ctx, targetID, key)
	if err != nil {
		return wrapError("index add", err)
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("index add", err)
	}

	if err := idx.IndexBucket.Put(key, targetID); err != nil {
		return wrapError("index add", err)
	}

	return wrapError("index add", checkContext(ctx))
}

// Remove a value from the unique index
func (idx *ListIndex) Remove(ctx context.Context, value []byte) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("index remove", err)
	}

	var err error
	var keys [][]byte

	ids, err := idx.ids()
	if err != nil {
		return wrapError("index remove", err)
	}

	c := idx.IndexBucket.Cursor()
	prefix := generatePrefix(value)
	if prefix == nil {
		return nil
	}

	for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if err := checkContext(ctx); err != nil {
			return wrapError("index remove", err)
		}
		// Skip nested buckets such as the storm__ids sentinel, which the
		// prefix of the value "storm" also matches.
		if v == nil {
			continue
		}
		// Defensive copy: cursor buffer is not stable across mutations.
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		keys = append(keys, keyCopy)
	}

	for _, k := range keys {
		if err := checkContext(ctx); err != nil {
			return wrapError("index remove", err)
		}
		err = idx.IndexBucket.Delete(k)
		if err != nil {
			return wrapError("index remove", err)
		}
		if err := checkContext(ctx); err != nil {
			return wrapError("index remove", err)
		}

		// The IDs bucket is keyed by the target ID, which sits between the
		// "<value>__" prefix and the trailing value-length word.
		id := k[len(prefix) : len(k)-listValueLenSize]
		err = ids.RemoveID(ctx, id)
		if err != nil {
			return wrapError("index remove", err)
		}
	}

	return wrapError("index remove", checkContext(ctx))
}

// HasValueID reports whether id is indexed under value. It is O(1): the ID
// bucket maps an ID straight to its full index key.
func (idx *ListIndex) HasValueID(ctx context.Context, value, id []byte) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, wrapError("index has value id", err)
	}
	ids, err := idx.ids()
	if err != nil {
		return false, wrapError("index has value id", err)
	}
	entry, err := ids.Get(ctx, id)
	if err != nil || entry == nil {
		return false, err
	}
	prefix := generatePrefix(value)
	if prefix == nil {
		return false, nil
	}
	return bytes.HasPrefix(entry, prefix), nil
}

// RemoveID removes an ID from the list index
func (idx *ListIndex) RemoveID(ctx context.Context, targetID []byte) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("index remove id", err)
	}

	ids, err := idx.ids()
	if err != nil {
		return wrapError("index remove id", err)
	}

	value, err := ids.Get(ctx, targetID)
	if err != nil {
		return wrapError("index remove id", err)
	}
	if value == nil {
		return nil
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("index remove id", err)
	}

	err = idx.IndexBucket.Delete(value)
	if err != nil {
		return wrapError("index remove id", err)
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("index remove id", err)
	}

	err = idx.IDs.Remove(ctx, targetID)
	if err != nil {
		return wrapError("index remove id", err)
	}

	return wrapError("index remove id", checkContext(ctx))
}

// Get the first ID corresponding to the given value
func (idx *ListIndex) Get(ctx context.Context, value []byte) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index get", err)
	}

	c := idx.IndexBucket.Cursor()
	prefix := generatePrefix(value)
	if prefix == nil {
		return nil, nil
	}

	for k, id := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, id = c.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index get", err)
		}
		// The storm__ids sentinel is a nested bucket (nil value) that shares
		// the prefix of the value "storm".
		if id == nil {
			continue
		}
		return id, nil
	}

	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index get", err)
	}

	return nil, nil
}

// All the IDs corresponding to the given value
func (idx *ListIndex) All(ctx context.Context, value []byte, opts *Options) ([][]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index all", err)
	}

	list := resultSlice(resultCap(idx.IndexBucket, opts))
	c := idx.IndexBucket.Cursor()
	cur := internal.Cursor{C: c, Reverse: opts != nil && opts.Reverse}

	prefix := generatePrefix(value)
	if prefix == nil {
		return nil, nil
	}

	k, id := c.Seek(prefix)
	if cur.Reverse {
		var count int
		kc := k
		idc := id
		for ; kc != nil && bytes.HasPrefix(kc, prefix); kc, idc = c.Next() {
			if err := checkContext(ctx); err != nil {
				return nil, wrapError("index all", err)
			}
			if idc != nil {
				count++
			}
			k, id = kc, idc
		}
		if kc != nil {
			k, id = c.Prev()
		}
		list = make([][]byte, 0, count)
	}

	for ; bytes.HasPrefix(k, prefix); k, id = cur.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index all", err)
		}

		// Skip the storm__ids sentinel and any other nested bucket.
		if id == nil {
			continue
		}

		if opts != nil && opts.Skip > 0 {
			opts.Skip--
			continue
		}

		if opts != nil && opts.Limit == 0 {
			break
		}

		if opts != nil && opts.Limit > 0 {
			opts.Limit--
		}

		list = append(list, id)
	}

	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index all", err)
	}

	return list, nil
}

// AllRecords returns all the IDs of this index
func (idx *ListIndex) AllRecords(ctx context.Context, opts *Options) ([][]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index all records", err)
	}

	list := resultSlice(resultCap(idx.IndexBucket, opts))

	c := internal.Cursor{C: idx.IndexBucket.Cursor(), Reverse: opts != nil && opts.Reverse}

	for k, id := c.First(); k != nil; k, id = c.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index all records", err)
		}

		if id == nil || bytes.Equal(k, []byte("storm__ids")) {
			continue
		}

		if opts != nil && opts.Skip > 0 {
			opts.Skip--
			continue
		}

		if opts != nil && opts.Limit == 0 {
			break
		}

		if opts != nil && opts.Limit > 0 {
			opts.Limit--
		}

		list = append(list, id)
	}

	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index all records", err)
	}

	return list, nil
}

// Range returns the ids corresponding to the given range of values
func (idx *ListIndex) Range(ctx context.Context, min []byte, max []byte, opts *Options) ([][]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index range", err)
	}

	list := resultSlice(resultCap(idx.IndexBucket, opts))

	c := internal.RangeCursor{
		C:       idx.IndexBucket.Cursor(),
		Reverse: opts != nil && opts.Reverse,
		Min:     min,
		Max:     max,
		CompareFn: func(val, limit []byte) int {
			if n, ok := ListKeyValueLen(val); ok {
				return bytes.Compare(val[:n], limit)
			}
			return bytes.Compare(val, limit)
		},
	}

	for k, id := c.First(); c.Continue(k); k, id = c.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index range", err)
		}

		if id == nil || bytes.Equal(k, []byte("storm__ids")) {
			continue
		}

		if opts != nil && opts.Skip > 0 {
			opts.Skip--
			continue
		}

		if opts != nil && opts.Limit == 0 {
			break
		}

		if opts != nil && opts.Limit > 0 {
			opts.Limit--
		}

		list = append(list, id)
	}

	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index range", err)
	}

	return list, nil
}

// Prefix returns the ids whose values have the given prefix.
func (idx *ListIndex) Prefix(ctx context.Context, prefix []byte, opts *Options) ([][]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index prefix", err)
	}

	list := resultSlice(resultCap(idx.IndexBucket, opts))

	c := internal.PrefixCursor{
		C:       idx.IndexBucket.Cursor(),
		Reverse: opts != nil && opts.Reverse,
		Prefix:  prefix,
	}

	for k, id := c.First(); k != nil && c.Continue(k); k, id = c.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index prefix", err)
		}

		if id == nil || bytes.Equal(k, []byte("storm__ids")) {
			continue
		}

		if opts != nil && opts.Skip > 0 {
			opts.Skip--
			continue
		}

		if opts != nil && opts.Limit == 0 {
			break
		}

		if opts != nil && opts.Limit > 0 {
			opts.Limit--
		}

		list = append(list, id)
	}

	if err := checkContext(ctx); err != nil {
		return nil, wrapError("index prefix", err)
	}

	return list, nil
}

// generatePrefix returns the "<value>__" search prefix. An empty value has no
// valid prefix, because Add rejects empty values; returning nil lets callers
// report "no match" instead of scanning every index key.
// listValueLenSize is the trailing big-endian word that records the length of
// the value component in a list-index key.
const listValueLenSize = 8

// EncodeListKey builds "value ++ __ ++ id ++ uint64(len(value))". The trailing
// length makes the split unambiguous even when the value or the id contains
// "__".
func EncodeListKey(value, id []byte) []byte {
	key := make([]byte, 0, len(value)+2+len(id)+listValueLenSize)
	key = append(key, value...)
	key = append(key, '_', '_')
	key = append(key, id...)
	var length [listValueLenSize]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	return append(key, length[:]...)
}

// ListKeyValueLen returns the length of the value component of a list-index
// key, reporting false for a malformed key.
func ListKeyValueLen(key []byte) (int, bool) {
	if len(key) < listValueLenSize+2 {
		return 0, false
	}
	n := int(binary.BigEndian.Uint64(key[len(key)-listValueLenSize:]))
	if n < 0 || n > len(key)-listValueLenSize-2 {
		return 0, false
	}
	return n, true
}

func generatePrefix(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	prefix := make([]byte, 0, len(value)+2)
	prefix = append(prefix, value...)
	return append(prefix, '_', '_')
}
