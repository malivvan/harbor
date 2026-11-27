package index

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/malivvan/tempest/internal"
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
		ids, err := NewUniqueIndex(b, []byte("tempest__ids"))
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
	ids, err := NewUniqueIndex(idx.IndexBucket, []byte("tempest__ids"))
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
		// memory. EncodeListKey below builds the replacement in a fresh buffer.
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
		// Skip nested buckets such as the tempest__ids sentinel, which the
		// prefix of the value "tempest" also matches.
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
		// Skip nested buckets such as the tempest__ids sentinel.
		if id == nil {
			continue
		}
		// The prefix includes the value terminator, so every hit is an exact
		// value match; keys for "a__b" never match the value "a".
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

		// Skip the tempest__ids sentinel and any other nested bucket.
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

		if id == nil || bytes.Equal(k, []byte("tempest__ids")) {
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

	// Seek on the escaped form: the raw logical value is not a valid cursor
	// position because NUL bytes are escaped on disk.
	var seekMin, seekMax []byte
	if len(min) > 0 {
		seekMin = escapeListValue(min)
	}
	if len(max) > 0 {
		seekMax = escapeListValue(max)
	}

	c := internal.RangeCursor{
		C:       idx.IndexBucket.Cursor(),
		Reverse: opts != nil && opts.Reverse,
		Min:     min,
		Max:     max,
		SeekMin: seekMin,
		SeekMax: seekMax,
		CompareFn: func(val, limit []byte) int {
			value, ok := ListKeyValue(val)
			if !ok {
				return bytes.Compare(val, limit)
			}
			return bytes.Compare(value, limit)
		},
	}

	for k, id := c.First(); c.Continue(k); k, id = c.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index range", err)
		}

		if id == nil || bytes.Equal(k, []byte("tempest__ids")) {
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

	// The terminator keeps a raw prefix scan close, but a NUL in the search
	// prefix needs the escaped form to match encoded keys. Decode the value
	// component and match on the logical bytes for exactness.
	scanPrefix := escapeListValue(prefix)
	c := internal.PrefixCursor{
		C:       idx.IndexBucket.Cursor(),
		Reverse: opts != nil && opts.Reverse,
		Prefix:  scanPrefix,
	}

	for k, id := c.First(); k != nil && c.Continue(k); k, id = c.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, wrapError("index prefix", err)
		}

		if id == nil || bytes.Equal(k, []byte("tempest__ids")) {
			continue
		}
		if value, ok := ListKeyValue(k); !ok || !bytes.HasPrefix(value, prefix) {
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

// listValueLenSize is the trailing big-endian word that records the length of
// the escaped value component in a list-index key.
const listValueLenSize = 8

// listValueTerminator ends the escaped value component of a list-index key. It
// cannot occur inside an escaped value because NUL bytes are escaped as
// 0x00 0xFF.
var listValueTerminator = []byte{0x00, 0x00}

// escapeListValue encodes a value so that the byte ordering of encoded values
// matches the byte ordering of the original values and the terminator is
// unambiguous. NUL is the only byte that needs escaping: 0x00 becomes
// 0x00 0xFF, everything else is copied verbatim.
func escapeListValue(value []byte) []byte {
	escaped := make([]byte, 0, len(value))
	for _, b := range value {
		if b == 0x00 {
			escaped = append(escaped, 0x00, 0xFF)
			continue
		}
		escaped = append(escaped, b)
	}
	return escaped
}

// unescapeListValue reverses escapeListValue for the given encoded value.
func unescapeListValue(encoded []byte) ([]byte, bool) {
	value := make([]byte, 0, len(encoded))
	for i := 0; i < len(encoded); i++ {
		if encoded[i] != 0x00 {
			value = append(value, encoded[i])
			continue
		}
		if i+1 >= len(encoded) || encoded[i+1] != 0xFF {
			return nil, false
		}
		value = append(value, 0x00)
		i++
	}
	return value, true
}

// EncodeListKey builds "escape(value) ++ 0x00 0x00 ++ id ++
// uint64(len(escape(value)))".
//
// The escaped, terminated value keeps the index sorted by value (rather than by
// the raw concatenation), and makes exact-value lookups unambiguous even when
// one value is a prefix of another or contains NUL or "__". The trailing length
// locates the value component without scanning, which matters because IDs are
// arbitrary bytes.
func EncodeListKey(value, id []byte) []byte {
	escaped := escapeListValue(value)
	key := make([]byte, 0, len(escaped)+2+len(id)+listValueLenSize)
	key = append(key, escaped...)
	key = append(key, listValueTerminator...)
	key = append(key, id...)
	var length [listValueLenSize]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(escaped)))
	return append(key, length[:]...)
}

// ListKeyValueLen returns the length of the escaped value component of a
// list-index key, reporting false for a malformed key.
func ListKeyValueLen(key []byte) (int, bool) {
	if len(key) < listValueLenSize+len(listValueTerminator) {
		return 0, false
	}
	n := int(binary.BigEndian.Uint64(key[len(key)-listValueLenSize:]))
	if n < 0 || n > len(key)-listValueLenSize-2 {
		return 0, false
	}
	if key[n] != 0x00 || key[n+1] != 0x00 {
		return 0, false
	}
	return n, true
}

// ListKeyValue returns the logical value encoded in a list-index key.
func ListKeyValue(key []byte) ([]byte, bool) {
	n, ok := ListKeyValueLen(key)
	if !ok {
		return nil, false
	}
	return unescapeListValue(key[:n])
}

// DecodeListKey splits a list-index key into its logical value and ID.
func DecodeListKey(key []byte) (value, id []byte, ok bool) {
	n, ok := ListKeyValueLen(key)
	if !ok {
		return nil, nil, false
	}
	value, ok = unescapeListValue(key[:n])
	if !ok {
		return nil, nil, false
	}
	return value, key[n+2 : len(key)-listValueLenSize], true
}

// generatePrefix returns the search prefix for an exact value: the escaped
// value followed by its terminator. An empty value has no valid prefix, because
// Add rejects empty values; returning nil lets callers report "no match"
// instead of scanning every index key.
func generatePrefix(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	prefix := escapeListValue(value)
	return append(prefix, listValueTerminator...)
}
