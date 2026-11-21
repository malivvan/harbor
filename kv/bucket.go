package kv

import (
	"context"

	"github.com/malivvan/storm/codec"
	stormdb "github.com/malivvan/storm/db"
	bolt "go.etcd.io/bbolt"
)

// Bucket provides key/value operations scoped to one named bucket.
type Bucket struct {
	store *Store
	name  string
	node  stormdb.Node
}

// Name returns the bucket name.
func (b *Bucket) Name() string {
	if b == nil {
		return ""
	}
	return b.name
}

// Store returns the Store that owns the bucket.
func (b *Bucket) Store() *Store {
	if b == nil {
		return nil
	}
	return b.store
}

// Node returns the Storm node for this bucket. It can be used for advanced
// operations outside the key/value API.
func (b *Bucket) Node() stormdb.Node {
	if b == nil {
		return nil
	}
	return b.node
}

// Codec returns the codec used by the bucket.
func (b *Bucket) Codec() codec.MarshalUnmarshaler {
	if b == nil || b.node == nil {
		return nil
	}
	return b.node.Codec()
}

// WithCodec returns a copy of the bucket using the given codec. The returned
// bucket is not added to the Store's cache. A nil codec returns the receiver.
func (b *Bucket) WithCodec(c codec.MarshalUnmarshaler) *Bucket {
	if b == nil || b.node == nil || c == nil {
		return b
	}
	copy := *b
	copy.node = b.node.WithCodec(c)
	return &copy
}

// Get decodes the value stored at key into to.
func (b *Bucket) Get(ctx context.Context, key any, to any) error {
	if b == nil || b.node == nil {
		return stormdb.ErrNilParam
	}
	return b.node.Get(ctx, "", key, to)
}

// Set encodes and stores value at key.
func (b *Bucket) Set(ctx context.Context, key any, value any) error {
	if b == nil || b.node == nil {
		return stormdb.ErrNilParam
	}
	return b.node.Set(ctx, "", key, value)
}

// Delete removes key from the bucket.
func (b *Bucket) Delete(ctx context.Context, key any) error {
	if b == nil || b.node == nil {
		return stormdb.ErrNilParam
	}
	return b.node.Delete(ctx, "", key)
}

// GetBytes returns the raw encoded value stored at key.
func (b *Bucket) GetBytes(ctx context.Context, key any) ([]byte, error) {
	if b == nil || b.node == nil {
		return nil, stormdb.ErrNilParam
	}
	return b.node.GetBytes(ctx, "", key)
}

// SetBytes stores a raw encoded value at key.
func (b *Bucket) SetBytes(ctx context.Context, key any, value []byte) error {
	if b == nil || b.node == nil {
		return stormdb.ErrNilParam
	}
	return b.node.SetBytes(ctx, "", key, value)
}

// Exists reports whether key exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, key any) (bool, error) {
	if b == nil || b.node == nil {
		return false, stormdb.ErrNilParam
	}
	return b.node.KeyExists(ctx, "", key)
}

// ForEach calls fn for each key/value pair in the bucket. Nested buckets are
// skipped. Keys and values are the raw encoded bytes; fn must not retain them
// after returning. Iteration stops at the first error returned by fn or by the
// context.
func (b *Bucket) ForEach(ctx context.Context, fn func(key, value []byte) error) error {
	if fn == nil {
		return stormdb.ErrNilParam
	}
	return b.view(ctx, func(bucket *bolt.Bucket) error {
		return bucket.ForEach(func(k, v []byte) error {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if v == nil {
				return nil
			}
			return fn(k, v)
		})
	})
}

// Keys returns copies of the raw encoded keys stored in the bucket. Nested
// buckets are excluded.
func (b *Bucket) Keys(ctx context.Context) ([][]byte, error) {
	if b == nil || b.node == nil {
		return nil, stormdb.ErrNilParam
	}

	var keys [][]byte
	err := b.ForEach(ctx, func(key, _ []byte) error {
		keys = append(keys, append([]byte(nil), key...))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// Len returns the number of key/value pairs in the bucket. Nested buckets are
// excluded.
func (b *Bucket) Len(ctx context.Context) (int, error) {
	if b == nil || b.node == nil {
		return 0, stormdb.ErrNilParam
	}

	count := 0
	err := b.ForEach(ctx, func(_, _ []byte) error {
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Drop deletes the bucket from the database and removes it from the Store's
// cache.
func (b *Bucket) Drop(ctx context.Context) error {
	if b == nil || b.store == nil {
		return stormdb.ErrNilParam
	}
	return b.store.DeleteBucket(ctx, b.name)
}

func (b *Bucket) view(ctx context.Context, fn func(*bolt.Bucket) error) error {
	if b == nil || b.store == nil || b.store.db == nil {
		return stormdb.ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	return b.store.db.NativeDB().View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		bucket := b.nativeBucket(tx)
		if bucket == nil {
			return stormdb.ErrNotFound
		}
		return fn(bucket)
	})
}

func (b *Bucket) nativeBucket(tx *bolt.Tx) *bolt.Bucket {
	var parent *bolt.Bucket
	if len(b.store.root) > 0 {
		parent = tx.Bucket(b.store.root)
		if parent == nil {
			return nil
		}
	}

	if b.name == "" {
		return parent
	}
	if parent == nil {
		return tx.Bucket([]byte(b.name))
	}
	return parent.Bucket([]byte(b.name))
}
