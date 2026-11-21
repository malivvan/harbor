package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"reflect"

	"github.com/malivvan/storm/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	metadataBucket = "__storm_metadata"
	metaCodec      = "codec"
)

// Bucket provides key/value operations scoped to one named bucket.
type Bucket struct {
	store *Store
	name  string
	codec codec.MarshalUnmarshaler
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

// Codec returns the codec used by the bucket.
func (b *Bucket) Codec() codec.MarshalUnmarshaler {
	if b == nil {
		return nil
	}
	return b.codec
}

// WithCodec returns a copy of the bucket using the given codec. The returned
// bucket is not added to the Store's cache. A nil codec returns the receiver.
func (b *Bucket) WithCodec(c codec.MarshalUnmarshaler) *Bucket {
	if b == nil || c == nil {
		return b
	}
	copy := *b
	copy.codec = c
	return &copy
}

// Get decodes the value stored at key into to.
func (b *Bucket) Get(ctx context.Context, key any, to any) error {
	if b == nil || b.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if key == nil {
		return ErrNilParam
	}

	ref := reflect.ValueOf(to)
	if !ref.IsValid() || ref.Kind() != reflect.Ptr || ref.IsNil() {
		return ErrPtrNeeded
	}

	id, err := keyBytes(key, b.codec)
	if err != nil {
		return err
	}

	return b.view(ctx, func(bucket *bolt.Bucket) error {
		raw := bucket.Get(id)
		if raw == nil {
			return ErrNotFound
		}

		temporary := reflect.New(ref.Elem().Type())
		if err := b.codec.Unmarshal(raw, temporary.Interface()); err != nil {
			return err
		}
		ref.Elem().Set(temporary.Elem())
		return nil
	})
}

// Set encodes and stores value at key.
func (b *Bucket) Set(ctx context.Context, key any, value any) error {
	if b == nil || b.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if key == nil {
		return ErrNilParam
	}

	id, err := keyBytes(key, b.codec)
	if err != nil {
		return err
	}

	var data []byte
	if value != nil {
		data, err = b.codec.Marshal(value)
		if err != nil {
			return err
		}
	}

	return b.write(ctx, true, func(bucket *bolt.Bucket) error {
		if err := ensureMetadata(bucket, b.codec); err != nil {
			return err
		}
		return bucket.Put(id, data)
	})
}

// Delete removes key from the bucket.
func (b *Bucket) Delete(ctx context.Context, key any) error {
	if b == nil || b.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if key == nil {
		return ErrNilParam
	}

	id, err := keyBytes(key, b.codec)
	if err != nil {
		return err
	}

	return b.write(ctx, false, func(bucket *bolt.Bucket) error {
		return bucket.Delete(id)
	})
}

// GetBytes returns the raw encoded value stored at key.
func (b *Bucket) GetBytes(ctx context.Context, key any) ([]byte, error) {
	if b == nil || b.store == nil {
		return nil, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if key == nil {
		return nil, ErrNilParam
	}

	id, err := keyBytes(key, b.codec)
	if err != nil {
		return nil, err
	}

	var value []byte
	err = b.view(ctx, func(bucket *bolt.Bucket) error {
		raw := bucket.Get(id)
		if raw == nil {
			return ErrNotFound
		}
		value = append([]byte(nil), raw...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// SetBytes stores a raw encoded value at key.
func (b *Bucket) SetBytes(ctx context.Context, key any, value []byte) error {
	if b == nil || b.store == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if key == nil {
		return ErrNilParam
	}

	id, err := keyBytes(key, b.codec)
	if err != nil {
		return err
	}

	return b.write(ctx, true, func(bucket *bolt.Bucket) error {
		if err := ensureMetadata(bucket, b.codec); err != nil {
			return err
		}
		return bucket.Put(id, value)
	})
}

// Exists reports whether key exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, key any) (bool, error) {
	if b == nil || b.store == nil {
		return false, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if key == nil {
		return false, ErrNilParam
	}

	id, err := keyBytes(key, b.codec)
	if err != nil {
		return false, err
	}

	var exists bool
	err = b.view(ctx, func(bucket *bolt.Bucket) error {
		exists = bucket.Get(id) != nil
		return nil
	})
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ForEach calls fn for each key/value pair in the bucket. Nested buckets are
// skipped. Keys and values are the raw encoded bytes; fn must not retain them
// after returning. Iteration stops at the first error returned by fn or by the
// context.
func (b *Bucket) ForEach(ctx context.Context, fn func(key, value []byte) error) error {
	if fn == nil {
		return ErrNilParam
	}
	if b == nil || b.store == nil {
		return ErrNilParam
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
	if b == nil || b.store == nil {
		return nil, ErrNilParam
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
	if b == nil || b.store == nil {
		return 0, ErrNilParam
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
		return ErrNilParam
	}
	return b.store.DeleteBucket(ctx, b.name)
}

func (b *Bucket) view(ctx context.Context, fn func(*bolt.Bucket) error) error {
	if b == nil || b.store == nil || b.store.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	return b.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket := b.getBucket(tx)
		if bucket == nil {
			return ErrNotFound
		}
		if err := fn(bucket); err != nil {
			return err
		}
		return checkContext(ctx)
	})
}

func (b *Bucket) write(ctx context.Context, create bool, fn func(*bolt.Bucket) error) error {
	if b == nil || b.store == nil || b.store.b == nil {
		return ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	return b.store.b.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		var bucket *bolt.Bucket
		var err error
		if create {
			bucket, err = b.createBucket(tx)
		} else {
			bucket = b.getBucket(tx)
			if bucket == nil {
				return ErrNotFound
			}
		}
		if err != nil {
			return err
		}

		if err := fn(bucket); err != nil {
			return err
		}
		return checkContext(ctx)
	})
}

func (b *Bucket) getBucket(tx *bolt.Tx) *bolt.Bucket {
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

func (b *Bucket) createBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if b.name == "" {
		if len(b.store.root) == 0 {
			return nil, ErrNilParam
		}
		if root := tx.Bucket(b.store.root); root != nil {
			return root, nil
		}
		return tx.CreateBucket(b.store.root)
	}

	if len(b.store.root) == 0 {
		return tx.CreateBucketIfNotExists([]byte(b.name))
	}

	root, err := tx.CreateBucketIfNotExists(b.store.root)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists([]byte(b.name))
}

func ensureMetadata(bucket *bolt.Bucket, c codec.MarshalUnmarshaler) error {
	metadata := bucket.Bucket([]byte(metadataBucket))
	if metadata != nil {
		name := metadata.Get([]byte(metaCodec))
		if string(name) != c.Name() {
			return ErrDifferentCodec
		}
		return nil
	}

	metadata, err := bucket.CreateBucket([]byte(metadataBucket))
	if err != nil {
		return err
	}
	return metadata.Put([]byte(metaCodec), []byte(c.Name()))
}

func keyBytes(key any, c codec.MarshalUnmarshaler) ([]byte, error) {
	switch t := key.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case int:
		return numberBytes(int64(t))
	case uint:
		return numberBytes(uint64(t))
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return numberBytes(t)
	default:
		return c.Marshal(key)
	}
}

func numberBytes(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
