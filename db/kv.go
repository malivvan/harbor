package db

import (
	"context"
	"reflect"

	bolt "go.etcd.io/bbolt"
)

// KeyValueStore can store and fetch values by key
type KeyValueStore interface {
	// Get a value from a bucket
	Get(ctx context.Context, bucketName string, key any, to any) error
	// Set a key/value pair into a bucket. A nil value stores a zero-length
	// value; Get yields the destination's zero value for it.
	Set(ctx context.Context, bucketName string, key any, value any) error
	// Delete deletes a key from a bucket
	Delete(ctx context.Context, bucketName string, key any) error
	// GetBytes gets a raw value from a bucket.
	GetBytes(ctx context.Context, bucketName string, key any) ([]byte, error)
	// SetBytes sets a raw value into a bucket. A nil value stores a
	// zero-length value.
	SetBytes(ctx context.Context, bucketName string, key any, value []byte) error
	// KeyExists reports the presence of a key in a bucket.
	KeyExists(ctx context.Context, bucketName string, key any) (bool, error)
}

// GetBytes gets a raw value from a bucket. The returned bytes are exactly what
// SetBytes stored: encrypted stores seal and open them transparently.
func (n *node) GetBytes(ctx context.Context, bucketName string, key any) ([]byte, error) {
	raw, err := n.getBytesRaw(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}

	val, err := n.open(raw)
	if err != nil {
		return nil, wrapError("kv get bytes", err)
	}
	return val, nil
}

// getBytesRaw returns the stored value of a key without opening it.
func (n *node) getBytesRaw(ctx context.Context, bucketName string, key any) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("kv get bytes", err)
	}

	id, err := n.keyBytes(key, keyDomainKV)
	if err != nil {
		return nil, wrapError("kv get bytes", err)
	}

	if err := checkContext(ctx); err != nil {
		return nil, wrapError("kv get bytes", err)
	}

	var val []byte
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		raw, err := n.getBytes(ctx, tx, bucketName, id)
		if err != nil {
			return err
		}

		if err := checkContext(ctx); err != nil {
			return err
		}

		val = make([]byte, len(raw))
		copy(val, raw)
		return checkContext(ctx)
	})
	if err != nil {
		return nil, wrapError("kv get bytes", err)
	}

	return val, nil
}

func (n *node) getBytes(ctx context.Context, tx *bolt.Tx, bucketName string, id []byte) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	bucket := n.getBucket(tx, bucketName)
	if bucket == nil {
		return nil, ErrNotFound
	}

	raw := bucket.Get(id)
	if raw == nil {
		return nil, ErrNotFound
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	return raw, nil
}

// SetBytes sets a raw value into a bucket. A nil value stores a zero-length
// value; the typed Get reports the destination's zero value for it, and
// GetBytes returns an empty slice.
func (n *node) SetBytes(ctx context.Context, bucketName string, key any, value []byte) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("kv set bytes", err)
	}

	if key == nil {
		return wrapError("kv set bytes", ErrNilParam)
	}

	id, err := n.keyBytes(key, keyDomainKV)
	if err != nil {
		return wrapError("kv set bytes", err)
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("kv set bytes", err)
	}

	// Values are sealed at rest; GetBytes opens them again, so a SetBytes /
	// GetBytes round trip is unchanged.
	sealed, err := n.seal(value)
	if err != nil {
		return wrapError("kv set bytes", err)
	}

	return wrapError("kv set bytes", n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		return n.setBytes(ctx, tx, bucketName, id, sealed)
	}))
}

func (n *node) setBytes(ctx context.Context, tx *bolt.Tx, bucketName string, id, data []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	bucket, err := n.createBucketIfNotExists(tx, bucketName)
	if err != nil {
		return err
	}

	// save node configuration in the bucket
	_, err = newMeta(bucket, n)
	if err != nil {
		return err
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	if err := bucket.Put(id, data); err != nil {
		return err
	}
	return checkContext(ctx)
}

// Get a value from a bucket
func (n *node) Get(ctx context.Context, bucketName string, key any, to any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("kv get", err)
	}

	ref := reflect.ValueOf(to)

	if !ref.IsValid() || ref.Kind() != reflect.Ptr || ref.IsNil() {
		return wrapError("kv get", ErrPtrNeeded)
	}

	id, err := n.keyBytes(key, keyDomainKV)
	if err != nil {
		return wrapError("kv get", err)
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("kv get", err)
	}

	return wrapError("kv get", n.readTx(ctx, func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		raw, err := n.getBytes(ctx, tx, bucketName, id)
		if err != nil {
			return err
		}

		if err := checkContext(ctx); err != nil {
			return err
		}

		// A zero-length stored value has no encoded form. Treat it as the zero
		// value instead of failing the decode, so records written by the raw
		// SetBytes API stay readable.
		if len(raw) == 0 {
			ref.Elem().Set(reflect.Zero(ref.Elem().Type()))
			return checkContext(ctx)
		}

		// Decode into a temporary to preserve destination on error.
		temporary := reflect.New(ref.Elem().Type())
		if err := n.unmarshal(raw, temporary.Interface()); err != nil {
			return err
		}

		if err := checkContext(ctx); err != nil {
			return err
		}

		ref.Elem().Set(temporary.Elem())
		return nil
	}))
}

// Set a key/value pair into a bucket. A nil value stores a zero-length value;
// the typed Get returns the destination's zero value rather than a decode
// error. Use a typed nil pointer to store an encoded JSON null instead.
func (n *node) Set(ctx context.Context, bucketName string, key any, value any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("kv set", err)
	}

	var data []byte
	var err error
	if value != nil {
		if err := checkContext(ctx); err != nil {
			return wrapError("kv set", err)
		}
		data, err = n.codec.Marshal(value)
		if err != nil {
			return wrapError("kv set", err)
		}
		if err := checkContext(ctx); err != nil {
			return wrapError("kv set", err)
		}
	}

	return wrapError("kv set", n.SetBytes(ctx, bucketName, key, data))
}

// Delete deletes a key from a bucket
func (n *node) Delete(ctx context.Context, bucketName string, key any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("kv delete", err)
	}

	id, err := n.keyBytes(key, keyDomainKV)
	if err != nil {
		return wrapError("kv delete", err)
	}

	if err := checkContext(ctx); err != nil {
		return wrapError("kv delete", err)
	}

	return wrapError("kv delete", n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		return n.delete(ctx, tx, bucketName, id)
	}))
}

func (n *node) delete(ctx context.Context, tx *bolt.Tx, bucketName string, id []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	bucket := n.getBucket(tx, bucketName)
	if bucket == nil {
		return ErrNotFound
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	if err := bucket.Delete(id); err != nil {
		return err
	}
	return checkContext(ctx)
}

// KeyExists reports the presence of a key in a bucket.
func (n *node) KeyExists(ctx context.Context, bucketName string, key any) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, wrapError("kv key exists", err)
	}

	id, err := n.keyBytes(key, keyDomainKV)
	if err != nil {
		return false, wrapError("kv key exists", err)
	}

	if err := checkContext(ctx); err != nil {
		return false, wrapError("kv key exists", err)
	}

	var exists bool
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket := n.getBucket(tx, bucketName)
		if bucket == nil {
			return nil
		}

		v := bucket.Get(id)
		exists = v != nil

		if err := checkContext(ctx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return false, wrapError("kv key exists", err)
	}

	return exists, nil
}
