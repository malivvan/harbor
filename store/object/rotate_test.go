package object_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/store/object"
)

func rotateKeys() (oldKey, newKey []byte) {
	return bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
}

func TestObjectRotateAll(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, bdb := openDirect(t, object.Config{Secret: oldKey, EncryptValues: true})

	_, err := s.Put(ctx, "tenant-a", "a/b.txt", strings.NewReader("hello"), 5, "")
	require.NoError(t, err)
	_, err = s.Put(ctx, "tenant-b", "c.txt", strings.NewReader("world"), 5, "")
	require.NoError(t, err)

	require.NoError(t, s.RotateAll(ctx, newKey))
	require.NoError(t, s.VerifyAll(ctx))

	// The same store keeps working through the adopted key.
	data, err := s.ReadAll(ctx, "tenant-a", "a/b.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	// A fresh store over the same handle opens with the new key only.
	again, err := object.Open(ctx, bdb, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        newKey,
		EncryptValues: true,
	})
	require.NoError(t, err)
	data, err = again.ReadAll(ctx, "tenant-b", "c.txt")
	require.NoError(t, err)
	require.Equal(t, "world", string(data))

	// The previous key no longer decrypts the canary.
	_, err = object.Open(ctx, bdb, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        oldKey,
		EncryptValues: true,
	})
	require.ErrorIs(t, err, crypto.ErrAuthFailed)
}

func TestObjectVerifyAll(t *testing.T) {
	ctx := context.Background()
	key, _ := rotateKeys()
	s, bdb := openDirect(t, object.Config{Secret: key, EncryptValues: true})
	_, err := s.Put(ctx, "tenant", "k", strings.NewReader("payload"), 7, "")
	require.NoError(t, err)
	require.NoError(t, s.VerifyAll(ctx))

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket.OBJECT)).Bucket([]byte("tenant"))
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			corrupt := append([]byte(nil), v...)
			corrupt[len(corrupt)-1] ^= 0xff
			return b.Put(append([]byte(nil), k...), corrupt)
		})
	}))
	require.ErrorIs(t, s.VerifyAll(ctx), crypto.ErrAuthFailed)
}

func TestObjectVerifyAllPlaintext(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	require.ErrorIs(t, s.VerifyAll(ctx), object.ErrInvalidConfig)
	require.ErrorIs(t, s.RotateAll(ctx, bytes.Repeat([]byte{3}, 32)), object.ErrInvalidConfig)
}

func TestObjectReEncryptBucket(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, bdb := openDirect(t, object.Config{Secret: oldKey, EncryptValues: true})

	_, err := s.Put(ctx, "a", "k", strings.NewReader("one"), 3, "")
	require.NoError(t, err)
	_, err = s.Put(ctx, "b", "k", strings.NewReader("two"), 3, "")
	require.NoError(t, err)

	require.NoError(t, s.ReEncryptBucket(ctx, "a", newKey))
	// The old key stays available as a fallback until RotateAll.
	data, err := s.ReadAll(ctx, "a", "k")
	require.NoError(t, err)
	require.Equal(t, "one", string(data))
	data, err = s.ReadAll(ctx, "b", "k")
	require.NoError(t, err)
	require.Equal(t, "two", string(data))

	require.NoError(t, s.RotateAll(ctx, newKey))
	again, err := object.Open(ctx, bdb, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        newKey,
		EncryptValues: true,
	})
	require.NoError(t, err)
	data, err = again.ReadAll(ctx, "b", "k")
	require.NoError(t, err)
	require.Equal(t, "two", string(data))

	_, err = object.Open(ctx, bdb, object.Config{
		Root:          [][]byte{[]byte(bucket.OBJECT)},
		Secret:        oldKey,
		EncryptValues: true,
	})
	require.ErrorIs(t, err, crypto.ErrAuthFailed)
}
