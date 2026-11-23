package vector

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// aeadSnapshot captures every collection bucket (physical name → entries).
func aeadSnapshot(t *testing.T, bdb *bbolt.DB) map[string]map[string][]byte {
	t.Helper()

	out := map[string]map[string][]byte{}
	require.NoError(t, bdb.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if string(name) == collectionsMetaBucket {
				return nil
			}
			entries := map[string][]byte{}
			if err := b.ForEach(func(k, v []byte) error {
				if v != nil {
					entries[string(k)] = append([]byte(nil), v...)
				}
				return nil
			}); err != nil {
				return err
			}
			out[string(name)] = entries
			return nil
		})
	}))
	return out
}

// aeadContainsValue reports whether a snapshot holds want as a value.
func aeadContainsValue(snapshot map[string]map[string][]byte, want []byte) bool {
	for _, entries := range snapshot {
		for _, value := range entries {
			if bytes.Equal(value, want) {
				return true
			}
		}
	}
	return false
}

// TestAEADRotateAllReEncryptsEverything rotates a fully encrypted store and
// checks that the data survives, that every stored name and value changed, and
// that the old key stops working.
func TestAEADRotateAllReEncryptsEverything(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	keyA, keyB := aeadKey(31), aeadKey(32)

	store, bdb := aeadOpenStore(t, path, false, aeadNamesConfig(keyA))
	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.EnsureCollection("receipts"))
	require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))
	require.NoError(t, store.UpsertPoints("receipts", aeadTestPoints()[:1]))

	meta := CollectionMeta{Name: "invoices", VectorLen: 3, Metric: Cosine}
	require.NoError(t, store.SaveCollectionMeta("invoices", meta))

	before := aeadSnapshot(t, bdb)
	beforeMeta := aeadRawEntries(t, bdb, collectionsMetaBucket)
	require.Len(t, before, 2)

	require.NoError(t, store.RotateAll(ctx, keyB))
	require.NoError(t, store.VerifyAll(ctx))

	after := aeadSnapshot(t, bdb)
	require.Len(t, after, 2)
	for name := range before {
		require.NotContains(t, after, name, "encrypted collection names must change with the key")
	}
	for _, entries := range before {
		for _, value := range entries {
			require.False(t, aeadContainsValue(after, value), "point blobs must be re-sealed under the new key")
		}
	}

	afterMeta := aeadRawEntries(t, bdb, collectionsMetaBucket)
	for name, value := range beforeMeta {
		if name == markerKey {
			continue
		}
		require.NotContains(t, afterMeta, name, "encrypted metadata names must change with the key")
		require.False(t, aeadContainsValue(map[string]map[string][]byte{"meta": afterMeta}, value))
	}

	// Everything stays reachable with the rotated key.
	collections, err := store.ListCollections()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"invoices", "receipts"}, collections)

	loaded, err := store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))

	loadedMeta, err := store.LoadCollectionMeta("invoices")
	require.NoError(t, err)
	require.Equal(t, meta, *loadedMeta)
	require.NoError(t, store.Close())

	// The old key must no longer open the store; the new key must.
	bdb, err = bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	_, err = Open(ctx, bdb, aeadNamesConfig(keyA))
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, bdb.Close())

	reopened, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer reopened.Close()

	store, err = Open(ctx, reopened, aeadNamesConfig(keyB))
	require.NoError(t, err)
	require.NoError(t, store.VerifyAll(ctx))

	loaded, err = store.LoadCollection("receipts")
	require.NoError(t, err)
	require.Equal(t, []string{"p1"}, aeadPointIDs(loaded))
}

// TestAEADRotatePointsOnly rotates a store that encrypts values but not names:
// bucket and point keys stay clear and only the blobs are re-sealed.
func TestAEADRotatePointsOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	keyA, keyB := aeadKey(33), aeadKey(34)

	store, bdb := aeadOpenStore(t, path, false, Config{Secret: keyA, EncryptPoints: true})
	defer store.Close()

	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))

	before := aeadSnapshot(t, bdb)
	require.NoError(t, store.RotateAll(ctx, keyB))
	after := aeadSnapshot(t, bdb)

	require.Equal(t, len(before), len(after))
	for name, entries := range before {
		afterEntries, ok := after[name]
		require.True(t, ok, "plaintext bucket names are unchanged by a value rotation")
		require.Equal(t, len(entries), len(afterEntries))
		for key, value := range entries {
			require.Contains(t, afterEntries, key, "plaintext point IDs are unchanged")
			require.NotEqual(t, value, afterEntries[key], "point blobs must be re-sealed")
		}
	}

	require.NoError(t, store.VerifyAll(ctx))
	loaded, err := store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))
}

// TestAEADReEncryptCollectionKeepsOthersReadable proves a single collection can
// be rotated on its own: the previous key stays available for decryption, so the
// collections that were not rewritten keep working, and RotateAll finishes the
// job and drops it.
func TestAEADReEncryptCollectionKeepsOthersReadable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	keyA, keyB := aeadKey(35), aeadKey(36)

	store, _ := aeadOpenStore(t, path, false, Config{Secret: keyA, EncryptPoints: true})
	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.EnsureCollection("receipts"))
	require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))
	require.NoError(t, store.UpsertPoints("receipts", aeadTestPoints()[:1]))
	require.NoError(t, store.SaveCollectionMeta("invoices", CollectionMeta{Name: "invoices", VectorLen: 3, Metric: Cosine}))

	require.NoError(t, store.ReEncryptCollection(ctx, "invoices", keyB))

	loaded, err := store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))

	loadedMeta, err := store.LoadCollectionMeta("invoices")
	require.NoError(t, err)
	require.Equal(t, "invoices", loadedMeta.Name)

	// The collection that was not rotated yet keeps working through the
	// retained fallback key.
	receipts, err := store.LoadCollection("receipts")
	require.NoError(t, err)
	require.Equal(t, []string{"p1"}, aeadPointIDs(receipts))
	require.NoError(t, store.VerifyAll(ctx))

	// RotateAll rewrites everything and drops the old key.
	require.NoError(t, store.RotateAll(ctx, keyB))
	require.NoError(t, store.VerifyAll(ctx))
	require.NoError(t, store.Close())

	// Reopening with the new key alone reads both collections.
	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	store, err = Open(ctx, bdb, Config{Secret: keyB, EncryptPoints: true})
	require.NoError(t, err)
	require.NoError(t, store.VerifyAll(ctx))

	for _, name := range []string{"invoices", "receipts"} {
		loaded, err := store.LoadCollection(name)
		require.NoError(t, err)
		require.NotEmpty(t, loaded)
	}
}

// TestAEADReEncryptCollectionRejectedWithEncryptedKeys proves a single
// collection cannot be rotated when deterministic keys are encrypted: the new
// key could neither find nor decrypt the remaining collections, so the call is
// rejected instead of leaving the store unrecoverable.
func TestAEADReEncryptCollectionRejectedWithEncryptedKeys(t *testing.T) {
	for name, cfg := range map[string]Config{
		"point ids":        {Secret: aeadKey(47), EncryptPoints: true, EncryptPointIDs: true},
		"collection names": {Secret: aeadKey(48), EncryptPoints: true, EncryptCollectionNames: true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "enc.db")

			store, _ := aeadOpenStore(t, path, false, cfg)
			defer store.Close()
			require.NoError(t, store.EnsureCollection("invoices"))
			require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()[:1]))

			err := store.ReEncryptCollection(ctx, "invoices", aeadKey(49))
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), "RotateAll")

			// The store is untouched and still readable.
			loaded, err := store.LoadCollection("invoices")
			require.NoError(t, err)
			require.Equal(t, []string{"p1"}, aeadPointIDs(loaded))
			require.NoError(t, store.VerifyAll(ctx))
		})
	}
}

// TestAEADRotateRejectsInvalidInput covers rotation preconditions.
func TestAEADRotateRejectsInvalidInput(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	plain, _ := aeadOpenStore(t, path, false, Config{})
	require.ErrorIs(t, plain.RotateAll(ctx, aeadKey(37)), ErrInvalidConfig)
	require.ErrorIs(t, plain.VerifyAll(ctx), ErrInvalidConfig)
	require.ErrorIs(t, plain.ReEncryptCollection(ctx, "invoices", aeadKey(37)), ErrInvalidConfig)
	require.NoError(t, plain.Close())

	store, _ := aeadOpenStore(t, path, false, Config{Secret: aeadKey(37), EncryptPoints: true})
	defer store.Close()

	require.ErrorIs(t, store.RotateAll(ctx, []byte("short")), ErrInvalidKey)
	require.ErrorIs(t, store.ReEncryptCollection(ctx, "invoices", []byte("short")), ErrInvalidKey)
	require.ErrorIs(t, store.ReEncryptCollection(ctx, "", aeadKey(38)), ErrInvalidConfig)
	require.Error(t, store.ReEncryptCollection(ctx, "missing", aeadKey(38)))
}

// TestAEADVerifyAllDetectsTampering proves VerifyAll walks every encrypted
// structure, not only the marker.
func TestAEADVerifyAllDetectsTampering(t *testing.T) {
	t.Run("point blob", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		store, bdb := aeadOpenStore(t, path, false, Config{Secret: aeadKey(39), EncryptPoints: true})
		defer store.Close()
		require.NoError(t, store.EnsureCollection("invoices"))
		require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))
		require.NoError(t, store.VerifyAll(ctx))

		require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("invoices"))
			raw := append([]byte(nil), b.Get([]byte("p1"))...)
			raw[len(raw)-1] ^= 0xff
			return b.Put([]byte("p1"), raw)
		}))

		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("metadata", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		store, bdb := aeadOpenStore(t, path, false, Config{Secret: aeadKey(40), EncryptPoints: true})
		defer store.Close()
		require.NoError(t, store.EnsureCollection("invoices"))
		require.NoError(t, store.SaveCollectionMeta("invoices", CollectionMeta{Name: "invoices", VectorLen: 3, Metric: Cosine}))
		require.NoError(t, store.VerifyAll(ctx))

		require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(collectionsMetaBucket))
			raw := append([]byte(nil), b.Get([]byte("invoices"))...)
			raw[0] ^= 0xff
			return b.Put([]byte("invoices"), raw)
		}))

		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("encrypted point id", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		store, bdb := aeadOpenStore(t, path, false, Config{Secret: aeadKey(41), EncryptPoints: true, EncryptPointIDs: true})
		defer store.Close()
		require.NoError(t, store.EnsureCollection("invoices"))
		require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))
		require.NoError(t, store.VerifyAll(ctx))

		key, err := store.keyBytes("p1")
		require.NoError(t, err)
		require.NotEmpty(t, key)
		require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("invoices"))
			value := append([]byte(nil), b.Get(key)...)
			tampered := append([]byte(nil), key...)
			tampered[0] ^= 0xff
			if err := b.Delete(key); err != nil {
				return err
			}
			return b.Put(tampered, value)
		}))

		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("marker canary", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		store, bdb := aeadOpenStore(t, path, false, Config{Secret: aeadKey(42), EncryptPoints: true})
		defer store.Close()
		require.NoError(t, store.EnsureCollection("invoices"))

		require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(collectionsMetaBucket))
			raw := append([]byte(nil), b.Get([]byte(markerKey))...)
			raw[len(raw)-1] ^= 0xff
			return b.Put([]byte(markerKey), raw)
		}))

		require.ErrorIs(t, store.VerifyAll(ctx), ErrAuthFailed)
	})
}
