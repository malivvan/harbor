package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// aeadRotateConfig is the strongest configuration: values, metadata and keys.
func aeadRotateConfig(key []byte) Config {
	return Config{
		Secret:          key,
		EncryptValues:   true,
		EncryptMetadata: true,
		EncryptKeys:     true,
	}
}

// aeadTamperValue flips the last byte of the first record value it finds.
func aeadTamperValue(t *testing.T, bdb *bolt.DB, bucket string) {
	t.Helper()
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		require.NotNil(t, b)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				continue
			}
			tampered := append([]byte(nil), v...)
			tampered[len(tampered)-1] ^= 0xff
			return b.Put(append([]byte(nil), k...), tampered)
		}
		return errors.New("no entry to tamper with")
	}))
}

// TestAEADRotateReEncryptsEverything rotates a fully encrypted store and checks
// that the data survives, that the bytes on disk changed and that the old key
// stops working.
func TestAEADRotateReEncryptsEverything(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	keyA, keyB := aeadKey(31), aeadKey(32)

	bdb := aeadOpenBolt(t, path)
	st := aeadOpenStore(t, bdb, aeadRotateConfig(keyA))

	users := []*aeadUser{
		{Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30},
		{Name: "bob", Email: "bob@example.com", Group: "staff", Age: 42},
	}
	for _, u := range users {
		require.NoError(t, st.Save(ctx, u))
	}
	require.NoError(t, st.Set(ctx, "aeadkv", "customer", aeadPayload{Name: "acme", N: 7}))
	require.NoError(t, st.SetBytes(ctx, "aeadkv", "raw", []byte("raw-bytes")))

	before := aeadRawEntries(t, bdb, "aeadUser")
	require.Len(t, before, 2)

	require.NoError(t, st.ReEncrypt(ctx, keyB))
	require.NoError(t, st.VerifyAll(ctx))

	after := aeadRawEntries(t, bdb, "aeadUser")
	require.Len(t, after, 2)
	for key, value := range after {
		require.NotContains(t, before, key, "record keys must be re-derived under the new key")
		require.False(t, aeadContainsValue(before, value), "record values must be re-sealed under the new key")
	}

	// Everything stays reachable with the rotated key.
	var byID aeadUser
	require.NoError(t, st.One(ctx, "ID", users[0].ID, &byID))
	require.Equal(t, "alice", byID.Name)

	var byEmail aeadUser
	require.NoError(t, st.One(ctx, "Email", "bob@example.com", &byEmail))
	require.Equal(t, "bob", byEmail.Name)

	var staff []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &staff))
	require.Len(t, staff, 2, "a non-indexed field is matched by scanning")

	var payload aeadPayload
	require.NoError(t, st.Get(ctx, "aeadkv", "customer", &payload))
	require.Equal(t, "acme", payload.Name)

	raw, err := st.GetBytes(ctx, "aeadkv", "raw")
	require.NoError(t, err)
	require.Equal(t, "raw-bytes", string(raw), "raw SetBytes values are preserved verbatim")

	count, err := st.Count(ctx, &aeadUser{})
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.NoError(t, bdb.Close())

	// The old key must no longer open the store; the new key must.
	bdb = aeadOpenBolt(t, path)
	_, err = Open(ctx, bdb, aeadRotateConfig(keyA))
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, bdb.Close())

	bdb = aeadOpenBolt(t, path)
	defer bdb.Close()
	st = aeadOpenStore(t, bdb, aeadRotateConfig(keyB))
	require.NoError(t, st.VerifyAll(ctx))
	var alice aeadUser
	require.NoError(t, st.One(ctx, "Email", "alice@example.com", &alice))
	require.Equal(t, users[0].ID, alice.ID)
}

// aeadContainsValue reports whether values contains want.
func aeadContainsValue(values map[string][]byte, want []byte) bool {
	for _, value := range values {
		if string(value) == string(want) {
			return true
		}
	}
	return false
}

// TestAEADRotateValuesOnly rotates a store that encrypts values but not keys:
// ordered queries keep working and only the values are re-sealed.
func TestAEADRotateValuesOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	keyA, keyB := aeadKey(33), aeadKey(34)

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, Config{Secret: keyA, EncryptValues: true, EncryptMetadata: true})

	require.NoError(t, st.Save(ctx, &aeadUser{Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))
	require.NoError(t, st.Save(ctx, &aeadUser{Name: "bob", Email: "bob@example.com", Group: "staff", Age: 42}))

	before := aeadRawEntries(t, bdb, "aeadUser")
	require.NoError(t, st.RotateAll(ctx, keyB))
	after := aeadRawEntries(t, bdb, "aeadUser")

	require.Equal(t, len(before), len(after))
	for key := range before {
		require.Contains(t, after, key, "plaintext keys are unchanged by a value rotation")
	}

	require.NoError(t, st.VerifyAll(ctx))

	var ranged []aeadUser
	require.NoError(t, st.Range(ctx, "Age", 26, 45, &ranged))
	require.Len(t, ranged, 2)

	var prefixed []aeadUser
	require.NoError(t, st.Prefix(ctx, "Name", "al", &prefixed))
	require.Len(t, prefixed, 1)

	var all []aeadUser
	require.NoError(t, st.All(ctx, &all))
	require.Equal(t, []int{1, 2}, aeadIDs(aeadSortUsers(all)))
}

// TestAEADRotateRejectsInvalidInput covers rotation preconditions.
func TestAEADRotateRejectsInvalidInput(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()

	// Plaintext stores have nothing to rotate.
	plain := aeadOpenStore(t, bdb, Config{})
	err := plain.ReEncrypt(ctx, aeadKey(35))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.ErrorIs(t, plain.VerifyAll(ctx), ErrInvalidConfig)

	enc := aeadOpenStore(t, bdb, Config{Secret: aeadKey(35), EncryptValues: true})
	err = enc.ReEncrypt(ctx, []byte("short"))
	require.ErrorIs(t, err, ErrInvalidKey)
}

// TestAEADVerifyAllDetectsTampering proves VerifyAll walks every encrypted
// structure, not only the marker.
func TestAEADVerifyAllDetectsTampering(t *testing.T) {
	t.Run("record value", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		bdb := aeadOpenBolt(t, path)
		defer bdb.Close()
		st := aeadOpenStore(t, bdb, aeadRotateConfig(aeadKey(36)))
		require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))
		require.NoError(t, st.VerifyAll(ctx))

		aeadTamperValue(t, bdb, "aeadUser")
		require.ErrorIs(t, st.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("index entry", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		bdb := aeadOpenBolt(t, path)
		defer bdb.Close()
		st := aeadOpenStore(t, bdb, aeadRotateConfig(aeadKey(37)))
		require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))

		var indexName string
		require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("aeadUser"))
			return b.ForEach(func(k, v []byte) error {
				if v == nil && string(k) != metadataBucket {
					indexName = string(k)
				}
				return nil
			})
		}))
		require.NotEmpty(t, indexName, "the test needs at least one index bucket")

		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("aeadUser")).Bucket([]byte(indexName))
			c := b.Cursor()
			k, v := c.First()
			if v == nil {
				t.Fatalf("index bucket %q has no plain entries", indexName)
			}
			tampered := append([]byte(nil), k...)
			tampered[0] ^= 0xff
			if err := b.Delete(k); err != nil {
				return err
			}
			return b.Put(tampered, v)
		}))

		require.ErrorIs(t, st.VerifyAll(ctx), ErrAuthFailed)
	})

	t.Run("counter", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "enc.db")

		bdb := aeadOpenBolt(t, path)
		defer bdb.Close()
		st := aeadOpenStore(t, bdb, aeadRotateConfig(aeadKey(38)))
		require.NoError(t, st.Save(ctx, &aeadUser{Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))
		require.NoError(t, st.VerifyAll(ctx))

		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			meta := tx.Bucket([]byte("aeadUser")).Bucket([]byte(metadataBucket))
			raw := meta.Get([]byte("IDcounter"))
			tampered := append([]byte(nil), raw...)
			tampered[len(tampered)-1] ^= 0xff
			return meta.Put([]byte("IDcounter"), tampered)
		}))

		require.ErrorIs(t, st.VerifyAll(ctx), ErrAuthFailed)
	})
}
