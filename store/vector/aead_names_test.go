package vector

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// aeadNamesConfig returns a config that encrypts everything vec supports.
func aeadNamesConfig(key []byte) Config {
	return Config{
		Secret:                 key,
		EncryptPoints:          true,
		EncryptPointIDs:        true,
		EncryptCollectionNames: true,
	}
}

// TestAEADDeterministicNameEncryption covers the SIV helper directly:
// determinism, domain separation and authentication.
func TestAEADDeterministicNameEncryption(t *testing.T) {
	enc, err := resolveConfig(aeadNamesConfig(aeadKey(21)))
	require.NoError(t, err)
	require.NotNil(t, enc)

	id, err := enc.sealName("point-1", keyDomainID)
	require.NoError(t, err)
	idAgain, err := enc.sealName("point-1", keyDomainID)
	require.NoError(t, err)
	require.Equal(t, id, idAgain, "the same name in the same domain must map to the same key")

	name, err := enc.sealName("point-1", keyDomainName)
	require.NoError(t, err)
	require.NotEqual(t, id, name, "domains must be separated")
	require.NotContains(t, string(id), "point-1")

	other, err := enc.sealName("point-2", keyDomainID)
	require.NoError(t, err)
	require.NotEqual(t, id, other, "distinct names must produce distinct keys")

	opened, err := enc.openName(id, keyDomainID)
	require.NoError(t, err)
	require.Equal(t, "point-1", opened)

	_, err = enc.openName(id, keyDomainName)
	require.ErrorIs(t, err, ErrAuthFailed, "a key of another domain must not authenticate")

	// A plaintext store must not touch names at all.
	plain, err := resolveConfig(Config{})
	require.NoError(t, err)
	require.Nil(t, plain)
	raw, err := plain.sealName("point-1", keyDomainID)
	require.NoError(t, err)
	require.Equal(t, []byte("point-1"), raw)
}

// TestAEADEncryptedNamesHideKeys checks that stored IDs and collection names no
// longer reveal their plaintext.
func TestAEADEncryptedNamesHideKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	store, bdb := aeadOpenStore(t, path, false, aeadNamesConfig(aeadKey(22)))

	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))
	require.NoError(t, store.SaveCollectionMeta("invoices", CollectionMeta{Name: "invoices", VectorLen: 3, Metric: Cosine}))

	require.NoError(t, bdb.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte("invoices")), "collection names must be encrypted")

		var checked int
		err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if string(name) == collectionsMetaBucket {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				if v == nil {
					return nil
				}
				checked++
				require.NotContains(t, string(k), "p1", "point IDs must be encrypted")
				return nil
			})
		})
		require.NoError(t, err)
		require.NotZero(t, checked, "the test needs at least one stored point")
		return nil
	}))

	meta := aeadRawEntries(t, bdb, collectionsMetaBucket)
	require.NotContains(t, meta, "invoices", "metadata keys must be encrypted")
}

// TestAEADEncryptedNamesRoundTrip proves exact lookups and listings keep
// working when point IDs and collection names are encrypted.
func TestAEADEncryptedNamesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	store, bdb := aeadOpenStore(t, path, false, aeadNamesConfig(aeadKey(23)))
	defer store.Close()

	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.EnsureCollection("receipts"))
	points := aeadTestPoints()
	require.NoError(t, store.UpsertPoints("invoices", points))

	// Exact reload by ID.
	loaded, err := store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))
	require.Equal(t, int64(1), loaded["p1"].Payload["n"])

	// Listing returns the logical names, not the stored ciphertext.
	collections, err := store.ListCollections()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"invoices", "receipts"}, collections)

	// Metadata round trip and listing.
	meta := CollectionMeta{Name: "invoices", VectorLen: 3, Metric: Euclid}
	require.NoError(t, store.SaveCollectionMeta("invoices", meta))
	loadedMeta, err := store.LoadCollectionMeta("invoices")
	require.NoError(t, err)
	require.Equal(t, meta, *loadedMeta)

	metas, err := store.ListCollectionMetas()
	require.NoError(t, err)
	require.Len(t, metas, 1)
	require.Equal(t, "invoices", metas[0].Name)

	// Exact deletes.
	require.NoError(t, store.DeletePoints("invoices", []string{"p2"}))
	loaded, err = store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p3"}, aeadPointIDs(loaded))

	// A search built on stored points still works.
	col, err := store.NewCollection("invoices", 3, Euclid, false)
	require.NoError(t, err)
	scored, err := col.Search([]float32{1, 0, 0}, nil, 2)
	require.NoError(t, err)
	require.NotEmpty(t, scored)

	// Dropping removes the encrypted bucket and metadata.
	require.NoError(t, store.DropCollection("invoices"))
	collections, err = store.ListCollections()
	require.NoError(t, err)
	require.Equal(t, []string{"receipts"}, collections)

	meta = CollectionMeta{Name: "receipts", VectorLen: 3, Metric: Cosine}
	require.NoError(t, store.SaveCollectionMeta("receipts", meta))
	require.NoError(t, store.DropCollection("receipts"))
	metas, err = store.ListCollectionMetas()
	require.NoError(t, err)
	require.Empty(t, metas)

	// Nothing but the marker is left in the metadata bucket.
	rawMeta := aeadRawEntries(t, bdb, collectionsMetaBucket)
	require.Len(t, rawMeta, 1)
	require.Contains(t, rawMeta, markerKey)
}

// TestAEADEncryptedNamesRootScope proves encrypted names work under a root
// bucket.
func TestAEADEncryptedNamesRootScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	store, err := openStore(context.Background(), bdb, []byte("tenant"), aeadNamesConfig(aeadKey(24)))
	require.NoError(t, err)

	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))

	collections, err := store.ListCollections()
	require.NoError(t, err)
	require.Equal(t, []string{"invoices"}, collections)

	loaded, err := store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Len(t, loaded, 3)

	require.NoError(t, store.DropCollection("invoices"))
	collections, err = store.ListCollections()
	require.NoError(t, err)
	require.Empty(t, collections)
}

// TestAEADEncryptedNamesSurviveReopen proves encrypted names are stable across
// store instances opened with the same key.
func TestAEADEncryptedNamesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	cfg := aeadNamesConfig(aeadKey(25))

	store, _ := aeadOpenStore(t, path, false, cfg)
	require.NoError(t, store.EnsureCollection("invoices"))
	require.NoError(t, store.UpsertPoints("invoices", aeadTestPoints()))
	require.NoError(t, store.SaveCollectionMeta("invoices", CollectionMeta{Name: "invoices", VectorLen: 3, Metric: Cosine}))
	require.NoError(t, store.Close())

	store, _ = aeadOpenStore(t, path, false, cfg)
	defer store.Close()

	collections, err := store.ListCollections()
	require.NoError(t, err)
	require.Equal(t, []string{"invoices"}, collections)

	loaded, err := store.LoadCollection("invoices")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))

	var point = loaded["p1"]
	require.Equal(t, float32(1), point.Vector[0])
}
