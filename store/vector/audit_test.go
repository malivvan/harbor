package vector

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
	"time"

	hstore "github.com/malivvan/harbor/store"
	"github.com/malivvan/harbor/store/ledger"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func auditSeed() []byte {
	return []byte("000102030405060708090a0b0c0d0e0f")
}

func openAuditedStore(t *testing.T) (*Store, *bolt.DB, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ledger.DeriveKey(auditSeed(), "m/3'")
	require.NoError(t, err)

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "vec-ledger.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	rec := ledger.NewRecorder(bdb, map[string]ed25519.PrivateKey{ledger.StoreVector: priv})
	store, err := openStore(context.Background(), bdb, []byte(hstore.VECTOR), Config{Audit: rec})
	require.NoError(t, err)
	return store, bdb, pub
}

func TestAuditRecordsCollectionWrites(t *testing.T) {
	store, bdb, pub := openAuditedStore(t)

	col, err := store.NewCollection("docs", 3, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 2, 3}}}))
	require.NoError(t, col.Flush())
	_, err = col.Delete([]string{"a"}, nil)
	require.NoError(t, err)
	require.NoError(t, store.DropCollection("docs"))

	res, err := ledger.Verify(bdb, map[string]ed25519.PublicKey{ledger.StoreVector: pub})
	require.NoError(t, err)
	// create_collection (EnsureCollection) + save_collection_meta + upsert +
	// delete + drop_collection.
	require.Equal(t, uint64(5), res.Count)
}

func TestAuditDisabledRecordsNothing(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "vec-noaudit.db"))
	col, err := store.NewCollection("docs", 3, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 2, 3}}}))
	require.NoError(t, col.Flush())

	_, err = ledger.Verify(store.db, map[string]ed25519.PublicKey{ledger.StoreVector: nil})
	require.ErrorIs(t, err, ledger.ErrNoLog)
}

func TestAuditDigestStableAcrossOrder(t *testing.T) {
	// The batch digest sorts by point ID, so the same points in a different
	// order produce the same digest.
	a := digestOf(encodePointsDigest([]PointStruct{{ID: "a", Vector: []float32{1}}, {ID: "b", Vector: []float32{2}}}))
	b := digestOf(encodePointsDigest([]PointStruct{{ID: "b", Vector: []float32{2}}, {ID: "a", Vector: []float32{1}}}))
	require.Equal(t, a, b)
	require.Len(t, a, 32)
}
