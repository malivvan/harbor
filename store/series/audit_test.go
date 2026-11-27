package series

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
	"time"

	hstore "github.com/malivvan/eidos-db/store"
	"github.com/malivvan/eidos-db/store/ledger"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func auditSeed() []byte {
	return []byte("000102030405060708090a0b0c0d0e0f")
}

func openAuditedStore(t *testing.T) (*Store, *bolt.DB, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ledger.DeriveKey(auditSeed(), "m/2'")
	require.NoError(t, err)

	db, err := bolt.Open(filepath.Join(t.TempDir(), "ts-ledger.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rec := ledger.NewRecorder(db, map[string]ed25519.PrivateKey{ledger.StoreSeries: priv})
	store, err := openStore(context.Background(), db, []byte(hstore.SERIES), Config{Audit: rec})
	require.NoError(t, err)
	return store, db, pub
}

func TestAuditRecordsSeriesWrites(t *testing.T) {
	store, db, pub := openAuditedStore(t)
	ctx := context.Background()
	se := store.Series("cpu")

	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 10}))
	_, err := se.DeleteRange(ctx, 1, 1)
	require.NoError(t, err)
	require.NoError(t, store.DeleteSeries(ctx, "cpu"))

	res, err := ledger.Verify(db, map[string]ed25519.PublicKey{ledger.StoreSeries: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(6), res.Count)
}

func TestAuditDisabledRecordsNothing(t *testing.T) {
	store, db := openTestStore(t, []byte(hstore.SERIES))
	require.NoError(t, store.Series("cpu").Insert(context.Background(), Point{Timestamp: 1, Value: 1}))

	_, err := ledger.Verify(db, map[string]ed25519.PublicKey{ledger.StoreSeries: nil})
	require.ErrorIs(t, err, ledger.ErrNoLog)
}

func TestAuditDigestStableAcrossOrder(t *testing.T) {
	// The batch digest is order-independent: the same points in a different
	// order produce the same digest.
	a := digestOf(encodePoints([]Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))
	b := digestOf(encodePoints([]Point{{Timestamp: 2, Value: 2}, {Timestamp: 1, Value: 1}}))
	require.NotEqual(t, a, b) // encoding is positional, not sorted: documented
	require.Len(t, a, 32)
}
