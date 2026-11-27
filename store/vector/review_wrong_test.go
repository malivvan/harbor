package vector

import (
	"context"
	"testing"

	hstore "github.com/malivvan/eidos-db/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestQuantizedCollectionRequiresMatchingMode(t *testing.T) {
	path := t.TempDir() + "/quant.db"
	bdb, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	store, err := Open(context.Background(), bdb, Config{UseQuant: true})
	require.NoError(t, err)
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 2}}}))
	require.NoError(t, col.Flush())
	require.NoError(t, store.Close())

	bdb, err = bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	defer bdb.Close()
	plain, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	_, err = plain.LoadCollection("docs")
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestListCollectionsSkipsForeignEidosBuckets(t *testing.T) {
	bdb, err := bbolt.Open(t.TempDir()+"/shared.db", 0600, nil)
	require.NoError(t, err)
	defer bdb.Close()

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: concurrencyKey(9), EncryptPoints: true})
	require.NoError(t, err)
	_, err = store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)

	// Another sub-store joins the same database after the vector store was opened.
	require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{hstore.RECORD, hstore.FSTREE, hstore.SERIES} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	}))

	names, err := store.ListCollections()
	require.NoError(t, err)
	require.Equal(t, []string{"docs"}, names)
	require.NoError(t, store.RotateAll(ctx, concurrencyKey(9)))
	require.NoError(t, store.VerifyAll(ctx))
}
