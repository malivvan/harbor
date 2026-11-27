package index_test

import (
	"context"
	"testing"

	eidos "github.com/malivvan/eidos-db/store/record"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// eidosTestDB bundles an eidos-db store with the bbolt handle the test opened, so
// tests can manipulate raw buckets without a public escape hatch.
type eidosTestDB struct {
	*eidos.Store
	bdb *bolt.DB
}

// openEidosDB opens a bbolt database and wraps it with eidos-db for the test.
func openEidosDB(tb testing.TB, path string) *eidosTestDB {
	tb.Helper()

	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = bdb.Close() })

	db, err := eidos.Open(context.Background(), bdb, eidos.Config{})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })

	return &eidosTestDB{Store: db, bdb: bdb}
}
