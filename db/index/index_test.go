package index_test

import (
	"context"
	"testing"

	storm "github.com/malivvan/storm/db"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// stormTestDB bundles a Storm store with the bbolt handle the test opened, so
// tests can manipulate raw buckets without a public escape hatch.
type stormTestDB struct {
	*storm.Store
	bdb *bolt.DB
}

// openStormDB opens a bbolt database and wraps it with Storm for the test.
func openStormDB(tb testing.TB, path string) *stormTestDB {
	tb.Helper()

	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = bdb.Close() })

	db, err := storm.Open(context.Background(), bdb, storm.Config{})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })

	return &stormTestDB{Store: db, bdb: bdb}
}
