package index_test

import (
	"context"
	"testing"

	tempest "github.com/malivvan/tempest/store/record"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// tempestTestDB bundles a tempest store with the bbolt handle the test opened, so
// tests can manipulate raw buckets without a public escape hatch.
type tempestTestDB struct {
	*tempest.Store
	bdb *bolt.DB
}

// openTempestDB opens a bbolt database and wraps it with tempest for the test.
func openTempestDB(tb testing.TB, path string) *tempestTestDB {
	tb.Helper()

	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = bdb.Close() })

	db, err := tempest.Open(context.Background(), bdb, tempest.Config{})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })

	return &tempestTestDB{Store: db, bdb: bdb}
}
