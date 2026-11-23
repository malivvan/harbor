package index_test

import (
	"context"
	"testing"

	harbor "github.com/malivvan/harbor/store/record"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// harborTestDB bundles a Harbor store with the bbolt handle the test opened, so
// tests can manipulate raw buckets without a public escape hatch.
type harborTestDB struct {
	*harbor.Store
	bdb *bolt.DB
}

// openHarborDB opens a bbolt database and wraps it with Harbor for the test.
func openHarborDB(tb testing.TB, path string) *harborTestDB {
	tb.Helper()

	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = bdb.Close() })

	db, err := harbor.Open(context.Background(), bdb, harbor.Config{})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })

	return &harborTestDB{Store: db, bdb: bdb}
}
