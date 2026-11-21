package index_test

import (
	"context"
	"testing"

	storm "github.com/malivvan/storm/db"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// openStormDB opens a bbolt database and wraps it with Storm for the test.
func openStormDB(tb testing.TB, path string) *storm.Store {
	tb.Helper()

	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = bdb.Close() })

	db, err := storm.Open(context.Background(), bdb, nil, storm.Config{})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })

	return db
}
