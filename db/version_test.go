package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestOpenRejectsUnknownVersion(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "version.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, databaseInfo, versionKey, "9.9.9"))

	_, err = Open(ctx, bdb, Config{})
	require.ErrorIs(t, err, ErrIncompatibleVersion)
}
