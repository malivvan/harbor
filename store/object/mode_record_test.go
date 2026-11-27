package object_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	hstore "github.com/malivvan/tempest/store"
	"github.com/malivvan/tempest/store/object"
)

// TestOpenRejectsEmptyModeRecord covers a present-but-empty mode record: the
// open must report ErrWrongMode instead of indexing its first byte.
func TestOpenRejectsEmptyModeRecord(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "objects.db"), 0o600, nil)
	require.NoError(t, err)
	defer bdb.Close()

	root := [][]byte{[]byte(hstore.OBJECT)}
	_, err = object.Open(context.Background(), bdb, object.Config{Root: root})
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(hstore.OBJECT)).Put([]byte("__mode__"), []byte{})
	}))

	_, err = object.Open(context.Background(), bdb, object.Config{Root: root})
	require.ErrorIs(t, err, object.ErrWrongMode)
}
