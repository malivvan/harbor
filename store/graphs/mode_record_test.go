package graphs

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/eidos-db/store"
)

// TestOpenRejectsEmptyModeRecord covers a present-but-empty mode record: the
// open must report ErrWrongMode instead of indexing its first byte.
func TestOpenRejectsEmptyModeRecord(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "graphs.db"), 0o600, nil)
	require.NoError(t, err)
	defer bdb.Close()

	root := [][]byte{[]byte(store.GRAPHS)}
	_, err = Open(context.Background(), bdb, Config{Root: root})
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(store.GRAPHS)).Put([]byte(modeKey), []byte{})
	}))

	_, err = Open(context.Background(), bdb, Config{Root: root})
	require.ErrorIs(t, err, ErrWrongMode)
}
