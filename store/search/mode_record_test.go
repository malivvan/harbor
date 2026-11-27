package search

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/eidos-db/store"
)

// TestOpenIndexRejectsEmptyModeRecord covers a present-but-empty mode record:
// opening the index must report ErrWrongMode instead of indexing its first byte.
func TestOpenIndexRejectsEmptyModeRecord(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "search.db"), 0o600, nil)
	require.NoError(t, err)
	defer bdb.Close()

	root := [][]byte{[]byte(store.SEARCH)}
	st, err := Open(context.Background(), bdb, Config{Root: root})
	require.NoError(t, err)
	_, err = st.Open("idx", Config{})
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(store.SEARCH)).Bucket([]byte("idx")).Put([]byte(modeKey), []byte{})
	}))

	_, err = st.Open("idx", Config{})
	require.ErrorIs(t, err, ErrWrongMode)
}
