package file

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestOpenRejectsUnknownFormatVersion(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "version.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	_, err = store.Open("fs")
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, "fs")
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], 99)
		return b.state.Put([]byte(formatVersionKey), buf[:])
	}))

	_, err = store.Open("fs")
	require.ErrorIs(t, err, ErrIncompatibleVersion)
}
