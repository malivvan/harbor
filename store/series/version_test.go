package series

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	hstore "github.com/malivvan/tempest/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestOpenRejectsUnknownFormatVersion(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "version.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	ctx := context.Background()
	root := []byte(hstore.SERIES)
	_, err = Open(ctx, bdb, Config{Root: [][]byte{root}})
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(root).Bucket([]byte(metaBucketName))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], 99)
		return meta.Put([]byte(versionKey), buf[:])
	}))

	_, err = Open(ctx, bdb, Config{Root: [][]byte{root}})
	require.ErrorIs(t, err, ErrIncompatibleVersion)
}
