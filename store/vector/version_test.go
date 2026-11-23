package vector

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"
)

func TestLoadCollectionRejectsNewerFormat(t *testing.T) {
	bdb, err := bbolt.Open(filepath.Join(t.TempDir(), "version.db"), 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)

	require.NoError(t, store.SaveCollectionMeta("docs", CollectionMeta{
		Name:          "docs",
		VectorLen:     3,
		Metric:        Cosine,
		FormatVersion: CollectionFormatVersion + 1,
	}))
	_, err = store.LoadCollectionMeta("docs")
	require.ErrorIs(t, err, ErrInvalidConfig)
}
