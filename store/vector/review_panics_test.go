package vector

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestHNSWHugeTopKIsClamped(t *testing.T) {
	idx := NewHNSWIndex(Cosine)
	require.NoError(t, idx.Upsert([]PointStruct{{ID: "1", Vector: []float32{1, 0}}}))
	require.NotPanics(t, func() {
		results, err := idx.Search([]float32{1, 0}, nil, math.MaxInt)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

func TestAllEmptyRootBehavesAsDatabaseRoot(t *testing.T) {
	bdb, err := bbolt.Open(t.TempDir()+"/root.db", 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := Open(context.Background(), bdb, Config{Root: [][]byte{{}}})
	require.NoError(t, err)
	_, err = store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
}
