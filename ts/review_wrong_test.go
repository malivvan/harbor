package ts

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRotateAllSkipsCachedButUnwrittenSeries(t *testing.T) {
	ctx := context.Background()
	store, _ := openTodoStore(t, Config{Secret: testKey(80), EncryptValues: true, Root: [][]byte{[]byte("ts")}})

	require.NoError(t, store.Series("real").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	_ = store.Series("cached-only") // cached, never written

	require.NoError(t, store.RotateAll(ctx, testKey(81)))
	require.NoError(t, store.VerifyAll(ctx))
	points, err := store.Series("real").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, points, 1)
}
