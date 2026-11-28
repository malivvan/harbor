package vector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListPointsPageStableAndShapeChecked(t *testing.T) {
	_, col := openEmbedStore(t, false)
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}},
		{ID: "c", Vector: []float32{1, 1}},
		{ID: "d", Vector: []float32{2, 1}},
		{ID: "e", Vector: []float32{1, 2}},
	}))
	require.NoError(t, col.Flush())

	page1, cursor, err := col.ListPointsPage(nil, 2, nil)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.Equal(t, "a", page1[0].ID)
	require.Equal(t, "b", page1[1].ID)
	require.NotNil(t, cursor)

	// Inserting a key before the cursor does not disturb the next page.
	require.NoError(t, col.Upsert([]PointStruct{{ID: "bb", Vector: []float32{1, 0}}}))
	page2, cursor2, err := col.ListPointsPage(nil, 2, cursor)
	require.NoError(t, err)
	require.Equal(t, "bb", page2[0].ID)
	require.Equal(t, "c", page2[1].ID)
	require.NotNil(t, cursor2)

	page3, next, err := col.ListPointsPage(nil, 2, cursor2)
	require.NoError(t, err)
	require.Len(t, page3, 2) // d and e sort after c
	require.Nil(t, next)

	filter := &Filter{Must: []Condition{{Key: "topic", Type: MatchTypeExact, Match: MatchValue{Value: "x"}}}}
	_, _, err = col.ListPointsPage(filter, 2, cursor)
	require.ErrorIs(t, err, ErrCursorMismatch)
}
