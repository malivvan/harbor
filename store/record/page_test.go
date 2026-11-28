package record

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllPageStable(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, store.Save(ctx, &revisionDoc{ID: id, Name: id}))
	}

	var page1 []revisionDoc
	cursor, err := store.AllPage(ctx, &page1, 2, nil)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.Equal(t, "a", page1[0].ID)
	require.Equal(t, "b", page1[1].ID)
	require.NotNil(t, cursor)

	require.NoError(t, store.Save(ctx, &revisionDoc{ID: "bb", Name: "bb"}))

	var page2 []revisionDoc
	cursor2, err := store.AllPage(ctx, &page2, 2, cursor)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	require.Equal(t, "bb", page2[0].ID)
	require.Equal(t, "c", page2[1].ID)

	var page3 []revisionDoc
	next, err := store.AllPage(ctx, &page3, 2, cursor2)
	require.NoError(t, err)
	require.Len(t, page3, 2) // d and e
	require.Nil(t, next)

	seen := map[string]bool{}
	for _, page := range [][]revisionDoc{page1, page2, page3} {
		for _, doc := range page {
			require.False(t, seen[doc.ID], "duplicate %s", doc.ID)
			seen[doc.ID] = true
		}
	}
	require.Len(t, seen, 6)
}

func TestAllPagePointerElements(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, &revisionDoc{ID: "a", Name: "a"}))
	require.NoError(t, store.Save(ctx, &revisionDoc{ID: "b", Name: "b"}))

	var out []*revisionDoc
	next, err := store.AllPage(ctx, &out, 10, nil)
	require.NoError(t, err)
	require.Nil(t, next)
	require.Len(t, out, 2)
	require.Equal(t, "a", out[0].ID)
	require.Equal(t, "b", out[1].ID)
}
