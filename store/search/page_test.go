package search

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchPageStableAndShapeChecked(t *testing.T) {
	_, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	for i, id := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, idx.Upsert(ctx, id, "term "+string(rune('a'+i))))
	}

	all, err := idx.Search(ctx, "term", 0)
	require.NoError(t, err)
	require.Len(t, all, 5)

	// A write between pages must not duplicate a hit on a later page.
	require.NoError(t, idx.Upsert(ctx, "aa", "term aaa"))

	seen := map[string]bool{}
	var cursor *PageCursor
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "pagination did not terminate")
		page, next, err := idx.SearchPage(ctx, "term", 2, cursor)
		require.NoError(t, err)
		for _, hit := range page {
			require.False(t, seen[hit.ID], "duplicate %s", hit.ID)
			seen[hit.ID] = true
		}
		if next == nil {
			break
		}
		cursor = next
	}
	require.Len(t, seen, 6)
}

func TestSearchPageRejectsWrongQuery(t *testing.T) {
	_, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	require.NoError(t, idx.Upsert(ctx, "a", "term"))
	require.NoError(t, idx.Upsert(ctx, "b", "term"))
	_, cursor, err := idx.SearchPage(ctx, "term", 1, nil)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	_, _, err = idx.SearchPage(ctx, "other", 1, cursor)
	require.ErrorIs(t, err, ErrCursorMismatch)
}
