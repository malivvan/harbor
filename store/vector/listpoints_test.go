package vector

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func idsOf(points []PointStruct) []string {
	out := make([]string, len(points))
	for i, p := range points {
		out[i] = p.ID
	}
	return out
}

func TestListPoints(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "listpoints.db"))
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "d", Vector: []float32{4, 0}, Payload: Payload{"role": "guest"}},
		{ID: "b", Vector: []float32{2, 0}, Payload: Payload{"role": "admin"}},
		{ID: "a", Vector: []float32{1, 0}, Payload: Payload{"role": "admin"}},
		{ID: "c", Vector: []float32{3, 0}, Payload: Payload{"role": "guest"}},
	}))

	page, err := col.ListPoints(nil, ListOptions{Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, idsOf(page))

	page, err = col.ListPoints(nil, ListOptions{Offset: 1, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, idsOf(page))

	admin := &Filter{Must: []Condition{{Key: "role", Match: MatchValue{Value: "admin"}}}}
	page, err = col.ListPoints(admin, ListOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, idsOf(page))

	_, err = col.ListPoints(nil, ListOptions{Limit: -1})
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = col.ListPoints(nil, ListOptions{Offset: -1})
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestForEachPoint(t *testing.T) {
	store := openTestStorage(t, filepath.Join(t.TempDir(), "foreachpoint.db"))
	col, err := store.NewCollection("docs", 2, Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "c", Vector: []float32{3, 0}},
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{2, 0}},
	}))

	var order []string
	require.NoError(t, col.ForEachPoint(nil, func(p PointStruct) error {
		order = append(order, p.ID)
		return nil
	}))
	require.Equal(t, []string{"a", "b", "c"}, order)

	// ErrStopIteration stops without an error.
	var first []string
	require.NoError(t, col.ForEachPoint(nil, func(p PointStruct) error {
		first = append(first, p.ID)
		return ErrStopIteration
	}))
	require.Equal(t, []string{"a"}, first)

	// Any other error is propagated.
	sentinel := errors.New("boom")
	err = col.ForEachPoint(nil, func(PointStruct) error { return sentinel })
	require.ErrorIs(t, err, sentinel)

	require.ErrorIs(t, col.ForEachPoint(nil, nil), ErrInvalidOptions)
}
