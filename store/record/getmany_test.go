package record

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type manyRow struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

func TestGetManyOrderMissingDuplicates(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "many.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })

	for _, row := range []manyRow{{ID: "a", Name: "first"}, {ID: "b", Name: "second"}, {ID: "c", Name: "third"}} {
		row := row
		require.NoError(t, st.Save(ctx, &row))
	}

	var rows []manyRow
	missing, err := st.GetMany(ctx, []any{"c", "missing", "a", "a", "b"}, &rows)
	require.NoError(t, err)
	require.Equal(t, []any{"missing"}, missing)
	require.Len(t, rows, 4)
	require.Equal(t, []string{"c", "a", "a", "b"}, []string{rows[0].ID, rows[1].ID, rows[2].ID, rows[3].ID})
	require.Equal(t, "first", rows[1].Name)

	// Pointer slices work too.
	var ptrs []*manyRow
	missing, err = st.GetMany(ctx, []any{"b", "nope"}, &ptrs)
	require.NoError(t, err)
	require.Equal(t, []any{"nope"}, missing)
	require.Len(t, ptrs, 1)
	require.Equal(t, "second", ptrs[0].Name)
}

func TestGetManyEmptyAndWrongDestination(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "many.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })

	var rows []manyRow
	missing, err := st.GetMany(ctx, nil, &rows)
	require.NoError(t, err)
	require.Nil(t, missing)
	require.Empty(t, rows)

	if _, err := st.GetMany(ctx, []any{"a"}, &manyRow{}); err == nil {
		t.Fatal("non-slice destination: want an error")
	}
	if _, err := st.GetMany(ctx, []any{"a"}, []manyRow{}); err == nil {
		t.Fatal("non-pointer destination: want an error")
	}

	// A missing bucket reports every id as missing.
	missing, err = st.GetMany(ctx, []any{"a", "b"}, &rows)
	require.NoError(t, err)
	require.Equal(t, []any{"a", "b"}, missing)
}

func TestGetManyEncryptedKeys(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "many.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = byte(i + 1)
	}
	st, err := Open(ctx, bdb, Config{Secret: secret, EncryptKeys: true, EncryptValues: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })

	require.NoError(t, st.Save(ctx, &manyRow{ID: "secret", Name: "hidden"}))
	var rows []manyRow
	missing, err := st.GetMany(ctx, []any{"secret", "absent"}, &rows)
	require.NoError(t, err)
	require.Equal(t, []any{"absent"}, missing)
	require.Len(t, rows, 1)
	require.Equal(t, "hidden", rows[0].Name)
}
