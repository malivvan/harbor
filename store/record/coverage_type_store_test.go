package record

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type uninitializedRecord struct {
	ID   int `db:"id"`
	Name string
}

func TestToBytesAndNumberVariants(t *testing.T) {
	require.Nil(t, mustBytes(t, nil))

	signed := []any{int(-3), int8(-3), int16(-3), int32(-3), int64(-3)}
	for _, v := range signed {
		b := mustBytes(t, v)
		require.Len(t, b, 8, "%T", v)
	}
	unsigned := []any{uint(3), uint8(3), uint16(3), uint32(3), uint64(3)}
	for _, v := range unsigned {
		b := mustBytes(t, v)
		require.Len(t, b, 8, "%T", v)
	}
	for _, v := range []any{float32(1.5), float64(1.5)} {
		require.Len(t, mustBytes(t, v), 8, "%T", v)
	}
	require.Equal(t, []byte("raw"), mustBytes(t, []byte("raw")))
	require.Equal(t, []byte("str"), mustBytes(t, "str"))

	encoded := mustBytes(t, struct{ A int }{1})
	require.JSONEq(t, `{"A":1}`, string(encoded))

	for _, v := range []any{int64(1), int(1), uint64(1), uint(1)} {
		raw, err := numbertob(v)
		require.NoError(t, err)
		back, err := numberfromb(raw)
		require.NoError(t, err)
		require.EqualValues(t, 1, back)
	}
	_, err := numbertob("unsupported")
	require.Error(t, err)
}

func mustBytes(t *testing.T, v any) []byte {
	t.Helper()
	b, err := toBytes(v, JSONCodec)
	require.NoError(t, err)
	return b
}

func TestDeleteStructVariants(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	user := &User{Name: "a", Slug: "a"}
	require.NoError(t, db.Save(ctx, user))
	require.NoError(t, db.DeleteStruct(ctx, &User{ID: user.ID}))
	require.ErrorIs(t, db.DeleteStruct(ctx, &User{ID: user.ID}), ErrNotFound)

	// A bucket that was never created.
	require.ErrorIs(t, db.DeleteStruct(ctx, &SimpleUser{ID: 1}), ErrNotFound)

	other := &User{Name: "b", Slug: "b"}
	require.NoError(t, db.Save(ctx, other))
	require.NoError(t, db.Drop(ctx, &User{}))
	require.ErrorIs(t, db.DeleteStruct(ctx, &User{ID: other.ID}), ErrNotFound)

	// DeleteStruct rejects non-pointers and zero IDs.
	require.ErrorIs(t, db.DeleteStruct(ctx, User{}), ErrStructPtrNeeded)
	require.NoError(t, db.Init(ctx, &SimpleUser{}))
	require.ErrorIs(t, db.DeleteStruct(ctx, &SimpleUser{}), ErrNotFound)
}

func TestInitAndReIndexVariants(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	require.NoError(t, db.Init(ctx, &User{}))
	require.NoError(t, db.Init(ctx, &User{}))
	require.Error(t, db.Init(ctx, struct{}{}))

	// ReIndex on an empty, initialized bucket is fine.
	require.NoError(t, db.ReIndex(ctx, &User{}))

	for i := 0; i < 3; i++ {
		u := &User{Name: "same", Slug: string(rune('a' + i))}
		require.NoError(t, db.Save(ctx, u))
	}
	require.NoError(t, db.ReIndex(ctx, &User{}))

	var found []User
	require.NoError(t, db.Find(ctx, "Name", "same", &found))
	require.Len(t, found, 3)

	require.ErrorIs(t, db.ReIndex(ctx, struct{}{}), ErrStructPtrNeeded)
	require.ErrorIs(t, db.ReIndex(ctx, &uninitializedRecord{}), ErrNotFound)

	// A valid but unreachable record type round-trips through ReIndex.
	require.NoError(t, db.Init(ctx, &SimpleUser{}))
	require.NoError(t, db.Save(ctx, &SimpleUser{ID: 1, Name: "x"}))
	require.NoError(t, db.ReIndex(ctx, &SimpleUser{}))
}

func TestSaveUpdateErrors(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	require.ErrorIs(t, db.Save(ctx, &SimpleUser{Name: "no-id"}), ErrZeroID)
	require.ErrorIs(t, db.Save(ctx, nil), ErrStructPtrNeeded)
	require.ErrorIs(t, db.Update(ctx, &SimpleUser{}), ErrNoID)
	require.ErrorIs(t, db.Update(ctx, &SimpleUser{ID: 99, Name: "missing"}), ErrNotFound)

	saved := &SimpleUser{ID: 1, Name: "one"}
	require.NoError(t, db.Save(ctx, saved))

	require.ErrorIs(t, db.UpdateField(ctx, &SimpleUser{ID: 1}, "ID", 2), ErrImmutableID)
	require.ErrorIs(t, db.UpdateField(ctx, &SimpleUser{ID: 1}, "Missing", "x"), ErrNotFound)
	require.ErrorIs(t, db.UpdateField(ctx, &SimpleUser{ID: 1}, "Name", 42), ErrIncompatibleValue)
	require.NoError(t, db.UpdateField(ctx, &SimpleUser{ID: 1}, "Name", "renamed"))

	var got SimpleUser
	require.NoError(t, db.One(ctx, "ID", 1, &got))
	require.Equal(t, "renamed", got.Name)

	// A unique-index collision is surfaced as ErrAlreadyExists.
	first := &User{Name: "first", Slug: "dup"}
	require.NoError(t, db.Save(ctx, first))
	second := &User{Name: "second", Slug: "dup"}
	require.ErrorIs(t, db.Save(ctx, second), ErrAlreadyExists)

	// Update applies only non-zero fields unless forced.
	require.NoError(t, db.Update(ctx, &User{ID: first.ID, Name: "changed"}))
	require.NoError(t, db.One(ctx, "ID", first.ID, first))
	require.Equal(t, "changed", first.Name)
	require.Equal(t, "dup", first.Slug)
}

func TestPrefixExtraBranches(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	for _, name := range []string{"al", "alp", "alpha", "beta"} {
		u := &User{Name: name, Slug: name}
		require.NoError(t, db.Save(ctx, u))
	}

	var users []User
	users = nil
	require.NoError(t, db.Prefix(ctx, "Name", "al", &users, Limit(1), Reverse()))
	require.Len(t, users, 1)

	users = nil
	require.ErrorIs(t, db.Prefix(ctx, "Name", "zz", &users), ErrNotFound)

	// Unique index prefix.
	users = nil
	require.NoError(t, db.Prefix(ctx, "Slug", "a", &users))
	require.Len(t, users, 3)

	// Prefix on a dropped bucket yields an empty result.
	require.NoError(t, db.Drop(ctx, &User{}))
	users = nil
	require.NoError(t, db.Prefix(ctx, "Name", "a", &users))
	require.Empty(t, users)
}

func TestKVExtraBranches(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	require.ErrorIs(t, db.SetBytes(ctx, "bucket", nil, []byte("v")), ErrNilParam)

	require.NoError(t, db.Set(ctx, "bucket", "k", "value"))
	var got string
	require.NoError(t, db.Get(ctx, "bucket", "k", &got))
	require.Equal(t, "value", got)

	require.NoError(t, db.Set(ctx, "bucket", "empty", nil))
	raw, err := db.GetBytes(ctx, "bucket", "empty")
	require.NoError(t, err)
	require.Empty(t, raw)

	require.NoError(t, db.Delete(ctx, "bucket", "k"))
	require.NoError(t, db.Delete(ctx, "bucket", "missing"))
	require.ErrorIs(t, db.Delete(ctx, "missing-bucket", "k"), ErrNotFound)

	_, err = db.GetBytes(ctx, "bucket", "missing")
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, db.Get(ctx, "missing-bucket", "k", &got), ErrNotFound)

	exists, err := db.KeyExists(ctx, "bucket", "empty")
	require.NoError(t, err)
	require.True(t, exists)

	// An empty bucket name under an empty root has no name to create.
	require.Error(t, db.Set(ctx, "", "k", "v"))
}

func TestSinkCanceledPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	v := reflect.ValueOf(&User{})
	items := item{value: &v}

	var list []User
	ls, err := newListSink(nil, &list)
	require.NoError(t, err)
	require.ErrorIs(t, ls.add(ctx, items), context.Canceled)
	require.ErrorIs(t, ls.flush(ctx), context.Canceled)

	fs, err := newFirstSink(nil, &User{})
	require.NoError(t, err)
	require.ErrorIs(t, fs.add(ctx, items), context.Canceled)
	fs.found = true
	fs.pending = &v
	require.ErrorIs(t, fs.flush(ctx), context.Canceled)

	cs, err := newCountSink(nil, &User{})
	require.NoError(t, err)
	require.ErrorIs(t, cs.add(ctx, items), context.Canceled)
	require.ErrorIs(t, cs.flush(ctx), context.Canceled)

	ds, err := newDeleteSink(nil, &User{})
	require.NoError(t, err)
	require.ErrorIs(t, ds.add(ctx, items), context.Canceled)
	require.ErrorIs(t, ds.flush(ctx), context.Canceled)

	raw, _, err := newRawSink(nil)
	require.NoError(t, err)
	require.ErrorIs(t, raw.add(ctx, items), context.Canceled)
	require.ErrorIs(t, raw.flush(ctx), context.Canceled)

	es, err := newEachSink(&User{})
	require.NoError(t, err)
	es.execFn = func(any) error { return nil }
	require.ErrorIs(t, es.add(ctx, items), context.Canceled)
	require.ErrorIs(t, es.flush(ctx), context.Canceled)
}
