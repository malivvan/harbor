package record

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type pointerNamed struct {
	ID   int    `db:"id"`
	Name string `db:"index"`
}

// EidosBucketName is deliberately defined on the pointer receiver: Save must
// honour it rather than falling back to the static type name.
func (p *pointerNamed) EidosBucketName() string { return "pointer_named_bucket" }

func TestBucketNamerPointerReceiver(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &pointerNamed{ID: 1, Name: "alice"}))

	var got pointerNamed
	require.NoError(t, db.One(ctx, "Name", "alice", &got))
	require.Equal(t, 1, got.ID)

	var all []pointerNamed
	require.NoError(t, db.Select().Bucket("pointer_named_bucket").Find(ctx, &all))
	require.Len(t, all, 1)
}

func TestPrefixEscapesRegexpMetacharacters(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i, name := range []string{"a.b", "axb", "a.bc"} {
		require.NoError(t, db.Save(ctx, &User{ID: i + 1, Name: name, Slug: name}))
	}

	var users []User
	require.NoError(t, db.Prefix(ctx, "Slug", "a.", &users))
	require.Len(t, users, 2, "prefix must be literal")
}

// TestFindEmptyIndexedValue guards D1: an empty search value on a list-indexed
// field must report "not found" instead of panicking in generatePrefix.
func TestFindEmptyIndexedValue(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "alice"}))

	var users []User
	require.ErrorIs(t, db.Find(ctx, "Name", "", &users), ErrNotFound)

	var one User
	require.ErrorIs(t, db.One(ctx, "Name", "", &one), ErrNotFound)

	// Removing an empty value is a no-op rather than a panic.
	require.NoError(t, db.DeleteStruct(ctx, &User{ID: 1, Name: "alice"}))
}

// TestReverseRangeAboveMax guards D2: a reverse range whose upper bound sorts
// above every key must still return the rows below it.
func TestReverseRangeAboveMax(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i, name := range []string{"abc1", "abc2", "abc3"} {
		require.NoError(t, db.Save(ctx, &User{ID: i + 1, Name: name}))
	}

	var forward []User
	require.NoError(t, db.Range(ctx, "Name", "abc1", "zzz", &forward))
	require.Len(t, forward, 3)

	var reverse []User
	require.NoError(t, db.Range(ctx, "Name", "abc1", "zzz", &reverse, Reverse()))
	require.Len(t, reverse, 3, "reverse range above every key must not be empty")
	require.Equal(t, "abc3", reverse[0].Name)
}

// TestListIndexValueMatchingSentinel guards D3: the value "eidos" shares the
// "eidos__" prefix with the internal eidos__ids sentinel bucket.
func TestListIndexValueMatchingSentinel(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "eidos"}))
	require.NoError(t, db.Save(ctx, &User{ID: 2, Name: "eidosy"}))

	var found []User
	require.NoError(t, db.Find(ctx, "Name", "eidos", &found))
	require.Len(t, found, 1)
	require.Equal(t, 1, found[0].ID)

	var one User
	require.NoError(t, db.One(ctx, "Name", "eidos", &one))
	require.Equal(t, 1, one.ID)

	require.NoError(t, db.DeleteStruct(ctx, &User{ID: 1, Name: "eidos"}))

	var after []User
	require.ErrorIs(t, db.Find(ctx, "Name", "eidos", &after), ErrNotFound)
}

// TestKVSetNilReadsBackAsZero guards D4: a nil value is stored as a
// zero-length value and reads back as the destination's zero value.
func TestKVSetNilReadsBackAsZero(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Set(ctx, "kv", "key", nil))

	exists, err := db.KeyExists(ctx, "kv", "key")
	require.NoError(t, err)
	require.True(t, exists)

	raw, err := db.GetBytes(ctx, "kv", "key")
	require.NoError(t, err)
	require.Empty(t, raw)

	var got string
	require.NoError(t, db.Get(ctx, "kv", "key", &got))
	require.Equal(t, "", got)

	var ptr *struct{ N int }
	require.NoError(t, db.Get(ctx, "kv", "key", &ptr))
	require.Nil(t, ptr)
}

// TestReadTxChecksContextAfterCallback guards D8: a cancellation observed by
// the callback must be reported even though the read itself succeeded.
func TestReadTxChecksContextAfterCallback(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := db.Node.(*node)
	err := n.readTx(ctx, func(tx *bolt.Tx) error {
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
