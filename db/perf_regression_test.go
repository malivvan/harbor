package db

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/malivvan/storm/db/q"
	"github.com/stretchr/testify/require"
)

// countingCodec delegates to JSONCodec and counts decode calls. It is used to
// prove that paths which do not need record values skip unmarshalling.
type countingCodec struct {
	unmarshals atomic.Int64
}

func (c *countingCodec) Marshal(v interface{}) ([]byte, error) {
	return JSONCodec.Marshal(v)
}

func (c *countingCodec) Unmarshal(b []byte, v interface{}) error {
	c.unmarshals.Add(1)
	return JSONCodec.Unmarshal(b, v)
}

func (c *countingCodec) Name() string { return "counting-json" }

func TestCountAndDeleteDoNotDecodeRecords(t *testing.T) {
	ctx := context.Background()
	cc := &countingCodec{}
	db, cleanup := createDB(t, testCodec(cc))
	defer cleanup()

	for i := 0; i < 10; i++ {
		u := User{Name: "John"}
		require.NoError(t, db.Save(ctx, &u))
	}

	// Seeding already decoded nothing but marshalled; reset the counter.
	cc.unmarshals.Store(0)

	n, err := db.Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Zero(t, cc.unmarshals.Load(), "Count must not decode records")

	err = db.Select().Delete(ctx, &User{})
	require.NoError(t, err)
	require.Zero(t, cc.unmarshals.Load(), "Delete without matchers must not decode records")

	n, err = db.Count(ctx, &User{})
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestCountWithMatcherStillDecodes(t *testing.T) {
	ctx := context.Background()
	cc := &countingCodec{}
	db, cleanup := createDB(t, testCodec(cc))
	defer cleanup()

	for i := 0; i < 5; i++ {
		u := User{Name: "John", Group: "Staff"}
		require.NoError(t, db.Save(ctx, &u))
	}
	cc.unmarshals.Store(0)

	n, err := db.Select().Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Zero(t, cc.unmarshals.Load(), "an empty matcher list needs no decode")

	n, err = db.Select().OrderBy("Name").Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.NotZero(t, cc.unmarshals.Load(), "ordering forces decoding")
}

func TestUnindexedScanPresizedResults(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 0; i < 25; i++ {
		u := User{Name: "John", Group: "Staff"}
		require.NoError(t, db.Save(ctx, &u))
	}

	// All: the sink is pre-sized from the bucket key count, then truncated.
	var all []User
	require.NoError(t, db.All(ctx, &all))
	require.Len(t, all, 25)

	// Limit smaller than the bucket: pre-size must be bounded by the limit.
	var limited []User
	require.NoError(t, db.Select().Limit(3).Find(ctx, &limited))
	require.Len(t, limited, 3)

	// Skip: pre-sized slots before the first published record are reused.
	var skipped []User
	require.NoError(t, db.Select().Skip(23).Find(ctx, &skipped))
	require.Len(t, skipped, 2)

	// Pointer slices decode into the pre-sized slots as well.
	var ptrs []*User
	require.NoError(t, db.All(ctx, &ptrs))
	require.Len(t, ptrs, 25)
	for _, p := range ptrs {
		require.NotNil(t, p)
	}

	// No match: an over-sized slice must be truncated to empty, not published.
	var none []User
	err := db.Select(q.Eq("Group", "NoSuchGroup")).Find(ctx, &none)
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, none)

	// Reverse reads the same number of records.
	var reversed []User
	require.NoError(t, db.All(ctx, &reversed, Reverse()))
	require.Len(t, reversed, 25)
}

func TestStructMetaCacheBindsPerCall(t *testing.T) {
	a := &User{ID: 1, Name: "a"}
	b := &User{ID: 2, Name: "b"}

	va := reflect.ValueOf(a)
	ca, err := extract(&va)
	require.NoError(t, err)

	_, cached := structMetaCache.Load(reflect.TypeOf(User{}))
	require.True(t, cached, "User metadata should be cached")

	vb := reflect.ValueOf(b)
	cb, err := extract(&vb)
	require.NoError(t, err)

	require.Equal(t, 1, ca.ID.Value.Interface())
	require.Equal(t, 2, cb.ID.Value.Interface(), "values must be bound per call")
	require.False(t, ca.ID.IsZero)

	// The bound value must alias the caller's record, not a snapshot.
	a.Name = "changed"
	require.Equal(t, "changed", ca.Fields["Name"].Value.String())

	// Dynamic fields (zero state) are refreshed on every bind.
	c := &User{}
	vc := reflect.ValueOf(c)
	cc, err := extract(&vc)
	require.NoError(t, err)
	require.True(t, cc.ID.IsZero)
	require.True(t, cc.Fields["Name"].IsZero)
}

func TestStructMetaCachePromotedInlineFields(t *testing.T) {
	v := reflect.ValueOf(&NestedID{ToEmbed: ToEmbed{ID: "x"}, Name: "n"})
	cfg, err := extract(&v)
	require.NoError(t, err)
	require.Equal(t, "x", cfg.ID.Value.Interface())
	require.Equal(t, "x", cfg.Fields["ID"].Value.Interface())
	require.Equal(t, "NestedID", cfg.Name)
}

func TestStructMetaCacheSkipsPointerInline(t *testing.T) {
	v := reflect.ValueOf(&ClassicInline{})
	_, err := extract(&v)
	require.NoError(t, err)

	_, cached := structMetaCache.Load(reflect.TypeOf(ClassicInline{}))
	require.False(t, cached, "types with inline pointers are value-dependent and must not be cached")
}

func TestOrderByPromotedField(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	for _, id := range []int{3, 1, 2} {
		rec := UserWithEmbeddedIDField{UserWithIDField: UserWithIDField{ID: id, Name: "n"}}
		require.NoError(t, db.Save(ctx, &rec))
	}

	var asc []UserWithEmbeddedIDField
	require.NoError(t, db.Select().OrderBy("ID").Find(ctx, &asc))
	require.Len(t, asc, 3)
	require.Equal(t, []int{1, 2, 3}, []int{asc[0].ID, asc[1].ID, asc[2].ID})

	var desc []UserWithEmbeddedIDField
	require.NoError(t, db.Select().OrderBy("ID").Reverse().Find(ctx, &desc))
	require.Len(t, desc, 3)
	require.Equal(t, []int{3, 2, 1}, []int{desc[0].ID, desc[1].ID, desc[2].ID})

	// Unknown order fields still surface ErrNotFound.
	var bad []UserWithEmbeddedIDField
	err := db.Select().OrderBy("NoSuchField").Find(ctx, &bad)
	require.ErrorIs(t, err, ErrNotFound)
}

type incrementIDRecord struct {
	ID   int    `db:"increment"`
	Name string `db:"index"`
}

func TestIncrementTaggedIDUsesDirectKeyPath(t *testing.T) {
	ctx := context.Background()
	cc := &countingCodec{}
	db, cleanup := createDB(t, testCodec(cc))
	defer cleanup()

	for i := 0; i < 10; i++ {
		rec := incrementIDRecord{Name: "n"}
		require.NoError(t, db.Save(ctx, &rec))
		require.Equal(t, i+1, rec.ID)
	}

	cc.unmarshals.Store(0)
	var got incrementIDRecord
	require.NoError(t, db.One(ctx, "ID", 10, &got))
	require.Equal(t, 10, got.ID)
	require.EqualValues(t, 1, cc.unmarshals.Load(),
		"One by the primary key must take the direct key path, not a scan")

	// Find keeps working on the same field.
	cc.unmarshals.Store(0)
	var found []incrementIDRecord
	require.NoError(t, db.Find(ctx, "ID", 7, &found))
	require.Len(t, found, 1)
	require.Equal(t, 7, found[0].ID)
	require.EqualValues(t, 1, cc.unmarshals.Load())

	// Save with an explicit ID and update still index correctly.
	rec := incrementIDRecord{ID: 42, Name: "x"}
	require.NoError(t, db.Save(ctx, &rec))
	var byName []incrementIDRecord
	require.NoError(t, db.Find(ctx, "Name", "x", &byName))
	require.Len(t, byName, 1)
	require.Equal(t, 42, byName[0].ID)
}
