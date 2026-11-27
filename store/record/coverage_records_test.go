package record

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/store/record/q"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func corruptRecordValue(t *testing.T, store *Store, bucket, idField string, idValue any) {
	t.Helper()
	n := store.WithCodec(JSONCodec).(*node)
	id, err := n.keyBytes(idValue, keyDomainID)
	require.NoError(t, err)
	require.NoError(t, n.s.bolt.Update(func(tx *bolt.Tx) error {
		b := n.getBucket(tx, bucket)
		require.NotNil(t, b)
		return b.Put(id, []byte("not-json"))
	}))
	_ = idField
}

func deleteRecordValue(t *testing.T, store *Store, bucket string, idValue any) {
	t.Helper()
	n := store.WithCodec(JSONCodec).(*node)
	id, err := n.keyBytes(idValue, keyDomainID)
	require.NoError(t, err)
	require.NoError(t, n.s.bolt.Update(func(tx *bolt.Tx) error {
		b := n.getBucket(tx, bucket)
		require.NotNil(t, b)
		return b.Delete(id)
	}))
}

func TestCorruptRecordErrors(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	user := &User{Name: "corrupt", Slug: "corrupt"}
	require.NoError(t, db.Save(ctx, user))

	// Every read path that decodes a matched record must surface the codec
	// error instead of silently returning zero values.
	corruptRecordValue(t, db, "User", "ID", user.ID)

	var one User
	require.Error(t, db.One(ctx, "ID", user.ID, &one))
	require.Error(t, db.One(ctx, "Name", "corrupt", &one))
	require.Error(t, db.Find(ctx, "Name", "corrupt", &[]User{}))
	require.Error(t, db.AllByIndex(ctx, "Name", &[]User{}))
	require.Error(t, db.Range(ctx, "Name", "a", "z", &[]User{}))
	require.Error(t, db.Prefix(ctx, "Name", "cor", &[]User{}))
	require.Error(t, db.Select(q.Eq("Name", "corrupt")).Find(ctx, &[]User{}))
	_, err := db.Select(q.Eq("Name", "corrupt")).Count(ctx, &User{})
	require.Error(t, err)
	require.Error(t, db.Select(q.Eq("Name", "corrupt")).Delete(ctx, &User{}))
}

func TestIndexPointsAtMissingRecord(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	user := &User{Name: "ghost", Slug: "ghost"}
	require.NoError(t, db.Save(ctx, user))
	deleteRecordValue(t, db, "User", user.ID)

	var got User
	require.ErrorIs(t, db.One(ctx, "ID", user.ID, &got), ErrNotFound)
	require.ErrorIs(t, db.One(ctx, "Name", "ghost", &got), ErrNotFound)
	require.ErrorIs(t, db.Find(ctx, "Name", "ghost", &[]User{}), ErrNotFound)
	require.ErrorIs(t, db.AllByIndex(ctx, "Name", &[]User{}), ErrNotFound)
	require.ErrorIs(t, db.Range(ctx, "Name", "a", "z", &[]User{}), ErrNotFound)
	require.ErrorIs(t, db.Prefix(ctx, "Name", "gh", &[]User{}), ErrNotFound)
	require.ErrorIs(t, db.DeleteStruct(ctx, &User{ID: user.ID}), ErrNotFound)
}

type errorCodec struct{}

func (errorCodec) Marshal(any) ([]byte, error) { return nil, errors.New("marshal failed") }
func (errorCodec) Unmarshal([]byte, any) error { return errors.New("unmarshal failed") }
func (errorCodec) Name() string                { return "error-codec" }

type marshalErrorRecord struct {
	ID   int `db:"id"`
	Name string
}

func TestSaveMarshalError(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	rec := &marshalErrorRecord{ID: 1, Name: "x"}
	require.Error(t, db.WithCodec(errorCodec{}).Save(ctx, rec))
}

func TestInitCodecMismatch(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	user := &User{Name: "codec", Slug: "codec"}
	require.NoError(t, db.Save(ctx, user))

	require.ErrorIs(t, db.WithCodec(altCodec).Init(ctx, &User{}), ErrDifferentCodec)
}

func TestReIndexCorruptRecord(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	user := &User{Name: "broken", Slug: "broken"}
	require.NoError(t, db.Save(ctx, user))
	corruptRecordValue(t, db, "User", "ID", user.ID)

	require.Error(t, db.ReIndex(ctx, &User{}))
}

func TestQueryInsideTransactions(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	user := &User{Name: "tx", Slug: "tx"}
	require.NoError(t, db.Save(ctx, user))

	require.NoError(t, db.ReadTransaction(ctx, func(n Node) error {
		var got []User
		if err := n.Select(q.Eq("Name", "tx")).Find(ctx, &got); err != nil {
			return err
		}
		if len(got) != 1 {
			return errors.New("expected one record")
		}
		_, err := n.Select().Count(ctx, &User{})
		return err
	}))

	require.NoError(t, db.WriteTransaction(ctx, func(n Node) error {
		_, err := n.Select().Count(ctx, &User{})
		if err != nil {
			return err
		}
		// A delete sink runs inside the caller's write transaction.
		return n.Select(q.Eq("Name", "tx")).Delete(ctx, &User{})
	}))

	var remaining []User
	require.NoError(t, db.All(ctx, &remaining))
	require.Empty(t, remaining)
}

func TestAEADHelperFallbacks(t *testing.T) {
	var nilEnc *encState
	plain := []byte("plain")
	sealed, err := nilEnc.seal(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err := nilEnc.open(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)
	sealed, err = nilEnc.sealValues(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err = nilEnc.openValues(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)
	sealed, err = nilEnc.sealMetadata(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err = nilEnc.openMetadata(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)
	sealed, err = nilEnc.sealKey(plain, keyDomainIndex)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err = nilEnc.openKey(plain, keyDomainIndex)
	require.NoError(t, err)
	require.Equal(t, plain, opened)

	// Unknown key domains are rejected only when key encryption is on.
	enc := &encState{encKeys: true, sivs: map[string]*crypto.SIVNameCipher{}}
	_, err = enc.sealKey(plain, "bogus")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = enc.openKey(plain, "bogus")
	require.ErrorIs(t, err, ErrInvalidConfig)
	// Empty keys pass through even with key encryption enabled.
	sealed, err = enc.sealKey(nil, keyDomainIndex)
	require.NoError(t, err)
	require.Nil(t, sealed)

	require.Nil(t, encFromNode(nil))
	require.Nil(t, encFromNode(&node{}))
	var nilNode *node
	require.Nil(t, nilNode.nodeEnc())

	_, _, err = decodeMarker([]byte("garbage"))
	require.Error(t, err)

	// Plaintext nodes pass values and keys through unchanged.
	db, cleanup := createDB(t)
	defer cleanup()
	n := db.WithCodec(JSONCodec).(*node)
	require.Nil(t, n.nodeEnc())
	sealed, err = n.seal([]byte("v"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), sealed)
	key, err := n.keyBytes("k", keyDomainIndex)
	require.NoError(t, err)
	require.Equal(t, []byte("k"), key)
	_, err = n.keyBytes(make(chan int), keyDomainIndex)
	require.Error(t, err)
	_, _, _, err = n.loadMarker(context.Background())
	require.NoError(t, err)
	_, err = n.hasUserData(context.Background())
	require.NoError(t, err)
}

func TestCoerceKeyValue(t *testing.T) {
	require.Nil(t, coerceKeyValue(nil, nil))
	require.Equal(t, "x", coerceKeyValue("x", nil))

	fieldValue := reflect.ValueOf(uint64(0))
	field := &fieldConfig{Value: &fieldValue}
	// Same numeric class converts.
	require.Equal(t, uint64(5), coerceKeyValue(int(5), field))
	// Different class does not.
	require.Equal(t, "5", coerceKeyValue("5", field))
	// Nil field value leaves the value untouched.
	require.Equal(t, int(5), coerceKeyValue(int(5), &fieldConfig{}))
}

func TestFinderMissingBuckets(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	require.NoError(t, db.Init(ctx, &User{}))
	require.NoError(t, db.Drop(ctx, &User{}))

	var got User
	require.ErrorIs(t, db.One(ctx, "Name", "x", &got), ErrNotFound)
	require.ErrorIs(t, db.Find(ctx, "Name", "x", &[]User{}), ErrNotFound)
	require.ErrorIs(t, db.AllByIndex(ctx, "Name", &[]User{}), ErrNotFound)
	require.ErrorIs(t, db.DeleteStruct(ctx, &User{ID: 1}), ErrNotFound)

	var users []User
	require.NoError(t, db.Range(ctx, "Name", "a", "z", &users))
	require.Empty(t, users)
	require.NoError(t, db.Prefix(ctx, "Name", "a", &users))
	require.Empty(t, users)
}

func TestResolveIndexesUnknownKind(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()
	require.NoError(t, db.Save(ctx, &User{Name: "x", Slug: "x"}))

	n := db.WithCodec(JSONCodec).(*node)
	require.NoError(t, n.s.bolt.View(func(tx *bolt.Tx) error {
		b := n.getBucket(tx, "User")
		require.NotNil(t, b)
		_, err := resolveIndexes(b, &structConfig{Fields: map[string]*fieldConfig{
			"X": {Index: "unsupported"},
		}})
		require.ErrorIs(t, err, ErrIdxNotFound)
		return nil
	}))
}

func TestHasUserDataBranches(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()
	n := db.WithCodec(JSONCodec).(*node)

	_, err := n.hasUserData(ctx)
	require.NoError(t, err)

	require.NoError(t, db.Save(ctx, &User{Name: "x", Slug: "x"}))
	has, err := n.hasUserData(ctx)
	require.NoError(t, err)
	require.True(t, has)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = n.hasUserData(canceled)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIncrementCorruptCounter(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()
	require.NoError(t, db.Save(ctx, &User{Name: "first", Slug: "first"}))

	n := db.WithCodec(JSONCodec).(*node)
	require.NoError(t, n.s.bolt.Update(func(tx *bolt.Tx) error {
		b := n.getBucket(tx, "User")
		meta := b.Bucket([]byte(metadataBucket))
		require.NotNil(t, meta)
		return meta.Put([]byte("IDcounter"), []byte{1, 2, 3})
	}))

	require.Error(t, db.Save(ctx, &User{Name: "second", Slug: "second"}))
}
