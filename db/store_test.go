package db

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestNewStorm(t *testing.T) {
	db, err := Open(context.Background(), nil, Config{})

	require.Error(t, err)
	require.Nil(t, db)

	dir, err := os.MkdirTemp(os.TempDir(), "storm")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "storm.db")
	bdb, err := bolt.Open(file, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	db, err = Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	defer db.Close()

	require.Implements(t, (*Node)(nil), db)
	require.NotNil(t, db.bolt)
	require.Equal(t, defaultCodec, db.Codec())

	var v string
	err = db.Get(context.Background(), databaseInfo, "version", &v)
	require.NoError(t, err)
	require.Equal(t, Version, v)
}

func TestNewStormWithRootAndCodec(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "storm")
	defer os.RemoveAll(dir)

	dc := new(dummyCodec)

	bdb1, err := bolt.Open(filepath.Join(dir, "storm1.db"), 0660, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bdb1.Close()
	db1, err := Open(context.Background(), bdb1, Config{Root: [][]byte{[]byte("a")}, Codec: dc})
	require.NoError(t, err)
	require.Equal(t, dc, db1.Codec())
	require.Equal(t, bpath("a"), db1.Node.(*node).root)

	err = db1.Save(context.Background(), &SimpleUser{ID: 1})
	require.NoError(t, err)

	bdb2, err := bolt.Open(filepath.Join(dir, "storm2.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bdb2.Close()
	db2, err := Open(context.Background(), bdb2, Config{Codec: dc})
	require.NoError(t, err)
	require.Equal(t, dc, db2.Codec())
}

func TestBoltDB(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "storm")
	defer os.RemoveAll(dir)
	bDB, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	sDB, err := Open(context.Background(), bDB, Config{})
	require.NoError(t, err)
	defer sDB.Close()
	err = sDB.Save(context.Background(), &SimpleUser{ID: 10})
	require.NoError(t, err)
}

type dummyCodec int

func (c dummyCodec) Marshal(v interface{}) ([]byte, error) {
	return []byte("dummy"), nil
}

func (c dummyCodec) Unmarshal(b []byte, v interface{}) error {
	return nil
}

func (c dummyCodec) Name() string {
	return "dummy"
}

func TestCodec(t *testing.T) {
	u1 := &SimpleUser{Name: "John"}
	encoded, err := defaultCodec.Marshal(u1)
	require.Nil(t, err)
	u2 := &SimpleUser{}
	err = defaultCodec.Unmarshal(encoded, u2)
	require.Nil(t, err)
	if !reflect.DeepEqual(u1, u2) {
		t.Fatal("Codec mismatch")
	}
}

func TestToBytes(t *testing.T) {
	b, err := toBytes([]byte("a slice of bytes"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("a slice of bytes"), b)

	b, err = toBytes("a string", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("a string"), b)

	b, err = toBytes(&SimpleUser{ID: 10, Name: "John", age: 100}, JSONCodec)
	require.NoError(t, err)
	require.Equal(t, `{"ID":10,"Name":"John"}`, string(b))

	prev := []byte(nil)
	for i, v := range []int64{-math.MaxInt64, -1, 0, 1, math.MaxInt64} {
		b, err := toBytes(v, nil)
		require.NoError(t, err)
		require.Len(t, b, 8)
		if i > 0 {
			require.Less(t, string(prev), string(b), "int keys must be order-preserving")
		}
		prev = b
	}

	prev = nil
	for i, v := range []uint64{0, 1, math.MaxUint64} {
		b, err := toBytes(v, nil)
		require.NoError(t, err)
		require.Len(t, b, 8)
		if i > 0 {
			require.Less(t, string(prev), string(b), "uint keys must be order-preserving")
		}
		prev = b
	}

	prev = nil
	for i, v := range []float64{-math.MaxFloat64, -1, 0, 1, math.MaxFloat64} {
		b, err := toBytes(v, nil)
		require.NoError(t, err)
		require.Len(t, b, 8)
		if i > 0 {
			require.Less(t, string(prev), string(b), "float keys must be order-preserving")
		}
		prev = b
	}
}

func createDB(t errorHandler, opts ...testOpenOption) (*Store, func()) {
	cfg := defaultTestOpenOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	dir, err := os.MkdirTemp(os.TempDir(), "storm")
	if err != nil {
		t.Error(err)
	}

	bdb, err := bolt.Open(filepath.Join(dir, "storm.db"), cfg.mode, cfg.boltOptions)
	if err != nil {
		t.Error(err)
	}

	root := make([][]byte, len(cfg.root))
	for i, part := range cfg.root {
		root[i] = []byte(part)
	}

	db, err := Open(context.Background(), bdb, Config{Root: root, Codec: cfg.codec})
	if err != nil {
		t.Error(err)
	}

	return db, func() {
		if db != nil {
			_ = db.Close()
		}
		if bdb != nil {
			_ = bdb.Close()
		}
		_ = os.RemoveAll(dir)
	}
}

type errorHandler interface {
	Error(args ...interface{})
}

// errTestDecode is a stable sentinel returned by the test codec's Unmarshal.
var errTestDecode = errors.New("test-codec: deterministic decode error")

// testDecodeCodec is a private codec used only to prove that checkVersion
// preserves the underlying codec/decode error alongside ErrDifferentCodec.
type testDecodeCodec struct{}

func (testDecodeCodec) Name() string                          { return "test-decode-codec" }
func (testDecodeCodec) Marshal(v interface{}) ([]byte, error) { return []byte("x"), nil }
func (testDecodeCodec) Unmarshal(b []byte, v interface{}) error {
	return errTestDecode
}

// TestCheckVersion_EmptyVersionIsInitialized verifies that an existing version
// value that decodes as empty is replaced with the current version.
func TestCheckVersion_EmptyVersionIsInitialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty-version.db")
	ctx := context.Background()

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)
	require.NoError(t, db.Set(ctx, databaseInfo, "version", ""))
	require.NoError(t, db.Close())

	db, err = Open(ctx, bDB, Config{})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	var version string
	require.NoError(t, db.Get(ctx, databaseInfo, "version", &version))
	require.Equal(t, Version, version)
}

// TestCheckVersion_ErrorChainPreservation proves that when Open encounters a
// codec mismatch during checkVersion, the returned error matches both
// ErrDifferentCodec and the underlying codec/decode error.
func TestCheckVersion_ErrorChainPreservation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.db")

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	// Create a database with the default JSON codec. This writes
	// __database/version using the JSON codec.
	seedDB, err := Open(context.Background(), bDB, Config{})
	require.NoError(t, err, "create seed Store")
	require.NoError(t, seedDB.Close(), "close seed Store")

	// Open with an incompatible codec that deliberately fails Unmarshal.
	db, err := Open(context.Background(), bDB, Config{Codec: testDecodeCodec{}})
	require.Error(t, err)

	// Must match ErrDifferentCodec.
	require.ErrorIs(t, err, ErrDifferentCodec,
		"checkVersion must classify codec mismatch as ErrDifferentCodec")

	// The underlying decode error must remain discoverable.
	require.ErrorIs(t, err, errTestDecode,
		"checkVersion must preserve the underlying decode error in the chain")

	// No partial Store is returned.
	assert.Nil(t, db, "returned Store must be nil on initialization failure")

	// Reopening with the correct codec must succeed and data must be intact.
	db2, err := Open(context.Background(), bDB, Config{})
	require.NoError(t, err, "reopen with correct codec")
	require.NoError(t, db2.Close())
}

// TestOpenNilDBRejected verifies Open rejects a nil bbolt database.
func TestOpenNilDBRejected(t *testing.T) {
	db, err := Open(context.Background(), nil, Config{})
	require.ErrorIs(t, err, ErrNilParam)
	require.Nil(t, db)
}

// TestCallerOwnsLifecycle verifies that Storm borrows the supplied bbolt Store.
func TestCallerOwnsLifecycle(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	db, err := Open(context.Background(), bDB, Config{})
	require.NoError(t, err)

	// The store keeps the exact pointer supplied to Open.
	require.Same(t, bDB, db.bolt, "the store must keep the supplied bDB pointer")

	// Close releases the Storm wrapper without closing the caller's Store.
	require.NoError(t, db.Close(), "Close must return nil")

	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "bbolt Store must still be usable after Storm Close")

	// The caller remains responsible for closing the database.
	require.NoError(t, bDB.Close(), "caller must be able to close the Store after Storm Close")
}

// TestInitFailureDoesNotClose verifies a failed Open leaves the caller's Store open.
func TestInitFailureDoesNotClose(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed_no_close.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	// Pre-seed with a conflicting codec to trigger initialization failure.
	err = bDB.Update(func(tx *bolt.Tx) error {
		top, err := tx.CreateBucketIfNotExists([]byte(databaseInfo))
		if err != nil {
			return err
		}
		_, err = top.CreateBucket([]byte(metadataBucket))
		if err != nil {
			return err
		}
		mb := top.Bucket([]byte(metadataBucket))
		return mb.Put([]byte(metaCodec), []byte("not-json"))
	})
	require.NoError(t, err)

	db, err := Open(context.Background(), bDB, Config{})
	require.ErrorIs(t, err, ErrDifferentCodec)
	require.Nil(t, db)

	// The supplied Store must still be open.
	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "bbolt must still be usable after init failure")

	require.NoError(t, bDB.Close())
}

// TestCancellationDoesNotClose verifies a cancelled Open leaves the caller's Store open.
func TestCancellationDoesNotClose(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed_cancel.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := Open(cancelCtx, bDB, Config{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, db)

	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "bbolt must still be usable after cancelled Open")

	require.NoError(t, bDB.Close())
}

// TestDBNilClose verifies nil receiver and nil bolt Close behavior.
func TestDBNilClose(t *testing.T) {
	// (*Store)(nil).Close() returns ErrNilParam.
	var nilDB *Store
	err := nilDB.Close()
	require.ErrorIs(t, err, ErrNilParam)

	// Store with nil private database returns ErrNilParam from Close.
	db := &Store{} // bolt is nil
	err = db.Close()
	require.ErrorIs(t, err, ErrNilParam)
}

// TestNativeKVInteroperability verifies native writes are visible to Storm and vice versa.
func TestNativeKVInteroperability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "native_interop.db")

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	db, err := Open(context.Background(), bDB, Config{})
	require.NoError(t, err)

	ctx := context.Background()

	// Step 1: Storm write via KV API.
	err = db.Set(ctx, "interop", "key1", "storm_value")
	require.NoError(t, err)

	// Step 2: Verify via native DB (read back the raw bytes).
	var nativeVal []byte
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("interop"))
		if b == nil {
			return ErrNotFound
		}
		raw := b.Get([]byte("key1"))
		if raw == nil {
			return ErrNotFound
		}
		nativeVal = append([]byte(nil), raw...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, `"storm_value"`, string(nativeVal), "Storm value must be visible through native DB")

	// Step 3: Native write via native DB (write raw JSON-encoded string).
	err = db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("interop"))
		if b == nil {
			return ErrNotFound
		}
		return b.Put([]byte("key2"), []byte(`"native_value"`))
	})
	require.NoError(t, err)

	// Step 4: Verify via Storm.
	var got string
	err = db.Get(ctx, "interop", "key2", &got)
	require.NoError(t, err)
	require.Equal(t, "native_value", got, "native write must be visible through Storm")

	require.NoError(t, db.Close())
}
