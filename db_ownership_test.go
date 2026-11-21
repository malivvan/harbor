package storm

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestOpenNilDBRejected verifies Open rejects a nil bbolt database.
func TestOpenNilDBRejected(t *testing.T) {
	db, err := Open(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, ErrNilParam)
	require.Nil(t, db)
}

// TestCallerOwnsLifecycle verifies that Storm borrows the supplied bbolt DB.
func TestCallerOwnsLifecycle(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	db, err := Open(context.Background(), bDB, nil, nil)
	require.NoError(t, err)

	// NativeDB returns the exact pointer supplied to Open.
	require.Same(t, bDB, db.NativeDB(), "NativeDB must return same pointer as supplied bDB")

	// Close releases the Storm wrapper without closing the caller's DB.
	require.NoError(t, db.Close(), "Close must return nil")

	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "bbolt DB must still be usable after Storm Close")

	// The caller remains responsible for closing the database.
	require.NoError(t, bDB.Close(), "caller must be able to close the DB after Storm Close")
}

// TestInitFailureDoesNotClose verifies a failed Open leaves the caller's DB open.
func TestInitFailureDoesNotClose(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed_no_close.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	// Pre-seed with a conflicting codec to trigger initialization failure.
	err = bDB.Update(func(tx *bolt.Tx) error {
		top, err := tx.CreateBucketIfNotExists([]byte(dbinfo))
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

	db, err := Open(context.Background(), bDB, nil, nil)
	require.ErrorIs(t, err, ErrDifferentCodec)
	require.Nil(t, db)

	// The supplied DB must still be open.
	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "bbolt must still be usable after init failure")

	require.NoError(t, bDB.Close())
}

// TestCancellationDoesNotClose verifies a cancelled Open leaves the caller's DB open.
func TestCancellationDoesNotClose(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed_cancel.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := Open(cancelCtx, bDB, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, db)

	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "bbolt must still be usable after cancelled Open")

	require.NoError(t, bDB.Close())
}

// TestDBNilClose verifies nil receiver and nil bolt Close behavior.
func TestDBNilClose(t *testing.T) {
	// (*DB)(nil).Close() returns ErrNilParam.
	var nilDB *DB
	err := nilDB.Close()
	require.ErrorIs(t, err, ErrNilParam)
	require.Nil(t, nilDB.NativeDB())

	// DB with nil private database returns ErrNilParam from Close.
	db := &DB{} // bolt is nil
	err = db.Close()
	require.ErrorIs(t, err, ErrNilParam)
	require.Nil(t, db.NativeDB())
}

// TestNativeKVInteroperability verifies native writes are visible to Storm and vice versa.
func TestNativeKVInteroperability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "native_interop.db")

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	db, err := Open(context.Background(), bDB, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Step 1: Storm write via KV API.
	err = db.Set(ctx, "interop", "key1", "storm_value")
	require.NoError(t, err)

	// Step 2: Verify via NativeDB (read back the raw bytes).
	var nativeVal []byte
	err = db.NativeDB().View(func(tx *bolt.Tx) error {
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
	require.Equal(t, `"storm_value"`, string(nativeVal), "Storm value must be visible through NativeDB")

	// Step 3: Native write via NativeDB (write raw JSON-encoded string).
	err = db.NativeDB().Update(func(tx *bolt.Tx) error {
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
