package object_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/ledger"
	"github.com/malivvan/tempest/store/object"
)

// rawObjectBolt opens a bare bbolt database (no object store) so tests can
// craft the object root bucket by hand.
func rawObjectBolt(t *testing.T) *bolt.DB {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "raw.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	return bdb
}

func TestOpenErrors(t *testing.T) {
	ctx := context.Background()
	//lint:ignore SA1012 nil ctx must be rejected
	if _, err := object.Open(nil, nil, object.Config{}); err == nil {
		t.Fatal("Open(nil ctx) err = nil, want error")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := object.Open(cancelled, nil, object.Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open(cancelled) err = %v, want context.Canceled", err)
	}
	if _, err := object.Open(ctx, nil, object.Config{}); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("Open(nil db) err = %v, want ErrNoStore", err)
	}

	bdb := rawObjectBolt(t)
	if _, err := object.Open(ctx, bdb, object.Config{Root: [][]byte{[]byte("a/b")}}); !errors.Is(err, object.ErrInvalidBucket) {
		t.Fatalf("Open(bad root) err = %v, want ErrInvalidBucket", err)
	}
	if _, err := object.Open(ctx, bdb, object.Config{EncryptValues: true, Secret: []byte("short")}); err == nil {
		t.Fatal("Open(short secret) err = nil, want key error")
	}
	// A nil root leaves no bucket to create; the store must report ErrInvalidBucket.
	if _, err := object.Open(ctx, bdb, object.Config{}); !errors.Is(err, object.ErrInvalidBucket) {
		t.Fatalf("Open(nil root) err = %v, want ErrInvalidBucket", err)
	}
}

func TestOpenReadOnly(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "b", "k", strings.NewReader("payload"), 7, "")
	require.NoError(t, err)

	ro, err := object.OpenReadOnly(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(bucket.OBJECT)}, ro.Root())

	got, err := ro.ReadAll(ctx, "b", "k")
	require.NoError(t, err)
	require.Equal(t, "payload", string(got))
	require.NoError(t, ro.Flush())

	// Every write is rejected on a read-only store.
	_, err = ro.Put(ctx, "b", "k2", strings.NewReader("x"), 1, "")
	require.ErrorIs(t, err, object.ErrReadOnly)
	require.ErrorIs(t, ro.Delete(ctx, "b", "k"), object.ErrReadOnly)
	require.ErrorIs(t, ro.Move(ctx, "b", "k", "k3"), object.ErrReadOnly)

	// Closing a store makes further operations fail.
	require.NoError(t, ro.Close())
	require.Error(t, ro.Flush())
	_, _, err = ro.Get(ctx, "b", "k")
	require.Error(t, err)
}

func TestNilReceiversAndClose(t *testing.T) {
	ctx := context.Background()
	var ns *object.Store
	require.ErrorIs(t, ns.Flush(), object.ErrNoStore)
	require.NoError(t, ns.Close())
	require.Nil(t, ns.Root())
	require.Nil(t, ns.WithRoot("x"))
	require.Nil(t, ns.WithTransaction(nil))

	s, _ := openDirect(t, object.Config{})
	require.NoError(t, s.Flush())

	// A nil transaction yields no bound view.
	require.Nil(t, s.WithTransaction(nil))

	require.NoError(t, s.Close())
	_, err := s.ReadAll(ctx, "b", "k")
	require.Error(t, err)
	_, err = s.Put(ctx, "b", "k", strings.NewReader("x"), 1, "")
	require.Error(t, err)
}

func TestTraceReplay(t *testing.T) {
	ctx := context.Background()
	var recorded []ledger.Entry
	sink := ledger.SinkFunc(func(e ledger.Entry) error {
		recorded = append(recorded, e)
		return nil
	})
	s, _ := openDirect(t, object.Config{Trace: sink})
	_, err := s.Put(ctx, "b", "k", strings.NewReader("data"), 4, "")
	require.NoError(t, err)
	require.NoError(t, s.Move(ctx, "b", "k", "k2"))
	require.NoError(t, s.Delete(ctx, "b", "k2"))
	require.Len(t, recorded, 3)
	require.Equal(t, "put", recorded[0].Op)
	require.Equal(t, "move", recorded[1].Op)
	require.Equal(t, "delete", recorded[2].Op)

	// A failing sink surfaces through the write call.
	fail := ledger.SinkFunc(func(ledger.Entry) error { return errors.New("trace down") })
	s2, _ := openDirect(t, object.Config{Trace: fail})
	_, err = s2.Put(ctx, "b", "k", strings.NewReader("x"), 1, "")
	require.Error(t, err)
}

func TestMoveSemantics(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "b", "src", strings.NewReader("S"), 1, "")
	require.NoError(t, err)
	_, err = s.Put(ctx, "b", "dst", strings.NewReader("existing"), 8, "")
	require.NoError(t, err)

	// An existing destination is overwritten.
	require.NoError(t, s.Move(ctx, "b", "src", "dst"))
	got, err := s.ReadAll(ctx, "b", "dst")
	require.NoError(t, err)
	require.Equal(t, "S", string(got))
	require.False(t, s.Exists(ctx, "b", "src"))

	// Empty or equal keys are no-ops.
	require.NoError(t, s.Move(ctx, "b", "", "dst"))
	require.NoError(t, s.Move(ctx, "b", "dst", ""))
	require.NoError(t, s.Move(ctx, "b", "dst", "dst"))

	// A move inside a bucket that does not exist is a no-op.
	require.NoError(t, s.Move(ctx, "nobucket", "a", "b"))
}

func TestStatListGetMissingBucket(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	_, _, err := s.Get(ctx, "ghost", "k")
	require.ErrorIs(t, err, os.ErrNotExist)
	_, ok, err := s.Stat(ctx, "ghost", "k")
	require.NoError(t, err)
	require.False(t, ok)

	list, err := s.List(ctx, "ghost", "")
	require.NoError(t, err)
	require.Empty(t, list)

	// Several objects exercise sort ordering and prefix filtering.
	for _, k := range []string{"z.txt", "a.txt", "m.txt"} {
		_, err := s.Put(ctx, "b", k, strings.NewReader("d"), 1, "")
		require.NoError(t, err)
	}
	list, err = s.List(ctx, "b", "")
	require.NoError(t, err)
	require.Len(t, list, 3)
	require.Equal(t, "a.txt", list[0].Key)
	require.Equal(t, "z.txt", list[2].Key)

	list, err = s.List(ctx, "b", "x")
	require.NoError(t, err)
	require.Empty(t, list)
}

func TestObjectKeyAndPutErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	if _, err := s.Put(ctx, "b", "", strings.NewReader("x"), 1, ""); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("Put(empty key) err = %v, want ErrInvalidKey", err)
	}
	if _, err := s.Put(ctx, "b", ".", strings.NewReader("x"), 1, ""); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("Put(dot key) err = %v, want ErrInvalidKey", err)
	}
	if _, err := s.Put(ctx, "b", "k", nil, 0, ""); err == nil {
		t.Fatal("Put(nil reader) err = nil, want error")
	}
}

func TestRootedMissingBucket(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	// A rooted view whose bucket path was never created reads as missing.
	view := s.WithRoot("absence")
	_, _, err := view.Get(ctx, "b", "k")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWithRootFallback(t *testing.T) {
	s, _ := openDirect(t, object.Config{})
	require.NotNil(t, s.WithRoot("tenant"))
	// An invalid segment falls back to the receiver's root path.
	bad := s.WithRoot("a/b")
	require.Equal(t, s.Root(), bad.Root())
}

func TestCtxErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := s.Put(cancelled, "b", "k", strings.NewReader("x"), 1, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put(cancelled) err = %v", err)
	}
	if _, _, err := s.Get(cancelled, "b", "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get(cancelled) err = %v", err)
	}
	if err := s.Delete(cancelled, "b", "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete(cancelled) err = %v", err)
	}
	if err := s.Move(cancelled, "b", "a", "c"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Move(cancelled) err = %v", err)
	}
	if _, _, err := s.Stat(cancelled, "b", "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat(cancelled) err = %v", err)
	}
	if _, err := s.List(cancelled, "b", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("List(cancelled) err = %v", err)
	}
	// A nil context is rejected too.
	//lint:ignore SA1012 nil ctx must be rejected
	if _, err := s.Put(nil, "b", "k", strings.NewReader("x"), 1, ""); err == nil {
		t.Fatal("Put(nil ctx) err = nil, want error")
	}
}

func TestCloseThenUpdateRejected(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "b", "k", strings.NewReader("x"), 1, "")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// update() runs checkOpen first, so writes fail once closed.
	_, err = s.Put(ctx, "b", "k2", strings.NewReader("y"), 1, "")
	require.Error(t, err)
}

func TestMigrateMissingBucket(t *testing.T) {
	ctx := context.Background()
	store, err := tempest.Open(ctx, filepath.Join(t.TempDir(), "tempest.db"), 0o600, &tempest.Options{
		Password:   "object-password",
		KDFTime:    1,
		KDFMemory:  8 * tempest.MiB,
		KDFThreads: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	fsys, err := store.Fstree().Open("objects")
	require.NoError(t, err)

	// A source bucket that does not exist migrates nothing and is not an error.
	require.NoError(t, store.Objects().MigrateFromFile(ctx, fsys, "nonexistent"))
	// A nil file system is rejected.
	require.Error(t, store.Objects().MigrateFromFile(ctx, nil, "bucket"))
	// An invalid bucket name is rejected.
	require.ErrorIs(t, store.Objects().MigrateFromFile(ctx, fsys, "bad/name"), object.ErrInvalidBucket)
	// A store without a database is rejected.
	var ns *object.Store
	require.ErrorIs(t, ns.MigrateFromFile(ctx, fsys, "bucket"), object.ErrNoStore)
}

func TestOpenReadOnlyMetaErrors(t *testing.T) {
	ctx := context.Background()
	root := [][]byte{[]byte(bucket.OBJECT)}
	versionBytes := func(v uint64) []byte {
		var buf [8]byte
		for i := range buf {
			buf[i] = byte(v >> (8 * i))
		}
		return buf[:]
	}

	// Missing version record under a read-only open.
	bdb := rawObjectBolt(t)
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket.OBJECT))
		return err
	}))
	_, err := object.OpenReadOnly(ctx, bdb, object.Config{Root: root})
	require.ErrorIs(t, err, object.ErrIncompatibleVersion)

	// Malformed version record.
	bdb2 := rawObjectBolt(t)
	require.NoError(t, bdb2.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket.OBJECT))
		if err != nil {
			return err
		}
		return b.Put([]byte("__version__"), []byte{1, 2, 3})
	}))
	_, err = object.OpenReadOnly(ctx, bdb2, object.Config{Root: root})
	require.ErrorIs(t, err, object.ErrIncompatibleVersion)

	// Missing mode record under a read-only open.
	bdb3 := rawObjectBolt(t)
	require.NoError(t, bdb3.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket.OBJECT))
		if err != nil {
			return err
		}
		return b.Put([]byte("__version__"), versionBytes(object.FormatVersion))
	}))
	_, err = object.OpenReadOnly(ctx, bdb3, object.Config{Root: root})
	require.ErrorIs(t, err, object.ErrWrongMode)
}

func TestNilReceiverNoStore(t *testing.T) {
	ctx := context.Background()
	var ns *object.Store
	if _, err := ns.Put(ctx, "b", "k", strings.NewReader("x"), 1, ""); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("nil Put err = %v, want ErrNoStore", err)
	}
	if _, _, err := ns.Get(ctx, "b", "k"); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("nil Get err = %v, want ErrNoStore", err)
	}
	if err := ns.Delete(ctx, "b", "k"); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("nil Delete err = %v, want ErrNoStore", err)
	}
	if err := ns.Move(ctx, "b", "a", "c"); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("nil Move err = %v, want ErrNoStore", err)
	}
	if _, _, err := ns.Stat(ctx, "b", "k"); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("nil Stat err = %v, want ErrNoStore", err)
	}
	if _, err := ns.List(ctx, "b", ""); !errors.Is(err, object.ErrNoStore) {
		t.Fatalf("nil List err = %v, want ErrNoStore", err)
	}
}

func TestInvalidBucketAndKeyPaths(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	// An invalid bucket name flows through the low-level bucket lookup.
	if _, _, err := s.Get(ctx, "bad/name", "k"); !errors.Is(err, object.ErrInvalidBucket) {
		t.Fatalf("Get(bad bucket) err = %v, want ErrInvalidBucket", err)
	}
	if _, _, err := s.Stat(ctx, "bad/name", "k"); !errors.Is(err, object.ErrInvalidBucket) {
		t.Fatalf("Stat(bad bucket) err = %v, want ErrInvalidBucket", err)
	}
	if _, err := s.List(ctx, "bad/name", ""); !errors.Is(err, object.ErrInvalidBucket) {
		t.Fatalf("List(bad bucket) err = %v, want ErrInvalidBucket", err)
	}

	// Empty keys.
	if err := s.Delete(ctx, "b", ""); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("Delete(empty key) err = %v, want ErrInvalidKey", err)
	}
	if _, _, err := s.Stat(ctx, "b", ""); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("Stat(empty key) err = %v, want ErrInvalidKey", err)
	}

	// A ".." prefix on List is rejected as a key.
	if _, err := s.List(ctx, "b", "../x"); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("List(parent prefix) err = %v, want ErrInvalidKey", err)
	}

	// Move rejects a ".." source or destination.
	if err := s.Move(ctx, "b", "../x", "y"); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("Move(bad src) err = %v, want ErrInvalidKey", err)
	}
	if err := s.Move(ctx, "b", "x", "../y"); !errors.Is(err, object.ErrInvalidKey) {
		t.Fatalf("Move(bad dst) err = %v, want ErrInvalidKey", err)
	}

	// Delete and Move against a bucket that was never created are no-ops.
	require.NoError(t, s.Delete(ctx, "nobucket", "k"))
	require.NoError(t, s.Move(ctx, "nobucket", "a", "b"))
}

func TestEmptyWithRootPart(t *testing.T) {
	s, _ := openDirect(t, object.Config{})
	view := s.WithRoot("", "tenant")
	require.NotNil(t, view)
	require.Equal(t, [][]byte{[]byte("tenant")}, view.Root())
}

func TestNestedRootTraversal(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})
	// Create the first-level bucket so rootBucket takes the nested branch.
	_, err := s.WithRoot("outer").Put(ctx, "b", "k", strings.NewReader("v"), 1, "")
	require.NoError(t, err)
	// A two-segment root where the leaf is missing reads as not-found.
	_, _, err = s.WithRoot("outer", "inner").Get(ctx, "b", "k")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestBoundTransactionRead(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "b", "k", strings.NewReader("value"), 5, "")
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		bound := s.WithTransaction(tx)
		got, err := bound.ReadAll(ctx, "b", "k")
		if err != nil {
			return err
		}
		require.Equal(t, "value", string(got))
		return nil
	}))
}

func TestListCorruptRecord(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, object.Config{})
	// Seed one valid object so the bucket exists, then inject a bad record.
	_, err := s.Put(ctx, "b", "good", strings.NewReader("ok"), 2, "")
	require.NoError(t, err)
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.OBJECT)).Bucket([]byte("b")).Put([]byte("bad"), []byte("{not json"))
	}))
	_, err = s.List(ctx, "b", "")
	require.Error(t, err)
}

func TestOpenReadOnlyMissingRoot(t *testing.T) {
	ctx := context.Background()
	bdb := rawObjectBolt(t)
	// No object root bucket at all: a read-only open cannot create it.
	_, err := object.OpenReadOnly(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}})
	require.ErrorIs(t, err, object.ErrIncompatibleVersion)
}

func TestOpenReadOnlyCanaryMissing(t *testing.T) {
	ctx := context.Background()
	secret := []byte("0123456789abcdef0123456789abcdef")
	_, bdb := openDirect(t, object.Config{Secret: secret, EncryptValues: true})
	// Remove the canary but keep version+mode so the read-only open reaches the
	// canary check.
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.OBJECT)).Delete([]byte("__canary__"))
	}))
	_, err := object.OpenReadOnly(ctx, bdb, object.Config{Root: [][]byte{[]byte(bucket.OBJECT)}, Secret: secret, EncryptValues: true})
	require.ErrorIs(t, err, object.ErrWrongMode)
}

func TestListSkipsSubBuckets(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, object.Config{})
	_, err := s.Put(ctx, "b", "k", strings.NewReader("v"), 1, "")
	require.NoError(t, err)
	// A nested bucket inside the object bucket is skipped by List.
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte(bucket.OBJECT)).Bucket([]byte("b")).CreateBucketIfNotExists([]byte("sub"))
		return err
	}))
	list, err := s.List(ctx, "b", "")
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "k", list[0].Key)
}
