package db

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// canceledCtx returns a context that is already canceled.
func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// timedOutCtx returns a context that has already exceeded its deadline.
func timedOutCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	<-ctx.Done()
	return ctx
}

// ============================================================================
// 16.1 Open
// ============================================================================

func TestOpen_CancelledContextDoesNotInitializeDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storm.db")

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	db, err := Open(canceledCtx(), bDB, Config{})
	require.Error(t, err)
	require.Nil(t, db)
	require.True(t, errors.Is(err, context.Canceled))

	// No initialization work must have been performed.
	err = bDB.View(func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte(databaseInfo)))
		return nil
	})
	require.NoError(t, err)
}

func TestOpen_InitializationErrorDoesNotCloseBorrowedDatabase(t *testing.T) {
	dir := t.TempDir()

	bDBPath := filepath.Join(dir, "borrowed.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	// Pre-seed the version bucket with a non-default codec marker so the default
	// (json) codec used by the second Open will conflict during checkVersion.
	err = bDB.Update(func(tx *bolt.Tx) error {
		top, err := tx.CreateBucketIfNotExists([]byte(databaseInfo))
		if err != nil {
			return err
		}
		// Mirror what newMeta writes: a nested metadata bucket carrying the
		// codec name. Set via a non-default codec name to force ErrDifferentCodec.
		_, err = top.CreateBucket([]byte(metadataBucket))
		if err != nil {
			return err
		}
		mb := top.Bucket([]byte(metadataBucket))
		return mb.Put([]byte(metaCodec), []byte(altCodec.Name()))
	})
	require.NoError(t, err)

	// Step 2: hand the borrowed Store to Storm with the default (json) codec.
	// checkVersion will call Set on __database, which builds the data
	// bucket via setBytes -> newMeta, and newMeta will compare codec names and
	// return ErrDifferentCodec. That is our deterministic initialization error.
	db, err := Open(context.Background(), bDB, Config{})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrDifferentCodec), "expected ErrDifferentCodec, got %v", err)
	require.Nil(t, db)

	// The borrowed bolt.Store must NOT have been closed by Storm. We confirm
	// by performing a fresh close on bDB directly: if Storm had closed it,
	// this would fail with "database not open".
	require.NoError(t, bDB.Close())

	// And we can reopen the file from scratch, proving it is not locked.
	bDB2, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	require.NoError(t, bDB2.Close())
}

// ============================================================================
// 16.2 Struct operations
// ============================================================================

func TestStructOps_CancelledContextPreventsWork(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed one record so update/delete targets exist on the happy path.
	require.NoError(t, db.Save(context.Background(), &User{ID: 1, Name: "seed"}))

	tests := []struct {
		name string
		op   func(ctx context.Context) error
		// verifyNoPersist, when not nil, confirms that no record was written.
		verifyNoPersist func(t *testing.T)
	}{
		{
			name: "Init",
			op: func(ctx context.Context) error {
				return db.Init(ctx, &IndexedNameUser{})
			},
			verifyNoPersist: func(t *testing.T) {
				err := db.bolt.View(func(tx *bolt.Tx) error {
					require.Nil(t, tx.Bucket([]byte("IndexedNameUser")))
					return nil
				})
				require.NoError(t, err)
			},
		},
		{
			name: "ReIndex",
			op: func(ctx context.Context) error {
				return db.ReIndex(ctx, &User{})
			},
		},
		{
			name: "Save",
			op: func(ctx context.Context) error {
				return db.Save(ctx, &User{ID: 42, Name: "canceled"})
			},
			verifyNoPersist: func(t *testing.T) {
				var u User
				err := db.One(context.Background(), "ID", 42, &u)
				require.ErrorIs(t, err, ErrNotFound)
			},
		},
		{
			name: "Update",
			op: func(ctx context.Context) error {
				return db.Update(ctx, &User{ID: 1, Name: "updated"})
			},
			verifyNoPersist: func(t *testing.T) {
				var u User
				require.NoError(t, db.One(context.Background(), "ID", 1, &u))
				require.Equal(t, "seed", u.Name)
			},
		},
		{
			name: "UpdateField",
			op: func(ctx context.Context) error {
				return db.UpdateField(ctx, &User{ID: 1}, "Name", "updated")
			},
			verifyNoPersist: func(t *testing.T) {
				var u User
				require.NoError(t, db.One(context.Background(), "ID", 1, &u))
				require.Equal(t, "seed", u.Name)
			},
		},
		{
			name: "Drop",
			op: func(ctx context.Context) error {
				return db.Drop(ctx, "User")
			},
			verifyNoPersist: func(t *testing.T) {
				count, err := db.Count(context.Background(), &User{})
				require.NoError(t, err)
				require.Positive(t, count)
			},
		},
		{
			name: "DeleteStruct",
			op: func(ctx context.Context) error {
				return db.DeleteStruct(ctx, &User{ID: 1, Name: "seed"})
			},
			verifyNoPersist: func(t *testing.T) {
				count, err := db.Count(context.Background(), &User{})
				require.NoError(t, err)
				require.Positive(t, count)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op(canceledCtx())
			require.Error(t, err)
			require.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
			if tc.verifyNoPersist != nil {
				tc.verifyNoPersist(t)
			}
		})
	}

	// Also verify a timed-out context returns context.DeadlineExceeded.
	err := db.Save(timedOutCtx(), &User{ID: 7, Name: "x"})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}

// ============================================================================
// 16.3 Finder and Query
// ============================================================================

func TestFinder_CancelledContextPreventsReads(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &IndexedNameUser{ID: 1, Name: "John"}))

	t.Run("One", func(t *testing.T) {
		var u IndexedNameUser
		err := db.One(canceledCtx(), "Name", "John", &u)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Find", func(t *testing.T) {
		var us []IndexedNameUser
		err := db.Find(canceledCtx(), "Name", "John", &us)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Copy", func(t *testing.T) {
		var us []IndexedNameUser
		err := db.All(canceledCtx(), &us)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Count", func(t *testing.T) {
		_, err := db.Count(canceledCtx(), &IndexedNameUser{})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})
}

func TestQuery_CancelledContextPreventsTerminals(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &Score{Value: 1}))

	t.Run("Find", func(t *testing.T) {
		var scores []Score
		err := db.Select().Find(canceledCtx(), &scores)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("First", func(t *testing.T) {
		var s Score
		err := db.Select().First(canceledCtx(), &s)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Delete", func(t *testing.T) {
		err := db.Select().Delete(canceledCtx(), &Score{})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Count", func(t *testing.T) {
		_, err := db.Select().Count(canceledCtx(), &Score{})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Raw", func(t *testing.T) {
		_, err := db.Select().Bucket("Score").Raw(canceledCtx(), &Score{})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("RawEach", func(t *testing.T) {
		called := false
		err := db.Select().Bucket("Score").RawEach(canceledCtx(), &Score{}, func(k, v []byte) error {
			called = true
			return nil
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
		require.False(t, called, "callback must not run under a canceled context")
	})

	t.Run("Each", func(t *testing.T) {
		called := false
		err := db.Select().Each(canceledCtx(), new(Score), func(_ interface{}) error {
			called = true
			return nil
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
		require.False(t, called, "callback must not run under a canceled context")
	})
}

// ============================================================================
// 16.4 KV
// ============================================================================

func TestKV_CancelledContextPreventsWork(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Set(ctx, "trash", "k", []byte("v")))

	t.Run("Get", func(t *testing.T) {
		var b []byte
		err := db.Get(canceledCtx(), "trash", "k", &b)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Set", func(t *testing.T) {
		err := db.Set(canceledCtx(), "trash", "newkey", "newvalue")
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))

		var s string
		gErr := db.Get(ctx, "trash", "newkey", &s)
		require.Error(t, gErr)
		require.ErrorIs(t, gErr, ErrNotFound, "canceled Set must not persist")
	})

	t.Run("Delete", func(t *testing.T) {
		err := db.Delete(canceledCtx(), "trash", "k")
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("GetBytes", func(t *testing.T) {
		_, err := db.GetBytes(canceledCtx(), "trash", "k")
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("SetBytes", func(t *testing.T) {
		err := db.SetBytes(canceledCtx(), "trash", "newk", []byte("newv"))
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))

		_, gErr := db.GetBytes(ctx, "trash", "newk")
		require.ErrorIs(t, gErr, ErrNotFound, "canceled SetBytes must not persist")
	})

	t.Run("KeyExists", func(t *testing.T) {
		_, err := db.KeyExists(canceledCtx(), "trash", "k")
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})
}

// ============================================================================
// 16.5 Scanner
// ============================================================================

func TestScanner_CancelledContextReturnsErrorAndNoResults(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	node := db.From("node")
	require.NoError(t, node.Save(ctx, &SimpleUser{ID: 1, Name: "John"}))

	t.Run("PrefixScan", func(t *testing.T) {
		nodes, err := node.PrefixScan(canceledCtx(), "2")
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
		require.Empty(t, nodes)
	})

	t.Run("RangeScan", func(t *testing.T) {
		nodes, err := node.RangeScan(canceledCtx(), "1", "9")
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
		require.Empty(t, nodes)
	})
}

// ============================================================================
// 16.6 Manual transactions
// ============================================================================

// Manual transactions (Begin/Commit/Rollback) are not part of the API.
// Copy transactional behavior is exercised via managed transactions
// (ReadTransaction / WriteTransaction). See managed_transaction_test.go
// and managed_transaction_lifecycle_test.go.

// ---------------------------------------------------------------------------
// cancellationCodec wraps a delegate codec and can cancel a context during
// Marshal or Unmarshal. Used only in tests.
// ---------------------------------------------------------------------------

type cancellationCodec struct {
	delegate    Codec
	mu          sync.Mutex
	onMarshal   func()
	onUnmarshal func()
}

func (c *cancellationCodec) Name() string { return c.delegate.Name() }

func (c *cancellationCodec) Marshal(v interface{}) ([]byte, error) {
	raw, err := c.delegate.Marshal(v)
	c.mu.Lock()
	fn := c.onMarshal
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
	return raw, err
}

func (c *cancellationCodec) Unmarshal(raw []byte, to interface{}) error {
	err := c.delegate.Unmarshal(raw, to)
	c.mu.Lock()
	fn := c.onUnmarshal
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
	return err
}

func (c *cancellationCodec) setOnMarshal(fn func()) {
	c.mu.Lock()
	c.onMarshal = fn
	c.mu.Unlock()
}

func (c *cancellationCodec) setOnUnmarshal(fn func()) {
	c.mu.Lock()
	c.onUnmarshal = fn
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 17.1 Save cancelled during marshal
// ---------------------------------------------------------------------------

func TestSave_IndexCancellationRollsBackTransaction(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnMarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)

	// Save updates all configured indexes before marshaling the final record.
	// Canceling in Marshal therefore proves that the enclosing transaction rolls
	// back both the already-applied index changes and the record write.
	err := n.Save(ctx, &UniqueNameUser{ID: 1, Name: "unique-marshal-cancel", Age: 10})
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)

	// Record must not exist.
	var u UniqueNameUser
	err = db.One(context.Background(), "ID", 1, &u)
	require.True(t, errors.Is(err, ErrNotFound))

	// Unique index must have no orphan.
	err = db.One(context.Background(), "Name", "unique-marshal-cancel", &u)
	require.True(t, errors.Is(err, ErrNotFound))

	// A new Save with the same unique value must succeed.
	err = db.Save(context.Background(), &UniqueNameUser{ID: 2, Name: "unique-marshal-cancel", Age: 20})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// 17.2 Update cancelled during marshal
// ---------------------------------------------------------------------------

func TestUpdate_CancellationDuringMarshalPreservesRecordAndIndexes(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Seed old value.
	err := db.Save(ctx, &UniqueNameUser{ID: 1, Name: "old-name", Age: 10})
	require.NoError(t, err)

	// Update with cancellation during marshal.
	updateCtx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnMarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)

	updated := UniqueNameUser{ID: 1, Name: "new-name", Age: 20}
	err = n.Update(updateCtx, &updated)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)

	// Old record must remain intact.
	var u UniqueNameUser
	err = db.One(ctx, "ID", 1, &u)
	require.NoError(t, err)
	require.Equal(t, "old-name", u.Name)
	require.Equal(t, 10, u.Age)

	// Old unique index must remain.
	err = db.One(ctx, "Name", "old-name", &u)
	require.NoError(t, err)

	// New unique index must not exist.
	err = db.One(ctx, "Name", "new-name", &u)
	require.True(t, errors.Is(err, ErrNotFound))

	// A new record can use the new unique value.
	err = db.Save(ctx, &UniqueNameUser{ID: 2, Name: "new-name", Age: 30})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// 17.3 UpdateField cancelled during marshal
// ---------------------------------------------------------------------------

func TestUpdateField_CancellationDuringMarshalPreservesStoredValue(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	err := db.Save(ctx, &UniqueNameUser{ID: 1, Name: "original", Age: 10})
	require.NoError(t, err)

	updateCtx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnMarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)

	err = n.UpdateField(updateCtx, &UniqueNameUser{ID: 1}, "Name", "changed")
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)

	// Original value must remain.
	var u UniqueNameUser
	err = db.One(ctx, "ID", 1, &u)
	require.NoError(t, err)
	require.Equal(t, "original", u.Name)
}

// ---------------------------------------------------------------------------
// 18.1 Set cancelled during marshal
// ---------------------------------------------------------------------------

func TestSet_CancellationDuringMarshalDoesNotPersist(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnMarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)

	err := n.Set(ctx, "test-bucket", "key1", "some-value")
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)

	// Nothing persisted.
	var s string
	err = db.Get(context.Background(), "test-bucket", "key1", &s)
	require.True(t, errors.Is(err, ErrNotFound))
}

// ---------------------------------------------------------------------------
// 18.2 Get cancelled during unmarshal preserves destination
// ---------------------------------------------------------------------------

func TestGet_CancellationDuringUnmarshalPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Seed data with a normal codec.
	err := db.Set(ctx, "bucket", "key", "preserved-value")
	require.NoError(t, err)

	// Now read with a codec that cancels during unmarshal.
	getCtx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)

	// Initialize destination with sentinel value.
	dest := "SENTINEL"
	err = n.Get(getCtx, "bucket", "key", &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)

	// Destination must be unchanged.
	require.Equal(t, "SENTINEL", dest,
		"destination must not be mutated on cancellation")
}

// ---------------------------------------------------------------------------
// 18.3 Get codec error preserves destination
// ---------------------------------------------------------------------------

func TestGet_CodecErrorPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Seed raw bytes that cause json unmarshal to fail.
	err := db.SetBytes(ctx, "bucket", "key", []byte("not-valid-json{{{}}"))
	require.NoError(t, err)

	dest := "SENTINEL"
	err = db.Get(ctx, "bucket", "key", &dest)
	require.Error(t, err)

	// Destination must be unchanged.
	require.Equal(t, "SENTINEL", dest,
		"destination must not be mutated on codec error")
}

// ---------------------------------------------------------------------------
// 18.4 Get temporary decode supports destination shapes
// ---------------------------------------------------------------------------

func TestGet_TemporaryDecodeSupportsDestinationShapes(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Struct.
	err := db.Save(ctx, &SimpleUser{ID: 1, Name: "struct-test"})
	require.NoError(t, err)

	var su SimpleUser
	err = db.Get(ctx, "SimpleUser", 1, &su)
	require.NoError(t, err)
	require.Equal(t, "struct-test", su.Name)

	// Scalar.
	err = db.Set(ctx, "scalars", "age", 42)
	require.NoError(t, err)

	var age int
	err = db.Get(ctx, "scalars", "age", &age)
	require.NoError(t, err)
	require.Equal(t, 42, age)

	// Slice.
	err = db.Set(ctx, "slices", "ids", []int{1, 2, 3})
	require.NoError(t, err)

	var ids []int
	err = db.Get(ctx, "slices", "ids", &ids)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, ids)
}

// ---------------------------------------------------------------------------
// 18.6 GetBytes error does not return data
// ---------------------------------------------------------------------------

func TestGetBytes_ErrorDoesNotReturnData(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// A missing bucket returns ErrNotFound and no partial value.
	value, err := db.GetBytes(context.Background(), "nonexistent", "key")
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, value, "GetBytes must return nil on error")
}

// ---------------------------------------------------------------------------
// 18.7 KeyExists
// ---------------------------------------------------------------------------

func TestKeyExists_Behavior(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	err := db.Set(ctx, "bucket", "existing", "value")
	require.NoError(t, err)

	// Existent key.
	ok, err := db.KeyExists(ctx, "bucket", "existing")
	require.NoError(t, err)
	require.True(t, ok)

	// Absent key in existing bucket.
	ok, err = db.KeyExists(ctx, "bucket", "missing")
	require.NoError(t, err)
	require.False(t, ok)

	// A missing bucket simply means the key does not exist.
	ok, err = db.KeyExists(ctx, "missing-bucket", "key")
	require.NoError(t, err)
	require.False(t, ok)

	// Cancelled context returns false, error.
	ok, err = db.KeyExists(canceledCtx(), "bucket", "existing")
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.False(t, ok, "KeyExists must return false on cancellation")
}

// ---------------------------------------------------------------------------
// readWriteTx pre-commit cancellation
// ---------------------------------------------------------------------------

func TestWriteOperation_RollbackOnPreCommitCancellation(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	n := db.Node.(*node)

	err := n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("precommit"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}

		// The callback succeeds after writing, but cancellation must be observed
		// by readWriteTx before bbolt is allowed to commit.
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)

	err = db.bolt.View(func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte("precommit")), "canceled transaction must roll back bucket creation")
		return nil
	})
	require.NoError(t, err)
}
