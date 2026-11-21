package db

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/malivvan/storm/db/index"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// ============================================================================
// Operation wrapping tests
// ============================================================================

// ---------------------------------------------------------------------------
// 1. wrapError helper
// ---------------------------------------------------------------------------

func TestWrapError_NilReturnsNil(t *testing.T) {
	require.Nil(t, wrapError("save", nil))
}

func TestWrapError_SentinelMatches(t *testing.T) {
	err := wrapError("save", ErrNotFound)
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, err, index.ErrNotFound)
}

func TestWrapError_ContextErrorMatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wrapError("save", ctx.Err())
	require.ErrorIs(t, err, context.Canceled)
}

func TestWrapError_ExactPrefix(t *testing.T) {
	err := wrapError("save", ErrNotFound)
	require.True(t, strings.HasPrefix(err.Error(), "storm save:"),
		"message must start with 'storm save:'")
}

func TestWrapError_NoFormattingArtifacts(t *testing.T) {
	err := wrapError("init", errors.New("test error"))
	msg := err.Error()
	require.NotContains(t, msg, "%w")
	require.NotContains(t, msg, "%s")
	require.NotContains(t, msg, "%v")
	require.NotContains(t, msg, "MISSING")
}

// ---------------------------------------------------------------------------
// 2. Open
// ---------------------------------------------------------------------------

func TestWrap_OpenNilContext(t *testing.T) {
	var nilCtx context.Context
	db, err := Open(nilCtx, nil, nil, nil)
	require.ErrorIs(t, err, ErrNilContext)
	require.True(t, strings.HasPrefix(err.Error(), "storm open:"),
		"error must have open prefix")
	require.Nil(t, db)
}

func TestWrap_OpenNilDB(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, nil, nil, nil)
	require.ErrorIs(t, err, ErrNilParam)
	require.True(t, strings.HasPrefix(err.Error(), "storm open:"))
	require.Nil(t, db)
}

func TestWrap_OpenCanceledContext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "canceled.db")

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := Open(ctx, bDB, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, strings.HasPrefix(err.Error(), "storm open:"))
	require.Nil(t, db)

	// The database remains usable by its owner.
	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err)
}

func TestWrap_OpenBorrowedInitFailure(t *testing.T) {
	dir := t.TempDir()
	bDBPath := filepath.Join(dir, "borrowed.db")

	bDB, err := bolt.Open(bDBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	err = bDB.Update(func(tx *bolt.Tx) error {
		top, cerr := tx.CreateBucketIfNotExists([]byte(databaseInfo))
		if cerr != nil {
			return cerr
		}
		_, cerr = top.CreateBucket([]byte(metadataBucket))
		if cerr != nil {
			return cerr
		}
		mb := top.Bucket([]byte(metadataBucket))
		return mb.Put([]byte(metaCodec), []byte("not-json"))
	})
	require.NoError(t, err)

	db, err := Open(context.Background(), bDB, nil, nil)
	require.ErrorIs(t, err, ErrDifferentCodec)
	require.True(t, strings.HasPrefix(err.Error(), "storm open:"))
	require.Nil(t, db)

	// Borrowed Store must still be open.
	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "borrowed Store must remain open after init failure")
	require.NoError(t, bDB.Close())
}

// ---------------------------------------------------------------------------
// 3. Close
// ---------------------------------------------------------------------------

func TestWrap_CloseNilReceiver(t *testing.T) {
	var nilDB *Store
	err := nilDB.Close()
	require.ErrorIs(t, err, ErrNilParam)
	require.True(t, strings.HasPrefix(err.Error(), "storm close:"))
}

func TestWrap_CloseNilBackend(t *testing.T) {
	db := &Store{}
	err := db.Close()
	require.ErrorIs(t, err, ErrNilParam)
	require.True(t, strings.HasPrefix(err.Error(), "storm close:"))
}

func TestWrap_CloseDoesNotCloseBackend(t *testing.T) {
	dir := t.TempDir()
	bDB, err := bolt.Open(filepath.Join(dir, "b.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	db, err := Open(context.Background(), bDB, nil, nil)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err, "Close must return nil")

	err = bDB.View(func(tx *bolt.Tx) error { return nil })
	require.NoError(t, err, "Close must not close the caller-owned Store")
	require.NoError(t, bDB.Close())
}

func TestWrap_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	bDB, err := bolt.Open(filepath.Join(dir, "borrowed.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	db, err := Open(context.Background(), bDB, nil, nil)
	require.NoError(t, err)

	require.NoError(t, db.Close())
	require.NoError(t, db.Close())
}

// ---------------------------------------------------------------------------
// 4. Managed transactions
// ---------------------------------------------------------------------------

func TestWrap_ReadTransactionNilContext(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	var nilCtx context.Context
	err := db.ReadTransaction(nilCtx, func(Node) error { return nil })
	require.ErrorIs(t, err, ErrNilContext)
	require.True(t, strings.HasPrefix(err.Error(), "storm read transaction:"))
}

func TestWrap_ReadTransactionNilCallback(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	err := db.ReadTransaction(context.Background(), nil)
	require.ErrorIs(t, err, ErrNilParam)
	require.True(t, strings.HasPrefix(err.Error(), "storm read transaction:"))
}

func TestWrap_WriteTransactionNilContext(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	var nilCtx context.Context
	err := db.WriteTransaction(nilCtx, func(Node) error { return nil })
	require.ErrorIs(t, err, ErrNilContext)
	require.True(t, strings.HasPrefix(err.Error(), "storm write transaction:"))
}

func TestWrap_WriteTransactionNilCallback(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	err := db.WriteTransaction(context.Background(), nil)
	require.ErrorIs(t, err, ErrNilParam)
	require.True(t, strings.HasPrefix(err.Error(), "storm write transaction:"))
}

func TestWrap_WriteTransactionCallbackErrorDiscoverable(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	errCallback := errors.New("custom callback failure")
	err := db.WriteTransaction(context.Background(), func(Node) error {
		return errCallback
	})
	require.ErrorIs(t, err, errCallback)
	require.True(t, strings.HasPrefix(err.Error(), "storm write transaction:"))
}

func TestWrap_WriteTransactionCallbackErrorBeatsCancel(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	errCallback := errors.New("my error")

	err := db.WriteTransaction(ctx, func(Node) error {
		cancel()
		return errCallback
	})
	require.ErrorIs(t, err, errCallback)
	require.NotErrorIs(t, err, context.Canceled, "callback error must take precedence")
	require.True(t, strings.HasPrefix(err.Error(), "storm write transaction:"))
}

func TestWrap_WriteTransactionPreCommitCancel(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	err := db.WriteTransaction(ctx, func(txNode Node) error {
		if e := txNode.Save(ctx, &User{ID: 1, Name: "precommit", Slug: "pc"}); e != nil {
			return e
		}
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, strings.HasPrefix(err.Error(), "storm write transaction:"))

	var user User
	err = db.One(context.Background(), "ID", 1, &user)
	require.ErrorIs(t, err, ErrNotFound, "pre-commit cancellation must roll back the write")
}

func TestWrap_WriteTransactionPanicPreserved(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	panicValue := &struct{ Name string }{Name: "wrap-boom"}

	recovered := capturePanic(func() {
		_ = db.WriteTransaction(context.Background(), func(Node) error {
			panic(panicValue)
		})
	})
	require.Same(t, panicValue, recovered, "panic value identity must be preserved")
}

func TestWrap_WriteTransactionSuccessfulCommitNotWrapped(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	err := db.WriteTransaction(ctx, func(txNode Node) error {
		return txNode.Save(ctx, &User{ID: 99, Name: "commit-ok", Slug: "ok"})
	})
	require.NoError(t, err, "successful commit must not be wrapped")

	var user User
	require.NoError(t, db.One(ctx, "ID", 99, &user))
	require.Equal(t, "commit-ok", user.Name)
}

// ---------------------------------------------------------------------------
// 5. CRUD operations
// ---------------------------------------------------------------------------

func TestWrap_CRUD_OperationPrefixes(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Ensure a record exists for update/delete tests.
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "base", Slug: "base"}))

	tests := []struct {
		name   string
		prefix string
		op     func() error
	}{
		{
			name:   "init",
			prefix: "storm init:",
			op:     func() error { return db.Init(ctx, 10) },
		},
		{
			name:   "reindex",
			prefix: "storm reindex:",
			op:     func() error { return db.ReIndex(ctx, 10) },
		},
		{
			name:   "save",
			prefix: "storm save:",
			op:     func() error { return db.Save(ctx, nil) },
		},
		{
			name:   "update",
			prefix: "storm update:",
			op:     func() error { return db.Update(ctx, nil) },
		},
		{
			name:   "update field",
			prefix: "storm update field:",
			op:     func() error { return db.UpdateField(ctx, nil, "", nil) },
		},
		{
			name:   "drop",
			prefix: "storm drop:",
			op:     func() error { return db.Drop(ctx, "nonexistent-bucket-xyz") },
		},
		{
			name:   "delete struct",
			prefix: "storm delete struct:",
			op:     func() error { return db.DeleteStruct(ctx, nil) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op()
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tc.prefix),
				"expected prefix %q, got error %q", tc.prefix, err.Error())
		})
	}
}

func TestWrap_CRUD_Classification(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	// Seed for Update/UpdateField/DeleteStruct tests.
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "base", Slug: "base"}))
	require.NoError(t, db.Save(ctx, &UniqueNameUser{ID: 1, Name: "unique", Age: 10}))

	t.Run("InitBadType", func(t *testing.T) {
		err := db.Init(ctx, 10)
		require.ErrorIs(t, err, ErrBadType)
		require.True(t, strings.HasPrefix(err.Error(), "storm init:"))
	})

	t.Run("ReIndexBadType", func(t *testing.T) {
		err := db.ReIndex(ctx, 10)
		require.ErrorIs(t, err, ErrStructPtrNeeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm reindex:"))
	})

	t.Run("SaveNilData", func(t *testing.T) {
		err := db.Save(ctx, nil)
		require.ErrorIs(t, err, ErrStructPtrNeeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm save:"))
	})

	t.Run("SaveDuplicateUnique", func(t *testing.T) {
		err := db.Save(ctx, &UniqueNameUser{ID: 2, Name: "unique", Age: 20})
		require.ErrorIs(t, err, ErrAlreadyExists)
		require.True(t, strings.HasPrefix(err.Error(), "storm save:"))
	})

	t.Run("UpdateNilData", func(t *testing.T) {
		err := db.Update(ctx, nil)
		require.ErrorIs(t, err, ErrStructPtrNeeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm update:"))
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		err := db.Update(ctx, &User{ID: 999, Name: "nope"})
		require.ErrorIs(t, err, ErrNotFound)
		require.True(t, strings.HasPrefix(err.Error(), "storm update:"))
	})

	t.Run("UpdateFieldNilData", func(t *testing.T) {
		err := db.UpdateField(ctx, nil, "", nil)
		require.ErrorIs(t, err, ErrStructPtrNeeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm update field:"))
	})

	t.Run("UpdateFieldIncompatibleValue", func(t *testing.T) {
		err := db.UpdateField(ctx, &User{ID: 1}, "Name", 42)
		require.ErrorIs(t, err, ErrIncompatibleValue)
		require.True(t, strings.HasPrefix(err.Error(), "storm update field:"))
	})

	t.Run("DropNonexistentBucket", func(t *testing.T) {
		// Drop a bucket that doesn't exist -> bbolt BucketNotFound.
		err := db.Drop(ctx, "nonexistent-bucket-xyz")
		require.True(t, strings.HasPrefix(err.Error(), "storm drop:"))
	})

	t.Run("DeleteStructNilData", func(t *testing.T) {
		err := db.DeleteStruct(ctx, nil)
		require.ErrorIs(t, err, ErrStructPtrNeeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm delete struct:"))
	})

	t.Run("DeleteStructNotFound", func(t *testing.T) {
		err := db.DeleteStruct(ctx, &User{ID: 999})
		require.ErrorIs(t, err, ErrNotFound)
		require.True(t, strings.HasPrefix(err.Error(), "storm delete struct:"))
	})
}

func TestWrap_CRUD_ContextErrors(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "ctx", Slug: "ctx"}))

	t.Run("SaveCanceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := db.Save(ctx, &User{ID: 2, Name: "x", Slug: "x"})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, strings.HasPrefix(err.Error(), "storm save:"))
	})

	t.Run("UpdateDeadlineExceeded", func(t *testing.T) {
		err := db.Update(timedOutCtx(), &User{ID: 1, Name: "updated"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm update:"))
	})
}

// ---------------------------------------------------------------------------
// 6. KV operations
// ---------------------------------------------------------------------------

func TestWrap_KV_OperationPrefixes(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Set(ctx, "bucket", "key", "value"))

	tests := []struct {
		name   string
		prefix string
		op     func() error
	}{
		{
			name:   "kv get",
			prefix: "storm kv get:",
			op:     func() error { return db.Get(ctx, "bucket", "key", nil) },
		},
		{
			name:   "kv set",
			prefix: "storm kv set:",
			op:     func() error { return db.Set(ctx, "bucket", nil, 100) },
		},
		{
			name:   "kv delete",
			prefix: "storm kv delete:",
			op:     func() error { return db.Delete(ctx, "nonexistent", "key") },
		},
		{
			name:   "kv get bytes",
			prefix: "storm kv get bytes:",
			op:     func() error { _, err := db.GetBytes(ctx, "bucket", 999); return err },
		},
		{
			name:   "kv set bytes",
			prefix: "storm kv set bytes:",
			op:     func() error { return db.SetBytes(ctx, "bucket", nil, []byte("x")) },
		},
		{
			name:   "kv key exists",
			prefix: "storm kv key exists:",
			op:     func() error { _, err := db.KeyExists(ctx, "nonexistent", "key"); return err },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op()
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tc.prefix),
				"expected prefix %q, got error %q", tc.prefix, err.Error())
		})
	}
}

func TestWrap_KV_Classification(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Set(ctx, "bucket", "key", "value"))

	t.Run("GetNilDest", func(t *testing.T) {
		err := db.Get(ctx, "bucket", "key", nil)
		require.ErrorIs(t, err, ErrPtrNeeded)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv get:"))
	})

	t.Run("GetNotFound", func(t *testing.T) {
		var s string
		err := db.Get(ctx, "bucket", "missing", &s)
		require.ErrorIs(t, err, ErrNotFound)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv get:"))
	})

	t.Run("SetNilKey", func(t *testing.T) {
		err := db.Set(ctx, "bucket", nil, 100)
		require.ErrorIs(t, err, ErrNilParam)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv set:"))
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		err := db.Delete(ctx, "nonexistent-b", "key")
		require.ErrorIs(t, err, ErrNotFound)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv delete:"))
	})

	t.Run("GetBytesNotFound", func(t *testing.T) {
		_, err := db.GetBytes(ctx, "bucket", 999)
		require.ErrorIs(t, err, ErrNotFound)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv get bytes:"))
	})

	t.Run("SetBytesNilKey", func(t *testing.T) {
		err := db.SetBytes(ctx, "bucket", nil, []byte("x"))
		require.ErrorIs(t, err, ErrNilParam)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv set bytes:"))
	})

	t.Run("KeyExistsNotFound", func(t *testing.T) {
		_, err := db.KeyExists(ctx, "nonexistent-b", "key")
		require.ErrorIs(t, err, ErrNotFound)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv key exists:"))
	})
}

func TestWrap_KV_ZeroResultsOnError(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("GetBytes", func(t *testing.T) {
		val, err := db.GetBytes(ctx, "bucket", "key")
		require.Error(t, err)
		require.Nil(t, val, "GetBytes must return nil result on error")
	})

	t.Run("KeyExists", func(t *testing.T) {
		exists, err := db.KeyExists(ctx, "bucket", "key")
		require.Error(t, err)
		require.False(t, exists, "KeyExists must return false on error")
	})
}

func TestWrap_KV_ContextErrors(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Set(ctx, "bucket", "key", "value"))

	t.Run("GetCanceled", func(t *testing.T) {
		var s string
		err := db.Get(canceledCtx(), "bucket", "key", &s)
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv get:"))
	})

	t.Run("SetBytesCanceled", func(t *testing.T) {
		err := db.SetBytes(canceledCtx(), "bucket", "k", []byte("v"))
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, strings.HasPrefix(err.Error(), "storm kv set bytes:"))
	})
}

// ---------------------------------------------------------------------------
// 7. Sensitive data exclusion
// ---------------------------------------------------------------------------

func TestWrap_NoSensitiveDataInMessages(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Use distinctive secret-like values.
	const secretBucket = "s3cr3t-buck3t-abc123"
	const secretKey = "s3cr3t-k3y-xyz789"
	const secretField = "s3cr3t-f13ld-qwe456"
	const secretRecord = "top-secret-record-value"

	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "base", Slug: "base"}))

	t.Run("SaveFail_NoBucketLeak", func(t *testing.T) {
		err := db.From(secretBucket).Save(ctx, nil)
		require.Error(t, err)
		msg := err.Error()
		require.NotContains(t, msg, secretBucket)
		require.NotContains(t, msg, secretKey)
		require.NotContains(t, msg, secretField)
		require.NotContains(t, msg, secretRecord)
	})

	t.Run("GetFail_NoKeyLeak", func(t *testing.T) {
		var s string
		err := db.Get(ctx, secretBucket, secretKey, &s)
		require.ErrorIs(t, err, ErrNotFound)
		msg := err.Error()
		require.NotContains(t, msg, secretBucket)
		require.NotContains(t, msg, secretKey)
	})

	t.Run("KVSetFail_NoBucketLeak", func(t *testing.T) {
		err := db.Set(ctx, secretBucket, nil, secretRecord)
		require.Error(t, err)
		msg := err.Error()
		require.NotContains(t, msg, secretBucket)
		require.NotContains(t, msg, secretRecord)
	})

	t.Run("UpdateFieldFail_NoFieldLeak", func(t *testing.T) {
		err := db.UpdateField(ctx, &User{ID: 1}, secretField, "value")
		require.ErrorIs(t, err, ErrNotFound) // field doesn't exist
		msg := err.Error()
		require.NotContains(t, msg, secretField)
	})

	t.Run("InitFail_NoBucketLeak", func(t *testing.T) {
		err := db.From(secretBucket).Init(ctx, 10)
		require.Error(t, err)
		msg := err.Error()
		require.NotContains(t, msg, secretBucket)
	})
}

// ---------------------------------------------------------------------------
// 8. Nested wrapping is preserved (checkVersion calls Get which is wrapped)
// ---------------------------------------------------------------------------

func TestWrap_OpenNestedWrapping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested.db")

	// Pre-seed with conflicting codec to trigger a nested KV error during init.
	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	err = bDB.Update(func(tx *bolt.Tx) error {
		top, cerr := tx.CreateBucketIfNotExists([]byte(databaseInfo))
		if cerr != nil {
			return cerr
		}
		_, cerr = top.CreateBucket([]byte(metadataBucket))
		if cerr != nil {
			return cerr
		}
		mb := top.Bucket([]byte(metadataBucket))
		return mb.Put([]byte(metaCodec), []byte(altCodec.Name()))
	})
	require.NoError(t, err)

	// Open with default json codec — checkVersion calls Set which calls SetBytes,
	// triggering nested KV wrapping under the open prefix.
	_, err = Open(context.Background(), bDB, nil, nil)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "storm open:"),
		"outermost error must start with requested operation label")
	require.NoError(t, bDB.Close())
}

// ---------------------------------------------------------------------------
// 9. All operation labels covered
// ---------------------------------------------------------------------------

func TestWrap_AllOperationLabelsCovered(t *testing.T) {
	// Gather every call site to prove coverage.
	expectedLabels := []string{
		"open",
		"close",
		"read transaction",
		"write transaction",
		"init",
		"reindex",
		"save",
		"update",
		"update field",
		"drop",
		"delete struct",
		"kv get",
		"kv set",
		"kv delete",
		"kv get bytes",
		"kv set bytes",
		"kv key exists",
	}

	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	var nilCtx context.Context
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "a", Slug: "a"}))

	tests := []struct {
		label  string
		prefix string
		op     func() error
	}{
		{"open", "storm open:", func() error { _, err := Open(nilCtx, nil, nil, nil); return err }},
		{"close", "storm close:", func() error { return (*Store)(nil).Close() }},
		{"read transaction", "storm read transaction:", func() error { return db.ReadTransaction(nilCtx, nil) }},
		{"write transaction", "storm write transaction:", func() error { return db.WriteTransaction(nilCtx, nil) }},
		{"init", "storm init:", func() error { return db.Init(ctx, 10) }},
		{"reindex", "storm reindex:", func() error { return db.ReIndex(ctx, 10) }},
		{"save", "storm save:", func() error { return db.Save(ctx, nil) }},
		{"update", "storm update:", func() error { return db.Update(ctx, nil) }},
		{"update field", "storm update field:", func() error { return db.UpdateField(ctx, nil, "", nil) }},
		{"drop", "storm drop:", func() error { return db.Drop(ctx, "nonexistent") }},
		{"delete struct", "storm delete struct:", func() error { return db.DeleteStruct(ctx, nil) }},
		{"kv get", "storm kv get:", func() error { return db.Get(ctx, "b", "k", nil) }},
		{"kv set", "storm kv set:", func() error { return db.Set(ctx, "b", nil, 100) }},
		{"kv delete", "storm kv delete:", func() error { return db.Delete(ctx, "nx", "k") }},
		{"kv get bytes", "storm kv get bytes:", func() error { _, err := db.GetBytes(ctx, "b", 999); return err }},
		{"kv set bytes", "storm kv set bytes:", func() error { return db.SetBytes(ctx, "b", nil, []byte{}) }},
		{"kv key exists", "storm kv key exists:", func() error { _, err := db.KeyExists(ctx, "nx", "k"); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			err := tc.op()
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tc.prefix),
				"label %q: expected prefix %q, got %q", tc.label, tc.prefix, err.Error())
		})
	}

	require.Len(t, tests, len(expectedLabels), "all operation labels must be covered")
}

// ---------------------------------------------------------------------------
// 10. Existing error classification compatibility
// ---------------------------------------------------------------------------

func TestWrap_ErrorClassificationTestCompatibility(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &User{ID: 1, Name: "base", Slug: "base"}))
	require.NoError(t, db.Save(ctx, &UniqueNameUser{ID: 1, Name: "alice", Age: 30}))

	// Save duplicate unique value → ErrAlreadyExists
	err := db.Save(ctx, &UniqueNameUser{ID: 2, Name: "alice", Age: 25})
	require.ErrorIs(t, err, ErrAlreadyExists)

	// UpdateField with incompatible value → ErrIncompatibleValue
	err = db.UpdateField(ctx, &User{ID: 1}, "Name", 42)
	require.ErrorIs(t, err, ErrIncompatibleValue)

	// Canceled context → context.Canceled
	cCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = db.Save(cCtx, &User{ID: 1, Name: "x", Slug: "x"})
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrNilContext)

	// Expired context → context.DeadlineExceeded
	err = db.Save(timedOutCtx(), &User{ID: 1, Name: "x", Slug: "x"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrNilContext)
}

// ---------------------------------------------------------------------------
// 11. bbolt error classification
// ---------------------------------------------------------------------------

func TestWrap_BboltErrorClassification(t *testing.T) {
	dir := t.TempDir()
	bDB, err := bolt.Open(filepath.Join(dir, "bolt-err.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)

	db, err := Open(context.Background(), bDB, nil, nil)
	require.NoError(t, err)

	require.NoError(t, db.NativeDB().Close())

	ctx := context.Background()
	err = db.Save(ctx, &User{ID: 1, Name: "x", Slug: "x"})
	require.ErrorIs(t, err, bolterrors.ErrDatabaseNotOpen)
	require.True(t, strings.HasPrefix(err.Error(), "storm save:"))
}
