package harbor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/store/search"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
)

type txRecord struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

// txSeed writes one record, one file, one point and one vector through the live
// stores, so a rollback test has state to preserve.
func txSeed(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.Record().Save(ctx, &txRecord{ID: "keep", Name: "seed"}))

	fsys, err := s.File().Open("txfs")
	require.NoError(t, err)
	f, err := fsys.Create("/keep.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("seed"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, s.Series().Series("cpu").Insert(ctx, series.Point{Timestamp: 1_000, Value: 1}))
}

// TestWriteTransactionCommitsAcrossStores proves that one callback writes to all
// four sub-stores and that every write is visible after the commit.
func TestWriteTransactionCommitsAcrossStores(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)

	require.NoError(t, s.WriteTransaction(ctx, func(tx *Tx) error {
		if err := tx.Record().Save(ctx, &txRecord{ID: "r1", Name: "one"}); err != nil {
			return err
		}

		fsys, err := tx.File().Open("txfs")
		if err != nil {
			return err
		}
		f, err := fsys.Create("/a.txt")
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("payload")); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}

		if err := tx.Series().Series("requests").Insert(ctx, series.Point{Timestamp: 2_000, Value: 42}); err != nil {
			return err
		}

		col, err := tx.Vector().NewCollection("docs", 2, vector.Cosine, false)
		if err != nil {
			return err
		}
		return col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}})
	}))

	var rec txRecord
	require.NoError(t, s.Record().One(ctx, "ID", "r1", &rec))
	require.Equal(t, "one", rec.Name)

	fsys, err := s.File().Open("txfs")
	require.NoError(t, err)
	body, err := fsys.ReadFile("/a.txt")
	require.NoError(t, err)
	require.Equal(t, "payload", string(body))

	points, err := s.Series().Series("requests").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Equal(t, 42.0, points[0].Value)

	col, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	got, err := col.Get("p1")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0}, got.Vector)

	// The adopted live index answers searches without reloading from disk.
	col, err = s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	hits, err := col.Search([]float32{1, 0}, nil, 1)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "p1", hits[0].ID)
}

// TestWriteTransactionAbortLeavesNoTrace proves a returned error rolls back
// every store, including the vector index.
func TestWriteTransactionAbortLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)
	txSeed(t, s)

	wantErr := errors.New("abort")
	err := s.WriteTransaction(ctx, func(tx *Tx) error {
		if err := tx.Record().Save(ctx, &txRecord{ID: "gone", Name: "no"}); err != nil {
			return err
		}
		if err := tx.Record().Save(ctx, &txRecord{ID: "keep", Name: "changed"}); err != nil {
			return err
		}

		fsys, err := tx.File().Open("txfs")
		if err != nil {
			return err
		}
		f, err := fsys.Create("/new.txt")
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("new")); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}

		if err := tx.Series().Series("cpu").Insert(ctx, series.Point{Timestamp: 2_000, Value: 2}); err != nil {
			return err
		}

		col, err := tx.Vector().NewCollection("docs", 2, vector.Cosine, false)
		if err != nil {
			return err
		}
		if err := col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}}); err != nil {
			return err
		}
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)

	var rec txRecord
	require.Error(t, s.Record().One(ctx, "ID", "gone", &rec))
	require.NoError(t, s.Record().One(ctx, "ID", "keep", &rec))
	require.Equal(t, "seed", rec.Name, "an aborted update must not change the record")

	fsys, err := s.File().Open("txfs")
	require.NoError(t, err)
	if _, err := fsys.Stat("/new.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(/new.txt) err = %v, want not exist", err)
	}

	points, err := s.Series().Series("cpu").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Len(t, points, 1, "aborted ts insert")
	require.Equal(t, 1.0, points[0].Value)

	col, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	if _, err := col.Get("p1"); err == nil {
		t.Fatal("aborted vector upsert is visible")
	}
	hits, err := col.Search([]float32{1, 0}, nil, 10)
	require.NoError(t, err)
	require.Empty(t, hits)
}

// TestWriteTransactionCancellationRollsBack proves a canceled context discards a
// callback that returned nil.
func TestWriteTransactionCancellationRollsBack(t *testing.T) {
	base := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)

	ctx, cancel := context.WithCancel(base)
	err := s.WriteTransaction(ctx, func(tx *Tx) error {
		if err := tx.Record().Save(base, &txRecord{ID: "c1", Name: "cancel"}); err != nil {
			return err
		}
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)

	var rec txRecord
	require.Error(t, s.Record().One(base, "ID", "c1", &rec))
}

// TestWriteTransactionRejectsNilCallback guards the programming error.
func TestWriteTransactionRejectsNilCallback(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)
	require.ErrorIs(t, s.WriteTransaction(context.Background(), nil), ErrInvalidOptions)
	require.ErrorIs(t, s.ReadTransaction(context.Background(), nil), ErrInvalidOptions)
}

// TestReadTransactionIsConsistentAndReadOnly proves reads see the seeded state
// and that a write through a read transaction fails.
func TestReadTransactionIsConsistentAndReadOnly(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)
	txSeed(t, s)

	require.NoError(t, s.ReadTransaction(ctx, func(tx *Tx) error {
		if tx.Writable() {
			t.Fatal("Writable() = true for a read transaction")
		}
		var rec txRecord
		if err := tx.Record().One(ctx, "ID", "keep", &rec); err != nil {
			return err
		}
		if rec.Name != "seed" {
			t.Fatalf("read %q, want seed", rec.Name)
		}
		points, err := tx.Series().Series("cpu").Range(ctx, 0, 10_000)
		if err != nil {
			return err
		}
		if len(points) != 1 {
			t.Fatalf("read %d points, want 1", len(points))
		}
		// Writes are rejected by the underlying read-only transaction.
		if err := tx.Record().Save(ctx, &txRecord{ID: "nope"}); err == nil {
			t.Fatal("a write through a read transaction succeeded")
		}
		return nil
	}))
}

// TestWriteTransactionSerializesConcurrentWriters proves the store accepts
// concurrent transactions one at a time and keeps every commit.
func TestWriteTransactionSerializesConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)

	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- s.WriteTransaction(ctx, func(tx *Tx) error {
				return tx.Record().Save(ctx, &txRecord{ID: string(rune('a' + i))})
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var all []txRecord
	require.NoError(t, s.Record().All(ctx, &all))
	require.Len(t, all, writers)
}

// TestWriteTransactionAuditsAfterCommit proves audit events are collected
// inside the transaction and appended once it commits, with no deadlock
// against the write transaction.
func TestWriteTransactionAuditsAfterCommit(t *testing.T) {
	ctx := context.Background()
	opts := testOptions("tx-password")
	opts.Audit = AuditAll
	s, err := Open(ctx, filepath.Join(t.TempDir(), "tx.db"), 0o600, opts)
	require.NoError(t, err)
	defer mustClose(t, s)

	require.NoError(t, s.WriteTransaction(ctx, func(tx *Tx) error {
		if err := tx.Record().Save(ctx, &txRecord{ID: "audited"}); err != nil {
			return err
		}
		fsys, err := tx.File().Open("txfs")
		if err != nil {
			return err
		}
		f, err := fsys.Create("/a.txt")
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("a")); err != nil {
			return err
		}
		return f.Close()
	}))

	res, err := s.VerifyAudit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, res.Count, uint64(2), "db and fs writes must be audited")

	// The trail is append-only and valid after the commit.
	count, tip, err := s.Audit().Head()
	require.NoError(t, err)
	require.Equal(t, res.Count, count)
	require.NotEmpty(t, tip)
}

// TestWriteTransactionReadsOwnWrites proves every bound view sees the callback's
// uncommitted writes, including a vector search over a staged point.
func TestWriteTransactionReadsOwnWrites(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)

	require.NoError(t, s.WriteTransaction(ctx, func(tx *Tx) error {
		if err := tx.Record().Save(ctx, &txRecord{ID: "inner", Name: "visible"}); err != nil {
			return err
		}
		var rec txRecord
		if err := tx.Record().One(ctx, "ID", "inner", &rec); err != nil {
			return err
		}
		if rec.Name != "visible" {
			t.Fatalf("inside transaction read %q, want visible", rec.Name)
		}

		// The fs write is visible to a second handle opened on the same
		// transaction, before the commit.
		fsys, err := tx.File().Open("txfs")
		if err != nil {
			return err
		}
		f, err := fsys.Create("/inner.txt")
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("uncommitted")); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		body, err := fsys.ReadFile("/inner.txt")
		if err != nil {
			return err
		}
		if string(body) != "uncommitted" {
			t.Fatalf("inside transaction read %q", body)
		}

		col, err := tx.Vector().NewCollection("docs", 2, vector.Cosine, false)
		if err != nil {
			return err
		}
		if err := col.Upsert([]vector.PointStruct{{ID: "staged", Vector: []float32{0, 1}}}); err != nil {
			return err
		}
		hits, err := col.Search([]float32{0, 1}, nil, 1)
		if err != nil {
			return err
		}
		if len(hits) != 1 || hits[0].ID != "staged" {
			t.Fatalf("search inside transaction = %+v, want the staged point", hits)
		}
		return nil
	}))

	// Outside the transaction the index has adopted the staged point without
	// re-reading the database.
	col, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	hits, err := col.Search([]float32{0, 1}, nil, 1)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "staged", hits[0].ID)
}

// TestReadTransactionVecSnapshot proves a read transaction observes committed
// points through its bound view.
func TestReadTransactionVecSnapshot(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)

	col, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}}))
	require.NoError(t, col.Flush())

	require.NoError(t, s.ReadTransaction(ctx, func(tx *Tx) error {
		read, err := tx.Vector().NewCollection("docs", 2, vector.Cosine, false)
		if err != nil {
			return err
		}
		hits, err := read.Search([]float32{1, 0}, nil, 1)
		if err != nil {
			return err
		}
		if len(hits) != 1 || hits[0].ID != "p1" {
			t.Fatalf("read transaction search = %+v", hits)
		}
		return nil
	}))
}

// TestWriteTransactionFTS proves the full-text index participates in a
// cross-store transaction: a staged upsert is searchable inside the callback,
// visible after commit, and absent after an abort.
func TestWriteTransactionFTS(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tx.db"), "tx-password")
	defer mustClose(t, s)

	require.NoError(t, s.WriteTransaction(ctx, func(tx *Tx) error {
		idx, err := tx.Search().Open("docs", search.Config{})
		if err != nil {
			return err
		}
		if err := idx.Upsert(ctx, "d1", "harbor full text"); err != nil {
			return err
		}
		hits, err := idx.Search(ctx, "harbor", 10)
		if err != nil {
			return err
		}
		if len(hits) != 1 || hits[0].ID != "d1" {
			t.Fatalf("staged search = %+v", hits)
		}
		return nil
	}))

	idx, err := s.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	hits, err := idx.Search(ctx, "harbor", 10)
	require.NoError(t, err)
	require.Len(t, hits, 1)

	// An aborted upsert is not indexed.
	err = s.WriteTransaction(ctx, func(tx *Tx) error {
		idx, err := tx.Search().Open("docs", search.Config{})
		if err != nil {
			return err
		}
		if err := idx.Upsert(ctx, "d2", "aborted"); err != nil {
			return err
		}
		return errors.New("abort")
	})
	require.Error(t, err)

	idx, err = s.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	count, err := idx.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	hits, err = idx.Search(ctx, "aborted", 10)
	require.NoError(t, err)
	require.Empty(t, hits)
}
