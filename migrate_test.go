package tempest

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type migratedRow struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

func TestMigrateForward(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "migrate.db"), "migrate-password")
	defer mustClose(t, s)

	var order []uint64
	schema := Schema{
		Version: 2,
		Steps: []Step{
			{From: 0, To: 1, Apply: func(ctx context.Context, tx *Tx) error {
				order = append(order, 1)
				return tx.Record().Save(ctx, &migratedRow{ID: "one", Name: "first"})
			}},
			{From: 1, To: 2, Apply: func(ctx context.Context, tx *Tx) error {
				order = append(order, 2)
				return tx.Record().Save(ctx, &migratedRow{ID: "two", Name: "second"})
			}},
		},
	}
	require.NoError(t, s.Migrate(ctx, schema))
	require.Equal(t, []uint64{1, 2}, order)

	version, err := s.SchemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	var row migratedRow
	require.NoError(t, s.Record().One(ctx, "ID", "one", &row))
	require.Equal(t, "first", row.Name)
	require.NoError(t, s.Record().One(ctx, "ID", "two", &row))
	require.Equal(t, "second", row.Name)

	// Re-running the same schema is a no-op.
	order = nil
	require.NoError(t, s.Migrate(ctx, schema))
	require.Empty(t, order)
}

func TestMigrateRefusesNewerStore(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "migrate.db"), "migrate-password")
	defer mustClose(t, s)

	require.NoError(t, s.Migrate(ctx, Schema{Version: 3, Steps: []Step{
		{From: 0, To: 3, Apply: func(context.Context, *Tx) error { return nil }},
	}}))

	err := s.Migrate(ctx, Schema{Version: 2, Steps: []Step{
		{From: 0, To: 2, Apply: func(context.Context, *Tx) error { return nil }},
	}})
	require.ErrorIs(t, err, ErrSchemaTooNew)
}

func TestMigrateFailingStepRollsBack(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "migrate.db"), "migrate-password")
	defer mustClose(t, s)

	require.NoError(t, s.Migrate(ctx, Schema{Version: 1, Steps: []Step{
		{From: 0, To: 1, Apply: func(ctx context.Context, tx *Tx) error {
			return tx.Record().Save(ctx, &migratedRow{ID: "kept", Name: "first"})
		}},
	}}))

	boom := errors.New("boom")
	err := s.Migrate(ctx, Schema{Version: 2, Steps: []Step{
		{From: 0, To: 1, Apply: func(context.Context, *Tx) error { return nil }},
		{From: 1, To: 2, Apply: func(ctx context.Context, tx *Tx) error {
			if err := tx.Record().Save(ctx, &migratedRow{ID: "lost", Name: "second"}); err != nil {
				return err
			}
			return boom
		}},
	}})
	require.ErrorIs(t, err, boom)

	version, err := s.SchemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	var row migratedRow
	require.Error(t, s.Record().One(ctx, "ID", "lost", &row))
	require.NoError(t, s.Record().One(ctx, "ID", "kept", &row))
}

func TestMigrateInvalidSchemas(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "migrate.db"), "migrate-password")
	defer mustClose(t, s)

	bad := []Schema{
		{Version: 1}, // version without steps
		{Version: 2, Steps: []Step{{From: 0, To: 1}}}, // nil Apply
		{Version: 2, Steps: []Step{{From: 1, To: 1, Apply: func(context.Context, *Tx) error { return nil }}}},
		{Version: 2, Steps: []Step{{From: 0, To: 1, Apply: func(context.Context, *Tx) error { return nil }}}},                                                                           // ends at 1
		{Version: 3, Steps: []Step{{From: 1, To: 2, Apply: func(context.Context, *Tx) error { return nil }}, {From: 0, To: 1, Apply: func(context.Context, *Tx) error { return nil }}}}, // order
	}
	for i, schema := range bad {
		if err := s.Migrate(ctx, schema); !errors.Is(err, ErrInvalidSchema) {
			t.Errorf("schema %d: err = %v, want ErrInvalidSchema", i, err)
		}
	}

	// The zero schema is a no-op on a fresh store.
	require.NoError(t, s.Migrate(ctx, Schema{}))
	version, err := s.SchemaVersion(ctx)
	require.NoError(t, err)
	require.Zero(t, version)
}

func TestMigrateConcurrentCallsApplyOnce(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "migrate.db"), "migrate-password")
	defer mustClose(t, s)

	var applications atomic.Int32
	schema := Schema{Version: 1, Steps: []Step{
		{From: 0, To: 1, Apply: func(context.Context, *Tx) error {
			applications.Add(1)
			return nil
		}},
	}}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Migrate(ctx, schema)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), applications.Load())

	version, err := s.SchemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
}
