package graphs_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/graphs"
	hrecord "github.com/malivvan/tempest/store/record"
)

func openDirect(t *testing.T, cfg graphs.Config) (*graphs.Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "graphs.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	cfg.Root = [][]byte{[]byte(bucket.GRAPHS)}
	s, err := graphs.Open(context.Background(), bdb, cfg)
	require.NoError(t, err)
	return s, bdb
}

func seedGraph(t *testing.T, g *graphs.Graph) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person", Content: "alice"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "b", Type: "person", Content: "bob"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e1", From: "a", To: "b", Type: "knows"}))
}

func TestIndependentOpenAndPersistence(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, graphs.Config{})

	g, err := s.Open("social")
	require.NoError(t, err)
	seedGraph(t, g)

	got, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice", got.Content)
	edges, err := g.GetEdges(ctx, "a", "out")
	require.NoError(t, err)
	require.Len(t, edges, 1)

	// Reopen over the same file with a fresh Store wrapper.
	reopened, err := graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
	require.NoError(t, err)
	g2, err := reopened.Open("social")
	require.NoError(t, err)
	got, err = g2.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice", got.Content)
	edges, err = g2.GetEdges(ctx, "a", "out")
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

func TestIsolationFromRecordStore(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "both.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	gs, err := graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
	require.NoError(t, err)
	g, err := gs.Open("social")
	require.NoError(t, err)
	seedGraph(t, g)

	rec, err := hrecord.Open(ctx, bdb, hrecord.Config{Root: [][]byte{[]byte(bucket.RECORD)}})
	require.NoError(t, err)
	// The graph layout does not create the record-backed graph buckets.
	var nodes []*graphs.NodeRecord
	require.NoError(t, rec.All(ctx, &nodes))
	require.Empty(t, nodes)

	// A record with the same id is a separate record-store entity.
	type doc struct {
		ID   string `db:"id"`
		Name string
	}
	require.NoError(t, rec.Save(ctx, &doc{ID: "a", Name: "record-a"}))
	got, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice", got.Content)

	// The graph lives under its own root.
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte(bucket.GRAPHS)))
		require.Nil(t, tx.Bucket([]byte("tempestNodeRecord")))
		return nil
	}))
}

func TestRootStoreGraphsAccessAndTransactions(t *testing.T) {
	ctx := context.Background()
	store, err := tempest.Open(ctx, filepath.Join(t.TempDir(), "tempest.db"), 0o600, &tempest.Options{
		Password:   "graph-password",
		KDFTime:    1,
		KDFMemory:  8 * tempest.MiB,
		KDFThreads: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	g, err := store.Graphs().Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person"}))

	// Transactions expose a bound graph store; commit and abort behave like the
	// other sub-stores.
	require.NoError(t, store.WriteTransaction(ctx, func(tx *tempest.Tx) error {
		tg, err := tx.Graphs().Open("social")
		if err != nil {
			return err
		}
		return tg.UpsertNode(ctx, &graphs.Node{ID: "committed", Type: "person"})
	}))
	got, err := store.Graphs().Open("social")
	require.NoError(t, err)
	_, err = got.GetNode(ctx, "committed")
	require.NoError(t, err)

	abortErr := errors.New("abort")
	err = store.WriteTransaction(ctx, func(tx *tempest.Tx) error {
		tg, err := tx.Graphs().Open("social")
		if err != nil {
			return err
		}
		if err := tg.UpsertNode(ctx, &graphs.Node{ID: "aborted"}); err != nil {
			return err
		}
		return abortErr
	})
	require.ErrorIs(t, err, abortErr)
	_, err = got.GetNode(ctx, "aborted")
	require.ErrorIs(t, err, graphs.ErrNodeNotFound)

	// Rooted views are isolated from the conventional root.
	tenant, err := store.Graphs("tenant").Open("social")
	require.NoError(t, err)
	require.NoError(t, tenant.UpsertNode(ctx, &graphs.Node{ID: "t1"}))
	_, err = got.GetNode(ctx, "t1")
	require.ErrorIs(t, err, graphs.ErrNodeNotFound)
}

func TestGraphStoreEncryption(t *testing.T) {
	ctx := context.Background()
	secret := []byte("0123456789abcdef0123456789abcdef")
	s, bdb := openDirect(t, graphs.Config{Secret: secret, EncryptValues: true})
	g, err := s.Open("secret")
	require.NoError(t, err)
	seedGraph(t, g)

	again, err := graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}, Secret: secret, EncryptValues: true})
	require.NoError(t, err)
	g2, err := again.Open("secret")
	require.NoError(t, err)
	got, err := g2.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice", got.Content)

	// A wrong key fails the canary at open instead of decoding garbage.
	wrong := []byte("fedcba9876543210fedcba9876543210")
	_, err = graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}, Secret: wrong, EncryptValues: true})
	require.Error(t, err)

	// Opening with the wrong mode fails closed.
	_, err = graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
	require.ErrorIs(t, err, graphs.ErrWrongMode)
}

func TestMigrateFromRecordPreservesHistory(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "migrate.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	rec, err := hrecord.Open(ctx, bdb, hrecord.Config{Root: [][]byte{[]byte(bucket.RECORD)}})
	require.NoError(t, err)
	clock := func() time.Time { return time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC) }
	legacy := graphs.New(rec, graphs.WithClock(clock))
	require.NoError(t, legacy.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person", Content: "v1"}))
	require.NoError(t, legacy.UpsertNode(ctx, &graphs.Node{ID: "b", Type: "person"}))
	require.NoError(t, legacy.UpsertEdge(ctx, &graphs.Edge{ID: "e", From: "a", To: "b"}))
	// Move the clock and update to create a closed version.
	later := func() time.Time { return time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC) }
	legacy.SetClock(later)
	require.NoError(t, legacy.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person", Content: "v2"}))

	gs, err := graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
	require.NoError(t, err)
	require.NoError(t, gs.MigrateFromRecord(ctx, "migrated", rec))

	migrated, err := gs.Open("migrated")
	require.NoError(t, err)
	got, err := migrated.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "v2", got.Content)
	edges, err := migrated.GetEdges(ctx, "a", "out")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	history, err := migrated.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, history, 1, "the closed version must migrate with the live record")

	// Legacy data is still readable; migration never mutates or abandons it.
	still, err := legacy.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "v2", still.Content)
}

func TestFormatVersionFailsClosed(t *testing.T) {
	ctx := context.Background()
	s, bdb := openDirect(t, graphs.Config{})
	_, err := s.Open("g")
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket.GRAPHS)).Put([]byte("__version__"), []byte{9, 0, 0, 0, 0, 0, 0, 0})
	}))
	_, err = graphs.Open(ctx, bdb, graphs.Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
	require.ErrorIs(t, err, graphs.ErrIncompatibleVersion)
}
