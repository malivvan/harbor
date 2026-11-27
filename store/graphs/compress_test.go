package graphs_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/internal/compress"
	"github.com/malivvan/tempest/store/graphs"
)

func compressContent() string {
	return strings.Repeat("graph node content that repeats for compression ", 100)
}

func graphBytes(t *testing.T, db *bolt.DB, bucketName string) int {
	t.Helper()
	var total int
	var walk func(*bolt.Bucket) error
	walk = func(b *bolt.Bucket) error {
		return b.ForEach(func(k, v []byte) error {
			if v != nil {
				total += len(v)
				return nil
			}
			return walk(b.Bucket(k))
		})
	}
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketName))
		if root == nil {
			return nil
		}
		return walk(root)
	}))
	return total
}

func TestGraphsCompressRoundTrip(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	store, db := openDirect(t, graphs.Config{Secret: oldKey, EncryptValues: true, Compress: true})

	g, err := store.Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{
		ID: "a", Type: "doc", Content: compressContent(),
		Properties: map[string]any{"body": compressContent(), "n": 1},
	}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{
		ID: "e1", From: "a", To: "a", Type: "self",
		Properties: map[string]any{"body": compressContent()},
	}))

	got, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, compressContent(), got.Content)
	edges, err := g.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, compressContent(), edges[0].Properties["body"])

	stats, err := g.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Nodes)

	// Compare against an uncompressed store with the same graph.
	plain, pdb := openDirect(t, graphs.Config{Secret: oldKey, EncryptValues: true})
	pg, err := plain.Open("social")
	require.NoError(t, err)
	require.NoError(t, pg.UpsertNode(ctx, &graphs.Node{
		ID: "a", Type: "doc", Content: compressContent(),
		Properties: map[string]any{"body": compressContent(), "n": 1},
	}))
	require.NoError(t, pg.UpsertEdge(ctx, &graphs.Edge{
		ID: "e1", From: "a", To: "a", Type: "self",
		Properties: map[string]any{"body": compressContent()},
	}))

	compressedSize := graphBytes(t, db, bucket.GRAPHS)
	plainSize := graphBytes(t, pdb, bucket.GRAPHS)
	require.Positive(t, compressedSize)
	require.Less(t, compressedSize, plainSize)

	// Rotation carries the codec.
	require.NoError(t, store.RotateAll(ctx, newKey))
	again, err := graphs.Open(ctx, db, graphs.Config{
		Root:          [][]byte{[]byte(bucket.GRAPHS)},
		Secret:        newKey,
		EncryptValues: true,
	})
	require.NoError(t, err)
	g2, err := again.Open("social")
	require.NoError(t, err)
	node, err := g2.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, compressContent(), node.Content)
}

func TestGraphsCompressPlaintextStore(t *testing.T) {
	ctx := context.Background()
	store, db := openDirect(t, graphs.Config{Compress: true})
	g, err := store.Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "doc", Content: compressContent()}))

	node, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, compressContent(), node.Content)

	var found bool
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket.GRAPHS)).Bucket([]byte("social")).Bucket([]byte("nodes"))
		return b.ForEach(func(_, v []byte) error {
			if v != nil && bytes.HasPrefix(v, []byte(compress.Magic)) {
				found = true
			}
			return nil
		})
	}))
	require.True(t, found, "expected a ZCMP envelope in a plaintext compressed graph")
}

func TestGraphsCompressCanaryUnaffected(t *testing.T) {
	_, key := rotateKeys()
	store, _ := openDirect(t, graphs.Config{Secret: key, EncryptValues: true, Compress: true})
	// ensureMeta already verified the canary at Open; VerifyAll re-reads it
	// directly, so a double envelope would fail here.
	require.NoError(t, store.VerifyAll(context.Background()))
}
