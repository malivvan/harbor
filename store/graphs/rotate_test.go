package graphs_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/store/graphs"
)

func rotateKeys() (oldKey, newKey []byte) {
	return bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
}

func TestGraphsRotateAll(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, bdb := openDirect(t, graphs.Config{Secret: oldKey, EncryptValues: true})

	g, err := s.Open("social")
	require.NoError(t, err)
	seedGraph(t, g)

	require.NoError(t, s.RotateAll(ctx, newKey))
	require.NoError(t, s.VerifyAll(ctx))

	// The same store keeps working through the adopted key.
	node, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice", node.Content)

	// A fresh store over the same handle opens with the new key only.
	again, err := graphs.Open(ctx, bdb, graphs.Config{
		Root:          [][]byte{[]byte(bucket.GRAPHS)},
		Secret:        newKey,
		EncryptValues: true,
	})
	require.NoError(t, err)
	rotated, err := again.Open("social")
	require.NoError(t, err)
	node, err = rotated.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice", node.Content)
	edges, err := rotated.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)

	// The previous key no longer decrypts the canary.
	_, err = graphs.Open(ctx, bdb, graphs.Config{
		Root:          [][]byte{[]byte(bucket.GRAPHS)},
		Secret:        oldKey,
		EncryptValues: true,
	})
	require.ErrorIs(t, err, crypto.ErrAuthFailed)
}

func TestGraphsVerifyAll(t *testing.T) {
	ctx := context.Background()
	key, _ := rotateKeys()
	s, bdb := openDirect(t, graphs.Config{Secret: key, EncryptValues: true})
	g, err := s.Open("social")
	require.NoError(t, err)
	seedGraph(t, g)
	require.NoError(t, s.VerifyAll(ctx))

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket.GRAPHS)).Bucket([]byte("social")).Bucket([]byte("nodes"))
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			corrupt := append([]byte(nil), v...)
			corrupt[len(corrupt)-1] ^= 0xff
			return b.Put(append([]byte(nil), k...), corrupt)
		})
	}))
	require.ErrorIs(t, s.VerifyAll(ctx), crypto.ErrAuthFailed)
}

func TestGraphsVerifyAllPlaintext(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, graphs.Config{})
	require.ErrorIs(t, s.VerifyAll(ctx), graphs.ErrInvalidConfig)
	require.ErrorIs(t, s.RotateAll(ctx, bytes.Repeat([]byte{3}, 32)), graphs.ErrInvalidConfig)
}

func TestGraphsReEncryptGraph(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, bdb := openDirect(t, graphs.Config{Secret: oldKey, EncryptValues: true})

	for _, name := range []string{"a", "b"} {
		g, err := s.Open(name)
		require.NoError(t, err)
		require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "n", Type: "thing", Content: name}))
	}

	require.NoError(t, s.ReEncryptGraph(ctx, "a", newKey))
	// The old key stays available as a fallback until RotateAll.
	for _, name := range []string{"a", "b"} {
		g, err := s.Open(name)
		require.NoError(t, err)
		node, err := g.GetNode(ctx, "n")
		require.NoError(t, err)
		require.Equal(t, name, node.Content)
	}

	require.NoError(t, s.RotateAll(ctx, newKey))
	again, err := graphs.Open(ctx, bdb, graphs.Config{
		Root:          [][]byte{[]byte(bucket.GRAPHS)},
		Secret:        newKey,
		EncryptValues: true,
	})
	require.NoError(t, err)
	g, err := again.Open("b")
	require.NoError(t, err)
	node, err := g.GetNode(ctx, "n")
	require.NoError(t, err)
	require.Equal(t, "b", node.Content)

	_, err = graphs.Open(ctx, bdb, graphs.Config{
		Root:          [][]byte{[]byte(bucket.GRAPHS)},
		Secret:        oldKey,
		EncryptValues: true,
	})
	require.ErrorIs(t, err, crypto.ErrAuthFailed)
}

func TestGraphsReEncryptGraphMissing(t *testing.T) {
	oldKey, newKey := rotateKeys()
	s, _ := openDirect(t, graphs.Config{Secret: oldKey, EncryptValues: true})
	err := s.ReEncryptGraph(context.Background(), "missing", newKey)
	require.True(t, errors.Is(err, graphs.ErrInvalidName))
}
