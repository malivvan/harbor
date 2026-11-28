package object_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/store/object"
)

func readObject(t *testing.T, store *object.Store, bucket, key string) []byte {
	t.Helper()
	rc, _, err := store.Get(context.Background(), bucket, key)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return data
}

func TestPutStreamGetRangeAndMetadata(t *testing.T) {
	store, _ := openDirect(t, object.Config{})
	ctx := context.Background()
	data := bytes.Repeat([]byte("0123456789"), object.DefaultChunkSize/10+3)

	info, err := store.PutStream(ctx, "media", "big", bytes.NewReader(data), int64(len(data)), object.ObjectMeta{
		ContentType: "application/octet-stream",
		Metadata:    map[string]string{"owner": "alice", "kind": "blob"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), info.Size)
	require.Equal(t, "application/octet-stream", info.ContentType)
	require.Equal(t, "alice", info.Metadata["owner"])
	require.NotZero(t, info.CreatedAt)

	require.Equal(t, data, readObject(t, store, "media", "big"))

	rc, err := store.GetRange(ctx, "media", "big", 3, 11)
	require.NoError(t, err)
	part, err := io.ReadAll(rc)
	require.NoError(t, err)
	_ = rc.Close()
	require.Equal(t, data[3:14], part)

	// A range that crosses the first chunk boundary.
	rc, err = store.GetRange(ctx, "media", "big", object.DefaultChunkSize-5, 20)
	require.NoError(t, err)
	part, err = io.ReadAll(rc)
	require.NoError(t, err)
	_ = rc.Close()
	require.Equal(t, data[object.DefaultChunkSize-5:object.DefaultChunkSize+15], part)

	rc, err = store.GetRange(ctx, "media", "big", int64(len(data)), 10)
	require.NoError(t, err)
	part, err = io.ReadAll(rc)
	require.NoError(t, err)
	_ = rc.Close()
	require.Empty(t, part)

	infos, err := store.ListWithOptions(ctx, "media", object.ListOptions{Metadata: map[string]string{"owner": "alice"}})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	infos, err = store.ListWithOptions(ctx, "media", object.ListOptions{Metadata: map[string]string{"owner": "bob"}})
	require.NoError(t, err)
	require.Empty(t, infos)

	require.NoError(t, store.SetMetadata(ctx, "media", "big", object.ObjectMeta{
		ContentType: "text/plain",
		Metadata:    map[string]string{"owner": "bob"},
	}))
	stat, ok, err := store.Stat(ctx, "media", "big")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "text/plain", stat.ContentType)
	require.Equal(t, "bob", stat.Metadata["owner"])
	require.Equal(t, data, readObject(t, store, "media", "big"))
}

func TestPutStreamFailedReaderLeavesPrevious(t *testing.T) {
	store, _ := openDirect(t, object.Config{})
	ctx := context.Background()
	_, err := store.Put(ctx, "b", "k", bytes.NewReader([]byte("previous")), 8, "text/plain")
	require.NoError(t, err)

	_, err = store.PutStream(ctx, "b", "k", failingReader(object.DefaultChunkSize+10), 0, object.ObjectMeta{})
	require.Error(t, err)
	require.Equal(t, []byte("previous"), readObject(t, store, "b", "k"))
}

func TestPutStreamSizeMismatch(t *testing.T) {
	store, _ := openDirect(t, object.Config{})
	ctx := context.Background()
	_, err := store.Put(ctx, "b", "k", bytes.NewReader([]byte("previous")), 8, "")
	require.NoError(t, err)
	_, err = store.PutStream(ctx, "b", "k", bytes.NewReader([]byte("short")), 99, object.ObjectMeta{})
	require.Error(t, err)
	require.Equal(t, []byte("previous"), readObject(t, store, "b", "k"))
}

func TestPutStreamChunkReferences(t *testing.T) {
	store, bdb := openDirect(t, object.Config{})
	ctx := context.Background()
	data := bytes.Repeat([]byte("abcdefghij"), object.DefaultChunkSize/10+2)

	_, err := store.PutStream(ctx, "b", "one", bytes.NewReader(data), int64(len(data)), object.ObjectMeta{})
	require.NoError(t, err)
	_, err = store.PutStream(ctx, "b", "two", bytes.NewReader(data), int64(len(data)), object.ObjectMeta{})
	require.NoError(t, err)

	require.Equal(t, data, readObject(t, store, "b", "one"))
	require.NoError(t, store.Delete(ctx, "b", "one"))
	require.Equal(t, data, readObject(t, store, "b", "two"))
	require.NoError(t, store.Delete(ctx, "b", "two"))

	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte("object"))
		if root == nil {
			return nil
		}
		b := root.Bucket([]byte("__chunks__"))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			t.Fatalf("chunk %q survived both deletes", k)
			return nil
		})
	}))
}

func TestPutStreamRotationAndVersion(t *testing.T) {
	secret := bytes.Repeat([]byte{5}, 32)
	store, _ := openDirect(t, object.Config{Secret: secret, EncryptValues: true})
	ctx := context.Background()
	data := bytes.Repeat([]byte("rotate"), object.DefaultChunkSize/6+10)
	_, err := store.PutStream(ctx, "b", "k", bytes.NewReader(data), int64(len(data)), object.ObjectMeta{Metadata: map[string]string{"x": "y"}})
	require.NoError(t, err)

	require.NoError(t, store.VerifyAll(ctx))
	next := bytes.Repeat([]byte{6}, 32)
	require.NoError(t, store.RotateAll(ctx, next))
	require.NoError(t, store.VerifyAll(ctx))
	require.Equal(t, data, readObject(t, store, "b", "k"))
}

func TestPutStreamVersionIsTwo(t *testing.T) {
	store, bdb := openDirect(t, object.Config{})
	ctx := context.Background()
	_, err := store.PutStream(ctx, "b", "k", bytes.NewReader([]byte("data")), 4, object.ObjectMeta{})
	require.NoError(t, err)
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte("object"))
		require.NotNil(t, root)
		raw := root.Get([]byte("__version__"))
		require.Len(t, raw, 8)
		var v uint64
		for i := len(raw) - 1; i >= 0; i-- {
			v = v<<8 | uint64(raw[i])
		}
		require.Equal(t, object.FormatVersionChunked, v)
		return nil
	}))
}

func TestPutStreamEmpty(t *testing.T) {
	store, _ := openDirect(t, object.Config{})
	ctx := context.Background()
	info, err := store.PutStream(ctx, "b", "empty", bytes.NewReader(nil), 0, object.ObjectMeta{})
	require.NoError(t, err)
	require.Zero(t, info.Size)
	require.Empty(t, readObject(t, store, "b", "empty"))
}
