package object_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/object"
)

func TestObjectBuckets(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	for _, bucket := range []string{"zebra", "apple"} {
		_, err := s.Put(ctx, bucket, "k", strings.NewReader("x"), 1, "")
		require.NoError(t, err)
	}

	names, err := s.Buckets(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"apple", "zebra"}, names)
}

func TestObjectDropBucket(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, object.Config{})

	_, err := s.Put(ctx, "tenant-a", "a/b", strings.NewReader("payload"), 7, "")
	require.NoError(t, err)
	_, err = s.Put(ctx, "tenant-b", "k", strings.NewReader("keep"), 4, "")
	require.NoError(t, err)

	require.NoError(t, s.DropBucket(ctx, "tenant-a"))

	infos, err := s.List(ctx, "tenant-a", "")
	require.NoError(t, err)
	require.Empty(t, infos)

	names, err := s.Buckets(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-b"}, names)

	// A missing bucket is a no-op.
	require.NoError(t, s.DropBucket(ctx, "missing"))

	// Invalid names are rejected.
	require.ErrorIs(t, s.DropBucket(ctx, "__meta__"), object.ErrInvalidBucket)
}
