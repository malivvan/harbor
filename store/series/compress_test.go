package series

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/internal/compress"
)

func compressPoints(n int) []Point {
	points := make([]Point, 0, n)
	for i := 0; i < n; i++ {
		points = append(points, Point{Timestamp: int64(i) * int64(time.Second), Value: float64(i) * 1.5})
	}
	return points
}

func TestSeriesCompressBlock(t *testing.T) {
	ctx := context.Background()
	root := []byte(bucket.SERIES)
	blocked := int64(time.Minute)
	store, db := openAEADTestStore(t, root, Config{
		Secret:        testKey(1),
		EncryptValues: true,
		BlockDuration: blocked,
		Compress:      true,
	})
	ser := store.Series("cpu")
	want := compressPoints(500)
	require.NoError(t, ser.InsertMany(ctx, want))

	got, err := ser.Range(ctx, want[0].Timestamp, want[len(want)-1].Timestamp+1)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// Block mode stores one sealed, compressed block per minute: compare it
	// against the same store written without compression.
	compressedValues := seriesValues(t, db, root, "cpu")

	plainStore, plainDB := openAEADTestStore(t, root, Config{
		Secret:        testKey(1),
		EncryptValues: true,
		BlockDuration: blocked,
	})
	plainSer := plainStore.Series("cpu")
	require.NoError(t, plainSer.InsertMany(ctx, want))
	plainValues := seriesValues(t, plainDB, root, "cpu")

	var compressed, plain int
	for _, v := range compressedValues {
		compressed += len(v)
	}
	for _, v := range plainValues {
		plain += len(v)
	}
	require.Positive(t, compressed)
	require.Less(t, compressed, plain, "compressed blocks should be smaller")
}

func TestSeriesCompressSinglePointNoGrow(t *testing.T) {
	ctx := context.Background()
	key := testKey(2)
	root := []byte(bucket.SERIES)

	compressed, cdb := openAEADTestStore(t, root, Config{Secret: key, EncryptValues: true, Compress: true})
	require.NoError(t, compressed.Series("cpu").Insert(ctx, Point{Timestamp: int64(time.Second), Value: 1}))
	cValues := seriesValues(t, cdb, root, "cpu")

	plain, pdb := openAEADTestStore(t, root, Config{Secret: key, EncryptValues: true})
	require.NoError(t, plain.Series("cpu").Insert(ctx, Point{Timestamp: int64(time.Second), Value: 1}))
	pValues := seriesValues(t, pdb, root, "cpu")

	require.Len(t, cValues, len(pValues))
	for i := range cValues {
		require.Equal(t, len(pValues[i]), len(cValues[i]), "8-byte points must not grow")
	}
}

func TestSeriesCompressPlaintextStore(t *testing.T) {
	ctx := context.Background()
	root := []byte(bucket.SERIES)
	store, db := openAEADTestStore(t, root, Config{Compress: true})
	ser := store.Series("cpu")
	want := compressPoints(200)
	require.NoError(t, ser.InsertMany(ctx, want))

	got, err := ser.Range(ctx, want[0].Timestamp, want[len(want)-1].Timestamp+1)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// A point is 8 bytes and never grows: no envelope is written for it.
	for _, v := range seriesValues(t, db, root, "cpu") {
		require.False(t, bytes.HasPrefix(v, []byte(compress.Magic)))
	}
}

func TestSeriesCompressRotation(t *testing.T) {
	ctx := context.Background()
	root := []byte(bucket.SERIES)
	dbPath := filepath.Join(t.TempDir(), "series.db")
	oldKey, newKey := testKey(3), testKey(4)

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := Open(ctx, db, Config{
		Root:          [][]byte{root},
		Secret:        oldKey,
		EncryptValues: true,
		BlockDuration: int64(time.Minute),
		Compress:      true,
	})
	require.NoError(t, err)
	want := compressPoints(300)
	require.NoError(t, store.Series("cpu").InsertMany(ctx, want))
	require.NoError(t, store.RotateAll(ctx, newKey))
	require.NoError(t, db.Close())

	db2, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	again, err := Open(ctx, db2, Config{
		Root:          [][]byte{root},
		Secret:        newKey,
		EncryptValues: true,
		BlockDuration: int64(time.Minute),
	})
	require.NoError(t, err)
	got, err := again.Series("cpu").Range(ctx, want[0].Timestamp, want[len(want)-1].Timestamp+1)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
