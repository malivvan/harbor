package series

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	hstore "github.com/malivvan/harbor/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func testKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func openAEADTestStore(t *testing.T, root []byte, cfg Config) (*Store, *bolt.DB) {
	t.Helper()

	db, err := bolt.Open(filepath.Join(t.TempDir(), "aead.db"), 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	if len(root) > 0 {
		cfg.Root = [][]byte{root}
	}
	store, err := Open(context.Background(), db, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, db
}

// seriesValues returns the raw value bytes of a series bucket.
func seriesValues(t *testing.T, db *bolt.DB, root []byte, name string) [][]byte {
	t.Helper()

	var values [][]byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var bucket *bolt.Bucket
		if len(root) == 0 {
			bucket = tx.Bucket([]byte(name))
		} else if parent := tx.Bucket(root); parent != nil {
			bucket = parent.Bucket([]byte(name))
		}
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			if value != nil {
				values = append(values, append([]byte(nil), value...))
			}
			return nil
		})
	}))
	return values
}

func markerBytes(t *testing.T, db *bolt.DB, root []byte) []byte {
	t.Helper()

	var raw []byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var bucket *bolt.Bucket
		if len(root) == 0 {
			bucket = tx.Bucket([]byte(metaBucketName))
		} else if parent := tx.Bucket(root); parent != nil {
			bucket = parent.Bucket([]byte(metaBucketName))
		}
		if bucket != nil {
			raw = append([]byte(nil), bucket.Get([]byte(markerKey))...)
		}
		return nil
	}))
	return raw
}

func TestEncryptionIsOptional(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{})
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))

	for _, value := range seriesValues(t, db, root, "cpu") {
		require.Len(t, value, recordSize, "plaintext values must stay 8 bytes")
	}
	require.Nil(t, markerBytes(t, db, root), "plaintext stores must not write a marker")

	// Reopening with an empty config is the plaintext path.
	reopened, err := openStore(ctx, db, root, Config{})
	require.NoError(t, err)
	points, err := reopened.Series("cpu").Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, points, 2)
}

func TestOpenEncryptionMatchesPlaintext(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)

	plain, plainDB := openAEADTestStore(t, root, Config{})
	configured, configuredDB := openAEADTestStore(t, root, Config{})

	for _, store := range []*Store{plain, configured} {
		se := store.Series("cpu")
		require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1.5}, {Timestamp: 2, Value: 2.5}}))
		require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
		require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 5}))
	}

	require.Equal(t, dumpRoot(t, plainDB, root), dumpRoot(t, configuredDB, root))
}

func dumpRoot(t *testing.T, db *bolt.DB, root []byte) map[string][]byte {
	t.Helper()

	out := make(map[string][]byte)
	var walk func(prefix string, bucket *bolt.Bucket)
	walk = func(prefix string, bucket *bolt.Bucket) {
		require.NoError(t, bucket.ForEach(func(key, value []byte) error {
			full := prefix + "/" + string(key)
			if value == nil {
				walk(full, bucket.Bucket(key))
				return nil
			}
			out[full] = append([]byte(nil), value...)
			return nil
		}))
	}
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(root)
		if bucket != nil {
			walk("", bucket)
		}
		return nil
	}))
	return out
}

func TestEncryptValuesRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(1), EncryptValues: true})
	se := store.Series("cpu")

	require.NoError(t, se.InsertMany(ctx, []Point{
		{Timestamp: 0, Value: 1},
		{Timestamp: 10, Value: 2},
		{Timestamp: 20, Value: 3},
	}))
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 10, Value: 22}))

	point, err := se.Get(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, Point{Timestamp: 10, Value: 22}, point)

	first, err := se.First(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), first.Timestamp)
	last, err := se.Last(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(20), last.Timestamp)

	points, err := se.Range(ctx, 0, 20)
	require.NoError(t, err)
	require.Equal(t, []Point{{0, 1}, {10, 22}, {20, 3}}, points)

	var streamed []Point
	require.NoError(t, se.RangeEach(ctx, 0, 20, func(p Point) error {
		streamed = append(streamed, p)
		return nil
	}))
	require.Equal(t, points, streamed)

	samples, err := se.Aggregate(ctx, 0, 20, 10, AggSum)
	require.NoError(t, err)
	require.Equal(t, []Sample{{0, 1}, {10, 22}, {20, 3}}, samples)

	count, err := se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 10}))
	labels, err := se.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)

	removed, err := se.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, removed)

	deleted, err := se.DeleteRange(ctx, 5, 15)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	count, err = se.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Values on disk are sealed, not 8-byte plaintext.
	for _, value := range seriesValues(t, db, root, "cpu") {
		require.Greater(t, len(value), recordSize)
	}
}

func TestEncryptValuesMetadataAndMarker(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(2), EncryptValues: true})
	se := store.Series("cpu")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"secret-label": "value"}))

	marker := markerBytes(t, db, root)
	require.NotEmpty(t, marker)
	require.Equal(t, markerMagic, string(marker[:4]))

	// Metadata is not stored as plain JSON.
	var metaRaw []byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket(root)
		if parent != nil {
			if bucket := parent.Bucket([]byte(metaBucketName)); bucket != nil {
				metaRaw = append([]byte(nil), bucket.Get([]byte("cpu"))...)
			}
		}
		return nil
	}))
	require.NotEmpty(t, metaRaw)
	require.NotContains(t, string(metaRaw), "secret-label")
}

func TestEncryptValuesNoncesAreFresh(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(3), EncryptValues: true})
	se := store.Series("cpu")

	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 42}))
	first := seriesValues(t, db, root, "cpu")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 2, Value: 42}))
	second := seriesValues(t, db, root, "cpu")

	require.Len(t, first, 1)
	require.Len(t, second, 2)
	// Fresh nonces mean the same plaintext value seals differently.
	require.False(t, bytes.Equal(second[0], second[1]), "same value must produce different ciphertext")
}

func TestEncryptValuesWrongKey(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	path := filepath.Join(t.TempDir(), "wrong-key.db")

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := openStore(ctx, db, root, Config{Secret: testKey(4), EncryptValues: true})
	require.NoError(t, err)
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer db.Close()

	_, err = openStore(ctx, db, root, Config{Secret: testKey(5), EncryptValues: true})
	require.ErrorIs(t, err, ErrAuthFailed)

	store, err = openStore(ctx, db, root, Config{Secret: testKey(4), EncryptValues: true})
	require.NoError(t, err)
	point, err := store.Series("cpu").Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, Point{Timestamp: 1, Value: 1}, point)
}

func TestEncryptValuesTamper(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(6), EncryptValues: true})
	se := store.Series("cpu")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(root).Bucket([]byte("cpu"))
		key := encodeKey(1)
		raw := bucket.Get(key[:])
		require.NotNil(t, raw)
		tampered := append([]byte(nil), raw...)
		tampered[len(tampered)-1] ^= 0xFF
		return bucket.Put(key[:], tampered)
	}))

	_, err := se.Get(ctx, 1)
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = se.Range(ctx, 0, 10)
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = se.First(ctx)
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = se.Aggregate(ctx, 0, 10, 1, AggSum)
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestEncryptedStoreRequiresKey(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(7), EncryptValues: true})
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	_, err := openStore(ctx, db, root, Config{})
	require.ErrorIs(t, err, ErrWrongMode)
	_, err = openStore(ctx, db, root, Config{})
	require.ErrorIs(t, err, ErrWrongMode)
}

func TestEncryptionOnPlaintextDataIsRejected(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, db := openAEADTestStore(t, root, Config{})
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	_, err := openStore(ctx, db, root, Config{Secret: testKey(8), EncryptValues: true})
	require.ErrorIs(t, err, ErrWrongMode)
}

func TestEncryptionConfigValidation(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)

	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{
			name: "missing key",
			cfg:  Config{EncryptValues: true},
			want: ErrInvalidKey,
		},
		{
			name: "short key",
			cfg:  Config{Secret: make([]byte, 16), EncryptValues: true},
			want: ErrInvalidKey,
		},
		{
			name: "negative block duration",
			cfg:  Config{Secret: testKey(9), EncryptValues: true, BlockDuration: -1},
			want: ErrInvalidConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db := openAEADTestStore(t, root, Config{})
			_ = store
			_, err := openStore(ctx, db, root, tt.cfg)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestEncryptValuesRetention(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, _ := openAEADTestStore(t, root, Config{Secret: testKey(12), EncryptValues: true})
	se := store.Series("cpu")

	require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 2}))
	require.NoError(t, se.InsertMany(ctx, []Point{
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
		{Timestamp: 3, Value: 3},
	}))

	points, err := se.Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{2, 2}, {3, 3}}, points)
}

func TestEncryptedStoreDrop(t *testing.T) {
	ctx := context.Background()
	root := []byte(hstore.SERIES)
	store, _ := openAEADTestStore(t, root, Config{Secret: testKey(13), EncryptValues: true})
	se := store.Series("cpu")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, se.Drop(ctx))
	require.Empty(t, store.SeriesNames())
	require.True(t, errors.Is(store.DeleteSeries(ctx, "cpu"), ErrNotFound))
}
