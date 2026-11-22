package ts

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/malivvan/storm/internal/encryption"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestSIVNameEncryption(t *testing.T) {
	engine, err := encryption.NewSIVNameCipher(testKey(1), sivInfo)
	require.NoError(t, err)

	first, err := engine.SealName([]byte("cpu"))
	require.NoError(t, err)
	second, err := engine.SealName([]byte("cpu"))
	require.NoError(t, err)
	other, err := engine.SealName([]byte("mem"))
	require.NoError(t, err)

	require.Equal(t, first, second, "SIV must be deterministic")
	require.NotEqual(t, first, other)
	require.NotContains(t, string(first), "cpu")
	require.False(t, strings.Contains(string(first), "_"), "the encoding must avoid composite-key separators")

	name, err := engine.OpenName(first)
	require.NoError(t, err)
	require.Equal(t, "cpu", string(name))

	_, err = encryption.NewSIVNameCipher(make([]byte, 16), sivInfo)
	require.ErrorIs(t, err, ErrInvalidKey)

	bad := append([]byte(nil), first...)
	bad[len(bad)-1] ^= 0xFF
	_, err = engine.OpenName(bad)
	require.ErrorIs(t, err, ErrAuthFailed)
}

// bucketValues returns the raw value bytes of a physical series bucket.
func bucketValues(t *testing.T, db *bolt.DB, root, physical []byte) [][]byte {
	t.Helper()

	var values [][]byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket(root)
		if parent == nil {
			return nil
		}
		bucket := parent.Bucket(physical)
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

// physicalKeys returns the child bucket names and metadata keys stored under root.
func physicalKeys(t *testing.T, db *bolt.DB, root []byte) (buckets, metaKeys [][]byte) {
	t.Helper()

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket(root)
		if parent == nil {
			return nil
		}
		require.NoError(t, parent.ForEach(func(key, value []byte) error {
			if value == nil && string(key) != metaBucketName {
				buckets = append(buckets, append([]byte(nil), key...))
			}
			return nil
		}))
		if meta := parent.Bucket([]byte(metaBucketName)); meta != nil {
			require.NoError(t, meta.ForEach(func(key, _ []byte) error {
				// Internal markers are not series metadata.
				if string(key) != markerKey && string(key) != versionKey {
					metaKeys = append(metaKeys, append([]byte(nil), key...))
				}
				return nil
			}))
		}
		return nil
	}))
	return buckets, metaKeys
}

func TestEncryptSeriesNamesStore(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	key := testKey(2)
	path := filepath.Join(t.TempDir(), "names.db")

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	store, err := openStore(ctx, db, root, Config{Secret: key, EncryptSeriesNames: true})
	require.NoError(t, err)

	se := store.Series("secret-series")
	require.NoError(t, se.InsertMany(ctx, []Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, se.SetRetention(ctx, Retention{MaxPoints: 10}))
	require.Equal(t, []string{"secret-series"}, store.SeriesNames())

	buckets, metaKeys := physicalKeys(t, db, root)
	require.Len(t, buckets, 1)
	require.Len(t, metaKeys, 1)
	for _, physical := range append(buckets, metaKeys...) {
		require.NotContains(t, string(physical), "secret-series")
		require.False(t, strings.Contains(string(physical), "_"), "the encoding must avoid composite-key separators")
	}

	// Reopen with the same key and verify all data is readable.
	require.NoError(t, store.Close())
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer db.Close()
	store, err = openStore(ctx, db, root, Config{Secret: key, EncryptSeriesNames: true})
	require.NoError(t, err)

	se = store.Series("secret-series")
	points, err := se.Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []Point{{1, 1}, {2, 2}}, points)

	labels, err := se.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "a"}, labels)

	retention, err := se.Retention(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, retention.MaxPoints)

	removed, err := store.EnforceRetention(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, removed)

	require.NoError(t, se.Drop(ctx))
	require.Empty(t, store.SeriesNames())
	buckets, metaKeys = physicalKeys(t, db, root)
	require.Empty(t, buckets)
	require.Empty(t, metaKeys)
}

func TestEncryptSeriesNamesAndValues(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openAEADTestStore(t, root, Config{
		Secret:             testKey(3),
		EncryptValues:      true,
		EncryptSeriesNames: true,
	})

	se := store.Series("private")
	require.NoError(t, se.Insert(ctx, Point{Timestamp: 5, Value: 1.5}))
	require.NoError(t, se.SetLabels(ctx, map[string]string{"owner": "alice"}))

	buckets, _ := physicalKeys(t, db, root)
	require.Len(t, buckets, 1)
	require.NotEqual(t, "private", string(buckets[0]))

	values := bucketValues(t, db, root, buckets[0])
	require.NotEmpty(t, values)
	for _, value := range values {
		require.Greater(t, len(value), recordSize)
	}

	point, err := se.Get(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, Point{Timestamp: 5, Value: 1.5}, point)

	labels, err := se.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "alice"}, labels)
}

func TestEncryptSeriesNamesModeMismatch(t *testing.T) {
	ctx := context.Background()
	root := []byte("ts")
	store, db := openAEADTestStore(t, root, Config{Secret: testKey(4), EncryptSeriesNames: true})
	require.NoError(t, store.Series("cpu").Insert(ctx, Point{Timestamp: 1, Value: 1}))

	_, err := openStore(ctx, db, root, Config{Secret: testKey(4), EncryptValues: true})
	require.ErrorIs(t, err, ErrWrongMode)
}
