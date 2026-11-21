package vec

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/malivvan/binco"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// aeadRawProvider wraps a raw key as a KeyProvider.
func aeadRawProvider(t *testing.T, key []byte) KeyProvider {
	t.Helper()
	provider, err := NewRawKeyProvider(key)
	require.NoError(t, err)
	return provider
}

// aeadKey returns a deterministic 32-byte test key.
func aeadKey(fill byte) []byte { return bytes.Repeat([]byte{fill}, KeySize) }

// aeadOpenStore opens a vec Store on a temporary database file.
func aeadOpenStore(t *testing.T, path string, useQuant bool, cfg *Config) (*Store, *bbolt.DB) {
	t.Helper()

	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := OpenWithConfig(context.Background(), bdb, nil, useQuant, nil, cfg)
	require.NoError(t, err)
	return store, bdb
}

// aeadRawEntries returns the raw key/value pairs of a bucket, skipping nested
// buckets.
func aeadRawEntries(t *testing.T, bdb *bbolt.DB, path ...string) map[string][]byte {
	t.Helper()

	out := map[string][]byte{}
	require.NoError(t, bdb.View(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, name := range path {
			if b == nil {
				b = tx.Bucket([]byte(name))
			} else {
				b = b.Bucket([]byte(name))
			}
			if b == nil {
				return nil
			}
		}
		return b.ForEach(func(k, v []byte) error {
			if v != nil {
				out[string(k)] = append([]byte(nil), v...)
			}
			return nil
		})
	}))
	return out
}

// aeadPointIDs returns the sorted IDs of a loaded collection.
func aeadPointIDs(points map[string]*PointStruct) []string {
	ids := make([]string, 0, len(points))
	for id := range points {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// aeadTestPoints is the fixture used across the encryption tests.
func aeadTestPoints() []PointStruct {
	return []PointStruct{
		{ID: "p1", Version: 1, Vector: []float32{1, 0, 0}, Payload: Payload{"tag": "a", "n": int(1)}},
		{ID: "p2", Version: 2, Vector: []float32{0, 1, 0}, Payload: Payload{"tag": "b", "n": int(2)}},
		{ID: "p3", Version: 3, Vector: []float32{0.9, 0.1, 0}, Payload: Payload{"tag": "a", "n": int(3)}},
	}
}

// TestAEADOptionalityKeepsPlaintextFormat proves that a store opened without
// encryption writes byte-for-byte the same data as before: raw binco point
// blobs, raw JSON metadata, raw ID/name keys and no encryption marker.
func TestAEADOptionalityKeepsPlaintextFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plain.db")
	store, bdb := aeadOpenStore(t, path, false, nil)
	require.Nil(t, store.enc)

	require.NoError(t, store.EnsureCollection("docs"))
	points := aeadTestPoints()
	require.NoError(t, store.UpsertPoints("docs", points))

	rawPoints := aeadRawEntries(t, bdb, "docs")
	require.Len(t, rawPoints, len(points))
	for _, point := range points {
		raw, ok := rawPoints[point.ID]
		require.True(t, ok, "point keys must stay raw IDs")

		// A plaintext store must hold bare binco blobs, so they decode directly.
		stored, err := binco.DecodeExact(StoredPointBinco, raw)
		require.NoError(t, err, "stored blobs must be raw binco, not ciphertext")
		require.Equal(t, fromStoredPoint(toStoredPoint(&point)), fromStoredPoint(&stored))
	}

	meta := CollectionMeta{Name: "docs", VectorLen: 3, Metric: Cosine}
	require.NoError(t, store.SaveCollectionMeta("docs", meta))

	wantMeta, err := json.Marshal(meta)
	require.NoError(t, err)
	rawMeta := aeadRawEntries(t, bdb, collectionsMetaBucket)
	require.Equal(t, wantMeta, rawMeta["docs"])
	require.NotContains(t, rawMeta, markerKey, "a plaintext store must not write an encryption marker")

	require.NoError(t, store.Close())

	// Reopening with an explicit Config that enables no feature must leave the
	// bytes untouched.
	store, bdb = aeadOpenStore(t, path, false, &Config{})
	require.Nil(t, store.enc)
	require.Equal(t, rawPoints, aeadRawEntries(t, bdb, "docs"))
	rawMeta = aeadRawEntries(t, bdb, collectionsMetaBucket)
	require.NotContains(t, rawMeta, markerKey)
	require.Equal(t, wantMeta, rawMeta["docs"])
}

// aeadExercise runs the same operations against a store and returns everything
// an observer can see.
func aeadExercise(t *testing.T, store *Store) map[string]any {
	t.Helper()

	out := map[string]any{}

	require.NoError(t, store.EnsureCollection("docs"))
	require.NoError(t, store.UpsertPoints("docs", aeadTestPoints()))

	loaded, err := store.LoadCollection("docs")
	require.NoError(t, err)
	out["ids"] = aeadPointIDs(loaded)
	out["p1"] = loaded["p1"]
	out["p3Payload"] = loaded["p3"].Payload

	collections, err := store.ListCollections()
	require.NoError(t, err)
	sort.Strings(collections)
	out["collections"] = collections

	meta := CollectionMeta{Name: "docs", VectorLen: 3, Metric: Euclid}
	require.NoError(t, store.SaveCollectionMeta("docs", meta))

	loadedMeta, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	out["meta"] = *loadedMeta

	metas, err := store.ListCollectionMetas()
	require.NoError(t, err)
	sort.Slice(metas, func(i, j int) bool { return metas[i].Name < metas[j].Name })
	out["metas"] = metas

	// A collection built on top of stored points exercises the sealed load path
	// together with the in-memory index.
	col, err := store.NewCollection("docs", 3, Cosine, false)
	require.NoError(t, err)
	scored, err := col.Search([]float32{1, 0, 0}, nil, 2)
	require.NoError(t, err)
	out["searchIDs"] = aeadScoredIDs(scored)

	require.NoError(t, store.DeletePoints("docs", []string{"p2"}))
	afterDelete, err := store.LoadCollection("docs")
	require.NoError(t, err)
	out["afterDelete"] = aeadPointIDs(afterDelete)

	require.NoError(t, store.DropCollection("docs"))
	collections, err = store.ListCollections()
	require.NoError(t, err)
	out["afterDrop"] = collections

	_, err = store.LoadCollectionMeta("docs")
	out["metaAfterDropMissing"] = err != nil

	return out
}

// aeadScoredIDs returns the sorted IDs of search results.
func aeadScoredIDs(points []ScoredPoint) []string {
	ids := make([]string, 0, len(points))
	for _, p := range points {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids
}

// TestAEADRoundTripMatchesPlaintext runs the same operations on a plaintext
// control store and on an encrypted store and requires identical results.
func TestAEADRoundTripMatchesPlaintext(t *testing.T) {
	dir := t.TempDir()

	plain, _ := aeadOpenStore(t, filepath.Join(dir, "plain.db"), false, nil)
	want := aeadExercise(t, plain)
	require.NoError(t, plain.Close())

	enc, encDB := aeadOpenStore(t, filepath.Join(dir, "enc.db"), false, &Config{
		Key:           aeadKey(1),
		EncryptPoints: true,
	})
	got := aeadExercise(t, enc)

	// The stored points are sealed: no payload value and no vector is readable.
	for _, value := range aeadRawEntries(t, encDB, "docs") {
		require.NotContains(t, string(value), "tag")
	}

	require.Equal(t, want, got)
}

// TestAEADHidesPointsAndMetadata checks what an encrypted store leaves in the
// database: raw keys (Tier 1 does not encrypt names) but sealed payloads and
// sealed metadata.
func TestAEADHidesPointsAndMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	store, bdb := aeadOpenStore(t, path, false, &Config{Key: aeadKey(2), EncryptPoints: true})

	require.NoError(t, store.EnsureCollection("docs"))
	require.NoError(t, store.UpsertPoints("docs", aeadTestPoints()))
	require.NoError(t, store.SaveCollectionMeta("docs", CollectionMeta{Name: "docs", VectorLen: 3, Metric: Cosine}))

	rawPoints := aeadRawEntries(t, bdb, "docs")
	require.Contains(t, rawPoints, "p1", "tier 1 leaves point IDs readable")
	for _, value := range rawPoints {
		require.NotContains(t, string(value), "tag")
	}

	rawMeta := aeadRawEntries(t, bdb, collectionsMetaBucket)
	require.Contains(t, rawMeta, markerKey)
	require.NotContains(t, string(rawMeta["docs"]), `"name"`)

	loaded, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	require.Equal(t, "docs", loaded.Name)
}

// TestAEADWrongKeyFails proves the canary rejects an incorrect key while the
// right key keeps working.
func TestAEADWrongKeyFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	cfg := &Config{Key: aeadKey(3), EncryptPoints: true}

	store, _ := aeadOpenStore(t, path, false, cfg)
	require.NoError(t, store.EnsureCollection("docs"))
	require.NoError(t, store.UpsertPoints("docs", aeadTestPoints()))
	require.NoError(t, store.Close())

	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	_, err = OpenWithConfig(context.Background(), bdb, nil, false, nil, &Config{Key: aeadKey(4), EncryptPoints: true})
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, bdb.Close())

	store, _ = aeadOpenStore(t, path, false, cfg)
	defer store.Close()

	loaded, err := store.LoadCollection("docs")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))
}

// TestAEADTamperedPointFails proves authentication failures surface as
// ErrAuthFailed instead of a wrong value, a "not found" or a panic.
func TestAEADTamperedPointFails(t *testing.T) {
	mutations := map[string]func([]byte) []byte{
		"bit flip": func(raw []byte) []byte {
			out := append([]byte(nil), raw...)
			out[len(out)-1] ^= 0xff
			return out
		},
		"truncated": func(raw []byte) []byte {
			return append([]byte(nil), raw[:len(raw)-4]...)
		},
		// An emptied value stays empty, so it fails the binco decode instead of
		// the MAC.
		"empty": func([]byte) []byte { return []byte{} },
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "enc.db")
			store, bdb := aeadOpenStore(t, path, false, &Config{Key: aeadKey(5), EncryptPoints: true})
			require.NoError(t, store.EnsureCollection("docs"))
			require.NoError(t, store.UpsertPoints("docs", aeadTestPoints()))

			require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("docs"))
				return b.Put([]byte("p1"), mutate(append([]byte(nil), b.Get([]byte("p1"))...)))
			}))

			_, err := store.LoadCollection("docs")
			require.Error(t, err)
			if name != "empty" {
				require.ErrorIs(t, err, ErrAuthFailed)
			}

			// A tampered blob must never be reported as a missing collection.
			require.NotErrorIs(t, err, ErrWrongMode)
		})
	}
}

// TestAEADFreshNoncePerWrite proves two writes of equal data store different
// ciphertext.
func TestAEADFreshNoncePerWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	store, bdb := aeadOpenStore(t, path, false, &Config{Key: aeadKey(6), EncryptPoints: true})

	meta := CollectionMeta{Name: "docs", VectorLen: 3, Metric: Cosine}
	require.NoError(t, store.SaveCollectionMeta("docs", meta))
	first := aeadRawEntries(t, bdb, collectionsMetaBucket)["docs"]

	require.NoError(t, store.SaveCollectionMeta("docs", meta))
	second := aeadRawEntries(t, bdb, collectionsMetaBucket)["docs"]

	require.NotEqual(t, first, second, "each write must use a fresh nonce")
	require.NotContains(t, string(first), `"name"`)
}

// TestAEADQuantizationRoundTrip proves quantized vectors survive encryption.
func TestAEADQuantizationRoundTrip(t *testing.T) {
	dir := t.TempDir()

	plain, _ := aeadOpenStore(t, filepath.Join(dir, "plain.db"), true, nil)
	require.NoError(t, plain.EnsureCollection("docs"))
	require.NoError(t, plain.UpsertPoints("docs", aeadTestPoints()))
	plainLoaded, err := plain.LoadCollection("docs")
	require.NoError(t, err)
	require.NoError(t, plain.Close())
	require.NotEmpty(t, plainLoaded["p1"].Vector)

	enc, _ := aeadOpenStore(t, filepath.Join(dir, "enc.db"), true, &Config{Key: aeadKey(7), EncryptPoints: true})
	require.NoError(t, enc.EnsureCollection("docs"))
	require.NoError(t, enc.UpsertPoints("docs", aeadTestPoints()))
	encLoaded, err := enc.LoadCollection("docs")
	require.NoError(t, err)
	defer enc.Close()

	require.Equal(t, plainLoaded["p1"].Vector, encLoaded["p1"].Vector)
	require.Equal(t, aeadPointIDs(plainLoaded), aeadPointIDs(encLoaded))
}

// TestAEADModeMismatch covers every way the on-disk mode and the configured
// mode can disagree.
func TestAEADModeMismatch(t *testing.T) {
	ctx := context.Background()

	t.Run("plaintext open of encrypted store", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "enc.db")
		store, _ := aeadOpenStore(t, path, false, &Config{Key: aeadKey(8), EncryptPoints: true})
		require.NoError(t, store.EnsureCollection("docs"))
		require.NoError(t, store.Close())

		bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		defer bdb.Close()

		_, err = Open(ctx, bdb, nil, false, nil)
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("encrypted open of plaintext store with data", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plain.db")
		store, _ := aeadOpenStore(t, path, false, nil)
		require.NoError(t, store.EnsureCollection("docs"))
		require.NoError(t, store.UpsertPoints("docs", aeadTestPoints()))
		require.NoError(t, store.Close())

		bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		defer bdb.Close()

		_, err = OpenWithConfig(ctx, bdb, nil, false, nil, &Config{Key: aeadKey(8), EncryptPoints: true})
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("settings mismatch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "enc.db")
		store, _ := aeadOpenStore(t, path, false, &Config{Key: aeadKey(8), EncryptPoints: true})
		require.NoError(t, store.EnsureCollection("docs"))
		require.NoError(t, store.Close())

		bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		defer bdb.Close()

		_, err = OpenWithConfig(ctx, bdb, nil, false, nil, &Config{
			Key:                    aeadKey(8),
			EncryptPoints:          true,
			EncryptCollectionNames: true,
		})
		require.ErrorIs(t, err, ErrWrongMode)
	})
}

// TestAEADEmptyStoreAdoptsEncryption proves a store without collections can be
// opened with encryption afterwards.
func TestAEADEmptyStoreAdoptsEncryption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")

	plain, _ := aeadOpenStore(t, path, false, nil)
	require.NoError(t, plain.Close())

	enc, bdb := aeadOpenStore(t, path, false, &Config{Key: aeadKey(9), EncryptPoints: true})
	require.NotNil(t, enc.enc)
	require.Contains(t, aeadRawEntries(t, bdb, collectionsMetaBucket), markerKey)

	require.NoError(t, enc.EnsureCollection("docs"))
	require.NoError(t, enc.UpsertPoints("docs", aeadTestPoints()))
	loaded, err := enc.LoadCollection("docs")
	require.NoError(t, err)
	require.Len(t, loaded, 3)
	require.NoError(t, enc.Close())

	bdb, err = bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	_, err = Open(context.Background(), bdb, nil, false, nil)
	require.ErrorIs(t, err, ErrWrongMode)
}

// TestAEADInvalidConfig covers configuration validation.
func TestAEADInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	ctx := context.Background()

	_, err = OpenWithConfig(ctx, bdb, nil, false, nil, &Config{Key: []byte("short"), EncryptPoints: true})
	require.ErrorIs(t, err, ErrInvalidKey)

	_, err = OpenWithConfig(ctx, bdb, nil, false, nil, &Config{
		Key:           aeadKey(10),
		Cipher:        CipherSuite(99),
		EncryptPoints: true,
	})
	require.ErrorIs(t, err, ErrUnsupportedCipher)

	_, err = OpenWithConfig(ctx, bdb, nil, false, nil, &Config{
		Key:           aeadKey(10),
		KeyProvider:   aeadRawProvider(t, aeadKey(10)),
		EncryptPoints: true,
	})
	require.ErrorIs(t, err, ErrInvalidConfig)

	xchacha, err := OpenWithConfig(ctx, bdb, nil, false, nil, &Config{
		Key:           aeadKey(10),
		Cipher:        CipherXChaCha20Poly1305,
		EncryptPoints: true,
	})
	require.NoError(t, err)
	require.NotNil(t, xchacha.enc)
	require.NoError(t, xchacha.EnsureCollection("docs"))
	require.NoError(t, xchacha.UpsertPoints("docs", aeadTestPoints()))
	loaded, err := xchacha.LoadCollection("docs")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3"}, aeadPointIDs(loaded))
}
