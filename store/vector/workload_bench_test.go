package vector

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func benchVecKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// openBenchVecStore opens a standalone vec store for a benchmark, optionally
// with points, IDs and collection names encrypted at rest.
func openBenchVecStore(b *testing.B, encrypted bool) *Store {
	b.Helper()

	bdb, err := bolt.Open(filepath.Join(b.TempDir(), "vec-matrix.db"), 0o600, &bolt.Options{Timeout: 1e9})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bdb.Close() })

	cfg := Config{}
	if encrypted {
		cfg.Secret = benchVecKey(3)
		cfg.EncryptPoints = true
		cfg.EncryptPointIDs = true
		cfg.EncryptCollectionNames = true
	}
	store, err := Open(context.Background(), bdb, cfg)
	if err != nil {
		b.Fatal(err)
	}
	return store
}

func benchIndexLabel(hnsw bool) string {
	if hnsw {
		return "hnsw"
	}
	return "flat"
}

// BenchmarkSearchMatrix is the vector search workload matrix: metric × index ×
// encryption at rest. Setup (collection creation, loading and flush) is not
// timed; the search loop is.
func BenchmarkSearchMatrix(b *testing.B) {
	const (
		n   = 2000
		dim = 128
	)
	points := benchPoints(n, dim)
	query := benchQuery(dim)

	for _, metric := range []Distance{Cosine, Euclid, Dot} {
		for _, hnsw := range []bool{false, true} {
			for _, encrypted := range []bool{false, true} {
				name := fmt.Sprintf("metric=%s/index=%s/encrypted=%t", metric, benchIndexLabel(hnsw), encrypted)
				b.Run(name, func(b *testing.B) {
					store := openBenchVecStore(b, encrypted)
					col, err := store.NewCollection("bench", dim, metric, hnsw)
					if err != nil {
						b.Fatal(err)
					}
					if err := col.Upsert(points); err != nil {
						b.Fatal(err)
					}
					if err := col.Flush(); err != nil {
						b.Fatal(err)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := col.Search(query, nil, 10); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

// BenchmarkFilteredSearchMatrix covers post-filtering: no filter, a filter that
// matches one eighth of the corpus, and a filter that matches nothing (which
// drives HNSW onto the exact-scan fallback).
func BenchmarkFilteredSearchMatrix(b *testing.B) {
	const (
		n   = 2000
		dim = 128
	)
	points := benchPoints(n, dim)
	query := benchQuery(dim)

	selectivities := []struct {
		name   string
		filter *Filter
	}{
		{"all", nil},
		{"bucket", &Filter{Must: []Condition{{Key: "bucket", Match: MatchValue{Value: 2}}}}},
		{"absent", &Filter{Must: []Condition{{Key: "bucket", Match: MatchValue{Value: 99}}}}},
	}

	for _, hnsw := range []bool{false, true} {
		for _, sel := range selectivities {
			b.Run(fmt.Sprintf("index=%s/filter=%s", benchIndexLabel(hnsw), sel.name), func(b *testing.B) {
				store := openBenchVecStore(b, false)
				col, err := store.NewCollection("bench", dim, Cosine, hnsw)
				if err != nil {
					b.Fatal(err)
				}
				if err := col.Upsert(points); err != nil {
					b.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := col.Search(query, sel.filter, 10); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
