package vec

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func benchPoints(n, dim int) []PointStruct {
	points := make([]PointStruct, n)
	for i := 0; i < n; i++ {
		vector := make([]float32, dim)
		for d := range vector {
			vector[d] = float32((i*31+d*17)%101) / 101
		}
		points[i] = PointStruct{
			ID:      fmt.Sprintf("p%d", i),
			Vector:  vector,
			Version: uint64(i),
			Payload: map[string]any{"bucket": i % 8},
		}
	}
	return points
}

func benchQuery(dim int) []float32 {
	q := make([]float32, dim)
	for i := range q {
		q[i] = float32(i%7) / 7
	}
	return q
}

func BenchmarkFlatSearch(b *testing.B) {
	const (
		n   = 5000
		dim = 128
	)
	idx := NewFlatIndex(Euclid)
	if err := idx.Upsert(benchPoints(n, dim)); err != nil {
		b.Fatal(err)
	}
	query := benchQuery(dim)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(query, nil, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHNSWSearch(b *testing.B) {
	const (
		n   = 2000
		dim = 128
	)
	idx := NewHNSWIndex(Euclid)
	if err := idx.Upsert(benchPoints(n, dim)); err != nil {
		b.Fatal(err)
	}
	query := benchQuery(dim)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(query, nil, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHNSWBuild(b *testing.B) {
	const (
		n   = 500
		dim = 128
	)
	points := benchPoints(n, dim)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := NewHNSWIndex(Euclid)
		if err := idx.Upsert(points); err != nil {
			b.Fatal(err)
		}
	}
}

func benchStore(b *testing.B) (*Store, *Collection) {
	b.Helper()

	db, err := bolt.Open(filepath.Join(b.TempDir(), "vec-bench.db"), 0600, &bolt.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	store, err := Open(context.Background(), db, Config{})
	if err != nil {
		b.Fatal(err)
	}
	col, err := store.NewCollection("bench", 64, Cosine, false)
	if err != nil {
		b.Fatal(err)
	}
	return store, col
}

func BenchmarkStoreFlush(b *testing.B) {
	_, col := benchStore(b)
	batch := benchPoints(DefaultFlushThreshold, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range batch {
			batch[j].ID = fmt.Sprintf("p%d-%d", i, j)
		}
		if err := col.Upsert(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreLoad(b *testing.B) {
	store, col := benchStore(b)
	points := benchPoints(1000, 64)
	if err := col.Upsert(points); err != nil {
		b.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.LoadCollection("bench"); err != nil {
			b.Fatal(err)
		}
	}
}
