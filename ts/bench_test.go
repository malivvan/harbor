package ts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openBenchSeries(b *testing.B) *Series {
	b.Helper()

	db, err := bolt.Open(filepath.Join(b.TempDir(), "ts-bench.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})

	store, err := Open(context.Background(), db, nil)
	if err != nil {
		b.Fatal(err)
	}
	return store.Series("bench")
}

func BenchmarkInsert(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := series.Insert(ctx, Point{Timestamp: int64(i), Value: float64(i)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertMany(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	batch := make([]Point, 100)
	for i := range batch {
		batch[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := series.InsertMany(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRange(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	points := make([]Point, 10_000)
	for i := range points {
		points[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}
	if err := series.InsertMany(ctx, points); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := series.Range(ctx, 0, int64(len(points)-1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggregate(b *testing.B) {
	series := openBenchSeries(b)
	ctx := context.Background()

	points := make([]Point, 10_000)
	for i := range points {
		points[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}
	if err := series.InsertMany(ctx, points); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := series.Aggregate(ctx, 0, int64(len(points)-1), 100, AggAvg); err != nil {
			b.Fatal(err)
		}
	}
}
