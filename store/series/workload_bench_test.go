package series

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func benchTSKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// openBenchSeriesWithConfig opens a standalone series for a benchmark with the
// given storage configuration.
func openBenchSeriesWithConfig(b *testing.B, cfg Config) *Series {
	b.Helper()

	db, err := bolt.Open(filepath.Join(b.TempDir(), "ts-matrix.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	store, err := Open(context.Background(), db, Config{
		Root:               [][]byte{[]byte("bench")},
		Secret:             cfg.Secret,
		EncryptValues:      cfg.EncryptValues,
		EncryptSeriesNames: cfg.EncryptSeriesNames,
		BlockDuration:      cfg.BlockDuration,
	})
	if err != nil {
		b.Fatal(err)
	}
	return store.Series("bench")
}

// benchTSModes returns the storage modes shared by the time-series matrices:
// plaintext points, sealed values, and sealed block mode.
func benchTSModes() []struct {
	name string
	cfg  Config
} {
	return []struct {
		name string
		cfg  Config
	}{
		{"plain", Config{}},
		{"encrypted", Config{Secret: benchTSKey(1), EncryptValues: true}},
		{"block", Config{Secret: benchTSKey(2), BlockDuration: 1000}},
	}
}

func benchTSPoints() []Point {
	points := make([]Point, 10_000)
	for i := range points {
		points[i] = Point{Timestamp: int64(i), Value: float64(i)}
	}
	return points
}

// BenchmarkAggregateMatrix compares aggregation over plaintext points, sealed
// points and sealed block summaries.
func BenchmarkAggregateMatrix(b *testing.B) {
	ctx := context.Background()
	points := benchTSPoints()

	for _, mode := range benchTSModes() {
		b.Run(mode.name, func(b *testing.B) {
			series := openBenchSeriesWithConfig(b, mode.cfg)
			if err := series.InsertMany(ctx, points); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := series.Aggregate(ctx, 0, int64(len(points)-1), 100, AggAvg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRetentionMatrix compares retention pruning in the same storage
// modes; the policy has work to do on every call.
func BenchmarkRetentionMatrix(b *testing.B) {
	ctx := context.Background()
	points := benchTSPoints()

	for _, mode := range benchTSModes() {
		b.Run(mode.name, func(b *testing.B) {
			series := openBenchSeriesWithConfig(b, mode.cfg)
			if err := series.InsertMany(ctx, points); err != nil {
				b.Fatal(err)
			}
			if err := series.SetRetention(ctx, Retention{MaxPoints: 5_000}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := series.EnforceRetention(ctx, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
