package record

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/tempest/store/record/q"
	bolt "go.etcd.io/bbolt"
)

// benchRecord is the record shape shared by the db matrices.
type benchRecord struct {
	ID    int    `db:"increment"`
	Name  string `db:"index"`
	Group string `db:"index"`
}

func benchDBKey(seed byte) []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// openBenchDB opens a standalone store, optionally encrypting keys, values and
// metadata.
func openBenchDB(b *testing.B, encrypted bool) *Store {
	b.Helper()

	bdb, err := bolt.Open(filepath.Join(b.TempDir(), "db-matrix.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bdb.Close() })

	cfg := Config{}
	if encrypted {
		cfg.Secret = benchDBKey(1)
		cfg.EncryptKeys = true
		cfg.EncryptValues = true
		cfg.EncryptMetadata = true
	}
	store, err := Open(context.Background(), bdb, cfg)
	if err != nil {
		b.Fatal(err)
	}
	return store
}

func benchSeedRecords(b *testing.B, store *Store, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		if err := store.Save(context.Background(), &benchRecord{
			Name:  fmt.Sprintf("name-%d", i),
			Group: fmt.Sprintf("g-%d", i%10),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSaveFindMatrix measures the encryption overhead on the record write
// path, an indexed exact lookup and a matcher query returning a bounded page.
func BenchmarkSaveFindMatrix(b *testing.B) {
	ctx := context.Background()

	for _, encrypted := range []bool{false, true} {
		label := "plain"
		if encrypted {
			label = "encrypted"
		}

		b.Run(label+"/save", func(b *testing.B) {
			store := openBenchDB(b, encrypted)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := &benchRecord{Name: fmt.Sprintf("name-%d", i), Group: "g-1"}
				if err := store.Save(ctx, rec); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(label+"/find-index", func(b *testing.B) {
			store := openBenchDB(b, encrypted)
			benchSeedRecords(b, store, 1000)

			var out []benchRecord
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out = out[:0]
				if err := store.Find(ctx, "Name", "name-500", &out); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(label+"/query-filter", func(b *testing.B) {
			store := openBenchDB(b, encrypted)
			benchSeedRecords(b, store, 1000)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out []benchRecord
				if err := store.Select(q.Eq("Group", "g-5")).Limit(50).Find(ctx, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
