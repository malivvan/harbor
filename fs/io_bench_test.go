package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func benchFSKey(seed byte) []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// openBenchFS opens a standalone file system for a benchmark with the given
// storage configuration.
func openBenchFS(b *testing.B, cfg Config) *FS {
	b.Helper()

	db, err := bolt.Open(filepath.Join(b.TempDir(), "fs-matrix.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	store, err := Open(context.Background(), db, cfg)
	if err != nil {
		b.Fatal(err)
	}
	filesystem, err := store.Open("bench")
	if err != nil {
		b.Fatal(err)
	}
	return filesystem
}

// BenchmarkWriteReadMatrix compares whole-blob, chunked and encrypted-chunked
// content for whole-file writes and reads at two sizes.
func BenchmarkWriteReadMatrix(b *testing.B) {
	modes := []struct {
		name string
		cfg  Config
	}{
		{"whole", Config{}},
		{"chunked", Config{Secret: benchFSKey(1), ChunkSize: 4096}},
		{"encrypted-chunked", Config{Secret: benchFSKey(2), ChunkSize: 4096, EncryptMetadata: true}},
	}
	sizes := []int{4 << 10, 64 << 10}

	for _, mode := range modes {
		for _, size := range sizes {
			payload := bytes.Repeat([]byte("h"), size)

			b.Run(fmt.Sprintf("%s/size=%d/write", mode.name, size), func(b *testing.B) {
				filesystem := openBenchFS(b, mode.cfg)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f, err := filesystem.OpenFile("/bench.bin", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := f.Write(payload); err != nil {
						b.Fatal(err)
					}
					if err := f.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run(fmt.Sprintf("%s/size=%d/read", mode.name, size), func(b *testing.B) {
				filesystem := openBenchFS(b, mode.cfg)
				f, err := filesystem.OpenFile("/bench.bin", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := f.Write(payload); err != nil {
					b.Fatal(err)
				}
				if err := f.Close(); err != nil {
					b.Fatal(err)
				}

				buf := make([]byte, size)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r, err := filesystem.OpenFile("/bench.bin", os.O_RDONLY, 0)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := io.ReadFull(r, buf); err != nil {
						b.Fatal(err)
					}
					if err := r.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkFastWalkMatrix walks a fixed 50 × 20 tree with one and four workers.
func BenchmarkFastWalkMatrix(b *testing.B) {
	const (
		dirs  = 50
		files = 20
	)
	filesystem := openBenchFS(b, Config{})
	payload := bytes.Repeat([]byte("x"), 128)
	for d := 0; d < dirs; d++ {
		dir := fmt.Sprintf("/tree/d%02d", d)
		if err := filesystem.MkdirAll(dir, 0o755); err != nil {
			b.Fatal(err)
		}
		for f := 0; f < files; f++ {
			name := fmt.Sprintf("%s/f%02d.txt", dir, f)
			file, err := filesystem.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := file.Write(payload); err != nil {
				b.Fatal(err)
			}
			if err := file.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}

	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seen := 0
				walk := func(_ string, _ iofs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					seen++
					return nil
				}
				if err := FastWalkWithConfig(filesystem, "/tree", &FastWalkConfig{NumWorkers: workers}, walk); err != nil {
					b.Fatal(err)
				}
				if seen == 0 {
					b.Fatal("walk visited no entries")
				}
			}
		})
	}
}
