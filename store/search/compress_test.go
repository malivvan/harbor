package search

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/malivvan/tempest/internal/bucket"
	bolt "go.etcd.io/bbolt"
)

func compressText() string {
	return strings.Repeat("full text search compresses repeated terms well ", 100)
}

func compressSecret() []byte { return bytes.Repeat([]byte{7}, 32) }

func indexBytes(t *testing.T, bdb *bolt.DB, index string) int {
	t.Helper()
	var total int
	err := bdb.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucket.SEARCH))
		if root == nil {
			return nil
		}
		b := root.Bucket([]byte(index))
		if b == nil {
			return nil
		}
		for _, name := range []string{docsBucket, termsBucket} {
			sub := b.Bucket([]byte(name))
			if sub == nil {
				continue
			}
			if err := sub.ForEach(func(_, v []byte) error {
				if v != nil {
					total += len(v)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect index: %v", err)
	}
	return total
}

func TestSearchCompressRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := compressSecret()

	compressed, cdb := testStore(t, Config{Secret: key, EncryptValues: true, Compress: true})
	idx, err := compressed.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"d1", "d2", "d3"} {
		if err := idx.Upsert(ctx, id, compressText()+" id "+id); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
	}
	if n, err := idx.Count(ctx); err != nil || n != 3 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	hits, err := idx.Search(ctx, "compresses", 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("Search = %+v", hits)
	}

	plain, pdb := testStore(t, Config{Secret: key, EncryptValues: true})
	plainIdx, err := plain.Open("docs", Config{})
	if err != nil {
		t.Fatalf("plain Open: %v", err)
	}
	for _, id := range []string{"d1", "d2", "d3"} {
		if err := plainIdx.Upsert(ctx, id, compressText()+" id "+id); err != nil {
			t.Fatalf("plain Upsert %s: %v", id, err)
		}
	}

	compressedSize := indexBytes(t, cdb, "docs")
	plainSize := indexBytes(t, pdb, "docs")
	if compressedSize == 0 || compressedSize >= plainSize {
		t.Fatalf("compressed index %d bytes, plain %d bytes", compressedSize, plainSize)
	}
}

func TestSearchCompressOpenWithoutCompression(t *testing.T) {
	ctx := context.Background()
	key := compressSecret()
	path := filepath.Join(t.TempDir(), "fts.db")
	bdb, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })

	compressed, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        key,
		EncryptValues: true,
		Compress:      true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	idx, err := compressed.Open("docs", Config{})
	if err != nil {
		t.Fatalf("index Open: %v", err)
	}
	if err := idx.Upsert(ctx, "d1", compressText()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Reopening without compression still decodes the envelope.
	plain, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        key,
		EncryptValues: true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	plainIdx, err := plain.Open("docs", Config{})
	if err != nil {
		t.Fatalf("reopen index: %v", err)
	}
	hits, err := plainIdx.Search(ctx, "compresses", 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "d1" {
		t.Fatalf("Search = %+v", hits)
	}
}

func TestSearchCompressRotation(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := bytes.Repeat([]byte{8}, 32), bytes.Repeat([]byte{9}, 32)
	path := filepath.Join(t.TempDir(), "fts.db")
	bdb, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        oldKey,
		EncryptValues: true,
		EncryptTokens: true,
		Compress:      true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	idx, err := store.Open("docs", Config{})
	if err != nil {
		t.Fatalf("index Open: %v", err)
	}
	if err := idx.Upsert(ctx, "d1", compressText()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := store.RotateAll(ctx, newKey); err != nil {
		t.Fatalf("RotateAll: %v", err)
	}

	again, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        newKey,
		EncryptValues: true,
		EncryptTokens: true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	againIdx, err := again.Open("docs", Config{})
	if err != nil {
		t.Fatalf("reopen index: %v", err)
	}
	hits, err := againIdx.Search(ctx, "compresses", 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "d1" {
		t.Fatalf("Search after rotation = %+v", hits)
	}
}
