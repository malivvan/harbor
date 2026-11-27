package search

import (
	"bytes"
	"context"
	"errors"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/internal/crypto"
)

func rotateKeys() (oldKey, newKey []byte) {
	return bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
}

func TestSearchRotateAll(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, bdb := testStore(t, Config{Secret: oldKey, EncryptValues: true, EncryptTokens: true})

	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := idx.Upsert(ctx, "d1", "alpha beta"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := idx.Upsert(ctx, "d2", "beta gamma"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := s.RotateAll(ctx, newKey); err != nil {
		t.Fatalf("RotateAll: %v", err)
	}

	// The same store keeps serving reads through the adopted key.
	if err := s.VerifyAll(ctx); err != nil {
		t.Fatalf("VerifyAll after rotation: %v", err)
	}
	hits, err := idx.Search(ctx, "alpha", 0)
	if err != nil {
		t.Fatalf("Search after rotation: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "d1" {
		t.Fatalf("Search after rotation = %+v", hits)
	}

	// A fresh process opens the rotated store with the new key only.
	reopened, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        newKey,
		EncryptValues: true,
		EncryptTokens: true,
	})
	if err != nil {
		t.Fatalf("Open with new key: %v", err)
	}
	rotated, err := reopened.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open rotated index: %v", err)
	}
	hits, err = rotated.Search(ctx, "beta", 0)
	if err != nil {
		t.Fatalf("Search with new key: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("Search(beta) = %+v, want both documents", hits)
	}

	// The previous key no longer decrypts the rotated values.
	stale, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        oldKey,
		EncryptValues: true,
		EncryptTokens: true,
	})
	if err != nil {
		t.Fatalf("Open with old key: %v", err)
	}
	if _, err := stale.Open("docs", Config{}); !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("Open with old key = %v, want ErrAuthFailed", err)
	}
}

func TestSearchVerifyAll(t *testing.T) {
	ctx := context.Background()
	key, _ := rotateKeys()
	s, bdb := testStore(t, Config{Secret: key, EncryptValues: true})
	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := idx.Upsert(ctx, "d1", "alpha"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := s.VerifyAll(ctx); err != nil {
		t.Fatalf("VerifyAll: %v", err)
	}

	// Tamper with the sealed document value.
	if err := bdb.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucket.SEARCH))
		b := root.Bucket([]byte("docs")).Bucket([]byte(docsBucket))
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			corrupt := append([]byte(nil), v...)
			corrupt[len(corrupt)-1] ^= 0xff
			return b.Put(append([]byte(nil), k...), corrupt)
		})
	}); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	if err := s.VerifyAll(ctx); !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("VerifyAll after tamper = %v, want ErrAuthFailed", err)
	}
}

func TestSearchVerifyAllPlaintext(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	if err := s.VerifyAll(ctx); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("VerifyAll on plaintext store = %v, want ErrInvalidConfig", err)
	}
	if err := s.RotateAll(ctx, bytes.Repeat([]byte{3}, 32)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("RotateAll on plaintext store = %v, want ErrInvalidConfig", err)
	}
}

func TestSearchReEncryptIndex(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, bdb := testStore(t, Config{Secret: oldKey, EncryptValues: true})
	for _, name := range []string{"a", "b"} {
		idx, err := s.Open(name, Config{})
		if err != nil {
			t.Fatalf("Open %s: %v", name, err)
		}
		if err := idx.Upsert(ctx, "d1", "alpha "+name); err != nil {
			t.Fatalf("Upsert %s: %v", name, err)
		}
	}

	if err := s.ReEncryptIndex(ctx, "a", newKey); err != nil {
		t.Fatalf("ReEncryptIndex: %v", err)
	}
	// The store keeps the previous key as a fallback, so both indexes stay
	// readable in this process.
	for _, name := range []string{"a", "b"} {
		idx, err := s.Open(name, Config{})
		if err != nil {
			t.Fatalf("Open %s after partial rotation: %v", name, err)
		}
		hits, err := idx.Search(ctx, "alpha", 0)
		if err != nil || len(hits) != 1 {
			t.Fatalf("Search %s after partial rotation = %+v, %v", name, hits, err)
		}
	}

	// RotateAll completes the rotation and drops the fallback.
	if err := s.RotateAll(ctx, newKey); err != nil {
		t.Fatalf("RotateAll: %v", err)
	}
	reopened, err := Open(ctx, bdb, Config{
		Root:          [][]byte{[]byte(bucket.SEARCH)},
		Secret:        newKey,
		EncryptValues: true,
	})
	if err != nil {
		t.Fatalf("Open with new key: %v", err)
	}
	idx, err := reopened.Open("b", Config{})
	if err != nil {
		t.Fatalf("Open rotated index b: %v", err)
	}
	hits, err := idx.Search(ctx, "alpha", 0)
	if err != nil || len(hits) != 1 {
		t.Fatalf("Search b with new key = %+v, %v", hits, err)
	}
}

func TestSearchReEncryptIndexRejectsTokens(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := rotateKeys()
	s, _ := testStore(t, Config{Secret: oldKey, EncryptTokens: true})
	idx, err := s.Open("docs", Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := idx.Upsert(ctx, "d1", "alpha"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := s.ReEncryptIndex(ctx, "docs", newKey); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ReEncryptIndex with encrypted tokens = %v, want ErrInvalidConfig", err)
	}
}
