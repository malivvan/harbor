package fs

import (
	"context"
	"fmt"
	"os"

	"github.com/malivvan/storm/internal/encryption"
	bolt "go.etcd.io/bbolt"
)

// withKey returns an encryption state that writes with newKey while keeping the
// current encryption flags and cipher.
func (s *Store) withKey(newKey []byte) (*encState, error) {
	if s.enc == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(newKey))
	}

	engine, err := encryption.NewCipherEngine(s.enc.cipher, newKey)
	if err != nil {
		return nil, err
	}

	return &encState{
		engine:      engine,
		engines:     []CipherEngine{engine},
		cipher:      s.enc.cipher,
		encContent:  s.enc.encContent,
		encMetadata: s.enc.encMetadata,
		chunkSize:   s.enc.chunkSize,
	}, nil
}

// bucketKeys returns a copy of all keys in a bucket.
func bucketKeys(bucket *bolt.Bucket) [][]byte {
	var keys [][]byte
	_ = bucket.ForEach(func(key, _ []byte) error {
		keys = append(keys, append([]byte(nil), key...))
		return nil
	})
	return keys
}

// reEncryptFS rewrites one filesystem with next's key, using s.enc for reads.
func (s *Store) reEncryptFS(ctx context.Context, name string, next *encState) error {
	path := s.bucketPath(name)

	return s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		bb, err := openBucket(tx, path)
		if err != nil {
			return err
		}
		oldB := newFsBucket(bb)
		if oldB == nil {
			return os.ErrNotExist
		}
		oldB.enc = s.enc

		newB := newFsBucket(bb)
		newB.enc = next

		for _, key := range bucketKeys(oldB.inodes) {
			if len(key) != 8 {
				continue
			}
			node := new(iNode)
			if err := oldB.decodeNode(b2i(key), node); err != nil {
				return err
			}
			if err := newB.encodeNode(b2i(key), node); err != nil {
				return err
			}
		}

		for _, key := range bucketKeys(oldB.data) {
			plain, err := s.enc.openContent(oldB.data.Get(key))
			if err != nil {
				return err
			}
			sealed, err := next.sealContent(plain)
			if err != nil {
				return err
			}
			if err := newB.data.Put(key, sealed); err != nil {
				return err
			}
		}

		for _, key := range bucketKeys(oldB.symlinks) {
			plain, err := s.enc.openContent(oldB.symlinks.Get(key))
			if err != nil {
				return err
			}
			sealed, err := next.sealContent(plain)
			if err != nil {
				return err
			}
			if err := newB.symlinks.Put(key, sealed); err != nil {
				return err
			}
		}

		if oldB.chunks != nil {
			for _, key := range bucketKeys(oldB.chunks) {
				plain, err := s.enc.openContent(oldB.chunks.Get(key))
				if err != nil {
					return err
				}
				sealed, err := next.sealContent(plain)
				if err != nil {
					return err
				}
				if err := newB.chunks.Put(key, sealed); err != nil {
					return err
				}
			}
		}

		marker, err := next.encodeMarker()
		if err != nil {
			return err
		}
		return newB.state.Put([]byte(markerKey), marker)
	})
}

// ReEncryptFS rewrites one filesystem's inodes, content, chunks, symlinks and
// marker under newKey, then adopts the new key for the Store.
//
// When the Store manages several filesystems they all share one key; use
// RotateAll so every filesystem is re-encrypted before the old key is retired.
func (s *Store) ReEncryptFS(ctx context.Context, name string, newKey []byte) error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return ErrNilParam
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}
	if err := s.reEncryptFS(ctx, name, next); err != nil {
		return err
	}
	s.adoptEnc(next)
	return nil
}

// RotateAll re-encrypts every filesystem managed by the Store under newKey.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}

	names, err := s.FileSystems()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.reEncryptFS(ctx, name, next); err != nil {
			return err
		}
	}

	s.adoptEnc(next)
	return nil
}

// VerifyAll re-reads the marker, inodes, content, chunks and symlinks of every
// filesystem to confirm they decrypt with the configured keys.
func (s *Store) VerifyAll(ctx context.Context) error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	names, err := s.FileSystems()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.verifyFS(name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) verifyFS(name string) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, s.bucketPath(name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		if b == nil {
			return os.ErrNotExist
		}
		b.enc = s.enc

		if s.enc != nil {
			m, canary, found, err := readMarker(b)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("%w: missing encryption marker", ErrWrongMode)
			}
			if !m.matches(s.enc) {
				return fmt.Errorf("%w: encryption settings do not match the filesystem", ErrWrongMode)
			}
			if err := s.enc.verifyCanary(canary); err != nil {
				return err
			}
		}

		for _, key := range bucketKeys(b.inodes) {
			if len(key) != 8 {
				continue
			}
			node := new(iNode)
			if err := b.decodeNode(b2i(key), node); err != nil {
				return err
			}
		}

		for _, bucket := range []*bolt.Bucket{b.data, b.symlinks, b.chunks} {
			if bucket == nil {
				continue
			}
			for _, key := range bucketKeys(bucket) {
				if _, err := s.enc.openContent(bucket.Get(key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
