package fstree

import (
	"context"
	"fmt"
	"os"

	"github.com/malivvan/tempest/internal/crypto"
	bolt "go.etcd.io/bbolt"
)

// withKey returns an encryption state that writes with newKey while keeping the
// current encryption flags and cipher, dropping every current key.
func (s *Store) withKey(newKey []byte) (*encState, error) {
	return s.newKeyState(newKey, false)
}

// withKeyKeepingFallbacks returns an encryption state that writes with newKey
// and can still decrypt with the current keys. Per-filesystem rotation uses it
// so the filesystems that were not rotated yet stay readable; RotateAll drops
// the old keys once every filesystem is rewritten.
func (s *Store) withKeyKeepingFallbacks(newKey []byte) (*encState, error) {
	return s.newKeyState(newKey, true)
}

func (s *Store) newKeyState(newKey []byte, keepFallbacks bool) (*encState, error) {
	cur := s.encState()
	if !cur.encrypted() {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(newKey))
	}

	var (
		kc  *crypto.Keychain
		err error
	)
	if keepFallbacks {
		kc, err = cur.kc.WithKeyKeepingFallbacks(newKey)
	} else {
		kc, err = cur.kc.WithKey(newKey)
	}
	if err != nil {
		return nil, err
	}

	return &encState{
		kc:          kc,
		encContent:  cur.encContent,
		encMetadata: cur.encMetadata,
		chunkSize:   cur.chunkSize,
		comp:        cur.comp,
	}, nil
}

// reEncryptFSTx rewrites one filesystem with next's key inside tx, using old
// for reads.
func (s *Store) reEncryptFSTx(ctx context.Context, tx *bolt.Tx, name string, old, next *encState) error {
	path := s.bucketPath(name)
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
	oldB.enc = old

	newB := newFsBucket(bb)
	newB.enc = next

	inodeKeys, err := crypto.KeysOf(oldB.inodes)
	if err != nil {
		return err
	}
	for _, key := range inodeKeys {
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

	dataKeys, err := crypto.KeysOf(oldB.data)
	if err != nil {
		return err
	}
	for _, key := range dataKeys {
		plain, err := old.openContent(oldB.data.Get(key))
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

	symlinkKeys, err := crypto.KeysOf(oldB.symlinks)
	if err != nil {
		return err
	}
	for _, key := range symlinkKeys {
		plain, err := old.openContent(oldB.symlinks.Get(key))
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
		chunkKeys, err := crypto.KeysOf(oldB.chunks)
		if err != nil {
			return err
		}
		for _, key := range chunkKeys {
			plain, err := old.openContent(oldB.chunks.Get(key))
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
}

// ReEncryptFS rewrites one filesystem's inodes, content, chunks, symlinks and
// marker under newKey, then installs newKey as the write key for that
// filesystem.
//
// The previous key stays available for decryption, so the filesystems that were
// not rotated yet keep working. This is a partial rotation: the store is not
// self-consistent for a fresh process until RotateAll completes the migration.
// Do not close a store between ReEncryptFS and RotateAll.
func (s *Store) ReEncryptFS(ctx context.Context, name string, newKey []byte) error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return ErrInTransaction
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return ErrNilParam
	}

	next, err := s.withKeyKeepingFallbacks(newKey)
	if err != nil {
		return err
	}

	// Serialize writers on the target filesystem with the rewrite. The
	// in-process state then uses next as the primary key with the old key as a
	// decryption fallback; a fresh process still needs RotateAll to complete
	// the migration.
	filesystem := s.trackedFS(name)
	if filesystem != nil {
		filesystem.mu.Lock()
		defer filesystem.mu.Unlock()
	}
	old := s.encState()
	if err := s.update(func(tx *bolt.Tx) error {
		return s.reEncryptFSTx(ctx, tx, name, old, next)
	}); err != nil {
		return err
	}
	s.adoptEnc(next)
	return nil
}

// RotateAll re-encrypts every filesystem managed by the Store under newKey and
// drops the previous key once all of them are rewritten.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error {
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if s.tx != nil {
		return ErrInTransaction
	}
	if s.base != nil {
		return s.base.RotateAll(ctx, newKey)
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

	// Block writes to every filesystem this Store opened for the duration of
	// the rotation. All filesystems are rewritten in one transaction, so a
	// failure or cancellation leaves the whole store under the old key.
	s.mu.Lock()
	filesystems := append([]*FS(nil), s.filesystems...)
	s.mu.Unlock()
	for _, filesystem := range filesystems {
		filesystem.mu.Lock()
	}
	defer func() {
		for _, filesystem := range filesystems {
			filesystem.mu.Unlock()
		}
	}()

	old := s.encState()
	if err := s.update(func(tx *bolt.Tx) error {
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := s.reEncryptFSTx(ctx, tx, name, old, next); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
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
	if s.tx != nil {
		return ErrInTransaction
	}
	if s.base != nil {
		return s.base.VerifyAll(ctx)
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
	return s.view(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, s.bucketPath(name))
		if err != nil {
			return err
		}
		b := newFsBucket(bb)
		if b == nil {
			return os.ErrNotExist
		}
		enc := s.encState()
		b.enc = enc

		if enc.encrypted() {
			m, canary, found, err := readMarker(b)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("%w: missing encryption marker", ErrWrongMode)
			}
			if !m.matches(enc) {
				return fmt.Errorf("%w: encryption settings do not match the filesystem", ErrWrongMode)
			}
			if err := enc.verifyCanary(canary, m); err != nil {
				return err
			}
		}

		verifyInodeKeys, err := crypto.KeysOf(b.inodes)
		if err != nil {
			return err
		}
		for _, key := range verifyInodeKeys {
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
			bucketKeys, err := crypto.KeysOf(bucket)
			if err != nil {
				return err
			}
			for _, key := range bucketKeys {
				if _, err := enc.openContent(bucket.Get(key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
