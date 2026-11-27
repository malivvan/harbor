package graphs

import (
	"bytes"
	"context"
	"fmt"

	"github.com/malivvan/tempest/internal/crypto"
	bolt "go.etcd.io/bbolt"
)

// withKey returns the encryption state a full rotation moves onto: records and
// the canary are written with newKey and no previous key is kept.
func (s *Store) withKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if cur == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != crypto.KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, crypto.KeySize, len(newKey))
	}
	kc, err := crypto.NewKeychain([][]byte{append([]byte(nil), newKey...)})
	if err != nil {
		return nil, err
	}
	return &encState{kc: kc}, nil
}

// withRotationKey keeps the previous key available for decryption, so the
// graphs that were not rewritten yet stay readable until RotateAll.
func (s *Store) withRotationKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if cur == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != crypto.KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, crypto.KeySize, len(newKey))
	}
	kc, err := cur.kc.WithKeyKeepingFallbacks(newKey)
	if err != nil {
		return nil, err
	}
	return &encState{kc: kc}, nil
}

// adoptEnc installs next as the encryption state of the store.
func (s *Store) adoptEnc(next *encState) {
	s.rootStore().enc.Store(next)
}

// RotateAll re-encrypts every live and closed node and edge record plus the
// canary under newKey in one write transaction, then adopts newKey and drops
// the previous one.
//
// It requires exclusive access: normal operations take the read side of the
// rotation lock. It is rejected on a store bound to a caller-owned transaction.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s != nil && s.base != nil {
		return s.base.RotateAll(ctx, newKey)
	}
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.readOnly {
		return ErrReadOnly
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}

	s.rotMu.Lock()
	defer s.rotMu.Unlock()

	err = s.db.Update(func(tx *bolt.Tx) error {
		r := &rotation{ctx: ctx, store: s, old: s.encState(), next: next}
		names, err := r.graphNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := r.graph(tx, name); err != nil {
				return err
			}
		}
		return r.canary(tx)
	})
	if err != nil {
		return err
	}
	s.adoptEnc(next)
	return nil
}

// VerifyAll re-reads the canary, every live record and every closed version to
// confirm they decrypt with the configured keys. A value that fails
// authentication is reported as crypto.ErrAuthFailed.
func (s *Store) VerifyAll(ctx context.Context) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s != nil && s.base != nil {
		return s.base.VerifyAll(ctx)
	}
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.view(ctx, func(tx *bolt.Tx) error {
		enc := s.encState()
		if enc == nil {
			return fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
		}
		root := s.rootBucket(tx)
		if root == nil {
			return fmt.Errorf("%w: missing root bucket", ErrWrongMode)
		}
		raw := root.Get([]byte(canaryKey))
		if raw == nil {
			return fmt.Errorf("%w: missing canary", ErrWrongMode)
		}
		plain, err := enc.kc.Open(raw)
		if err != nil {
			return err
		}
		if string(plain) != canaryPlaintext+"/0x01" {
			return ErrWrongMode
		}

		r := &rotation{ctx: ctx, store: s, old: enc}
		names, err := r.graphNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := r.graph(tx, name); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReEncryptGraph rewrites one graph's records under newKey and installs newKey
// as the write key, keeping the previous key as a fallback. Graph names and
// record keys are plaintext, so the remaining graphs stay readable until
// RotateAll completes the rotation.
func (s *Store) ReEncryptGraph(ctx context.Context, name string, newKey []byte) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s != nil && s.base != nil {
		return s.base.ReEncryptGraph(ctx, name, newKey)
	}
	if s == nil || s.db == nil {
		return ErrNilParam
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.readOnly {
		return ErrReadOnly
	}
	if err := validateName(name); err != nil {
		return err
	}
	next, err := s.withRotationKey(newKey)
	if err != nil {
		return err
	}

	s.rotMu.Lock()
	defer s.rotMu.Unlock()

	err = s.db.Update(func(tx *bolt.Tx) error {
		root := s.rootBucket(tx)
		if root == nil || root.Bucket([]byte(name)) == nil {
			return fmt.Errorf("%w: %q", ErrInvalidName, name)
		}
		r := &rotation{ctx: ctx, store: s, old: s.encState(), next: next}
		return r.graph(tx, name)
	})
	if err != nil {
		return err
	}
	s.adoptEnc(next)
	return nil
}

// rotation walks the encrypted records of a store. With next set it
// re-encrypts; with next nil it only verifies that everything decrypts.
type rotation struct {
	ctx   context.Context
	store *Store
	old   *encState
	next  *encState
}

// verify reports whether the walk is read-only.
func (r *rotation) verify() bool { return r.next == nil }

// graphNames lists the graph buckets under the store's root.
func (r *rotation) graphNames(tx *bolt.Tx) ([]string, error) {
	root := r.store.rootBucket(tx)
	if root == nil {
		return nil, nil
	}
	var names []string
	if err := root.ForEach(func(k, v []byte) error {
		if v == nil {
			names = append(names, string(k))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

// graph rotates (or verifies) the four record buckets of one graph.
func (r *rotation) graph(tx *bolt.Tx, name string) error {
	root := r.store.rootBucket(tx)
	if root == nil {
		return nil
	}
	gb := root.Bucket([]byte(name))
	if gb == nil {
		return nil
	}
	for _, bucket := range []string{nodesBucket, nodeVersionsBucket, edgesBucket, edgeVersionsBucket} {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		b := gb.Bucket([]byte(bucket))
		if b == nil {
			continue
		}
		var (
			drop    [][]byte
			changed []crypto.Pair
		)
		if err := b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			plain, err := r.old.kc.Open(v)
			if err != nil {
				return fmt.Errorf("graphs: graph %q bucket %q: decrypt record %q: %w", name, bucket, k, err)
			}
			if r.verify() {
				return nil
			}
			sealed, err := r.next.kc.Seal(plain)
			if err != nil {
				return err
			}
			if bytes.Equal(sealed, v) {
				return nil
			}
			drop = append(drop, append([]byte(nil), k...))
			changed = append(changed, crypto.Pair{Key: append([]byte(nil), k...), Value: sealed})
			return nil
		}); err != nil {
			return err
		}
		if r.verify() || len(changed) == 0 {
			continue
		}
		if err := crypto.ApplyEntries(b, drop, changed); err != nil {
			return err
		}
	}
	return nil
}

// canary re-seals the store canary under the next key.
func (r *rotation) canary(tx *bolt.Tx) error {
	root := r.store.rootBucket(tx)
	if root == nil {
		return fmt.Errorf("%w: missing root bucket", ErrWrongMode)
	}
	raw := root.Get([]byte(canaryKey))
	if raw == nil {
		return fmt.Errorf("%w: missing canary", ErrWrongMode)
	}
	plain, err := r.old.kc.Open(raw)
	if err != nil {
		return err
	}
	sealed, err := r.next.kc.Seal(plain)
	if err != nil {
		return err
	}
	return root.Put([]byte(canaryKey), sealed)
}
