package search

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/malivvan/tempest/internal/crypto"
	bolt "go.etcd.io/bbolt"
)

// withKey returns the encryption state a full rotation moves onto: values and
// term keys are written with newKey, and no previous key is kept.
func (s *Store) withKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if !cur.encrypted() {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != crypto.KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, crypto.KeySize, len(newKey))
	}
	next, err := resolveEnc(Config{
		Secret:        append([]byte(nil), newKey...),
		EncryptValues: cur.encValues,
		EncryptTokens: cur.encTokens,
	})
	if err != nil {
		return nil, err
	}
	next.comp = cur.comp
	return next, nil
}

// withRotationKey returns the state a single-index rotation moves onto: the
// previous key stays available for decryption, so the indexes that were not
// rewritten keep working until RotateAll.
func (s *Store) withRotationKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if !cur.encrypted() {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != crypto.KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, crypto.KeySize, len(newKey))
	}
	kc, err := cur.kc.WithKeyKeepingFallbacks(newKey)
	if err != nil {
		return nil, err
	}
	siv, err := crypto.NewSIVNameCipher(newKey, "tempest/fts/v1")
	if err != nil {
		return nil, err
	}
	return &encState{kc: kc, siv: siv, encValues: cur.encValues, encTokens: cur.encTokens, comp: cur.comp}, nil
}

// adoptEnc installs next as the encryption state of the store.
func (s *Store) adoptEnc(next *encState) {
	s.rootStore().enc.Store(next)
}

// RotateAll re-encrypts the metadata, every document value, every posting list
// and any SIV-encrypted term key under newKey in one write transaction, then
// adopts newKey and drops the previous one.
//
// It requires exclusive access: normal operations take the read side of the
// rotation lock, so no writer or reader observes a half-rotated index. It is
// rejected on a store bound to a caller-owned transaction.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s != nil && s.base != nil {
		return s.base.RotateAll(ctx, newKey)
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: store is not open", ErrInvalidConfig)
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}

	s.rotMu.Lock()
	defer s.rotMu.Unlock()

	err = s.db.Update(func(tx *bolt.Tx) error {
		r := &rotation{ctx: ctx, store: s, old: s.encState(), next: next}
		return r.all(tx)
	})
	if err != nil {
		return err
	}
	s.adoptEnc(next)
	return nil
}

// VerifyAll re-reads the metadata, every document value, every posting list and
// every encrypted term key to confirm they decrypt with the configured keys.
// Any value that fails authentication is reported as crypto.ErrAuthFailed.
func (s *Store) VerifyAll(ctx context.Context) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s != nil && s.base != nil {
		return s.base.VerifyAll(ctx)
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: store is not open", ErrInvalidConfig)
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.view(func(tx *bolt.Tx) error {
		enc := s.encState()
		if !enc.encrypted() {
			return fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
		}
		r := &rotation{ctx: ctx, store: s, old: enc}
		return r.all(tx)
	})
}

// ReEncryptIndex rewrites one index under newKey, keeping the previous key as a
// fallback. It is rejected with ErrInvalidConfig when term keys are encrypted:
// those deterministic keys are derived from one master key, so once the new key
// is installed the indexes that were not rewritten could no longer be found.
//
// A partial rotation must be completed with RotateAll before the store is
// closed: a fresh process cannot read the whole store until then.
func (s *Store) ReEncryptIndex(ctx context.Context, name string, newKey []byte) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s != nil && s.base != nil {
		return s.base.ReEncryptIndex(ctx, name, newKey)
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: store is not open", ErrInvalidConfig)
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidName)
	}
	if err := validateName(name); err != nil {
		return err
	}
	enc := s.encState()
	if enc != nil && enc.encTokens {
		return fmt.Errorf("%w: a single index cannot be rotated when term keys are encrypted; use RotateAll", ErrInvalidConfig)
	}

	next, err := s.withRotationKey(newKey)
	if err != nil {
		return err
	}

	s.rotMu.Lock()
	defer s.rotMu.Unlock()

	err = s.db.Update(func(tx *bolt.Tx) error {
		b, err := s.indexBucket(tx, name)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, name)
		}
		r := &rotation{ctx: ctx, store: s, old: enc, next: next}
		return r.index(tx, name)
	})
	if err != nil {
		return err
	}
	s.adoptEnc(next)
	return nil
}

// rotation walks the encrypted parts of a store. With next set it re-encrypts;
// with next nil it only verifies that everything decrypts.
type rotation struct {
	ctx   context.Context
	store *Store
	old   *encState
	next  *encState
}

// verify reports whether the walk is read-only.
func (r *rotation) verify() bool { return r.next == nil }

// all rotates (or verifies) every index in the store.
func (r *rotation) all(tx *bolt.Tx) error {
	names, err := r.indexNames(tx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if err := r.index(tx, name); err != nil {
			return err
		}
	}
	return nil
}

// indexNames lists the index buckets under the store's root.
func (r *rotation) indexNames(tx *bolt.Tx) ([]string, error) {
	var names []string
	add := func(name []byte) {
		names = append(names, string(name))
	}
	if len(r.store.root) > 0 {
		root := r.store.rootBucket(tx)
		if root == nil {
			return nil, nil
		}
		if err := root.ForEach(func(k, v []byte) error {
			if v == nil {
				add(k)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	} else if err := tx.ForEach(func(k []byte, _ *bolt.Bucket) error {
		if len(k) > 0 && k[0] == '_' {
			return nil
		}
		add(k)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// index rotates (or verifies) one index: metadata, documents and posting lists.
func (r *rotation) index(tx *bolt.Tx, name string) error {
	b, err := r.store.indexBucket(tx, name)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("%w: %q", ErrIndexNotFound, name)
	}

	// Metadata.
	if raw := b.Get([]byte(metaKey)); raw != nil {
		value, changed, err := r.value(name, "metadata", raw)
		if err != nil {
			return err
		}
		if changed {
			if err := crypto.ApplyEntries(b, [][]byte{append([]byte(nil), []byte(metaKey)...)},
				[]crypto.Pair{{Key: []byte(metaKey), Value: value}}); err != nil {
				return err
			}
		}
	}

	if docs := b.Bucket([]byte(docsBucket)); docs != nil {
		if err := r.entries(name, docs, false); err != nil {
			return err
		}
	}
	if terms := b.Bucket([]byte(termsBucket)); terms != nil {
		if err := r.entries(name, terms, true); err != nil {
			return err
		}
	}
	return nil
}

// entries rotates (or verifies) one sub-bucket of an index. keys says whether
// the bucket keys may be SIV-encrypted term keys.
func (r *rotation) entries(name string, b *bolt.Bucket, keys bool) error {
	var (
		drop    [][]byte
		changed []crypto.Pair
	)
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if err := r.ctx.Err(); err != nil {
			return err
		}
		newKey := k
		keyChanged := false
		if keys && r.old.encTokens {
			plain, err := r.old.siv.OpenName(k)
			if err != nil {
				return fmt.Errorf("search: index %q: decrypt term key: %w", name, err)
			}
			if !r.verify() {
				newKey, err = r.next.siv.SealName(plain)
				if err != nil {
					return err
				}
				keyChanged = !bytes.Equal(newKey, k)
			}
		}
		newValue, valueChanged, err := r.value(name, "value", v)
		if err != nil {
			return err
		}
		if r.verify() || (!keyChanged && !valueChanged) {
			return nil
		}
		drop = append(drop, append([]byte(nil), k...))
		changed = append(changed, crypto.Pair{
			Key:   append([]byte(nil), newKey...),
			Value: append([]byte(nil), newValue...),
		})
		return nil
	}); err != nil {
		return err
	}
	if r.verify() || len(changed) == 0 {
		return nil
	}
	return crypto.ApplyEntries(b, drop, changed)
}

// value re-seals raw under the next key, or verifies it decrypts. changed
// reports whether the ciphertext differs from the stored bytes.
func (r *rotation) value(index, what string, raw []byte) (value []byte, changed bool, err error) {
	if !r.old.encValues {
		return raw, false, nil
	}
	plain, err := r.old.kc.Open(raw)
	if err != nil {
		return nil, false, fmt.Errorf("search: index %q: decrypt %s: %w", index, what, err)
	}
	if r.verify() {
		return raw, false, nil
	}
	sealed, err := r.next.kc.Seal(plain)
	if err != nil {
		return nil, false, err
	}
	return sealed, !bytes.Equal(sealed, raw), nil
}
