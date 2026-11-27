package vector

import (
	"bytes"
	"context"
	"fmt"

	"github.com/malivvan/tempest/internal/crypto"
	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// withRotationKey returns the state a single-collection rotation moves onto: the
// previous key stays available for decryption, so the collections that were not
// rewritten keep working until RotateAll.
func (s *Store) withRotationKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if !cur.encrypted() {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}

	kc, err := cur.kc.WithKeyKeepingFallbacks(newKey)
	if err != nil {
		return nil, err
	}
	return &encState{
		kc:        kc,
		encPoints: cur.encPoints,
		encIDs:    cur.encIDs,
		encNames:  cur.encNames,
		sivs:      cur.sivs,
		comp:      cur.comp,
	}, nil
}

// withKey returns an encryption state that writes with newKey while keeping the
// current cipher and flags, dropping every current key.
func (s *Store) withKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if !cur.encrypted() {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(newKey))
	}

	next, err := resolveConfig(Config{
		Secret:                 append([]byte(nil), newKey...),
		EncryptPoints:          cur.encPoints,
		EncryptPointIDs:        cur.encIDs,
		EncryptCollectionNames: cur.encNames,
	})
	if err != nil {
		return nil, err
	}
	next.comp = cur.comp
	return next, nil
}

// adoptEnc installs next as the encryption state of the store.
func (s *Store) adoptEnc(next *encState) {
	s.rootStore().enc.Store(next)
}

// ReEncryptCollection rewrites one collection and its metadata under newKey and
// installs newKey as the write key.
//
// The marker intentionally stays under the current key, and the previous key
// remains available for decryption in this process, so the collections that
// were not rotated yet keep working. This is a partial rotation: a fresh
// process cannot read the whole store until RotateAll completes it. Do not
// close a store after ReEncryptCollection; call RotateAll first.
//
// With EncryptPointIDs or EncryptCollectionNames it cannot run at all: those
// deterministic keys are derived from one master key, so once the new key is
// installed the collections that were not rewritten can neither be found nor
// decrypted. Those stores must be rotated with RotateAll, and ReEncryptCollection
// reports ErrInvalidConfig instead of leaving the database unrecoverable.
//
// Rotation must not run concurrently with other operations on the store.
func (s *Store) ReEncryptCollection(ctx context.Context, name string, newKey []byte) error {
	if s != nil && s.tx != nil {
		return ErrInTransaction
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: store is not open", ErrInvalidConfig)
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("%w: collection name is required", ErrInvalidConfig)
	}
	enc := s.encState()
	if enc != nil && (enc.encIDs || enc.encNames) {
		return fmt.Errorf("%w: a single collection cannot be rotated when point IDs or collection names are encrypted; use RotateAll", ErrInvalidConfig)
	}

	next, err := s.withRotationKey(newKey)
	if err != nil {
		return err
	}

	s.rotMu.Lock()
	defer s.rotMu.Unlock()

	err = s.db.Update(func(tx *bbolt.Tx) error {
		r := &rotation{ctx: ctx, store: s, old: enc, next: next}

		physical, err := s.bucketKey(name)
		if err != nil {
			return fmt.Errorf("failed to derive the bucket key for collection %q: %w", name, err)
		}
		if !r.bucketExists(tx, physical) {
			return fmt.Errorf("collection bucket %s not found", name)
		}
		if err := r.collection(tx, string(physical)); err != nil {
			return err
		}
		if err := r.metadataEntry(tx, name); err != nil {
			return err
		}
		// Keep the old marker: the other collections still hold old-key
		// ciphertext. RotateAll publishes the new marker once everything is
		// rewritten.
		return nil
	})
	if err != nil {
		return err
	}

	s.adoptEnc(next)
	return nil
}

// RotateAll re-encrypts every collection, all collection metadata and the marker
// canary under newKey, then adopts the new key and drops the previous one.
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
	if err := checkContext(ctx); err != nil {
		return err
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return err
	}

	s.rotMu.Lock()
	defer s.rotMu.Unlock()

	err = s.db.Update(func(tx *bbolt.Tx) error {
		r := &rotation{ctx: ctx, store: s, old: s.encState(), next: next}

		names, err := r.collectionNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if err := r.collection(tx, name); err != nil {
				return err
			}
		}
		if err := r.metadata(tx); err != nil {
			return err
		}
		return r.writeMarker(tx)
	})
	if err != nil {
		return err
	}

	s.adoptEnc(next)
	return nil
}

// VerifyAll re-reads the marker, every point, every collection metadata entry and
// every encrypted name to confirm they decrypt with the configured keys.
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
	if err := checkContext(ctx); err != nil {
		return err
	}
	return s.view(func(tx *bbolt.Tx) error {
		enc := s.encState()
		if !enc.encrypted() {
			return fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
		}
		if err := checkContext(ctx); err != nil {
			return err
		}

		meta, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if meta == nil {
			return fmt.Errorf("%w: missing encryption marker", ErrWrongMode)
		}
		raw := meta.Get([]byte(markerKey))
		if raw == nil {
			return fmt.Errorf("%w: missing encryption marker", ErrWrongMode)
		}
		m, canary, err := decodeMarker(raw)
		if err != nil {
			return err
		}
		if !m.matches(enc) {
			return fmt.Errorf("%w: encryption settings do not match the store", ErrWrongMode)
		}
		if err := enc.verifyCanary(canary, m); err != nil {
			return err
		}

		r := &rotation{ctx: ctx, store: s, old: enc}

		names, err := r.collectionNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if err := r.collection(tx, name); err != nil {
				return err
			}
		}

		return r.metadata(tx)
	})
}

// rotation walks every encrypted structure of a store. With next set it
// re-encrypts; with next nil it only verifies that everything decrypts.
type rotation struct {
	ctx   context.Context
	store *Store
	old   *encState
	next  *encState
}

// verify reports whether the walk is read-only.
func (r *rotation) verify() bool {
	return r.next == nil
}

// collectionNames returns the stored (physical) names of all collection
// buckets, skipping the metadata bucket.
func (r *rotation) collectionNames(tx *bbolt.Tx) ([]string, error) {
	var names []string

	add := func(name []byte) error {
		if string(name) == collectionsMetaBucket {
			return nil
		}
		if r.verify() {
			// Verification must also prove the stored name decrypts.
			if _, err := r.old.openName(name, keyDomainName); err != nil {
				return fmt.Errorf("failed to decrypt collection name %q: %w", name, err)
			}
		}
		names = append(names, string(name))
		return nil
	}

	if len(r.store.root) > 0 {
		root := r.store.rootBucket(tx)
		if root == nil {
			return nil, nil
		}
		if err := root.ForEach(func(name, value []byte) error {
			if value != nil {
				return nil
			}
			return add(name)
		}); err != nil {
			return nil, err
		}
		return names, nil
	}

	if err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		if reservedTopLevelBuckets[string(name)] {
			return nil
		}
		return add(name)
	}); err != nil {
		return nil, err
	}
	return names, nil
}

// collection rotates (or verifies) one collection bucket, renaming it when the
// encrypted name changes.
func (r *rotation) collection(tx *bbolt.Tx, physicalName string) error {
	b := r.bucket(tx, []byte(physicalName))
	if b == nil {
		return nil
	}

	var (
		changed []crypto.Pair
		drop    [][]byte
	)

	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if err := checkContext(r.ctx); err != nil {
			return err
		}

		newKey := k
		if r.old.encIDs {
			plainID, err := r.old.openName(k, keyDomainID)
			if err != nil {
				return fmt.Errorf("failed to decrypt point ID %q: %w", k, err)
			}
			if !r.verify() {
				newKey, err = r.next.sealName(plainID, keyDomainID)
				if err != nil {
					return err
				}
			}
		}

		newValue := v
		if r.old.encPoints {
			plain, err := r.old.openPoints(v)
			if err != nil {
				return fmt.Errorf("failed to decrypt point %q: %w", k, err)
			}
			if !r.verify() {
				newValue, err = r.next.sealPoints(plain)
				if err != nil {
					return err
				}
			}
		}

		if r.verify() || (bytes.Equal(newKey, k) && bytes.Equal(newValue, v)) {
			return nil
		}

		changed = append(changed, crypto.Pair{Key: append([]byte(nil), newKey...), Value: append([]byte(nil), newValue...)})
		drop = append(drop, append([]byte(nil), k...))
		return nil
	}); err != nil {
		return err
	}

	if r.verify() {
		return nil
	}

	// A renamed collection needs a new bucket: bbolt cannot rename one.
	newPhysical := []byte(physicalName)
	if r.old.encNames || r.next.encNames {
		plainName, err := r.old.openName([]byte(physicalName), keyDomainName)
		if err != nil {
			return fmt.Errorf("failed to decrypt collection name %q: %w", physicalName, err)
		}
		newPhysical, err = r.next.sealName(plainName, keyDomainName)
		if err != nil {
			return err
		}
	}

	if bytes.Equal(newPhysical, []byte(physicalName)) {
		return crypto.ApplyEntries(b, drop, changed)
	}

	target, err := r.createBucket(tx, newPhysical)
	if err != nil {
		return err
	}
	if err := crypto.ApplyEntries(target, drop, changed); err != nil {
		return err
	}
	return r.deleteBucket(tx, []byte(physicalName))
}

// metadata rotates (or verifies) every collection metadata entry.
func (r *rotation) metadata(tx *bbolt.Tx) error {
	b, err := r.store.metaBucket(tx, false)
	if err != nil || b == nil {
		return err
	}

	var (
		changed []crypto.Pair
		drop    [][]byte
	)

	if err := b.ForEach(func(k, v []byte) error {
		if v == nil || string(k) == markerKey {
			return nil
		}
		if err := checkContext(r.ctx); err != nil {
			return err
		}

		newKey := k
		if r.old.encNames {
			plainName, err := r.old.openName(k, keyDomainName)
			if err != nil {
				return fmt.Errorf("failed to decrypt metadata name %q: %w", k, err)
			}
			if !r.verify() {
				newKey, err = r.next.sealName(plainName, keyDomainName)
				if err != nil {
					return err
				}
			}
		}

		newValue := v
		if r.old.encPoints {
			plain, err := r.old.openPoints(v)
			if err != nil {
				return fmt.Errorf("failed to decrypt metadata %q: %w", k, err)
			}
			if !r.verify() {
				newValue, err = r.next.sealPoints(plain)
				if err != nil {
					return err
				}
			}
		}

		if r.verify() || (bytes.Equal(newKey, k) && bytes.Equal(newValue, v)) {
			return nil
		}

		changed = append(changed, crypto.Pair{Key: append([]byte(nil), newKey...), Value: append([]byte(nil), newValue...)})
		drop = append(drop, append([]byte(nil), k...))
		return nil
	}); err != nil {
		return err
	}

	if r.verify() {
		return nil
	}

	return crypto.ApplyEntries(b, drop, changed)
}

// metadataEntry rotates one collection metadata entry by its logical name.
func (r *rotation) metadataEntry(tx *bbolt.Tx, name string) error {
	b, err := r.store.metaBucket(tx, false)
	if err != nil || b == nil {
		return err
	}

	oldKey, err := r.old.sealName(name, keyDomainName)
	if err != nil {
		return err
	}
	data := b.Get(oldKey)
	if data == nil {
		// Metadata is optional for a collection.
		return nil
	}

	plain, err := r.old.openPoints(data)
	if err != nil {
		return fmt.Errorf("failed to decrypt metadata for %s: %w", name, err)
	}

	newValue, err := r.next.sealPoints(plain)
	if err != nil {
		return err
	}
	newKey, err := r.next.sealName(name, keyDomainName)
	if err != nil {
		return err
	}

	if !bytes.Equal(newKey, oldKey) {
		if err := b.Delete(oldKey); err != nil {
			return err
		}
	}
	return b.Put(newKey, newValue)
}

// writeMarker rewrites the marker canary with the new key.
func (r *rotation) writeMarker(tx *bbolt.Tx) error {
	if r.verify() {
		return nil
	}

	b, err := r.store.metaBucket(tx, true)
	if err != nil {
		return err
	}

	marker, err := encodeMarker(r.next)
	if err != nil {
		return err
	}
	return b.Put([]byte(markerKey), marker)
}

// bucket returns a collection bucket by its physical name.
func (r *rotation) bucket(tx *bbolt.Tx, name []byte) *bbolt.Bucket {
	if len(r.store.root) == 0 {
		return tx.Bucket(name)
	}
	root := r.store.rootBucket(tx)
	if root == nil {
		return nil
	}
	return root.Bucket(name)
}

// bucketExists reports whether a collection bucket exists.
func (r *rotation) bucketExists(tx *bbolt.Tx, name []byte) bool {
	return r.bucket(tx, name) != nil
}

// createBucket creates a collection bucket by its physical name.
func (r *rotation) createBucket(tx *bbolt.Tx, name []byte) (*bbolt.Bucket, error) {
	if len(r.store.root) == 0 {
		return tx.CreateBucketIfNotExists(name)
	}
	root, err := r.store.createRootBucket(tx)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists(name)
}

// deleteBucket deletes a collection bucket by its physical name.
func (r *rotation) deleteBucket(tx *bbolt.Tx, name []byte) error {
	var err error
	if len(r.store.root) == 0 {
		err = tx.DeleteBucket(name)
	} else if root := r.store.rootBucket(tx); root != nil {
		err = root.DeleteBucket(name)
	}
	if err != nil && err != bolterrors.ErrBucketNotFound {
		return err
	}
	return nil
}
