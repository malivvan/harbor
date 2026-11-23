package record

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/malivvan/harbor/internal/crypto"
	"github.com/malivvan/harbor/pkg/metrics"
	"github.com/malivvan/harbor/store"
	"github.com/malivvan/harbor/store/record/index"
	bolt "go.etcd.io/bbolt"
)

// listIndexIDsBucket is the nested bucket that list indexes use to map ids back
// to their index entries.
const listIndexIDsBucket = "harbor__ids"

// withKey returns an encryption state that writes with newKey while keeping the
// current cipher and flags.
func (s *Store) withKey(newKey []byte) (*encState, error) {
	cur := s.encState()
	if cur == nil {
		return nil, fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig)
	}
	if len(newKey) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(newKey))
	}

	return resolveConfig(Config{
		Secret:          append([]byte(nil), newKey...),
		EncryptValues:   cur.encValues,
		EncryptMetadata: cur.encMetadata,
		EncryptKeys:     cur.encKeys,
	})
}

// adoptEnc installs next as the encryption state of the store. Every node
// reaches it through the same Store, so derived nodes and transactions see the
// new key immediately.
func (s *Store) adoptEnc(next *encState) {
	s.rootStore().enc.Store(next)
}

// ReEncrypt rewrites the whole database under newKey: every record and
// key/value entry, every encrypted key, every auto-increment counter and the
// marker canary. The rewrite runs in a single bbolt write transaction, so the
// rotation is atomic, and the store adopts newKey once it commits.
//
// ReEncrypt requires an already encrypted store; there is no plaintext
// migration path. Rotation must not run concurrently with other operations on
// the store.
func (s *Store) ReEncrypt(ctx context.Context, newKey []byte) error {
	if s == nil || s.bolt == nil {
		return wrapError("reencrypt", ErrNilParam)
	}
	if err := checkContext(ctx); err != nil {
		return wrapError("reencrypt", err)
	}

	next, err := s.withKey(newKey)
	if err != nil {
		return wrapError("reencrypt", err)
	}

	// Block normal operations for the whole rewrite + adoption: a writer that
	// starts after the new marker is committed must see the new key.
	s.rotMuOf().Lock()
	defer s.rotMuOf().Unlock()

	started := time.Now()
	if err := s.rewrite(ctx, next); err != nil {
		s.observeRotate(time.Since(started), err)
		return wrapError("reencrypt", err)
	}

	s.adoptEnc(next)
	s.observeRotate(time.Since(started), nil)
	return nil
}

// observeRotate reports one key rotation to the configured observer.
func (s *Store) observeRotate(d time.Duration, err error) {
	if s == nil || s.obs == nil {
		return
	}
	metrics.Histogram(s.obs, metrics.MetricRotateDuration, float64(d), metrics.Labels{
		metrics.LabelStore:  store.RECORD,
		metrics.LabelOp:     "rotate",
		metrics.LabelResult: resultLabel(err),
	})
}

// RotateAll is ReEncrypt under the name used by the ts and fs stores. A Harbor
// database has a single key space, so both rotate everything.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error {
	return s.ReEncrypt(ctx, newKey)
}

// rewrite walks the whole database and re-encrypts it with next.
func (s *Store) rewrite(ctx context.Context, next *encState) error {
	root, ok := s.Node.(*node)
	if !ok {
		return ErrBadType
	}

	return s.bolt.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		r := &rotation{ctx: ctx, root: root, old: s.encState(), next: next}
		if err := r.run(tx); err != nil {
			return err
		}
		return checkContext(ctx)
	})
}

// VerifyAll re-reads the marker, every record, key/value entry, index entry and
// auto-increment counter to confirm they decrypt with the configured keys. Any
// value that fails authentication is reported as ErrAuthFailed.
func (s *Store) VerifyAll(ctx context.Context) error {
	if s == nil || s.bolt == nil {
		return wrapError("verify all", ErrNilParam)
	}
	if err := checkContext(ctx); err != nil {
		return wrapError("verify all", err)
	}
	root, ok := s.Node.(*node)
	if !ok {
		return wrapError("verify all", ErrBadType)
	}

	s.rotMuOf().RLock()
	defer s.rotMuOf().RUnlock()

	enc := s.encState()
	if enc == nil {
		return wrapError("verify all", fmt.Errorf("%w: store is not encrypted", ErrInvalidConfig))
	}

	return wrapError("verify all", s.bolt.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		b := root.getBucket(tx, databaseInfo)
		if b == nil {
			return fmt.Errorf("%w: missing encryption marker", ErrWrongMode)
		}
		raw := b.Get([]byte(markerKey))
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

		r := &rotation{ctx: ctx, root: root, old: enc}
		if err := r.run(tx); err != nil {
			return err
		}
		return checkContext(ctx)
	}))
}

// rotation walks the encrypted structure of a database. With next set it
// re-encrypts; with next nil it only verifies that everything decrypts.
type rotation struct {
	ctx  context.Context
	root *node
	old  *encState
	next *encState
}

// verify reports whether the walk is read-only.
func (r *rotation) verify() bool {
	return r.next == nil
}

// check propagates context cancellation into the walk.
func (r *rotation) check() error {
	return checkContext(r.ctx)
}

// run walks every bucket below the store root.
func (r *rotation) run(tx *bolt.Tx) error {
	c := r.root.cursor(tx)
	if c == nil {
		return nil
	}

	var names []string
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := r.check(); err != nil {
			return err
		}
		if v != nil {
			continue
		}
		names = append(names, string(k))
	}

	for _, name := range names {
		if err := r.check(); err != nil {
			return err
		}
		b := r.root.getBucket(tx, name)
		if b == nil {
			continue
		}
		if name == databaseInfo {
			if err := r.databaseBucket(b); err != nil {
				return fmt.Errorf("record: rotating bucket %q: %w", name, err)
			}
			continue
		}
		if err := r.dataBucket(b); err != nil {
			return fmt.Errorf("record: rotating bucket %q: %w", name, err)
		}
	}

	return nil
}

// dataBucket rotates one record or key/value bucket together with its metadata
// bucket, its index buckets and any nested data buckets.
func (r *rotation) dataBucket(b *bolt.Bucket) error {
	if err := r.entries(b, keyDomainID, keyDomainKV); err != nil {
		return err
	}

	var children []string
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			children = append(children, string(k))
		}
		return nil
	}); err != nil {
		return err
	}

	for _, child := range children {
		if err := r.check(); err != nil {
			return err
		}
		cb := b.Bucket([]byte(child))
		if cb == nil {
			continue
		}

		var err error
		switch {
		case child == metadataBucket:
			err = r.metadataBucket(cb)
		case strings.HasPrefix(child, indexPrefix):
			err = r.indexBucket(cb)
		default:
			err = r.dataBucket(cb)
		}
		if err != nil {
			return fmt.Errorf("record: rotating bucket %q: %w", child, err)
		}
	}

	return nil
}

// databaseBucket rotates the store metadata bucket and rewrites the marker
// canary with the new key.
func (r *rotation) databaseBucket(b *bolt.Bucket) error {
	if err := r.entries(b, keyDomainKV); err != nil {
		return err
	}

	if meta := b.Bucket([]byte(metadataBucket)); meta != nil {
		if err := r.metadataBucket(meta); err != nil {
			return err
		}
	}

	if r.verify() {
		return nil
	}

	marker, err := encodeMarker(r.next)
	if err != nil {
		return err
	}
	return b.Put([]byte(markerKey), marker)
}

// entries rotates the plain key/value entries of a bucket. Keys are encrypted
// in any of the given domains; values are sealed when the store encrypts
// values.
func (r *rotation) entries(b *bolt.Bucket, domains ...string) error {
	var (
		changed []crypto.Pair
		drop    [][]byte
	)

	err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if err := r.check(); err != nil {
			return err
		}
		if string(k) == markerKey {
			return nil
		}

		newKey := k
		if r.old.encKeys {
			plain, domain, err := r.openKeyAnyDomain(k, domains)
			if err != nil {
				return err
			}
			if !r.verify() {
				newKey, err = r.next.sealKey(plain, domain)
				if err != nil {
					return err
				}
			}
		}

		newValue := v
		if r.old.encValues {
			plain, err := r.old.openValues(v)
			if err != nil {
				return err
			}
			if !r.verify() {
				newValue, err = r.next.sealValues(plain)
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
	})
	if err != nil {
		return err
	}

	return crypto.ApplyEntries(b, drop, changed)
}

// metadataBucket rotates the sealed auto-increment counters of one bucket. The
// codec name stays readable: it is what ErrDifferentCodec compares.
func (r *rotation) metadataBucket(b *bolt.Bucket) error {
	if !r.old.encMetadata {
		return nil
	}

	var (
		changed []crypto.Pair
		drop    [][]byte
	)

	err := b.ForEach(func(k, v []byte) error {
		if v == nil || string(k) == metaCodec {
			return nil
		}
		if err := r.check(); err != nil {
			return err
		}

		plain, err := r.old.openMetadata(v)
		if err != nil {
			return err
		}
		if r.verify() {
			return nil
		}

		sealed, err := r.next.sealMetadata(plain)
		if err != nil {
			return err
		}

		changed = append(changed, crypto.Pair{Key: append([]byte(nil), k...), Value: sealed})
		drop = append(drop, append([]byte(nil), k...))
		return nil
	})
	if err != nil {
		return err
	}

	return crypto.ApplyEntries(b, drop, changed)
}

// indexBucket rotates one index bucket: a list index keeps its nested id
// bucket, a unique index is flat.
func (r *rotation) indexBucket(b *bolt.Bucket) error {
	if !r.old.encKeys {
		return nil
	}

	if ids := b.Bucket([]byte(listIndexIDsBucket)); ids != nil {
		return r.listIndex(b, ids)
	}
	return r.uniqueIndex(b)
}

// uniqueIndex rotates a unique index: key is the encrypted value, value is the
// encrypted primary key.
func (r *rotation) uniqueIndex(b *bolt.Bucket) error {
	var (
		changed []crypto.Pair
		drop    [][]byte
	)

	err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if err := r.check(); err != nil {
			return err
		}

		value, err := r.old.openKey(k, keyDomainIndex)
		if err != nil {
			return err
		}
		id, err := r.old.openKey(v, keyDomainID)
		if err != nil {
			return err
		}
		if r.verify() {
			return nil
		}

		newKey, err := r.next.sealKey(value, keyDomainIndex)
		if err != nil {
			return err
		}
		newValue, err := r.next.sealKey(id, keyDomainID)
		if err != nil {
			return err
		}

		changed = append(changed, crypto.Pair{Key: newKey, Value: newValue})
		drop = append(drop, append([]byte(nil), k...))
		return nil
	})
	if err != nil {
		return err
	}

	return crypto.ApplyEntries(b, drop, changed)
}

// listIndex rotates a list index: entries are
// escape(value) ++ 0x00 0x00 ++ id ++ uint64(len(escape(value))) with the id
// as value, and the nested id bucket mirrors them the other way around.
func (r *rotation) listIndex(b *bolt.Bucket, ids *bolt.Bucket) error {
	var (
		changed []crypto.Pair
		drop    [][]byte
	)

	err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if err := r.check(); err != nil {
			return err
		}

		valueEnc, idEnc, ok := index.DecodeListKey(k)
		if !ok {
			return fmt.Errorf("%w: malformed list index entry", ErrWrongMode)
		}

		value, err := r.old.openKey(valueEnc, keyDomainIndex)
		if err != nil {
			return err
		}
		id, err := r.old.openKey(idEnc, keyDomainID)
		if err != nil {
			return err
		}
		if r.verify() {
			return nil
		}

		newID, err := r.next.sealKey(id, keyDomainID)
		if err != nil {
			return err
		}
		encValue, err := r.next.sealKey(value, keyDomainIndex)
		if err != nil {
			return err
		}
		newKey := index.EncodeListKey(encValue, newID)

		changed = append(changed, crypto.Pair{Key: newKey, Value: newID})
		drop = append(drop, append([]byte(nil), k...))
		return nil
	})
	if err != nil {
		return err
	}

	if err := crypto.ApplyEntries(b, drop, changed); err != nil {
		return err
	}

	var (
		idChanged []crypto.Pair
		idDrop    [][]byte
	)

	err = ids.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if err := r.check(); err != nil {
			return err
		}

		valueEnc, idEnc, ok := index.DecodeListKey(v)
		if !ok {
			return fmt.Errorf("%w: malformed list index entry", ErrWrongMode)
		}
		if !bytes.Equal(idEnc, k) {
			return fmt.Errorf("%w: malformed list index entry", ErrWrongMode)
		}

		id, err := r.old.openKey(k, keyDomainID)
		if err != nil {
			return err
		}
		value, err := r.old.openKey(valueEnc, keyDomainIndex)
		if err != nil {
			return err
		}
		if r.verify() {
			return nil
		}

		newKey, err := r.next.sealKey(id, keyDomainID)
		if err != nil {
			return err
		}
		encValue, err := r.next.sealKey(value, keyDomainIndex)
		if err != nil {
			return err
		}
		newValue := index.EncodeListKey(encValue, newKey)

		idChanged = append(idChanged, crypto.Pair{Key: newKey, Value: newValue})
		idDrop = append(idDrop, append([]byte(nil), k...))
		return nil
	})
	if err != nil {
		return err
	}

	return crypto.ApplyEntries(ids, idDrop, idChanged)
}

// openKeyAnyDomain decrypts a key that may have been encrypted in any of the
// given domains.
func (r *rotation) openKeyAnyDomain(cipherKey []byte, domains []string) ([]byte, string, error) {
	for _, domain := range domains {
		plain, err := r.old.openKey(cipherKey, domain)
		if err == nil {
			return plain, domain, nil
		}
		if !errors.Is(err, ErrAuthFailed) {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("%w: encrypted key does not belong to any expected domain", ErrAuthFailed)
}
