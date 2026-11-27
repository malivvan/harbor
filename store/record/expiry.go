package record

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	expiryPrefix      = "expiry:"
	expiryIndexBucket = "__tempest_expiry_"
	expirySeparator   = 0x00
)

// ErrExpiryDisabled reports SetExpiry or SweepExpired on a store that was
// opened without Config.Expiry and has never written an expiry.
var ErrExpiryDisabled = errors.New("record: expiry is not enabled")

type expiryContextKey struct{}

// WithExpired makes reads include records whose expiry has passed. It is the
// audit and maintenance escape hatch; normal reads exclude expired records.
func WithExpired(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, expiryContextKey{}, true)
}

func expiredAllowed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(expiryContextKey{}).(bool)
	return v
}

// SetNow replaces the clock used by expiry checks and sweeps. Passing nil
// restores time.Now. It is intended for tests and for applications that need a
// shared logical clock.
func (s *Store) SetNow(fn func() time.Time) {
	if s == nil {
		return
	}
	base := s.rootStore()
	base.nowMu.Lock()
	base.nowFn = fn
	base.nowMu.Unlock()
}

func (s *Store) now() time.Time {
	base := s.rootStore()
	if base == nil {
		return time.Now()
	}
	base.nowMu.RLock()
	fn := base.nowFn
	base.nowMu.RUnlock()
	if fn != nil {
		return fn()
	}
	return time.Now()
}

func expiryMetadataKey(id []byte) []byte {
	key := make([]byte, len(expiryPrefix)+len(id))
	copy(key, expiryPrefix)
	copy(key[len(expiryPrefix):], id)
	return key
}

// expiry reads the stored expiry for id.
func (m *meta) expiry(id []byte) (time.Time, bool, error) {
	if m == nil || m.bucket == nil {
		return time.Time{}, false, nil
	}
	raw := m.bucket.Get(expiryMetadataKey(id))
	if raw == nil {
		return time.Time{}, false, nil
	}
	plain, err := m.enc.openMetadata(raw)
	if err != nil {
		return time.Time{}, false, err
	}
	if len(plain) != 8 {
		return time.Time{}, false, fmt.Errorf("record: corrupt expiry record: %d bytes", len(plain))
	}
	ns := int64(binary.BigEndian.Uint64(plain))
	return time.Unix(0, ns).UTC(), true, nil
}

// setExpiry stores at as the expiry for id. A zero time removes the expiry.
func (m *meta) setExpiry(id []byte, at time.Time) error {
	if at.IsZero() {
		return m.deleteExpiry(id)
	}
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(at.UnixNano()))
	sealed, err := m.enc.sealMetadata(raw)
	if err != nil {
		return err
	}
	return m.bucket.Put(expiryMetadataKey(id), sealed)
}

func (m *meta) deleteExpiry(id []byte) error {
	if m == nil || m.bucket == nil {
		return nil
	}
	if m.bucket.Get(expiryMetadataKey(id)) == nil {
		return nil
	}
	return m.bucket.Delete(expiryMetadataKey(id))
}

// expirySortKey maps a time to a byte string that sorts chronologically.
func expirySortKey(at time.Time) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(at.UnixNano())^(1<<63))
	return key
}

func makeExpiryIndexKey(at time.Time, bucketName string, id []byte) []byte {
	sort := expirySortKey(at)
	key := make([]byte, 0, len(sort)+len(bucketName)+1+len(id))
	key = append(key, sort[:]...)
	key = append(key, bucketName...)
	key = append(key, expirySeparator)
	key = append(key, id...)
	return key
}

func parseExpiryIndexKey(key []byte) (at time.Time, bucketName string, id []byte, ok bool) {
	if len(key) < 9 {
		return time.Time{}, "", nil, false
	}
	u := binary.BigEndian.Uint64(key[:8]) ^ (1 << 63)
	at = time.Unix(0, int64(u)).UTC()
	rest := key[8:]
	i := bytes.IndexByte(rest, expirySeparator)
	if i < 0 {
		return time.Time{}, "", nil, false
	}
	return at, string(rest[:i]), rest[i+1:], true
}

func (n *node) addExpiryIndex(tx *bolt.Tx, bucketName string, id []byte, at time.Time) error {
	b, err := n.createBucketIfNotExists(tx, expiryIndexBucket)
	if err != nil {
		return err
	}
	return b.Put(makeExpiryIndexKey(at, bucketName, id), nil)
}

func (n *node) deleteExpiryIndex(tx *bolt.Tx, bucketName string, id []byte, at time.Time) error {
	b := n.getBucket(tx, expiryIndexBucket)
	if b == nil {
		return nil
	}
	return b.Delete(makeExpiryIndexKey(at, bucketName, id))
}

// expiredKey reports whether the record at id in bucket has an expiry at or
// before the store clock. It is cheap enough for the read path: one metadata
// lookup, no record decode.
func (n *node) expiredKey(bucket *bolt.Bucket, id []byte) bool {
	if n == nil || n.s == nil || !n.s.expiry {
		return false
	}
	m := openMeta(n, bucket)
	if m == nil {
		return false
	}
	at, ok, err := m.expiry(id)
	if err != nil || !ok {
		return false
	}
	return !at.After(n.s.now())
}

// SetExpiry stores an expiry for the record identified by data's primary key.
// A zero time clears it. The expiry is metadata, so the record bytes and its
// indexes are not rewritten.
func (n *node) SetExpiry(ctx context.Context, data any, at time.Time) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("set expiry", err)
	}
	if n == nil || n.s == nil || !n.s.expiry {
		return wrapError("set expiry", ErrExpiryDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return wrapError("set expiry", err)
	}
	if cfg.ID.IsZero {
		return wrapError("set expiry", ErrNoID)
	}
	err = n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		id, err := n.keyBytes(cfg.ID.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		b := n.getBucket(tx, cfg.Name)
		if b == nil || b.Get(id) == nil {
			return ErrNotFound
		}
		m, err := newMeta(b, n)
		if err != nil {
			return err
		}
		old, hadOld, err := m.expiry(id)
		if err != nil {
			return err
		}
		if err := m.setExpiry(id, at); err != nil {
			return err
		}
		if hadOld {
			if err := n.deleteExpiryIndex(tx, cfg.Name, id, old); err != nil {
				return err
			}
		}
		if !at.IsZero() {
			if err := n.addExpiryIndex(tx, cfg.Name, id, at); err != nil {
				return err
			}
		}
		return n.ensureCapability(ctx, tx, CapExpiry)
	})
	if err != nil {
		return wrapError("set expiry", err)
	}
	return nil
}

// Expiry returns the stored expiry for data's primary key.
func (n *node) Expiry(ctx context.Context, data any) (time.Time, bool, error) {
	if err := checkContext(ctx); err != nil {
		return time.Time{}, false, wrapError("expiry", err)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return time.Time{}, false, wrapError("expiry", err)
	}
	if cfg.ID.IsZero {
		return time.Time{}, false, wrapError("expiry", ErrNoID)
	}
	var (
		at  time.Time
		has bool
	)
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		id, err := n.keyBytes(cfg.ID.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		b := n.getBucket(tx, cfg.Name)
		if b == nil || b.Get(id) == nil {
			return ErrNotFound
		}
		m := openMeta(n, b)
		if m == nil {
			return nil
		}
		at, has, err = m.expiry(id)
		return err
	})
	if err != nil {
		return time.Time{}, false, wrapError("expiry", err)
	}
	return at, has, nil
}

// SweepExpired deletes up to limit expired records of the type of data. A
// limit <= 0 removes every expired record of that type. The sweep is
// idempotent: index entries left behind by an earlier crash are discarded.
func (n *node) SweepExpired(ctx context.Context, data any, now time.Time, limit int) (int, error) {
	if err := checkContext(ctx); err != nil {
		return 0, wrapError("sweep expired", err)
	}
	if n == nil || n.s == nil || !n.s.expiry {
		return 0, wrapError("sweep expired", ErrExpiryDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return 0, wrapError("sweep expired", err)
	}
	if now.IsZero() {
		now = n.s.now()
	}
	removed := 0
	err = n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		b := n.getBucket(tx, expiryIndexBucket)
		if b == nil {
			return nil
		}
		type entry struct {
			key []byte
			id  []byte
		}
		var entries []entry
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			at, bucketName, id, ok := parseExpiryIndexKey(k)
			if !ok {
				continue
			}
			if at.After(now) {
				break
			}
			if bucketName != cfg.Name {
				continue
			}
			entries = append(entries, entry{key: append([]byte(nil), k...), id: append([]byte(nil), id...)})
			if limit > 0 && len(entries) >= limit {
				break
			}
		}
		for _, e := range entries {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if err := n.deleteStruct(ctx, tx, cfg, e.id); err != nil {
				if errors.Is(err, ErrNotFound) {
					if err := b.Delete(e.key); err != nil {
						return err
					}
					continue
				}
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, wrapError("sweep expired", err)
	}
	return removed, nil
}

// structConfigOf validates and extracts a pointer-to-struct.
func structConfigOf(data any) (*structConfig, error) {
	ref := reflect.ValueOf(data)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return nil, ErrStructPtrNeeded
	}
	cfg, err := extract(&ref)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
