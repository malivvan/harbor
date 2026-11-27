package search

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	expiryBucket     = "__expiry__"
	expiryAtBucket   = "at"
	expiryByIDBucket = "byid"
	expirySeparator  = 0x00
)

// SetNow replaces the clock used by expiry checks and sweeps. Passing nil
// restores time.Now.
func (s *Store) SetNow(fn func() time.Time) {
	if s == nil {
		return
	}
	base := s.baseStore()
	base.nowMu.Lock()
	base.nowFn = fn
	base.nowMu.Unlock()
}

func (s *Store) baseStore() *Store {
	if s != nil && s.base != nil {
		return s.base
	}
	return s
}

func (s *Store) now() time.Time {
	base := s.baseStore()
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

func expiryAtKey(ns int64, id string) []byte {
	var sort [8]byte
	binary.BigEndian.PutUint64(sort[:], uint64(ns)^(1<<63))
	key := make([]byte, 0, 8+1+len(id))
	key = append(key, sort[:]...)
	key = append(key, expirySeparator)
	key = append(key, id...)
	return key
}

func parseExpiryAtKey(key []byte) (int64, string, bool) {
	if len(key) < 9 {
		return 0, "", false
	}
	u := binary.BigEndian.Uint64(key[:8]) ^ (1 << 63)
	rest := key[8:]
	i := bytes.IndexByte(rest, expirySeparator)
	if i < 0 {
		return 0, "", false
	}
	return int64(u), string(rest[i+1:]), true
}

// ensureExpiry upgrades an index to format 2 and creates the expiry buckets.
func (idx *Index) ensureExpiry(tx *bolt.Tx, b *bolt.Bucket) (*bolt.Bucket, error) {
	if err := idx.store.setVersion(b, FormatVersionExpiry); err != nil {
		return nil, err
	}
	eb, err := b.CreateBucketIfNotExists([]byte(expiryBucket))
	if err != nil {
		return nil, err
	}
	if _, err := eb.CreateBucketIfNotExists([]byte(expiryAtBucket)); err != nil {
		return nil, err
	}
	if _, err := eb.CreateBucketIfNotExists([]byte(expiryByIDBucket)); err != nil {
		return nil, err
	}
	return eb, nil
}

// setExpiry records ns (UnixNano; zero clears) for id and maintains the two
// expiry indexes used by lookups and sweeps.
func (idx *Index) setExpiry(tx *bolt.Tx, b *bolt.Bucket, id string, ns int64) error {
	eb := b.Bucket([]byte(expiryBucket))
	if eb == nil {
		if ns == 0 {
			return nil
		}
		var err error
		eb, err = idx.ensureExpiry(tx, b)
		if err != nil {
			return err
		}
	}
	byID := eb.Bucket([]byte(expiryByIDBucket))
	at := eb.Bucket([]byte(expiryAtBucket))
	if byID == nil || at == nil {
		var err error
		eb, err = idx.ensureExpiry(tx, b)
		if err != nil {
			return err
		}
		byID = eb.Bucket([]byte(expiryByIDBucket))
		at = eb.Bucket([]byte(expiryAtBucket))
	}

	if old := byID.Get([]byte(id)); old != nil && len(old) == 8 {
		if err := at.Delete(expiryAtKey(int64(binary.BigEndian.Uint64(old)), id)); err != nil {
			return err
		}
	}
	if ns == 0 {
		return byID.Delete([]byte(id))
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ns))
	if err := byID.Put([]byte(id), buf[:]); err != nil {
		return err
	}
	return at.Put(expiryAtKey(ns, id), nil)
}

// clearExpiry removes an id's expiry entries without creating the buckets.
func (idx *Index) clearExpiry(tx *bolt.Tx, b *bolt.Bucket, id string) error {
	if b.Bucket([]byte(expiryBucket)) == nil {
		return nil
	}
	return idx.setExpiry(tx, b, id, 0)
}

// SetExpiry records an expiry for an existing document. A zero time clears it.
func (idx *Index) SetExpiry(ctx context.Context, id string, at time.Time) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("%w: document id is required", ErrNilParam)
	}
	ns := int64(0)
	if !at.IsZero() {
		ns = at.UnixNano()
	}
	if err := idx.store.update(func(tx *bolt.Tx) error {
		b, docs, _, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		doc, exists, err := idx.loadDoc(docs, id)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: %q", ErrDocNotFound, id)
		}
		doc.ExpiresAt = ns
		if err := idx.saveDoc(docs, id, doc); err != nil {
			return err
		}
		return idx.setExpiry(tx, b, id, ns)
	}); err != nil {
		return err
	}
	return idx.store.traceEntry("expiry", idx.name+":"+id, nil)
}

// Expiry returns a document's stored expiry.
func (idx *Index) Expiry(ctx context.Context, id string) (time.Time, bool, error) {
	if ctx == nil {
		return time.Time{}, false, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	var (
		at  time.Time
		has bool
	)
	err := idx.store.view(func(tx *bolt.Tx) error {
		_, docs, _, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		doc, exists, err := idx.loadDoc(docs, id)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: %q", ErrDocNotFound, id)
		}
		if doc.ExpiresAt != 0 {
			at, has = time.Unix(0, doc.ExpiresAt).UTC(), true
		}
		return nil
	})
	return at, has, err
}

// UpsertWithExpiry indexes text and records an expiry in one logical
// operation. A zero expiry leaves the document without one.
func (idx *Index) UpsertWithExpiry(ctx context.Context, id, text string, at time.Time) error {
	if err := idx.Upsert(ctx, id, text); err != nil {
		return err
	}
	if at.IsZero() {
		return nil
	}
	return idx.SetExpiry(ctx, id, at)
}

// SweepExpired deletes every document whose expiry is at or before now.
func (idx *Index) SweepExpired(ctx context.Context, now time.Time) (int, error) {
	if ctx == nil {
		return 0, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		now = idx.store.now()
	}
	cutoff := now.UnixNano()
	var ids []string
	err := idx.store.view(func(tx *bolt.Tx) error {
		b, _, _, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		eb := b.Bucket([]byte(expiryBucket))
		if eb == nil {
			return nil
		}
		at := eb.Bucket([]byte(expiryAtBucket))
		if at == nil {
			return nil
		}
		c := at.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			ns, id, ok := parseExpiryAtKey(k)
			if !ok {
				continue
			}
			if ns > cutoff {
				break
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if err := idx.Delete(ctx, ids...); err != nil {
		return 0, err
	}
	return len(ids), nil
}
