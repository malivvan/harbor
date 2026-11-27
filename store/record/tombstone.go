package record

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

const tombstonePrefix = "deleted:"

// ErrTombstoneDisabled reports a soft-delete operation on a store that was
// opened without Config.Tombstone and has never written a tombstone.
var ErrTombstoneDisabled = errors.New("record: tombstone is not enabled")

// DeleteOptions tunes DeleteStructWith.
type DeleteOptions struct {
	// Soft hides the record without removing its bytes or indexes.
	Soft bool
	// Cascade is reserved for the cross-store link registry. It currently has
	// no effect because a soft delete does not remove the record.
	Cascade bool
	// At stamps the tombstone. Zero selects the store clock.
	At time.Time
}

// DeletedRef is one soft-deleted record.
type DeletedRef struct {
	// Key is the encoded primary key.
	Key []byte
	// At is when the tombstone was written.
	At time.Time
}

type deletedContextKey struct{}

// WithDeleted makes reads include soft-deleted records. It is the audit and
// maintenance escape hatch; normal reads exclude them.
func WithDeleted(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, deletedContextKey{}, true)
}

func deletedAllowed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(deletedContextKey{}).(bool)
	return v
}

func tombstoneMetadataKey(id []byte) []byte {
	key := make([]byte, len(tombstonePrefix)+len(id))
	copy(key, tombstonePrefix)
	copy(key[len(tombstonePrefix):], id)
	return key
}

func (m *meta) tombstone(id []byte) (time.Time, bool, error) {
	if m == nil || m.bucket == nil {
		return time.Time{}, false, nil
	}
	raw := m.bucket.Get(tombstoneMetadataKey(id))
	if raw == nil {
		return time.Time{}, false, nil
	}
	plain, err := m.enc.openMetadata(raw)
	if err != nil {
		return time.Time{}, false, err
	}
	if len(plain) != 8 {
		return time.Time{}, false, fmt.Errorf("record: corrupt tombstone: %d bytes", len(plain))
	}
	ns := int64(binary.BigEndian.Uint64(plain))
	return time.Unix(0, ns).UTC(), true, nil
}

func (m *meta) setTombstone(id []byte, at time.Time) error {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(at.UnixNano()))
	sealed, err := m.enc.sealMetadata(raw)
	if err != nil {
		return err
	}
	return m.bucket.Put(tombstoneMetadataKey(id), sealed)
}

func (m *meta) deleteTombstone(id []byte) error {
	if m == nil || m.bucket == nil {
		return nil
	}
	if m.bucket.Get(tombstoneMetadataKey(id)) == nil {
		return nil
	}
	return m.bucket.Delete(tombstoneMetadataKey(id))
}

// tombstonedKey reports whether a tombstone exists for id.
func (n *node) tombstonedKey(bucket *bolt.Bucket, id []byte) bool {
	if n == nil || n.s == nil || !n.s.tombstone {
		return false
	}
	m := openMeta(n, bucket)
	if m == nil {
		return false
	}
	_, ok, err := m.tombstone(id)
	return err == nil && ok
}

// hiddenKey reports whether a key is hidden from ordinary reads by either the
// expiry or the tombstone gate. Context escape hatches are honoured per gate,
// so WithExpired does not reveal a soft-deleted record and vice versa.
func (n *node) hiddenKey(ctx context.Context, bucket *bolt.Bucket, id []byte) bool {
	if n == nil || n.s == nil {
		return false
	}
	if n.s.expiry && !expiredAllowed(ctx) && n.expiredKey(bucket, id) {
		return true
	}
	if n.s.tombstone && !deletedAllowed(ctx) && n.tombstonedKey(bucket, id) {
		return true
	}
	return false
}

// DeleteStructWith deletes data. Soft writes a tombstone and keeps the bytes
// and indexes; the default is DeleteStruct's hard delete.
func (n *node) DeleteStructWith(ctx context.Context, data any, opts DeleteOptions) error {
	if !opts.Soft {
		return n.DeleteStruct(ctx, data)
	}
	if err := checkContext(ctx); err != nil {
		return wrapError("delete struct", err)
	}
	if n == nil || n.s == nil || !n.s.tombstone {
		return wrapError("delete struct", ErrTombstoneDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return wrapError("delete struct", err)
	}
	if cfg.ID.IsZero {
		return wrapError("delete struct", ErrNoID)
	}
	at := opts.At
	if at.IsZero() {
		at = n.s.now()
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
		if err := m.setTombstone(id, at); err != nil {
			return err
		}
		return n.ensureCapability(ctx, tx, CapTombstone)
	})
	if err != nil {
		return wrapError("delete struct", err)
	}
	n.emitDelete(cfg)
	return n.traceRecordStruct("soft_delete", cfg, nil)
}

// Restore clears a soft-delete tombstone. Restoring a live record is a no-op.
func (n *node) Restore(ctx context.Context, data any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("restore", err)
	}
	if n == nil || n.s == nil || !n.s.tombstone {
		return wrapError("restore", ErrTombstoneDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return wrapError("restore", err)
	}
	if cfg.ID.IsZero {
		return wrapError("restore", ErrNoID)
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
		return m.deleteTombstone(id)
	})
	if err != nil {
		return wrapError("restore", err)
	}
	n.emitUpsert(cfg, data)
	return n.traceRecordStruct("restore", cfg, data)
}

// Deleted lists soft-deleted records of the type of data, oldest first. A
// non-positive limit returns every tombstone.
func (n *node) Deleted(ctx context.Context, data any, limit int) ([]DeletedRef, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("deleted", err)
	}
	if n == nil || n.s == nil || !n.s.tombstone {
		return nil, wrapError("deleted", ErrTombstoneDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return nil, wrapError("deleted", err)
	}
	var refs []DeletedRef
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		b := n.getBucket(tx, cfg.Name)
		if b == nil {
			return nil
		}
		m := openMeta(n, b)
		if m == nil {
			return nil
		}
		c := m.bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if !bytes.HasPrefix(k, []byte(tombstonePrefix)) {
				continue
			}
			id := append([]byte(nil), k[len(tombstonePrefix):]...)
			at, ok, err := m.tombstone(id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			refs = append(refs, DeletedRef{Key: id, At: at})
		}
		return nil
	})
	if err != nil {
		return nil, wrapError("deleted", err)
	}
	sort.Slice(refs, func(i, j int) bool {
		if !refs[i].At.Equal(refs[j].At) {
			return refs[i].At.Before(refs[j].At)
		}
		return bytes.Compare(refs[i].Key, refs[j].Key) < 0
	})
	if limit > 0 && len(refs) > limit {
		refs = refs[:limit]
	}
	return refs, nil
}

// PurgeDeleted hard-deletes tombstones older than before. A zero before
// removes every tombstone. It returns the number of records removed.
func (n *node) PurgeDeleted(ctx context.Context, data any, before time.Time) (int, error) {
	if err := checkContext(ctx); err != nil {
		return 0, wrapError("purge deleted", err)
	}
	if n == nil || n.s == nil || !n.s.tombstone {
		return 0, wrapError("purge deleted", ErrTombstoneDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return 0, wrapError("purge deleted", err)
	}
	removed := 0
	err = n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		b := n.getBucket(tx, cfg.Name)
		if b == nil {
			return nil
		}
		m := openMeta(n, b)
		if m == nil {
			return nil
		}
		type entry struct {
			id []byte
			at time.Time
		}
		var entries []entry
		c := m.bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if !bytes.HasPrefix(k, []byte(tombstonePrefix)) {
				continue
			}
			id := append([]byte(nil), k[len(tombstonePrefix):]...)
			at, ok, err := m.tombstone(id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if !before.IsZero() && !at.Before(before) {
				continue
			}
			entries = append(entries, entry{id: id, at: at})
		}
		for _, e := range entries {
			if err := checkContext(ctx); err != nil {
				return err
			}
			if err := n.deleteStruct(ctx, tx, cfg, e.id); err != nil {
				if errors.Is(err, ErrNotFound) {
					if err := m.deleteTombstone(e.id); err != nil {
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
		return removed, wrapError("purge deleted", err)
	}
	return removed, nil
}
