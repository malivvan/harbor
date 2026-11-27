package record

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"

	bolt "go.etcd.io/bbolt"
)

// revisionPrefix namespaces per-record revision counters inside a bucket's
// __metadata child. The bucket already holds the codec name and the
// auto-increment counters, so the revision lives beside them and is sealed
// when EncryptMetadata is enabled.
const revisionPrefix = "revision:"

// Precondition is the expected revision for a conditional write.
//
// Revision 0 means "the record must not exist yet". A positive value means
// "the stored record must be at exactly this revision". UpdateIf and DeleteIf
// compare the precondition and the stored revision inside one write
// transaction, so a concurrent writer cannot slip between the check and the
// write.
type Precondition struct {
	Revision uint64
}

// revisionKey returns the metadata key holding the revision for id.
func revisionKey(id []byte) []byte {
	key := make([]byte, len(revisionPrefix)+len(id))
	copy(key, revisionPrefix)
	copy(key[len(revisionPrefix):], id)
	return key
}

// revision reads the current revision for id. A missing counter means zero,
// which is the revision of a record that has never been written.
func (m *meta) revision(id []byte) (uint64, error) {
	if m == nil || m.bucket == nil {
		return 0, nil
	}
	raw := m.bucket.Get(revisionKey(id))
	if raw == nil {
		return 0, nil
	}
	plain, err := m.enc.openMetadata(raw)
	if err != nil {
		return 0, err
	}
	if len(plain) != 8 {
		return 0, fmt.Errorf("record: corrupt revision counter: %d bytes", len(plain))
	}
	return binary.BigEndian.Uint64(plain), nil
}

// setRevision writes rev as the revision for id.
func (m *meta) setRevision(id []byte, rev uint64) error {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, rev)
	sealed, err := m.enc.sealMetadata(raw)
	if err != nil {
		return err
	}
	return m.bucket.Put(revisionKey(id), sealed)
}

// bumpRevision increments and returns the revision for id.
func (m *meta) bumpRevision(id []byte) (uint64, error) {
	current, err := m.revision(id)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if next == 0 {
		// A wrap would make a conditional write match every future revision.
		// Refuse instead of pretending the counter is still meaningful.
		return 0, fmt.Errorf("record: revision counter overflow")
	}
	return next, m.setRevision(id, next)
}

// deleteRevision removes the counter for a deleted record.
func (m *meta) deleteRevision(id []byte) error {
	if m == nil || m.bucket == nil {
		return nil
	}
	if m.bucket.Get(revisionKey(id)) == nil {
		return nil
	}
	return m.bucket.Delete(revisionKey(id))
}

// openMeta returns the existing metadata bucket for b without creating it.
// It returns nil when the bucket does not exist, which is a zero revision.
func openMeta(n *node, b *bolt.Bucket) *meta {
	if b == nil {
		return nil
	}
	m := b.Bucket([]byte(metadataBucket))
	if m == nil {
		return nil
	}
	return &meta{node: n, bucket: m, enc: encFromNode(n)}
}

// revisionInTx reads the revision for id from an already-open transaction.
func (n *node) revisionInTx(tx *bolt.Tx, cfg *structConfig, id []byte) (uint64, error) {
	m := openMeta(n, n.getBucket(tx, cfg.Name))
	if m == nil {
		return 0, nil
	}
	return m.revision(id)
}

// Revision returns the current revision of the record identified by data's
// primary key. A record that has never been written returns ErrNotFound.
func (n *node) Revision(ctx context.Context, data any) (uint64, error) {
	if err := checkContext(ctx); err != nil {
		return 0, wrapError("revision", err)
	}
	ref := reflect.ValueOf(data)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return 0, wrapError("revision", ErrStructPtrNeeded)
	}
	cfg, err := extract(&ref)
	if err != nil {
		return 0, wrapError("revision", err)
	}
	if cfg.ID.IsZero {
		return 0, wrapError("revision", ErrNoID)
	}
	var rev uint64
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		id, err := n.keyBytes(cfg.ID.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		b := n.getBucket(tx, cfg.Name)
		if b == nil || b.Get(id) == nil {
			return ErrNotFound
		}
		rev, err = n.revisionInTx(tx, cfg, id)
		return err
	})
	if err != nil {
		return 0, wrapError("revision", err)
	}
	return rev, nil
}

// UpdateIf saves data only when the stored revision satisfies pre.
//
// A zero precondition creates a new record and fails with ErrConflict when the
// key already exists. A positive precondition updates an existing record with
// Update's merge semantics, and fails with ErrConflict when the key is missing
// or the stored revision differs. The returned revision is the one written.
func (n *node) UpdateIf(ctx context.Context, data any, pre Precondition) (uint64, error) {
	if err := checkContext(ctx); err != nil {
		return 0, wrapError("update if", err)
	}
	ref := reflect.ValueOf(data)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return 0, wrapError("update if", ErrStructPtrNeeded)
	}
	cfg, err := extract(&ref)
	if err != nil {
		return 0, wrapError("update if", err)
	}
	if cfg.ID.IsZero {
		return 0, wrapError("update if", ErrNoID)
	}

	var (
		next   uint64
		stored any
	)
	err = n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		id, err := n.keyBytes(cfg.ID.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		b := n.getBucket(tx, cfg.Name)
		exists := b != nil && b.Get(id) != nil
		current, err := n.revisionInTx(tx, cfg, id)
		if err != nil {
			return err
		}
		switch {
		case pre.Revision == 0 && exists:
			return fmt.Errorf("%w: record exists at revision %d", ErrConflict, current)
		case pre.Revision > 0 && !exists:
			return fmt.Errorf("%w: record not found", ErrConflict)
		case pre.Revision > 0 && current != pre.Revision:
			return fmt.Errorf("%w: have revision %d, want %d", ErrConflict, current, pre.Revision)
		}

		if exists {
			// Match Update: merge the caller's non-zero fields over the stored
			// value so a partial update cannot clear fields it did not name.
			loaded := reflect.New(reflect.Indirect(ref).Type())
			if err := n.withTransaction(tx).One(ctx, cfg.ID.Name, cfg.ID.Value.Interface(), loaded.Interface()); err != nil {
				return err
			}
			src := reflect.ValueOf(data).Elem()
			dst := loaded.Elem()
			if err := mergeUpdateFields(ctx, &src, &dst, cfg); err != nil {
				return err
			}
			if err := n.save(ctx, tx, cfg, loaded.Interface(), true); err != nil {
				return err
			}
			stored = loaded.Interface()
		} else {
			if err := n.save(ctx, tx, cfg, data, false); err != nil {
				return err
			}
			stored = data
		}

		next, err = n.revisionInTx(tx, cfg, id)
		return err
	})
	if err != nil {
		return 0, wrapError("update if", err)
	}
	n.emitUpsert(cfg, stored)
	if err := n.traceRecordStruct("update_if", cfg, stored); err != nil {
		return 0, wrapError("update if", err)
	}
	return next, nil
}

// DeleteIf deletes data only when the stored revision satisfies pre.
//
// A zero precondition deletes an existing record regardless of its revision.
// A positive precondition requires an exact match. A missing record returns
// ErrNotFound; a revision mismatch returns ErrConflict.
func (n *node) DeleteIf(ctx context.Context, data any, pre Precondition) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("delete if", err)
	}
	ref := reflect.ValueOf(data)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return wrapError("delete if", ErrStructPtrNeeded)
	}
	cfg, err := extract(&ref)
	if err != nil {
		return wrapError("delete if", err)
	}
	if cfg.ID.IsZero {
		return wrapError("delete if", ErrNoID)
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
		current, err := n.revisionInTx(tx, cfg, id)
		if err != nil {
			return err
		}
		if pre.Revision != 0 && current != pre.Revision {
			return fmt.Errorf("%w: have revision %d, want %d", ErrConflict, current, pre.Revision)
		}
		if err := n.enforceDeleteRefs(ctx, tx, cfg.Name, cfg.ID.Value.Interface(), map[string]bool{}, 0); err != nil {
			return err
		}
		return n.deleteStruct(ctx, tx, cfg, id)
	})
	if err != nil {
		return wrapError("delete if", err)
	}
	n.emitDelete(cfg)
	return n.traceRecordStruct("delete_if", cfg, nil)
}
