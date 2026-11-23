package db

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/malivvan/harbor/pkg/audit"
)

// ChangeOp is the kind of mutation a record bucket saw.
type ChangeOp string

const (
	ChangeUpsert ChangeOp = "upsert"
	ChangeDelete ChangeOp = "delete"
)

// Change is one committed record mutation. Data is the codec-encoded record
// (nil for deletes); Path is the node's bucket path, so a caller using
// From("actor") can tell replicas apart.
type Change struct {
	Bucket string
	ID     string
	Op     ChangeOp
	Data   []byte
	Path   []string
}

// ChangeHook receives committed record mutations. Hooks run synchronously after
// the transaction commits and must not block for long.
type ChangeHook func(Change)

var changeHookMu sync.Mutex

// AddChangeHook registers a hook and returns a function that removes it.
// Multiple hooks may be registered; each receives every change.
func (s *Store) AddChangeHook(fn ChangeHook) func() {
	if s == nil || fn == nil {
		return func() {}
	}
	s.changeMu.Lock()
	if s.changeHooks == nil {
		s.changeHooks = map[uint64]ChangeHook{}
	}
	id := s.nextHookID
	s.nextHookID++
	s.changeHooks[id] = fn
	s.changeMu.Unlock()
	return func() {
		s.changeMu.Lock()
		delete(s.changeHooks, id)
		s.changeMu.Unlock()
	}
}

// SetChangeHook replaces all hooks with a single hook. A nil hook clears them.
func (s *Store) SetChangeHook(fn ChangeHook) {
	if s == nil {
		return
	}
	s.changeMu.Lock()
	s.changeHooks = map[uint64]ChangeHook{}
	if fn != nil {
		s.changeHooks[0] = fn
		s.nextHookID = 1
	}
	s.changeMu.Unlock()
}

func (s *Store) emitChange(c Change) {
	if s == nil {
		return
	}
	s.changeMu.Lock()
	hooks := make([]ChangeHook, 0, len(s.changeHooks))
	for _, fn := range s.changeHooks {
		hooks = append(hooks, fn)
	}
	s.changeMu.Unlock()
	for _, fn := range hooks {
		fn(c)
	}
}

func (n *node) emitUpsert(cfg *structConfig, data any) {
	if n.tx != nil || n.s == nil {
		return
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return
	}
	id := ""
	if cfg.ID != nil {
		id = fmt.Sprint(cfg.ID.Value.Interface())
	}
	n.s.emitChange(Change{Bucket: cfg.Name, ID: id, Op: ChangeUpsert, Data: raw, Path: n.Bucket()})
}

func (n *node) emitDelete(cfg *structConfig) {
	if n.tx != nil || n.s == nil {
		return
	}
	id := ""
	if cfg.ID != nil {
		id = fmt.Sprint(cfg.ID.Value.Interface())
	}
	n.s.emitChange(Change{Bucket: cfg.Name, ID: id, Op: ChangeDelete, Path: n.Bucket()})
}

// TouchChange is used by callers that need to emit a change for a write they
// performed through raw key/value or transaction APIs.
func (s *Store) TouchChange(ctx context.Context, c Change) {
	_ = ctx
	s.emitChange(c)
}

// auditEntry records one committed write in the store's audit trail. It is a
// no-op when the store has no audit sink. Digest is the SHA-256 of the written
// payload, or nil for deletes. A sink error is returned so the caller can
// surface it; the data write has already committed at that point.
//
// Internal bookkeeping (the version record in __database and per-bucket
// metadata) is not an application write and is never recorded.
func (n *node) auditEntry(op, entity string, digest []byte) error {
	if n == nil || n.s == nil || n.s.audit == nil {
		return nil
	}
	return n.s.audit.Record(audit.Entry{
		Store:  audit.StoreDB,
		Op:     op,
		Entity: n.auditEntity(entity),
		Digest: digest,
	})
}

// auditEntryBucket is like auditEntry but skips the reserved internal buckets,
// so opening a store (which writes the version record) leaves no event.
func (n *node) auditEntryBucket(op, bucketName, entity string, digest []byte) error {
	if isInternalBucket(bucketName) {
		return nil
	}
	return n.auditEntry(op, entity, digest)
}

func isInternalBucket(name string) bool {
	return name == databaseInfo || name == metadataBucket
}

// auditEntity prefixes the bucket path so an event identifies the node the
// write landed on, matching the Change.Path convention.
func (n *node) auditEntity(entity string) string {
	path := n.Bucket()
	if len(path) == 0 {
		return entity
	}
	prefix := ""
	for _, part := range path {
		prefix += part + "/"
	}
	return prefix + entity
}

// digestOf returns the SHA-256 of data, or nil when data is empty so a delete
// and an empty write are distinguishable.
func digestOf(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	sum := sha256.Sum256(data)
	return sum[:]
}

// auditRecordStruct records a committed struct write. The digest is the
// SHA-256 of the codec-encoded record, matching what is stored.
func (n *node) auditRecordStruct(op string, cfg *structConfig, data any) error {
	if n == nil || n.s == nil || n.s.audit == nil {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	id := ""
	if cfg.ID != nil {
		id = fmt.Sprint(cfg.ID.Value.Interface())
	}
	return n.auditEntry(op, cfg.Name+":"+id, digestOf(raw))
}
