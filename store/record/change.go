package record

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/malivvan/tempest/store/ledger"
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

// AddChangeHook registers a hook and returns a function that removes it.
// Multiple hooks may be registered; each receives every change.
func (s *Store) AddChangeHook(fn ChangeHook) func() {
	if s == nil || fn == nil {
		return func() {}
	}
	owner := s.changeStore()
	owner.changeMu.Lock()
	if owner.changeHooks == nil {
		owner.changeHooks = map[uint64]ChangeHook{}
	}
	id := owner.nextHookID
	owner.nextHookID++
	owner.changeHooks[id] = fn
	owner.changeMu.Unlock()
	return func() {
		owner.changeMu.Lock()
		delete(owner.changeHooks, id)
		owner.changeMu.Unlock()
	}
}

// SetChangeHook replaces all hooks with a single hook. A nil hook clears them.
func (s *Store) SetChangeHook(fn ChangeHook) {
	if s == nil {
		return
	}
	owner := s.changeStore()
	owner.changeMu.Lock()
	owner.changeHooks = map[uint64]ChangeHook{}
	if fn != nil {
		owner.changeHooks[0] = fn
		owner.nextHookID = 1
	}
	owner.changeMu.Unlock()
}

func (s *Store) emitChange(c Change) {
	if s == nil {
		return
	}
	owner := s.changeStore()
	owner.changeMu.Lock()
	hooks := make([]ChangeHook, 0, len(owner.changeHooks))
	for _, fn := range owner.changeHooks {
		hooks = append(hooks, fn)
	}
	owner.changeMu.Unlock()
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

// traceEntry records one committed write in the store's trace log. It is a
// no-op when the store has no trace sink. Digest is the SHA-256 of the written
// payload, or nil for deletes. A sink error is returned so the caller can
// surface it; the data write has already committed at that point.
//
// Internal bookkeeping (the version record in __database and per-bucket
// metadata) is not an application write and is never recorded.
func (n *node) traceEntry(op, entity string, digest []byte) error {
	if n == nil || n.s == nil {
		return nil
	}
	sink := n.trace
	if sink == nil {
		sink = n.s.trace
	}
	if sink == nil {
		return nil
	}
	return sink.Record(ledger.Entry{
		Store:  ledger.StoreRecord,
		Op:     op,
		Entity: n.traceEntity(entity),
		Digest: digest,
	})
}

// traceEntryBucket is like traceEntry but skips the reserved internal buckets,
// so opening a store (which writes the version record) leaves no event.
func (n *node) traceEntryBucket(op, bucketName, entity string, digest []byte) error {
	if isInternalBucket(bucketName) {
		return nil
	}
	return n.traceEntry(op, entity, digest)
}

func isInternalBucket(name string) bool {
	return name == databaseInfo || name == metadataBucket
}

// traceEntity prefixes the bucket path so an event identifies the node the
// write landed on, matching the Change.Path convention.
func (n *node) traceEntity(entity string) string {
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

// traceRecordStruct records a committed struct write. The digest is the
// SHA-256 of the codec-encoded record, matching what is stored.
func (n *node) traceRecordStruct(op string, cfg *structConfig, data any) error {
	if n == nil || n.s == nil || n.s.trace == nil {
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
	return n.traceEntry(op, cfg.Name+":"+id, digestOf(raw))
}
