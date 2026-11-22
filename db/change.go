package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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
