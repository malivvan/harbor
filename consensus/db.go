package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/malivvan/harbor"
	"github.com/malivvan/harbor/db"
)

// DB is the replicated write view of the db sub-store. Reads should use
// Node.Store().DB() directly; writes go through the methods here so they are
// proposed through raft and applied on every node.
type DB struct {
	n    *Node
	path []string
}

// dbChange is the payload for record-level db writes.
type dbChange struct {
	Path []string        `json:"path"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// dbFieldChange is the payload for db.UpdateField.
type dbFieldChange struct {
	Path  []string        `json:"path"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data"`
	Field string          `json:"field"`
	Value json.RawMessage `json:"value"`
}

// DB returns the replicated db sub-store.
func (n *Node) DB() *DB {
	if n == nil {
		return nil
	}
	return &DB{n: n}
}

// From returns a DB view rooted at a nested bucket path, mirroring db.Node.From.
func (d *DB) From(addend ...string) *DB {
	if d == nil || d.n == nil {
		return nil
	}
	p := make([]string, 0, len(d.path)+len(addend))
	p = append(p, d.path...)
	p = append(p, addend...)
	return &DB{n: d.n, path: p}
}

// Save replicates db.Node.Save.
func (d *DB) Save(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opDBSave, "", "", data)
}

// Update replicates db.Node.Update.
func (d *DB) Update(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opDBUpdate, "", "", data)
}

// UpdateField replicates db.Node.UpdateField.
func (d *DB) UpdateField(ctx context.Context, data any, field string, value any) error {
	return d.recordWrite(ctx, opDBUpdateField, field, value, data)
}

// DeleteStruct replicates db.Node.DeleteStruct.
func (d *DB) DeleteStruct(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opDBDelete, "", "", data)
}

// Drop replicates db.Node.Drop.
func (d *DB) Drop(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opDBDrop, "", "", data)
}

func (d *DB) recordWrite(ctx context.Context, op, field string, value, data any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.n == nil {
		return ErrClosed
	}
	ref := reflect.ValueOf(data)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("consensus: db write requires a pointer to a struct")
	}
	name := d.n.registerType(ref.Elem().Type())
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("consensus: encode record: %w", err)
	}

	if op == opDBUpdateField {
		vraw, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("consensus: encode field value: %w", err)
		}
		_, err = d.n.apply(op, dbFieldChange{Path: d.path, Type: name, Data: raw, Field: field, Value: vraw})
		return err
	}
	_, err = d.n.apply(op, dbChange{Path: d.path, Type: name, Data: raw})
	return err
}

// dbNode resolves the db.Node for a bucket path.
func dbNode(st *harbor.Store, path []string) db.Node {
	var node db.Node = st.DB()
	if len(path) > 0 {
		node = node.From(path...)
	}
	return node
}

func (f *fsm) applyDBUpsert(ctx context.Context, a dbChange, update bool) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := f.node.newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	node := dbNode(st, a.Path)
	if update {
		return node.Update(ctx, ptr)
	}
	return node.Save(ctx, ptr)
}

func (f *fsm) applyDBUpdateField(ctx context.Context, a dbFieldChange) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := f.node.newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	// Decode the value into the field's actual type so JSON round-tripping does
	// not turn an int field into a float64.
	fieldVal := reflect.ValueOf(ptr).Elem().FieldByName(a.Field)
	if !fieldVal.IsValid() {
		return fmt.Errorf("consensus: unknown field %q", a.Field)
	}
	value := reflect.New(fieldVal.Type()).Interface()
	if err := json.Unmarshal(a.Value, value); err != nil {
		return fmt.Errorf("consensus: decode field %q: %w", a.Field, err)
	}
	node := dbNode(st, a.Path)
	return node.UpdateField(ctx, ptr, a.Field, reflect.ValueOf(value).Elem().Interface())
}

func (f *fsm) applyDBDelete(ctx context.Context, a dbChange) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := f.node.newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	return dbNode(st, a.Path).DeleteStruct(ctx, ptr)
}

func (f *fsm) applyDBDrop(ctx context.Context, a dbChange) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := f.node.newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	return dbNode(st, a.Path).Drop(ctx, ptr)
}
