package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/store/record"
)

// typeRegistry maps a record's reflect.Type name to its type, so the FSM can
// reconstruct db records from a command payload. It is process-global: every
// node in a cluster runs the same application binary, so a type registered on
// one node is registered on all. The leader registers types lazily on first
// write; callers of a multi-process deployment should still call Register on
// every node before writes are replicated so followers can apply them.
var typeRegistry sync.Map // string -> reflect.Type

// Register records a record type for replication. It accepts a value or a
// pointer to the record type. Call it on every node that must apply replicated
// db writes of that type.
func Register(v any) {
	if v == nil {
		return
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		typeRegistry.Store(t.String(), t)
	}
}

// registerType records t and returns its name.
func registerType(t reflect.Type) string {
	name := t.String()
	typeRegistry.Store(name, t)
	return name
}

// newValue reconstructs a pointer to the registered type and decodes raw into
// it.
func newValue(name string, raw json.RawMessage) (any, error) {
	v, ok := typeRegistry.Load(name)
	if !ok {
		return nil, fmt.Errorf("consensus: unregistered record type %q", name)
	}
	t := v.(reflect.Type)
	ptr := reflect.New(t).Interface()
	if err := json.Unmarshal(raw, ptr); err != nil {
		return nil, fmt.Errorf("consensus: decode record %s: %w", name, err)
	}
	return ptr, nil
}

// Record is the replicated write view of the record sub-store. Reads should use
// Node.Store().Record() directly; writes go through the methods here so they are
// proposed through raft and applied on every node.
type Record struct {
	n    *Node
	path []string
}

// recordChange is the payload for record-level db writes.
type recordChange struct {
	Path []string        `json:"path"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// recordFieldChange is the payload for db.UpdateField.
type recordFieldChange struct {
	Path  []string        `json:"path"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data"`
	Field string          `json:"field"`
	Value json.RawMessage `json:"value"`
}

// recordKVArg is the payload for raw key/value writes. Key and Value are already
// encoded: the leader encodes the key with the store's key encoding and, for
// Set, the value with the store's codec. Every node seals them with its own
// key material when applying, so only logical bytes cross the wire.
type recordKVArg struct {
	Path   []string `json:"path"`
	Bucket string   `json:"bucket"`
	Key    []byte   `json:"key"`
	Value  []byte   `json:"value,omitempty"`
}

// Record returns the replicated record sub-store.
func (n *Node) Record() *Record {
	if n == nil {
		return nil
	}
	return &Record{n: n}
}

// From returns a Record view rooted at a nested bucket path, mirroring db.Node.From.
func (d *Record) From(addend ...string) *Record {
	if d == nil || d.n == nil {
		return nil
	}
	p := make([]string, 0, len(d.path)+len(addend))
	p = append(p, d.path...)
	p = append(p, addend...)
	return &Record{n: d.n, path: p}
}

// Save replicates db.Node.Save.
func (d *Record) Save(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opRecordSave, "", "", data)
}

// Update replicates db.Node.Update.
func (d *Record) Update(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opRecordUpdate, "", "", data)
}

// UpdateField replicates db.Node.UpdateField.
func (d *Record) UpdateField(ctx context.Context, data any, field string, value any) error {
	return d.recordWrite(ctx, opRecordUpdateField, field, value, data)
}

// DeleteStruct replicates db.Node.DeleteStruct.
func (d *Record) DeleteStruct(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opRecordDelete, "", "", data)
}

// Drop replicates db.Node.Drop.
func (d *Record) Drop(ctx context.Context, data any) error {
	return d.recordWrite(ctx, opRecordDrop, "", "", data)
}

// Set replicates db.Node.Set. The value is encoded with the leader's codec
// once; the resulting bytes are stored (and sealed) identically on every node,
// so all nodes must run the same codec. A nil value stores a zero-length
// value, exactly like the direct API.
func (d *Record) Set(ctx context.Context, bucket string, key, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	node, err := d.localNode()
	if err != nil {
		return err
	}
	kb, err := record.EncodeKey(key, node.Codec())
	if err != nil {
		return fmt.Errorf("consensus: encode key: %w", err)
	}
	var raw []byte
	if value != nil {
		raw, err = node.Codec().Marshal(value)
		if err != nil {
			return fmt.Errorf("consensus: encode value: %w", err)
		}
	}
	_, err = d.n.apply(opRecordSet, recordKVArg{Path: d.path, Bucket: bucket, Key: kb, Value: raw})
	return err
}

// SetBytes replicates db.Node.SetBytes. The bytes are stored as-is (and
// sealed per node), so callers can replicate a value they encoded themselves.
func (d *Record) SetBytes(ctx context.Context, bucket string, key any, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	node, err := d.localNode()
	if err != nil {
		return err
	}
	kb, err := record.EncodeKey(key, node.Codec())
	if err != nil {
		return fmt.Errorf("consensus: encode key: %w", err)
	}
	_, err = d.n.apply(opRecordSet, recordKVArg{Path: d.path, Bucket: bucket, Key: kb, Value: value})
	return err
}

// Delete replicates db.Node.Delete for a key/value entry.
func (d *Record) Delete(ctx context.Context, bucket string, key any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	node, err := d.localNode()
	if err != nil {
		return err
	}
	kb, err := record.EncodeKey(key, node.Codec())
	if err != nil {
		return fmt.Errorf("consensus: encode key: %w", err)
	}
	_, err = d.n.apply(opRecordDeleteKey, recordKVArg{Path: d.path, Bucket: bucket, Key: kb})
	return err
}

// localNode returns the leader's local db node for this view. It is used to
// encode keys and values exactly as a direct write would.
func (d *Record) localNode() (record.Node, error) {
	if d == nil || d.n == nil {
		return nil, ErrClosed
	}
	st := d.n.storeHandle()
	if st == nil {
		return nil, ErrClosed
	}
	return recordNode(st, d.path), nil
}

func (d *Record) recordWrite(ctx context.Context, op, field string, value, data any) error {
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
	name := registerType(ref.Elem().Type())
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("consensus: encode record: %w", err)
	}

	if op == opRecordUpdateField {
		vraw, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("consensus: encode field value: %w", err)
		}
		_, err = d.n.apply(op, recordFieldChange{Path: d.path, Type: name, Data: raw, Field: field, Value: vraw})
		return err
	}
	_, err = d.n.apply(op, recordChange{Path: d.path, Type: name, Data: raw})
	return err
}

// recordNode resolves the db.Node for a bucket path.
func recordNode(st *tempest.Store, path []string) record.Node {
	var node record.Node = st.Record()
	if len(path) > 0 {
		node = node.From(path...)
	}
	return node
}

func (f *fsm) applyRecordUpsert(ctx context.Context, a recordChange, update bool) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	node := recordNode(st, a.Path)
	if update {
		return node.Update(ctx, ptr)
	}
	return node.Save(ctx, ptr)
}

func (f *fsm) applyRecordUpdateField(ctx context.Context, a recordFieldChange) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := newValue(a.Type, a.Data)
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
	node := recordNode(st, a.Path)
	return node.UpdateField(ctx, ptr, a.Field, reflect.ValueOf(value).Elem().Interface())
}

func (f *fsm) applyRecordDelete(ctx context.Context, a recordChange) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	return recordNode(st, a.Path).DeleteStruct(ctx, ptr)
}

func (f *fsm) applyRecordDrop(ctx context.Context, a recordChange) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	ptr, err := newValue(a.Type, a.Data)
	if err != nil {
		return err
	}
	return recordNode(st, a.Path).Drop(ctx, ptr)
}

func (f *fsm) applyRecordSet(ctx context.Context, a recordKVArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return recordNode(st, a.Path).SetBytes(ctx, a.Bucket, a.Key, a.Value)
}

func (f *fsm) applyRecordDeleteKey(ctx context.Context, a recordKVArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return recordNode(st, a.Path).Delete(ctx, a.Bucket, a.Key)
}
