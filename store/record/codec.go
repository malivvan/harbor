package record

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// jsonCodecName is the stable identifier stored in bucket metadata. It must not
// change: databases written by earlier versions record it.
const jsonCodecName = "json"

// Codec represents a codec used to marshal and unmarshal entities.
//
// # Pointer contract
//
// Marshal receives the caller's entity pointer for Save, Update and
// UpdateField; key/value Set passes its value unchanged, which may be a scalar
// or a map (the internal format record uses a string).
//
// Unmarshal receives a pointer whose shape depends on the read path:
//
//   - One(ctx, ..., &out) and Each(ctx, &T{}, fn): *T.
//   - Find/Range/Prefix/All into *[]T: *T, decoded into the element slot.
//   - Find/Range/Prefix/All into *[]*T: **T (the address of a pointer slot).
//   - Iterator: internal decodes use *T; Decode(&out) receives the caller's
//     pointer unchanged.
//   - Aggregate and reference enforcement decode into *map[string]any.
//
// A codec must therefore accept a pointer to the destination value, including
// one extra indirection for pointer-element slices. A codec that rejects **T
// makes []*T reads fail while []T reads succeed; db/README.md documents the
// matrix and codec_contract_test.go runs it against a strict codec.
//
// The destination is zeroed before Unmarshal is called, so a codec may assume
// it is writing into a fresh value.
type Codec interface {
	// Marshal encodes v.
	Marshal(v interface{}) ([]byte, error)

	// Unmarshal decodes b into v.
	Unmarshal(b []byte, v interface{}) error

	// Name returns the stable codec identifier stored in Harbor metadata.
	Name() string
}

// ProjectedCodec is an optional Codec capability: decode only the named
// top-level fields into v. A codec that does not implement it falls back to a
// full decode followed by zeroing the unprojected fields, which is correct but
// does not avoid the work.
type ProjectedCodec interface {
	UnmarshalProjected(b []byte, fields []string, v any) error
}

// JSONCodec encodes and decodes entities with encoding/json. It is the codec
// Harbor uses when Config.Codec is nil.
var JSONCodec Codec = jsonCodec{}

// jsonCodec implements Codec on top of encoding/json.
type jsonCodec struct{}

// Marshal encodes v.
func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes b into v.
func (jsonCodec) Unmarshal(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}

// Name returns the codec identifier stored in Harbor metadata.
func (jsonCodec) Name() string {
	return jsonCodecName
}

// UnmarshalProjected decodes only the requested top-level fields.
func (jsonCodec) UnmarshalProjected(b []byte, fields []string, v any) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	dest := reflect.ValueOf(v)
	if !dest.IsValid() || dest.Kind() != reflect.Pointer || dest.IsNil() || dest.Elem().Kind() != reflect.Struct {
		// The caller expects a struct; let encoding/json produce its usual
		// error instead of guessing.
		return json.Unmarshal(b, v)
	}
	elem := dest.Elem()
	elem.Set(reflect.Zero(elem.Type()))
	typ := elem.Type()

	for _, requested := range fields {
		field, ok := projectedField(typ, requested)
		if !ok {
			return fmt.Errorf("db: projected field %q does not exist", requested)
		}
		rawValue, ok := raw[jsonFieldName(field)]
		if !ok {
			continue // absent fields stay zero
		}
		target := elem.FieldByIndex(field.Index)
		if !target.CanAddr() {
			return fmt.Errorf("db: projected field %q is not settable", requested)
		}
		if err := json.Unmarshal(rawValue, target.Addr().Interface()); err != nil {
			return err
		}
	}
	return nil
}

// projectedField resolves a requested name to an exported struct field, by Go
// name or JSON tag.
func projectedField(typ reflect.Type, name string) (reflect.StructField, bool) {
	if name == "" || strings.Contains(name, ".") {
		return reflect.StructField{}, false
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		if field.Name == name || jsonFieldName(field) == name {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

// jsonFieldName returns the JSON key for a struct field.
func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return ""
	}
	name := strings.Split(tag, ",")[0]
	if name != "" {
		return name
	}
	return field.Name
}

// unmarshalProjected decodes with the codec's projection capability when it has
// one, or falls back to a full decode and zeroes the unprojected fields.
func unmarshalProjected(codec Codec, b []byte, fields []string, v any) error {
	if projected, ok := codec.(ProjectedCodec); ok {
		return projected.UnmarshalProjected(b, fields, v)
	}
	if err := codec.Unmarshal(b, v); err != nil {
		return err
	}
	dest := reflect.ValueOf(v)
	if !dest.IsValid() || dest.Kind() != reflect.Pointer || dest.IsNil() || dest.Elem().Kind() != reflect.Struct {
		return nil
	}
	elem := dest.Elem()
	typ := elem.Type()
	keep := make(map[string]struct{}, len(fields))
	for _, name := range fields {
		field, ok := projectedField(typ, name)
		if !ok {
			return fmt.Errorf("db: projected field %q does not exist", name)
		}
		keep[field.Name] = struct{}{}
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		if _, ok := keep[field.Name]; ok {
			continue
		}
		elem.Field(i).Set(reflect.Zero(field.Type))
	}
	return nil
}
