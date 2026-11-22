package q

import (
	"errors"
	"go/token"
	"reflect"
	"sync"
)

// ErrUnknownField is returned when an unknown field is passed.
var ErrUnknownField = errors.New("unknown field")

type fieldMatcherDelegate struct {
	FieldMatcher
	Field string
}

// NewFieldMatcher creates a Matcher for a given field.
func NewFieldMatcher(field string, fm FieldMatcher) Matcher {
	return fieldMatcherDelegate{Field: field, FieldMatcher: fm}
}

// FieldMatcher can be used in NewFieldMatcher as a simple way to create the
// most common Matcher: A Matcher that evaluates one field's value.
// For more complex scenarios, implement the Matcher interface directly.
type FieldMatcher interface {
	MatchField(v interface{}) (bool, error)
}

func (r fieldMatcherDelegate) Match(i interface{}) (bool, error) {
	v := reflect.Indirect(reflect.ValueOf(i))
	return r.MatchValue(&v)
}

func (r fieldMatcherDelegate) MatchValue(v *reflect.Value) (bool, error) {
	if v == nil || !v.IsValid() {
		return false, ErrUnknownField
	}
	rec := reflect.Indirect(*v)
	if rec.Kind() != reflect.Struct {
		return false, ErrUnknownField
	}
	path, ok := fieldPath(rec.Type(), r.Field)
	if !ok {
		return false, ErrUnknownField
	}
	field := rec.FieldByIndex(path)
	if !field.IsValid() || !field.CanInterface() {
		return false, ErrUnknownField
	}
	return r.MatchField(field.Interface())
}

// NewField2FieldMatcher creates a Matcher for a given field1 and field2.
func NewField2FieldMatcher(field1, field2 string, tok token.Token) Matcher {
	return field2fieldMatcherDelegate{Field1: field1, Field2: field2, Tok: tok}
}

type field2fieldMatcherDelegate struct {
	Field1, Field2 string
	Tok            token.Token
}

func (r field2fieldMatcherDelegate) Match(i interface{}) (bool, error) {
	v := reflect.Indirect(reflect.ValueOf(i))
	return r.MatchValue(&v)
}

func (r field2fieldMatcherDelegate) MatchValue(v *reflect.Value) (bool, error) {
	if v == nil || !v.IsValid() {
		return false, ErrUnknownField
	}
	rec := reflect.Indirect(*v)
	if rec.Kind() != reflect.Struct {
		return false, ErrUnknownField
	}
	path1, ok1 := fieldPath(rec.Type(), r.Field1)
	path2, ok2 := fieldPath(rec.Type(), r.Field2)
	if !ok1 || !ok2 {
		return false, ErrUnknownField
	}
	field1 := rec.FieldByIndex(path1)
	field2 := rec.FieldByIndex(path2)
	if !field1.IsValid() || !field1.CanInterface() || !field2.IsValid() || !field2.CanInterface() {
		return false, ErrUnknownField
	}
	return compare(field1.Interface(), field2.Interface(), r.Tok), nil
}

// fieldPathKey identifies a cached struct-field path.
type fieldPathKey struct {
	typ  reflect.Type
	name string
}

// fieldPathCache maps (type, field name) to a reflect field index path so
// matchers do not re-resolve FieldByName for every record.
var fieldPathCache sync.Map

func fieldPath(typ reflect.Type, name string) ([]int, bool) {
	key := fieldPathKey{typ: typ, name: name}
	if cached, ok := fieldPathCache.Load(key); ok {
		path, _ := cached.([]int)
		return path, path != nil
	}
	f, ok := typ.FieldByName(name)
	if !ok || f.PkgPath != "" {
		return nil, false
	}
	fieldPathCache.Store(key, f.Index)
	return f.Index, true
}
