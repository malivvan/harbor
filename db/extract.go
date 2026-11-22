package db

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/malivvan/storm/db/index"
	bolt "go.etcd.io/bbolt"
)

// DB tags
const (
	tagID        = "id"
	tagIdx       = "index"
	tagUniqueIdx = "unique"
	tagInline    = "inline"
	tagIncrement = "increment"
	indexPrefix  = "__storm_index_"
)

type fieldConfig struct {
	Name           string
	Index          string
	IsZero         bool
	IsID           bool
	Increment      bool
	IncrementStart int64
	IsInteger      bool
	Value          *reflect.Value
	ForceUpdate    bool
}

// structConfig is a structure gathering all the relevant informations about a model
type structConfig struct {
	Name   string
	Fields map[string]*fieldConfig
	ID     *fieldConfig
}

// bucketName resolves the bucket name for a data value.
// If the value implements BucketNamer, that name is returned.
// Otherwise the type's static name is used.
func bucketName(data interface{}) string {
	v := reflect.ValueOf(data)
	if v.IsValid() {
		if bn, ok := data.(BucketNamer); ok {
			return bn.StormBucketName()
		}
		if v.Kind() == reflect.Ptr && !v.IsNil() {
			return bucketNameFromValue(&v)
		}
	}
	return reflect.Indirect(v).Type().Name()
}

// bucketNameFromValue resolves the bucket name from a reflect.Value.
func bucketNameFromValue(v *reflect.Value) string {
	if v.Kind() == reflect.Ptr {
		if !v.IsNil() && v.CanInterface() {
			if bn, ok := v.Interface().(BucketNamer); ok {
				return bn.StormBucketName()
			}
		}
		e := v.Elem()
		v = &e
	}
	if v.CanInterface() {
		if bn, ok := v.Interface().(BucketNamer); ok {
			return bn.StormBucketName()
		}
	}
	if v.CanAddr() {
		if bn, ok := v.Addr().Interface().(BucketNamer); ok {
			return bn.StormBucketName()
		}
	}
	return v.Type().Name()
}

// structMeta is the static, per-type part of a structConfig. It is cached so
// tags are parsed once per type instead of once per operation.
type structMeta struct {
	fields map[string]*fieldMeta
	id     *fieldMeta
}

type fieldMeta struct {
	name           string
	index          string
	isID           bool
	increment      bool
	incrementStart int64
	isInteger      bool
	// path locates the field inside the struct (supporting promoted fields).
	path []int
}

// structMetaCache maps reflect.Type to *structMeta.
var structMetaCache sync.Map

func extract(s *reflect.Value, mi ...*structConfig) (*structConfig, error) {
	nameSrc := *s
	if s.Kind() == reflect.Ptr {
		e := s.Elem()
		s = &e
	}
	if s.Kind() != reflect.Struct {
		return nil, ErrBadType
	}

	typ := s.Type()

	if len(mi) > 0 {
		// Inline recursion: merge the nested fields into the parent config.
		if err := extractFields(s, mi[0], true); err != nil {
			return nil, err
		}
		return mi[0], nil
	}

	if cached, ok := structMetaCache.Load(typ); ok {
		if m, ok := cached.(*structMeta); ok && m != nil {
			return m.bind(s, &nameSrc), nil
		}
	}

	m := &structConfig{Fields: make(map[string]*fieldConfig)}
	m.Name = bucketNameFromValue(&nameSrc)
	if err := extractFields(s, m, false); err != nil {
		return nil, err
	}

	if m.ID == nil {
		return nil, ErrNoID
	}

	if cacheableType(typ) {
		if meta := makeStructMeta(typ, m); meta != nil {
			structMetaCache.Store(typ, meta)
		}
	}

	return m, nil
}

// extractFields walks the exported fields of s and merges them into m.
func extractFields(s *reflect.Value, m *structConfig, isChild bool) error {
	typ := s.Type()
	numFields := s.NumField()
	for i := 0; i < numFields; i++ {
		field := typ.Field(i)
		value := s.Field(i)

		if field.PkgPath != "" {
			continue
		}

		if err := extractField(&value, &field, m, isChild); err != nil {
			return err
		}
	}
	return nil
}

// bind builds a per-call structConfig from cached metadata, binding every
// field to the corresponding value of s and refreshing the dynamic parts.
func (m *structMeta) bind(s *reflect.Value, nameSrc *reflect.Value) *structConfig {
	cfg := &structConfig{
		Name:   bucketNameFromValue(nameSrc),
		Fields: make(map[string]*fieldConfig, len(m.fields)),
	}

	for name, fm := range m.fields {
		v := s.FieldByIndex(fm.path)
		fc := &fieldConfig{
			Name:           fm.name,
			Index:          fm.index,
			IsID:           fm.isID,
			Increment:      fm.increment,
			IncrementStart: fm.incrementStart,
			IsInteger:      fm.isInteger,
			Value:          &v,
		}
		fc.IsZero = v.IsZero()
		cfg.Fields[name] = fc
		if fm == m.id {
			cfg.ID = fc
		}
	}

	return cfg
}

// makeStructMeta converts a freshly extracted config into static metadata.
// It returns nil when a field cannot be addressed by an index path, in which
// case the config stays on the uncached path.
func makeStructMeta(typ reflect.Type, cfg *structConfig) *structMeta {
	meta := &structMeta{fields: make(map[string]*fieldMeta, len(cfg.Fields))}

	for name, fc := range cfg.Fields {
		sf, ok := typ.FieldByName(name)
		if !ok || len(sf.Index) == 0 {
			return nil
		}
		meta.fields[name] = &fieldMeta{
			name:           fc.Name,
			index:          fc.Index,
			isID:           fc.IsID,
			increment:      fc.Increment,
			incrementStart: fc.IncrementStart,
			isInteger:      fc.IsInteger,
			path:           sf.Index,
		}
	}

	if cfg.ID != nil {
		meta.id = meta.fields[cfg.ID.Name]
		if meta.id == nil {
			return nil
		}
	}

	return meta
}

// cacheableType reports whether the static field set of typ is independent of
// the concrete value. Inline pointer fields are skipped when nil by the
// dynamic extractor, so types containing them are not cached.
func cacheableType(typ reflect.Type) bool {
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !strings.Contains(f.Tag.Get("db"), tagInline) {
			continue
		}
		if f.Type.Kind() == reflect.Ptr {
			return false
		}
		if f.Type.Kind() == reflect.Struct && !cacheableType(f.Type) {
			return false
		}
	}
	return true
}

func extractField(value *reflect.Value, field *reflect.StructField, m *structConfig, isChild bool) error {
	var f *fieldConfig
	var err error

	tag := field.Tag.Get("db")
	if tag != "" {
		f = &fieldConfig{
			Name:           field.Name,
			IsZero:         isZero(value),
			IsInteger:      isInteger(value),
			Value:          value,
			IncrementStart: 1,
		}

		tags := strings.Split(tag, ",")

		for _, tag := range tags {
			switch tag {
			case tagID:
				f.IsID = true
				fallthrough
			case tagUniqueIdx, tagIdx:
				f.Index = tag
			case tagInline:
				if value.Kind() == reflect.Ptr {
					e := value.Elem()
					value = &e
				}
				if value.Kind() == reflect.Struct {
					a := value.Addr()
					_, err := extract(&a, m)
					if err != nil {
						return err
					}
				}
				// we don't need to save this field
				return nil
			default:
				if strings.HasPrefix(tag, tagIncrement) {
					f.Increment = true
					parts := strings.Split(tag, "=")
					if parts[0] != tagIncrement {
						return ErrUnknownTag
					}
					if len(parts) > 1 {
						f.IncrementStart, err = strconv.ParseInt(parts[1], 0, 64)
						if err != nil {
							return err
						}
					}
				} else {
					return ErrUnknownTag
				}
			}
		}

		if _, ok := m.Fields[f.Name]; !ok || !isChild {
			m.Fields[f.Name] = f
		}
	}

	if m.ID == nil && f != nil && f.IsID {
		m.ID = f
	}

	// the field is named ID and no ID field has been detected before
	if m.ID == nil && field.Name == "ID" {
		if f == nil {
			f = &fieldConfig{
				Index:          tagID,
				Name:           field.Name,
				IsZero:         isZero(value),
				IsInteger:      isInteger(value),
				IsID:           true,
				Value:          value,
				IncrementStart: 1,
			}
			m.Fields[field.Name] = f
		} else {
			// A tagged ID (for example `db:"increment"`) is still the
			// primary key: make it addressable by the direct key path.
			f.IsID = true
			f.Index = tagID
		}
		m.ID = f
	}

	return nil
}

// refresh recomputes the dynamic (value-dependent) part of a config whose
// Value pointers alias a record that was decoded in place.
func (m *structConfig) refresh() {
	for _, f := range m.Fields {
		if f.Value != nil && f.Value.IsValid() {
			f.IsZero = isZero(f.Value)
		}
	}
}

func extractSingleField(ref *reflect.Value, fieldName string) (*structConfig, error) {
	var cfg structConfig
	cfg.Fields = make(map[string]*fieldConfig)

	f, ok := ref.Type().FieldByName(fieldName)
	if !ok || f.PkgPath != "" {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	v := ref.FieldByName(fieldName)
	err := extractField(&v, &f, &cfg, false)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func getIndex(bucket *bolt.Bucket, idxKind string, fieldName string) (index.Index, error) {
	var idx index.Index
	var err error

	name := []byte(indexPrefix + fieldName)

	switch idxKind {
	case tagID:
		idx, err = index.NewIDIndex(bucket, name)
	case tagUniqueIdx:
		idx, err = index.NewUniqueIndex(bucket, name)
	case tagIdx:
		if bucket.Writable() {
			idx, err = index.NewListIndex(bucket, name)
		} else {
			// Read paths only need the value bucket; defer the IDs bucket.
			idx, err = index.NewListIndexRead(bucket, name)
		}
	default:
		err = ErrIdxNotFound
	}

	return idx, err
}

// resolveIndexes opens every index handle a config needs, once, so callers
// that touch several indexed fields (or several records) do not re-open the
// same buckets per field and per record.
func resolveIndexes(bucket *bolt.Bucket, cfg *structConfig) (map[string]index.Index, error) {
	indexes := make(map[string]index.Index, len(cfg.Fields))
	for fieldName, fieldCfg := range cfg.Fields {
		if fieldCfg.Index == "" {
			continue
		}
		idx, err := getIndex(bucket, fieldCfg.Index, fieldName)
		if err != nil {
			return nil, err
		}
		indexes[fieldName] = idx
	}
	return indexes, nil
}

func isZero(v *reflect.Value) bool {
	if v == nil || !v.IsValid() {
		return true
	}
	return v.IsZero()
}

func isInteger(v *reflect.Value) bool {
	return v != nil && v.Kind() >= reflect.Int && v.Kind() <= reflect.Uint64
}
