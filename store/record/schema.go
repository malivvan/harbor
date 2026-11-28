package record

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
)

const schemaVersionKey = "schema_version"

// ErrSchemaInvalid reports a record that violates its registered TypeSchema.
var ErrSchemaInvalid = errors.New("record: schema violation")

// ErrTypeSchemaTooNew reports a stored type version newer than the build's.
var ErrTypeSchemaTooNew = errors.New("record: type schema is newer than this build")

// ErrInvalidTypeSchema reports a malformed TypeSchema at open.
var ErrInvalidTypeSchema = errors.New("record: invalid type schema")

// TypeSchema describes the shape and migration history of one mapped type.
type TypeSchema struct {
	// Type is a sample value or pointer of the mapped struct. Its bucket name
	// selects the schema.
	Type any
	// Version is the version the type has after every step has been applied.
	Version uint64
	// Required lists fields that must be non-zero.
	Required []string
	// Enums restricts fields to the listed string values.
	Enums map[string][]string
	// Defaults are applied to zero-valued fields before validation.
	Defaults map[string]any
	// Steps migrate a stored type version to a newer one.
	Steps []TypeStep
}

// TypeStep is one typed migration. Apply runs inside the cross-store write
// transaction; returning an error rolls the step and the version back.
type TypeStep struct {
	From  uint64
	To    uint64
	Apply func(ctx context.Context, tx Tx) error
}

// WithSchema returns a copy of the config with one TypeSchema registered.
func (c Config) WithSchema(schema TypeSchema) Config {
	next := c
	next.Schemas = append(append([]TypeSchema(nil), c.Schemas...), schema)
	return next
}

type compiledSchema struct {
	spec TypeSchema
	name string
	typ  reflect.Type
}

func compileSchema(schema TypeSchema) (*compiledSchema, error) {
	if schema.Type == nil {
		return nil, fmt.Errorf("%w: Type is required", ErrInvalidTypeSchema)
	}
	typ := reflect.TypeOf(schema.Type)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: Type must be a struct or pointer to struct", ErrInvalidTypeSchema)
	}
	name := bucketName(schema.Type)
	if name == "" {
		return nil, fmt.Errorf("%w: Type has no bucket name", ErrInvalidTypeSchema)
	}
	for _, field := range schema.Required {
		if _, ok := typ.FieldByName(field); !ok {
			return nil, fmt.Errorf("%w: %s has no field %q", ErrInvalidTypeSchema, name, field)
		}
	}
	for field := range schema.Enums {
		if _, ok := typ.FieldByName(field); !ok {
			return nil, fmt.Errorf("%w: %s has no field %q", ErrInvalidTypeSchema, name, field)
		}
	}
	for field, value := range schema.Defaults {
		sf, ok := typ.FieldByName(field)
		if !ok {
			return nil, fmt.Errorf("%w: %s has no field %q", ErrInvalidTypeSchema, name, field)
		}
		if value == nil {
			continue
		}
		if !reflect.TypeOf(value).AssignableTo(sf.Type) {
			return nil, fmt.Errorf("%w: default for %s.%s is %T, field is %s", ErrInvalidTypeSchema, name, field, value, sf.Type)
		}
	}
	// Steps must be contiguous from the stored version to Version.
	if len(schema.Steps) > 0 {
		steps := append([]TypeStep(nil), schema.Steps...)
		sort.Slice(steps, func(i, j int) bool { return steps[i].From < steps[j].From })
		next := steps[0].From
		for _, step := range steps {
			if step.From != next || step.To <= step.From || step.Apply == nil {
				return nil, fmt.Errorf("%w: %s steps are not contiguous", ErrInvalidTypeSchema, name)
			}
			next = step.To
		}
		if next != schema.Version {
			return nil, fmt.Errorf("%w: %s steps end at %d, Version is %d", ErrInvalidTypeSchema, name, next, schema.Version)
		}
	} else if schema.Version > 0 {
		return nil, fmt.Errorf("%w: %s declares Version %d without steps", ErrInvalidTypeSchema, name, schema.Version)
	}
	return &compiledSchema{spec: schema, name: name, typ: typ}, nil
}

func (s *Store) compileSchemas() error {
	if len(s.schemas) > 0 {
		return nil
	}
	s.schemas = make(map[string]*compiledSchema, len(s.cfgSchemas))
	for _, schema := range s.cfgSchemas {
		compiled, err := compileSchema(schema)
		if err != nil {
			return err
		}
		s.schemas[compiled.name] = compiled
	}
	return nil
}

func (s *Store) schemaFor(name string) *compiledSchema {
	if s == nil || len(s.schemas) == 0 {
		return nil
	}
	return s.schemas[name]
}

// structValue resolves data to a settable struct value.
func structValue(data any) (reflect.Value, error) {
	v := reflect.ValueOf(data)
	if !v.IsValid() {
		return reflect.Value{}, ErrBadType
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}, ErrBadType
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, ErrBadType
	}
	return v, nil
}

// applyDefaults fills zero-valued fields from the schema defaults.
func (c *compiledSchema) applyDefaults(data any) error {
	if len(c.spec.Defaults) == 0 {
		return nil
	}
	v, err := structValue(data)
	if err != nil {
		return err
	}
	for field, value := range c.spec.Defaults {
		fv := v.FieldByName(field)
		if !fv.IsValid() || !fv.CanSet() || !fv.IsZero() || value == nil {
			continue
		}
		rv := reflect.ValueOf(value)
		if !rv.Type().AssignableTo(fv.Type()) {
			return fmt.Errorf("%w: default for %s.%s is %T", ErrSchemaInvalid, c.name, field, value)
		}
		fv.Set(rv)
	}
	return nil
}

// validate checks required fields and enums directly on the struct.
func (c *compiledSchema) validate(data any) error {
	v, err := structValue(data)
	if err != nil {
		return err
	}
	for _, field := range c.spec.Required {
		fv := v.FieldByName(field)
		if !fv.IsValid() || fv.IsZero() {
			return fmt.Errorf("%w: %s.%s is required", ErrSchemaInvalid, c.name, field)
		}
	}
	for field, allowed := range c.spec.Enums {
		fv := v.FieldByName(field)
		if !fv.IsValid() || fv.IsZero() {
			continue
		}
		value := fmt.Sprint(fv.Interface())
		found := false
		for _, candidate := range allowed {
			if value == candidate {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: %s.%s is %q, allowed %v", ErrSchemaInvalid, c.name, field, value, allowed)
		}
	}
	return nil
}

// Validate checks data against its registered schema without writing.
func (n *node) Validate(ctx context.Context, data any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("validate", err)
	}
	if schema := n.s.schemaFor(bucketName(data)); schema != nil {
		if err := schema.validate(data); err != nil {
			return wrapError("validate", err)
		}
	}
	return nil
}

// MigrateTypes applies every registered TypeSchema's steps inside one write
// transaction. A type at the target version is a no-op; a type ahead of the
// build fails with ErrTypeSchemaTooNew.
func (s *Store) MigrateTypes(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("migrate types", err)
	}
	if s == nil || len(s.schemas) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.schemas))
	for name := range s.schemas {
		names = append(names, name)
	}
	sort.Strings(names)

	return wrapError("migrate types", s.WriteTransaction(ctx, func(txNode Node) error {
		root, ok := txNode.(*node)
		if !ok {
			return ErrBadType
		}
		tx, ok := txNode.(Tx)
		if !ok {
			return ErrBadType
		}
		for _, name := range names {
			schema := s.schemas[name]
			bucket, err := root.createBucketIfNotExists(root.tx, name)
			if err != nil {
				return err
			}
			meta, err := newMeta(bucket, root)
			if err != nil {
				return err
			}
			current, err := meta.schemaVersion()
			if err != nil {
				return err
			}
			if current > schema.spec.Version {
				return fmt.Errorf("%w: %s is version %d, this build supports %d", ErrTypeSchemaTooNew, name, current, schema.spec.Version)
			}
			for current < schema.spec.Version {
				var step *TypeStep
				for i := range schema.spec.Steps {
					if schema.spec.Steps[i].From == current {
						step = &schema.spec.Steps[i]
						break
					}
				}
				if step == nil {
					return fmt.Errorf("%w: %s has no step from version %d", ErrInvalidTypeSchema, name, current)
				}
				if err := step.Apply(ctx, tx); err != nil {
					return err
				}
				current = step.To
				if err := meta.setSchemaVersion(current); err != nil {
					return err
				}
			}
		}
		return nil
	}))
}

func (m *meta) schemaVersion() (uint64, error) {
	if m == nil || m.bucket == nil {
		return 0, nil
	}
	raw := m.bucket.Get([]byte(schemaVersionKey))
	if raw == nil {
		return 0, nil
	}
	plain, err := m.enc.openMetadata(raw)
	if err != nil {
		return 0, err
	}
	if len(plain) != 8 {
		return 0, fmt.Errorf("record: corrupt schema version: %d bytes", len(plain))
	}
	return binary.BigEndian.Uint64(plain), nil
}

func (m *meta) setSchemaVersion(version uint64) error {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, version)
	sealed, err := m.enc.sealMetadata(raw)
	if err != nil {
		return err
	}
	return m.bucket.Put([]byte(schemaVersionKey), sealed)
}
