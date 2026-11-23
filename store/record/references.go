package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	bolt "go.etcd.io/bbolt"
)

// Reserved names for the reference registry. The registry records which fields
// reference which buckets so a delete or drop can enforce the relation without
// a compile-time schema.
const (
	refsBucketName  = "__harbor_refs__"
	refSchemaPrefix = "schema:"

	onDeleteRestrict   = "restrict"
	onDeleteCascade    = "cascade"
	maxRefCascadeDepth = 32

	idsIndexBucketName = "harbor__ids"
)

// refSchema is the persisted schema of one referencing bucket.
type refSchema struct {
	IDField string              `json:"id_field"`
	Fields  map[string]refField `json:"fields"`
}

// refField is one reference in a schema.
type refField struct {
	Target   string `json:"target"`
	OnDelete string `json:"on_delete,omitempty"`
}

// refRelation is an inbound reference found in the registry.
type refRelation struct {
	Source   string
	Field    string
	IDField  string
	OnDelete string
}

// refMatch is one source record that references a target value.
type refMatch struct {
	key []byte
	rec map[string]any
}

// refsBucket returns the registry bucket, creating it when create is set.
func (n *node) refsBucket(tx *bolt.Tx, create bool) (*bolt.Bucket, error) {
	if create {
		return n.createBucketIfNotExists(tx, refsBucketName)
	}
	return n.getBucket(tx, refsBucketName), nil
}

// registerRefSchema records the bucket's reference fields. It is called from
// save, inside the same transaction, so a failed save leaves no registration.
func (n *node) registerRefSchema(tx *bolt.Tx, cfg *structConfig) error {
	schema := refSchema{Fields: map[string]refField{}}
	for name, field := range cfg.Fields {
		if field.Ref == "" {
			continue
		}
		schema.Fields[name] = refField{Target: field.Ref, OnDelete: field.OnDelete}
	}
	if len(schema.Fields) == 0 {
		return nil
	}
	if cfg.ID != nil {
		schema.IDField = cfg.ID.Name
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("record: encode reference schema: %w", err)
	}
	b, err := n.refsBucket(tx, true)
	if err != nil {
		return err
	}
	if b == nil {
		return nil
	}
	return b.Put([]byte(refSchemaPrefix+cfg.Name), raw)
}

// validateRefFields checks that every non-zero reference points at an existing
// record. It runs inside the save transaction, so a missing target rolls the
// whole save back.
func (n *node) validateRefFields(tx *bolt.Tx, cfg *structConfig) error {
	for name, field := range cfg.Fields {
		if field.Ref == "" || field.Value == nil || !field.Value.IsValid() || isZero(field.Value) {
			continue
		}
		target := n.getBucket(tx, field.Ref)
		if target == nil {
			return fmt.Errorf("%w: %s.%s refers to %q", ErrReferenceTargetMissing, cfg.Name, name, field.Ref)
		}
		key, err := n.keyBytes(field.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		if target.Get(key) == nil {
			return fmt.Errorf("%w: %s.%s refers to %s:%v", ErrDanglingReference, cfg.Name, name, field.Ref, field.Value.Interface())
		}
	}
	return nil
}

// inboundRefs returns the relations that point at targetBucket.
func (n *node) inboundRefs(tx *bolt.Tx, targetBucket string) ([]refRelation, error) {
	b := n.getBucket(tx, refsBucketName)
	if b == nil {
		return nil, nil
	}
	var out []refRelation
	err := b.ForEach(func(k, v []byte) error {
		if v == nil || !bytes.HasPrefix(k, []byte(refSchemaPrefix)) {
			return nil
		}
		var schema refSchema
		if err := json.Unmarshal(v, &schema); err != nil {
			return fmt.Errorf("record: decode reference schema %q: %w", k, err)
		}
		source := string(k[len(refSchemaPrefix):])
		for field, ref := range schema.Fields {
			if ref.Target != targetBucket || source == targetBucket {
				continue
			}
			onDelete := ref.OnDelete
			if onDelete == "" {
				onDelete = onDeleteRestrict
			}
			out = append(out, refRelation{Source: source, Field: field, IDField: schema.IDField, OnDelete: onDelete})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// refMatches finds the source records whose reference field equals targetValue.
// It scans and decodes the source bucket: cascades are rare and this avoids
// depending on the encoding of an arbitrary reference field.
func (n *node) refMatches(ctx context.Context, tx *bolt.Tx, rel refRelation, targetValue any) ([]refMatch, error) {
	source := n.getBucket(tx, rel.Source)
	if source == nil {
		return nil, nil
	}
	want := canonicalKey([]any{targetValue})
	var matches []refMatch
	seen := 0
	err := source.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if seen%256 == 0 {
			if err := checkContext(ctx); err != nil {
				return err
			}
		}
		seen++
		var rec map[string]any
		if err := n.unmarshal(v, &rec); err != nil {
			return err
		}
		got, ok := rec[rel.Field]
		if !ok {
			return nil
		}
		if canonicalKey([]any{got}) != want {
			return nil
		}
		matches = append(matches, refMatch{key: append([]byte(nil), k...), rec: rec})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// enforceDeleteRefs enforces references pointing at (targetBucket, targetValue)
// before that record is deleted. Restrict returns an error; cascade recursively
// deletes the referencing records in the same transaction.
func (n *node) enforceDeleteRefs(ctx context.Context, tx *bolt.Tx, targetBucket string, targetValue any, visiting map[string]bool, depth int) error {
	if depth > maxRefCascadeDepth {
		return fmt.Errorf("%w: %s:%v", ErrReferenceCycle, targetBucket, targetValue)
	}
	relations, err := n.inboundRefs(tx, targetBucket)
	if err != nil {
		return err
	}
	for _, rel := range relations {
		matches, err := n.refMatches(ctx, tx, rel, targetValue)
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			continue
		}
		if rel.OnDelete != onDeleteCascade {
			return fmt.Errorf("%w: %s.%s refers to %s:%v", ErrReferenceViolation, rel.Source, rel.Field, targetBucket, targetValue)
		}
		for _, match := range matches {
			visitKey := rel.Source + "\x00" + string(match.key)
			if visiting[visitKey] {
				continue
			}
			visiting[visitKey] = true
			if err := n.enforceDeleteRefs(ctx, tx, rel.Source, match.rec[rel.IDField], visiting, depth+1); err != nil {
				return err
			}
			if err := n.deleteRecordRaw(ctx, tx, rel.Source, match.key); err != nil {
				return err
			}
			delete(visiting, visitKey)
		}
	}
	return nil
}

// enforceDropRefs enforces references pointing at a bucket that is about to be
// dropped. Restrict fails if any source record holds a non-zero reference;
// cascade deletes those source records.
func (n *node) enforceDropRefs(ctx context.Context, tx *bolt.Tx, targetBucket string, visiting map[string]bool, depth int) error {
	if depth > maxRefCascadeDepth {
		return fmt.Errorf("%w: %s", ErrReferenceCycle, targetBucket)
	}
	relations, err := n.inboundRefs(tx, targetBucket)
	if err != nil {
		return err
	}
	for _, rel := range relations {
		matches, err := n.anyRefMatches(ctx, tx, rel)
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			continue
		}
		if rel.OnDelete != onDeleteCascade {
			return fmt.Errorf("%w: %s.%s refers to %s", ErrReferenceViolation, rel.Source, rel.Field, targetBucket)
		}
		for _, match := range matches {
			visitKey := rel.Source + "\x00" + string(match.key)
			if visiting[visitKey] {
				continue
			}
			visiting[visitKey] = true
			if err := n.enforceDeleteRefs(ctx, tx, rel.Source, match.rec[rel.IDField], visiting, depth+1); err != nil {
				return err
			}
			if err := n.deleteRecordRaw(ctx, tx, rel.Source, match.key); err != nil {
				return err
			}
			delete(visiting, visitKey)
		}
	}
	return nil
}

// anyRefMatches finds source records with a non-zero value in a reference
// field. It is used when the whole target bucket is dropped.
func (n *node) anyRefMatches(ctx context.Context, tx *bolt.Tx, rel refRelation) ([]refMatch, error) {
	source := n.getBucket(tx, rel.Source)
	if source == nil {
		return nil, nil
	}
	var matches []refMatch
	seen := 0
	err := source.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if seen%256 == 0 {
			if err := checkContext(ctx); err != nil {
				return err
			}
		}
		seen++
		var rec map[string]any
		if err := n.unmarshal(v, &rec); err != nil {
			return err
		}
		got, ok := rec[rel.Field]
		if !ok || isEmptyRef(got) {
			return nil
		}
		matches = append(matches, refMatch{key: append([]byte(nil), k...), rec: rec})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// isEmptyRef reports whether a decoded reference value is the zero value.
func isEmptyRef(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case float64:
		return t == 0
	case bool:
		return !t
	}
	return false
}

// deleteRecordRaw deletes one record by its physical key and removes the id
// from every index bucket of the source bucket. It is used for cascaded
// deletes, where only the persisted schema is available.
func (n *node) deleteRecordRaw(ctx context.Context, tx *bolt.Tx, bucketName string, key []byte) error {
	bucket := n.getBucket(tx, bucketName)
	if bucket == nil {
		return nil
	}
	if err := bucket.Delete(key); err != nil {
		return err
	}

	var indexNames [][]byte
	if err := bucket.ForEach(func(name, v []byte) error {
		if v == nil && bytes.HasPrefix(name, []byte(indexPrefix)) {
			indexNames = append(indexNames, append([]byte(nil), name...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, name := range indexNames {
		idxBucket := bucket.Bucket(name)
		if idxBucket == nil {
			continue
		}
		var doomed [][]byte
		if err := idxBucket.ForEach(func(k, v []byte) error {
			if v != nil && bytes.Equal(v, key) {
				doomed = append(doomed, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range doomed {
			if err := idxBucket.Delete(k); err != nil {
				return err
			}
		}
		if ids := idxBucket.Bucket([]byte(idsIndexBucketName)); ids != nil {
			if err := ids.Delete(key); err != nil {
				return err
			}
		}
	}
	return checkContext(ctx)
}

// ValidateSchema verifies that every reference field in the supplied sample
// records names an existing bucket. It catches a typo at startup instead of on
// the first save. Samples are ordinary empty struct pointers; nothing is
// written.
func (n *node) ValidateSchema(ctx context.Context, samples ...any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("validate schema", err)
	}
	return wrapError("validate schema", n.readTx(ctx, func(tx *bolt.Tx) error {
		for _, sample := range samples {
			ref := reflect.ValueOf(sample)
			if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
				return ErrStructPtrNeeded
			}
			cfg, err := extract(&ref)
			if err != nil {
				return err
			}
			for name, field := range cfg.Fields {
				if field.Ref == "" {
					continue
				}
				if n.getBucket(tx, field.Ref) == nil {
					return fmt.Errorf("%w: %s.%s refers to %q", ErrReferenceTargetMissing, cfg.Name, name, field.Ref)
				}
			}
			if err := checkContext(ctx); err != nil {
				return err
			}
		}
		return nil
	}))
}
