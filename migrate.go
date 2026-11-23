package harbor

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// ErrSchemaTooNew reports a store whose application schema version is newer
// than the binary's. It was written by a later build and must not be
// downgraded.
var ErrSchemaTooNew = errors.New("harbor: store schema is newer than this build")

// ErrInvalidSchema reports a malformed migration plan.
var ErrInvalidSchema = errors.New("harbor: invalid schema")

// Reserved bucket and key for the application schema version. It is separate
// from the system header so existing stores keep opening.
const (
	schemaBucket = "harbor.schema"
	schemaKey    = "version"
)

// Step is one application schema migration. Apply runs inside the cross-store
// transaction; returning an error rolls the step and every earlier step back.
//
// Steps must be deterministic and idempotent: they must not depend on
// wall-clock time or randomness, and running a step whose effects are already
// present must not corrupt data (the version record is what normally prevents
// a re-run).
type Step struct {
	From  uint64
	To    uint64
	Apply func(ctx context.Context, tx *Tx) error
}

// Schema is an application migration plan. Version is the version the store
// has after every step has been applied.
type Schema struct {
	Version uint64
	Steps   []Step
}

// Migrate brings the store's application schema to schema.Version, applying
// the steps in order inside one cross-store transaction.
//
// A store at the same version is a no-op; a store at a newer version is
// refused with ErrSchemaTooNew instead of being downgraded. A failing step
// rolls back every change it and the preceding steps made.
func (s *Store) Migrate(ctx context.Context, schema Schema) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return ErrClosed
	}
	if err := schema.validate(); err != nil {
		return err
	}

	var current uint64
	err := s.View(func(tx *bolt.Tx) error {
		current = readSchemaVersion(tx)
		return nil
	})
	if err != nil {
		return err
	}
	if current > schema.Version {
		return fmt.Errorf("%w: store is at %d, this build supports %d", ErrSchemaTooNew, current, schema.Version)
	}
	if current == schema.Version {
		return nil
	}

	return s.WriteTransaction(ctx, func(tx *Tx) error {
		version := readSchemaVersion(tx.btx)
		if version > schema.Version {
			return fmt.Errorf("%w: store is at %d, this build supports %d", ErrSchemaTooNew, version, schema.Version)
		}
		for version < schema.Version {
			step, ok := schema.stepFrom(version)
			if !ok {
				return fmt.Errorf("%w: no step from version %d", ErrInvalidSchema, version)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := step.Apply(ctx, tx); err != nil {
				return fmt.Errorf("harbor: migrate %d -> %d: %w", step.From, step.To, err)
			}
			version = step.To
			if err := writeSchemaVersion(tx.btx, version); err != nil {
				return err
			}
		}
		return nil
	})
}

// SchemaVersion reports the store's application schema version. It is 0 for a
// store that has never been migrated.
func (s *Store) SchemaVersion(ctx context.Context) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var version uint64
	err := s.View(func(tx *bolt.Tx) error {
		version = readSchemaVersion(tx)
		return nil
	})
	return version, err
}

// validate checks that the steps are ordered, contiguous and end at Version.
func (s Schema) validate() error {
	if len(s.Steps) == 0 {
		if s.Version != 0 {
			return fmt.Errorf("%w: schema version %d has no steps", ErrInvalidSchema, s.Version)
		}
		return nil
	}
	previous := s.Steps[0].From
	for i, step := range s.Steps {
		if step.Apply == nil {
			return fmt.Errorf("%w: step %d has no Apply", ErrInvalidSchema, i)
		}
		if step.To <= step.From {
			return fmt.Errorf("%w: step %d has To <= From", ErrInvalidSchema, i)
		}
		if step.From != previous {
			return fmt.Errorf("%w: step %d starts at %d, previous ends at %d", ErrInvalidSchema, i, step.From, previous)
		}
		previous = step.To
	}
	if previous != s.Version {
		return fmt.Errorf("%w: last step ends at %d, schema version is %d", ErrInvalidSchema, previous, s.Version)
	}
	return nil
}

// stepFrom returns the step that starts at version.
func (s Schema) stepFrom(version uint64) (Step, bool) {
	for _, step := range s.Steps {
		if step.From == version {
			return step, true
		}
	}
	return Step{}, false
}

// readSchemaVersion reads the version record, defaulting to 0.
func readSchemaVersion(tx *bolt.Tx) uint64 {
	b := tx.Bucket([]byte(schemaBucket))
	if b == nil {
		return 0
	}
	raw := b.Get([]byte(schemaKey))
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

// writeSchemaVersion stores the version record.
func writeSchemaVersion(tx *bolt.Tx, version uint64) error {
	b, err := tx.CreateBucketIfNotExists([]byte(schemaBucket))
	if err != nil {
		return err
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], version)
	return b.Put([]byte(schemaKey), raw[:])
}
