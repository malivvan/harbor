package db

import (
	"context"
	"reflect"

	bolt "go.etcd.io/bbolt"
)

// GetMany returns records by primary key in one read transaction. It fills to
// — a pointer to a slice of structs or pointers to structs — in request order,
// repeating a record for each requested duplicate, and returns the IDs that
// were not found in request order. A missing index or bucket is not an error.
func (n *node) GetMany(ctx context.Context, ids []any, to any) (missing []any, err error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("get many", err)
	}

	dest := reflect.ValueOf(to)
	if !dest.IsValid() || dest.Kind() != reflect.Pointer || dest.IsNil() || dest.Elem().Kind() != reflect.Slice {
		return nil, wrapError("get many", ErrSlicePtrNeeded)
	}
	slice := dest.Elem()
	elemType := slice.Type().Elem()
	ptrElems := elemType.Kind() == reflect.Pointer
	structType := elemType
	if ptrElems {
		structType = elemType.Elem()
	}
	if structType.Kind() != reflect.Struct {
		return nil, wrapError("get many", ErrSlicePtrNeeded)
	}

	sample := reflect.New(structType)
	cfg, err := extract(&sample)
	if err != nil {
		return nil, wrapError("get many", err)
	}

	if len(ids) == 0 {
		slice.Set(reflect.MakeSlice(slice.Type(), 0, 0))
		return nil, nil
	}

	out := reflect.MakeSlice(slice.Type(), 0, len(ids))
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		bucket := n.getBucket(tx, cfg.Name)
		if bucket == nil {
			missing = append(missing, ids...)
			return nil
		}
		for i, id := range ids {
			if i%256 == 0 {
				if err := checkContext(ctx); err != nil {
					return err
				}
			}
			key, err := n.keyBytes(id, keyDomainID)
			if err != nil {
				return err
			}
			raw := bucket.Get(key)
			if raw == nil {
				missing = append(missing, id)
				continue
			}
			elem := reflect.New(structType)
			if err := n.unmarshal(raw, elem.Interface()); err != nil {
				return err
			}
			if ptrElems {
				out = reflect.Append(out, elem)
			} else {
				out = reflect.Append(out, elem.Elem())
			}
		}
		return nil
	})
	if err != nil {
		return nil, wrapError("get many", err)
	}
	slice.Set(out)
	return missing, nil
}
