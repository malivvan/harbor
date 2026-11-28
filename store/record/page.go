package record

import (
	"bytes"
	"context"
	"reflect"
)

// AllPage returns one stable page of records in primary-key order, starting
// after the encoded key cursor. It streams through Query.Iter, so only the
// returned page is materialized.
//
// after is the cursor returned by the previous call; nil starts at the
// beginning. A nil cursor back means the page reached the end. The key is the
// stored primary key, so inserting or deleting records between pages neither
// duplicates nor skips a record whose key sorts after the cursor.
func (n *node) AllPage(ctx context.Context, to any, limit int, after []byte) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("all page", err)
	}
	dest := reflect.ValueOf(to)
	if !dest.IsValid() || dest.Kind() != reflect.Pointer || dest.IsNil() || dest.Elem().Kind() != reflect.Slice {
		return nil, wrapError("all page", ErrSlicePtrNeeded)
	}
	slice := dest.Elem()
	elemType := slice.Type().Elem()
	ptrElems := elemType.Kind() == reflect.Pointer
	structType := elemType
	if ptrElems {
		structType = elemType.Elem()
	}
	if structType.Kind() != reflect.Struct {
		return nil, wrapError("all page", ErrSlicePtrNeeded)
	}

	sample := reflect.New(structType)
	iter, err := n.Select().Iter(ctx, sample.Interface())
	if err != nil {
		return nil, wrapError("all page", err)
	}
	defer func() { _ = iter.Close() }()

	out := reflect.MakeSlice(slice.Type(), 0, 0)
	keys := make([][]byte, 0, limit+1)
	var iterErr error
	// Read one record past the page so a full final page reports no cursor.
	max := 0
	if limit > 0 {
		max = limit + 1
	}
	for iter.Next() {
		if len(after) > 0 && bytes.Compare(iter.Key(), after) <= 0 {
			continue
		}
		elem := reflect.New(structType)
		if err := iter.Decode(elem.Interface()); err != nil {
			iterErr = err
			break
		}
		if ptrElems {
			out = reflect.Append(out, elem)
		} else {
			out = reflect.Append(out, elem.Elem())
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
		if max > 0 && out.Len() >= max {
			break
		}
	}
	if iterErr != nil {
		return nil, wrapError("all page", iterErr)
	}
	if err := iter.Err(); err != nil {
		return nil, wrapError("all page", err)
	}
	var cursor []byte
	if limit > 0 && out.Len() > limit {
		out = out.Slice(0, limit)
		cursor = keys[limit-1]
	}
	slice.Set(out)
	return cursor, nil
}
