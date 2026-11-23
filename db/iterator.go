package db

import (
	"context"
	"reflect"

	"github.com/malivvan/harbor/db/q"
	"github.com/malivvan/harbor/internal"
	bolt "go.etcd.io/bbolt"
)

// Iterator streams the records matched by a Query one at a time instead of
// materializing the whole result set. It keeps a read transaction open for its
// lifetime, so a large scan does not allocate a slice proportional to the
// number of matches and does not hold a write lock.
//
// Callers must call Close when they stop iterating, including after an early
// break and after Next returns false. Close is idempotent.
//
// An Iterator is not safe for concurrent use.
type Iterator struct {
	ctx    context.Context
	node   *node
	tx     *bolt.Tx
	ownsTx bool

	bucket *bolt.Bucket
	cursor *internal.Cursor
	tree   q.Matcher

	elemType reflect.Type
	scratch  reflect.Value
	decoded  bool

	skip  int
	limit int

	key    []byte
	value  []byte
	err    error
	done   bool
	closed bool
	// started records that the cursor was positioned once, so each Next call
	// resumes from the current position instead of restarting the scan.
	started bool
}

// Iter starts a streaming scan over the records matched by the query.
//
// kind is a sample record (a pointer to a struct) used to resolve the bucket
// name and to decode values for matchers. It has the same meaning as in Raw:
// pass nil only when the query has no matchers, or when the bucket is selected
// explicitly with Bucket.
//
// Skip, Limit, Reverse and Bucket are honored. OrderBy is not supported,
// because ordering requires materializing and sorting the complete result set;
// an ordered query returns ErrStreamOrdered instead of silently yielding
// records in the wrong order.
//
// When the query is attached to a caller-owned transaction (see Store.Attach),
// the iterator reads through that transaction and never closes it. Otherwise
// it opens a read-only transaction and holds it, and the store's rotation
// guard, until Close. A forgotten Close therefore keeps a read snapshot (and
// the memory it retains) alive.
//
// Cancellation is checked before each Next call; a canceled context stops the
// iteration with ctx.Err() reported by Err.
func (q *query) Iter(ctx context.Context, kind any) (*Iterator, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("query iter", err)
	}
	if len(q.orderBy) > 0 {
		return nil, wrapError("query iter", ErrStreamOrdered)
	}

	var elemType reflect.Type
	if kind != nil {
		ref := reflect.ValueOf(kind)
		if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
			return nil, wrapError("query iter", ErrStructPtrNeeded)
		}
		elemType = reflect.Indirect(ref).Type()
	} else if q.tree != nil {
		// Matchers must decode records, which needs the sample type. Report
		// this before the bucket check so it matches Raw's precedence.
		return nil, wrapError("query iter", ErrSampleRequired)
	}

	name := q.bucket
	if name == "" && kind != nil {
		name = bucketName(kind)
	}
	// An empty name is the node's root bucket. At the database root there is
	// no bucket name to fall back to.
	if name == "" && len(q.node.root) == 0 {
		return nil, wrapError("query iter", ErrNoName)
	}

	it := &Iterator{
		ctx:      ctx,
		node:     q.node,
		tree:     q.tree,
		elemType: elemType,
		skip:     q.skip,
		limit:    q.limit,
	}

	// Limit(0) explicitly asks for no records, matching Find/Raw.
	if q.limit == 0 {
		it.done = true
		return it, nil
	}

	if q.node.tx != nil {
		// The caller owns this transaction; the iterator must not close it.
		it.tx = q.node.tx
	} else {
		if q.node.s == nil || q.node.s.bolt == nil {
			return nil, wrapError("query iter", ErrNilParam)
		}
		// Match the ordering of managed reads: take the rotation guard before
		// opening the transaction, and release it only in Close.
		q.node.s.rotMu.RLock()
		tx, err := q.node.s.bolt.Begin(false)
		if err != nil {
			q.node.s.rotMu.RUnlock()
			return nil, wrapError("query iter", err)
		}
		it.tx = tx
		it.ownsTx = true
	}

	it.bucket = q.node.getBucket(it.tx, name)
	if it.bucket != nil {
		it.cursor = &internal.Cursor{C: it.bucket.Cursor(), Reverse: q.reverse}
	}

	return it, nil
}

// Next advances to the next matching record and reports whether one is
// available. When it returns false, Err distinguishes the end of the scan from
// a failure.
//
// After Next returns true, Key, Value and Decode describe the current record;
// the key and value slices remain valid only until the next call to Next or
// Close.
func (it *Iterator) Next() bool {
	if it == nil || it.closed || it.done {
		return false
	}
	if err := it.contextErr(); err != nil {
		return false
	}
	if it.cursor == nil {
		it.done = true
		return false
	}

	var k, v []byte
	for {
		if !it.started {
			k, v = it.cursor.First()
			it.started = true
		} else {
			k, v = it.cursor.Next()
		}
		if k == nil {
			it.done = true
			it.key, it.value = nil, nil
			return false
		}
		if v == nil {
			// Nested buckets are not records.
			continue
		}

		matched, err := it.match(v)
		if err != nil {
			it.fail(err)
			return false
		}
		if !matched {
			continue
		}
		if it.skip > 0 {
			it.skip--
			continue
		}

		// Check cancellation before publishing the record: a canceled scan
		// must not hand out another result.
		if err := it.contextErr(); err != nil {
			return false
		}

		it.key, it.value = k, v
		if it.limit > 0 {
			it.limit--
			if it.limit == 0 {
				it.done = true
			}
		}
		return true
	}
}

// match decodes raw when the query has matchers and evaluates them. It records
// whether the current record is already decoded, so Decode can reuse it.
func (it *Iterator) match(raw []byte) (bool, error) {
	it.decoded = false
	if it.tree == nil {
		return true, nil
	}
	if !it.scratch.IsValid() {
		it.scratch = reflect.New(it.elemType)
	}
	if err := it.node.unmarshal(raw, it.scratch.Interface()); err != nil {
		return false, err
	}
	ok, err := it.tree.Match(it.scratch.Interface())
	if err != nil || !ok {
		return false, err
	}
	it.decoded = true
	return true, nil
}

// Key returns the raw stored key of the current record, or nil when the
// iterator is not positioned on a record.
func (it *Iterator) Key() []byte {
	if it == nil {
		return nil
	}
	return it.key
}

// Value returns the raw stored value of the current record, or nil when the
// iterator is not positioned on a record. The value is the stored form, so it
// is decrypted but still encoded with the store's codec.
func (it *Iterator) Value() []byte {
	if it == nil {
		return nil
	}
	return it.value
}

// Decode decodes the current record into to, which must be a non-nil pointer
// to a struct. When a sample record was supplied to Iter, to must have the
// same type as the sample.
//
// Decode returns ErrNotFound when Next has not produced a record.
func (it *Iterator) Decode(to any) error {
	if it == nil {
		return wrapError("query iter decode", ErrNilParam)
	}
	if it.err != nil {
		return wrapError("query iter decode", it.err)
	}
	if it.value == nil {
		return wrapError("query iter decode", ErrNotFound)
	}

	ref := reflect.ValueOf(to)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.IsNil() || ref.Elem().Kind() != reflect.Struct {
		return wrapError("query iter decode", ErrStructPtrNeeded)
	}
	if it.elemType != nil && it.elemType != reflect.Indirect(ref).Type() {
		return wrapError("query iter decode", ErrIncompatibleValue)
	}

	if it.decoded {
		reflect.Indirect(ref).Set(it.scratch.Elem())
		return nil
	}
	return wrapError("query iter decode", it.node.unmarshal(it.value, to))
}

// Err returns the error that stopped the iteration, or nil when the scan
// finished normally or has not failed.
func (it *Iterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

// Close releases the read transaction and the rotation guard held by the
// iterator. It does not close a transaction supplied through Store.Attach.
// Close is idempotent and safe to defer.
func (it *Iterator) Close() error {
	if it == nil || it.closed {
		return nil
	}
	it.closed = true
	it.done = true
	it.key, it.value = nil, nil
	it.decoded = false

	if !it.ownsTx || it.tx == nil {
		return nil
	}

	// A read-only bbolt transaction is released with Rollback.
	err := it.tx.Rollback()
	it.tx = nil
	it.bucket = nil
	it.cursor = nil
	it.node.s.rotMu.RUnlock()
	if err != nil {
		return wrapError("query iter close", err)
	}
	return nil
}

func (it *Iterator) contextErr() error {
	if err := checkContext(it.ctx); err != nil {
		it.fail(err)
		return err
	}
	return nil
}

func (it *Iterator) fail(err error) {
	it.err = wrapError("query iter", err)
	it.done = true
}
