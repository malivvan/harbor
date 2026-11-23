package db

import (
	"context"
	"reflect"

	"github.com/malivvan/harbor/db/q"
	"github.com/malivvan/harbor/internal"
	bolt "go.etcd.io/bbolt"
)

// Select builds a query that scans for records matching all supplied matchers.
func (n *node) Select(matchers ...q.Matcher) Query {
	var tree q.Matcher
	if len(matchers) > 0 {
		tree = q.And(matchers...)
	}
	query := newQuery(n, tree)
	query.hasMatchers = len(matchers) > 0
	return query
}

// Query configures and executes scans over records in a bucket.
type Query interface {
	// Skip matching records by the given number.
	// It returns a new query; the receiver is not modified.
	Skip(int) Query

	// Limit the results by the given number.
	// It returns a new query; the receiver is not modified.
	Limit(int) Query

	// Order by the given fields, in descending precedence, left-to-right.
	// It returns a new query; the receiver is not modified.
	OrderBy(...string) Query

	// Reverse the order of the results.
	// It returns a new query; the receiver is not modified.
	Reverse() Query

	// Bucket specifies the bucket name.
	// It returns a new query; the receiver is not modified.
	Bucket(string) Query

	// Find a list of matching records
	Find(ctx context.Context, to any) error

	// First gets the first matching record
	First(ctx context.Context, to any) error

	// Delete all matching records
	Delete(ctx context.Context, kind any) error

	// Count all the matching records
	Count(ctx context.Context, kind any) (int, error)

	// Raw returns the raw values of all matching records without decoding them.
	// kind is a sample record (a pointer to a struct) used to decode each value
	// so matchers and ordering are evaluated. Pass nil only when the query has
	// no matchers and no ordering; the bucket is then dumped as-is.
	Raw(ctx context.Context, kind any) ([][]byte, error)

	// RawEach executes fn for each matching raw key/value pair. kind has the
	// same meaning as in Raw.
	// If the callback returns an error, processing stops and that error is returned.
	// Cancellation prevents further callbacks but does not undo callbacks already executed.
	RawEach(ctx context.Context, kind any, fn func(key, value []byte) error) error

	// Each decodes each matching record and executes fn.
	// If the callback returns an error, processing stops and that error is returned.
	// Cancellation prevents further callbacks but does not undo callbacks already executed.
	Each(ctx context.Context, kind any, fn func(any) error) error

	// Iter starts a streaming scan over the matching records.
	//
	// kind is a sample record (a pointer to a struct) used to resolve the bucket
	// name and decode values for matchers; pass nil only when the query has no
	// matchers or the bucket is selected with Bucket.
	//
	// Skip, Limit, Reverse and Bucket are honored; OrderBy is not supported and
	// returns ErrStreamOrdered. The caller must Close the returned Iterator.
	Iter(ctx context.Context, kind any) (*Iterator, error)
}

func newQuery(n *node, tree q.Matcher) *query {
	return &query{
		skip:  0,
		limit: -1,
		node:  n,
		tree:  tree,
	}
}

type query struct {
	limit       int
	skip        int
	reverse     bool
	tree        q.Matcher
	node        *node
	bucket      string
	orderBy     []string
	hasMatchers bool
}

// clone returns a shallow copy that a builder method can safely modify. The
// node, matcher tree and derived bucket name are immutable, so sharing them
// between copies is safe.
func (q *query) clone() *query {
	next := *q
	return &next
}

// Skip returns a copy of the query that skips the given number of matching
// records. The receiver is left untouched, so it can be reused as a base for
// several variants.
func (q *query) Skip(nb int) Query {
	next := q.clone()
	next.skip = nb
	return next
}

// Limit returns a copy of the query limited to the given number of results.
func (q *query) Limit(nb int) Query {
	next := q.clone()
	next.limit = nb
	return next
}

// OrderBy returns a copy of the query ordered by the given fields. The field
// names are copied, so mutating the caller's slice cannot change the query.
func (q *query) OrderBy(field ...string) Query {
	next := q.clone()
	next.orderBy = append([]string(nil), field...)
	return next
}

// Reverse returns a copy of the query that walks the records backwards.
func (q *query) Reverse() Query {
	next := q.clone()
	next.reverse = true
	return next
}

// Bucket returns a copy of the query bound to the given bucket name.
func (q *query) Bucket(bucketName string) Query {
	next := q.clone()
	next.bucket = bucketName
	return next
}

func (q *query) Find(ctx context.Context, to any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("query find", err)
	}

	sink, err := newListSink(q.node, to)
	if err != nil {
		return wrapError("query find", err)
	}

	return wrapError("query find", q.runQuery(ctx, sink))
}

// First gets the first matching record. It limits a copy of the query, so the
// receiver remains usable for a subsequent Find over the full result set.
func (q *query) First(ctx context.Context, to any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("query first", err)
	}

	sink, err := newFirstSink(q.node, to)
	if err != nil {
		return wrapError("query first", err)
	}

	limited := q.clone()
	limited.limit = 1
	return wrapError("query first", limited.runQuery(ctx, sink))
}

func (q *query) Delete(ctx context.Context, kind any) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("query delete", err)
	}

	sink, err := newDeleteSink(q.node, kind)
	if err != nil {
		return wrapError("query delete", err)
	}

	return wrapError("query delete", q.runQuery(ctx, sink))
}

func (q *query) Count(ctx context.Context, kind any) (int, error) {
	if err := checkContext(ctx); err != nil {
		return 0, wrapError("query count", err)
	}

	sink, err := newCountSink(q.node, kind)
	if err != nil {
		return 0, wrapError("query count", err)
	}

	err = q.runQuery(ctx, sink)
	if err != nil {
		return 0, wrapError("query count", err)
	}

	return sink.counter, nil
}

func (q *query) Raw(ctx context.Context, kind any) ([][]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("query raw", err)
	}
	if kind == nil && (q.hasMatchers || len(q.orderBy) > 0) {
		return nil, wrapError("query raw", ErrSampleRequired)
	}

	raw, sink, err := newRawSink(kind)
	if err != nil {
		return nil, wrapError("query raw", err)
	}

	if err := q.runQuery(ctx, sink); err != nil {
		return nil, wrapError("query raw", err)
	}

	return raw.results, nil
}

func (q *query) RawEach(ctx context.Context, kind any, fn func(key, value []byte) error) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("query raw each", err)
	}

	if fn == nil {
		return wrapError("query raw each", ErrNilParam)
	}
	if kind == nil && (q.hasMatchers || len(q.orderBy) > 0) {
		return wrapError("query raw each", ErrSampleRequired)
	}

	raw, sink, err := newRawSink(kind)
	if err != nil {
		return wrapError("query raw each", err)
	}

	raw.execFn = fn

	return wrapError("query raw each", q.runQuery(ctx, sink))
}

func (q *query) Each(ctx context.Context, kind any, fn func(any) error) error {
	if err := checkContext(ctx); err != nil {
		return wrapError("query each", err)
	}

	if fn == nil {
		return wrapError("query each", ErrNilParam)
	}

	sink, err := newEachSink(kind)
	if err != nil {
		return wrapError("query each", err)
	}

	sink.execFn = fn

	return wrapError("query each", q.runQuery(ctx, sink))
}

func (q *query) runQuery(ctx context.Context, sink sink) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	if q.node.tx != nil {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if sink.readOnly() {
			// q.query performs the final check before publishing read results.
			// Do not turn a successful publication into a cancellation afterward.
			return q.query(ctx, q.node.tx, sink)
		}
		if err := q.query(ctx, q.node.tx, sink); err != nil {
			return err
		}
		return checkContext(ctx)
	}
	if sink.readOnly() {
		q.node.s.rotMu.RLock()
		defer q.node.s.rotMu.RUnlock()
		return q.node.s.bolt.View(func(tx *bolt.Tx) error {
			if err := checkContext(ctx); err != nil {
				return err
			}
			// q.query performs the final check before publishing read results.
			return q.query(ctx, tx, sink)
		})
	}
	q.node.s.rotMu.RLock()
	defer q.node.s.rotMu.RUnlock()
	return q.node.s.bolt.Update(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		err := q.query(ctx, tx, sink)
		if err != nil {
			return err
		}
		// Check before commit — the Bolt callback return value
		// determines commit or rollback.
		if err := checkContext(ctx); err != nil {
			return err
		}
		return nil
	})
}

func (q *query) query(ctx context.Context, tx *bolt.Tx, sink sink) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	bucketName := q.bucket
	if bucketName == "" {
		bucketName = sink.bucketName()
	}
	// If still empty, try root fallback (keep as empty for root-bucket-as-data pattern)
	if bucketName == "" && len(q.node.root) == 0 {
		return ErrNoName
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	bucket := q.node.getBucket(tx, bucketName)

	if err := checkContext(ctx); err != nil {
		return err
	}

	if q.limit == 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		// Limit(0) asks for no records: a successful empty result. flush would
		// report ErrNotFound for an empty read and for a no-op delete.
		if ls, ok := sink.(*listSink); ok {
			reflect.Indirect(ls.ref).Set(reflect.MakeSlice(reflect.Indirect(ls.ref).Type(), 0, 0))
			return nil
		}
		if _, ok := sink.(*deleteSink); ok {
			return nil
		}
		return sink.flush(ctx)
	}

	sorter := newSorter(q.node, sink)
	sorter.orderBy = q.orderBy
	sorter.reverse = q.reverse
	sorter.skip = q.skip
	sorter.limit = q.limit
	if bucket != nil {
		if err := checkContext(ctx); err != nil {
			return err
		}

		// Pre-size unordered result slices so every record is decoded straight
		// into its final slot instead of into a fresh reflect.New per record.
		if len(q.orderBy) == 0 {
			if ls, ok := sink.(*listSink); ok {
				n := bucket.Stats().KeyN
				if q.limit >= 0 && q.limit < n {
					n = q.limit
				}
				ls.prealloc(n)
			}
		}

		c := internal.Cursor{C: bucket.Cursor(), Reverse: q.reverse}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}

			if v == nil {
				continue
			}

			stop, err := sorter.filter(ctx, q.tree, bucket, k, v)
			if err != nil {
				return err
			}

			if err := checkContext(ctx); err != nil {
				return err
			}

			if stop {
				break
			}
		}
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	return sorter.flush(ctx)
}
