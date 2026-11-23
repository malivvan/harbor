package db

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/malivvan/harbor/db/index"
	"github.com/malivvan/harbor/db/q"
	bolt "go.etcd.io/bbolt"
)

type item struct {
	value  *reflect.Value
	bucket *bolt.Bucket
	k      []byte
	v      []byte
}

func newSorter(n *node, snk sink) *sorter {
	return &sorter{
		node:  n,
		sink:  snk,
		skip:  0,
		limit: -1,
		list:  make([]*item, 0),
	}
}

type sorter struct {
	node       *node
	sink       sink
	list       []*item
	skip       int
	limit      int
	orderBy    []string
	orderPaths [][]int
	reverse    bool
	ctx        context.Context
	compareErr error
	swapper    func(i, j int)
}

// fieldPathKey identifies a cached struct-field path.
type fieldPathKey struct {
	typ  reflect.Type
	name string
}

// fieldPathCache maps (type, field name) to the reflect field index path.
var fieldPathCache sync.Map

// fieldPathFor resolves name on typ once and caches the result.
func fieldPathFor(typ reflect.Type, name string) ([]int, bool) {
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

// ensureOrderPaths resolves every OrderBy name against the record type once
// per sort instead of calling FieldByName on every comparison.
func (s *sorter) ensureOrderPaths(rec reflect.Value) bool {
	if s.orderPaths != nil {
		return true
	}
	paths := make([][]int, len(s.orderBy))
	for i, name := range s.orderBy {
		path, ok := fieldPathFor(rec.Type(), name)
		if !ok {
			s.compareErr = ErrNotFound
			return false
		}
		paths[i] = path
	}
	s.orderPaths = paths
	return true
}

func (s *sorter) filter(ctx context.Context, tree q.Matcher, bucket *bolt.Bucket, k, v []byte) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}

	itm := item{
		bucket: bucket,
		k:      k,
		v:      v,
	}
	rsink, ok := s.sink.(reflectSink)
	if !ok {
		return s.add(ctx, itm)
	}

	// Sinks that only need the key (count, delete, raw) can skip decoding the
	// record entirely unless a matcher or an ordering has to inspect it.
	if tree == nil && len(s.orderBy) == 0 && !s.sink.needsValue() {
		return s.add(ctx, itm)
	}

	newElem := rsink.elem()
	if err := s.node.unmarshal(v, newElem.Interface()); err != nil {
		return false, err
	}
	itm.value = &newElem

	if err := checkContext(ctx); err != nil {
		return false, err
	}

	if tree != nil {
		ok, err := tree.Match(newElem.Interface())
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	if len(s.orderBy) == 0 {
		return s.add(ctx, itm)
	}

	if _, ok := s.sink.(sliceSink); ok {
		// add directly to sink, we'll apply skip/limits after sorting
		return false, s.sink.add(ctx, itm)
	}

	s.list = append(s.list, &itm)

	return false, nil
}

func (s *sorter) add(ctx context.Context, itm item) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}

	if s.limit == 0 {
		return true, nil
	}

	if s.skip > 0 {
		s.skip--
		return false, nil
	}

	if s.limit > 0 {
		s.limit--
	}

	if err := s.sink.add(ctx, itm); err != nil {
		return false, err
	}

	return s.limit == 0, nil
}

func (s *sorter) compareValue(left reflect.Value, right reflect.Value) int {
	if !left.IsValid() || !right.IsValid() {
		if left.IsValid() {
			return 1
		}
		return -1
	}

	switch left.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		l, r := left.Int(), right.Int()
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		l, r := left.Uint(), right.Uint()
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
	case reflect.Float32, reflect.Float64:
		l, r := left.Float(), right.Float()
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
	case reflect.String:
		l, r := left.String(), right.String()
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
	case reflect.Bool:
		l, r := left.Bool(), right.Bool()
		if l == r {
			return 0
		}
		if !l {
			return -1
		}
		return 1
	case reflect.Struct:
		if lt, lok := left.Interface().(time.Time); lok {
			if rt, rok := right.Interface().(time.Time); rok {
				if lt.Equal(rt) {
					return 0
				}
				if lt.Before(rt) {
					return -1
				}
				return 1
			}
		}
	default:
		rawLeft, err := s.node.keyBytes(left.Interface(), keyDomainIndex)
		if err != nil {
			s.compareErr = err
			return -1
		}
		rawRight, err := s.node.keyBytes(right.Interface(), keyDomainIndex)
		if err != nil {
			s.compareErr = err
			return 1
		}

		l, r := string(rawLeft), string(rawRight)
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
	}

	return 0
}

func (s *sorter) less(leftElem reflect.Value, rightElem reflect.Value) bool {
	left := reflect.Indirect(leftElem)
	right := reflect.Indirect(rightElem)
	if !s.ensureOrderPaths(left) {
		return false
	}

	for _, path := range s.orderPaths {
		leftField := left.FieldByIndex(path)
		rightField := right.FieldByIndex(path)

		direction := 1
		if s.reverse {
			direction = -1
		}

		switch s.compareValue(leftField, rightField) * direction {
		case -1:
			return true
		case 1:
			return false
		default:
			continue
		}
	}

	return false
}

func (s *sorter) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	if len(s.orderBy) == 0 {
		return s.sink.flush(ctx)
	}

	// Synchronous sort — no goroutine.
	// Store ctx so Less can observe cancellation.
	s.ctx = ctx
	if ssink, ok := s.sink.(sliceSink); ok {
		// Build the swapper once; reflect.Swapper allocates a closure.
		s.swapper = reflect.Swapper(ssink.slice().Interface())
	}
	sort.Sort(s)

	if s.compareErr != nil {
		return s.compareErr
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	if ssink, ok := s.sink.(sliceSink); ok {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if !ssink.slice().IsValid() {
			return s.sink.flush(ctx)
		}
		if s.skip >= ssink.slice().Len() {
			ssink.reset()
			return s.sink.flush(ctx)
		}
		leftBound := s.skip
		if leftBound < 0 {
			leftBound = 0
		}
		limit := s.limit
		if s.limit < 0 {
			limit = 0
		}

		rightBound := leftBound + limit
		if rightBound > ssink.slice().Len() || rightBound == leftBound {
			rightBound = ssink.slice().Len()
		}
		ssink.setSlice(ssink.slice().Slice(leftBound, rightBound))
		return s.sink.flush(ctx)
	}

	for _, itm := range s.list {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if itm == nil {
			break
		}
		stop, err := s.add(ctx, *itm)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	return s.sink.flush(ctx)
}

func (s *sorter) Len() int {
	if ssink, ok := s.sink.(sliceSink); ok {
		return ssink.slice().Len()
	}
	return len(s.list)
}

func (s *sorter) Less(i, j int) bool {
	if s.compareErr != nil {
		return false
	}

	if err := checkContext(s.ctx); err != nil {
		s.compareErr = err
		return false
	}

	if ssink, ok := s.sink.(sliceSink); ok {
		result := s.less(ssink.slice().Index(i), ssink.slice().Index(j))

		if err := checkContext(s.ctx); err != nil {
			s.compareErr = err
			return false
		}

		return result
	}
	result := s.less(*s.list[i].value, *s.list[j].value)

	if err := checkContext(s.ctx); err != nil {
		s.compareErr = err
		return false
	}

	return result
}

func (s *sorter) Swap(i, j int) {
	if s.swapper != nil {
		s.swapper(i, j)
		return
	}
	s.list[i], s.list[j] = s.list[j], s.list[i]
}

type sink interface {
	bucketName() string
	flush(ctx context.Context) error
	add(ctx context.Context, i item) error
	readOnly() bool

	// needsValue reports whether the sink needs the decoded record value.
	// Sinks that only inspect keys can skip unmarshalling entirely when the
	// query has no matchers or ordering.
	needsValue() bool
}

type reflectSink interface {
	elem() reflect.Value
}

type sliceSink interface {
	slice() reflect.Value
	setSlice(reflect.Value)
	reset()
}

func newListSink(node Node, to interface{}) (*listSink, error) {
	ref := reflect.ValueOf(to)

	if ref.Kind() != reflect.Pointer || reflect.Indirect(ref).Kind() != reflect.Slice {
		return nil, ErrSlicePtrNeeded
	}

	sliceType := reflect.Indirect(ref).Type()
	elemType := sliceType.Elem()

	if elemType.Kind() == reflect.Pointer {
		elemType = elemType.Elem()
	}

	// Resolve bucket name: prefer BucketNamer on the element,
	// fall back to static type name. Empty name is allowed for
	// anonymous types when the caller provides the bucket later.
	name := ""
	proto := reflect.New(elemType)
	if bn, ok := proto.Interface().(BucketNamer); ok {
		name = bn.HarborBucketName()
	} else if proto.Elem().CanInterface() {
		if bn, ok := proto.Elem().Interface().(BucketNamer); ok {
			name = bn.HarborBucketName()
		}
	}
	if name == "" {
		name = elemType.Name()
	}

	return &listSink{
		node:     node,
		ref:      ref,
		isPtr:    sliceType.Elem().Kind() == reflect.Pointer,
		elemType: elemType,
		name:     name,
		results:  reflect.MakeSlice(reflect.Indirect(ref).Type(), 0, 0),
	}, nil
}

type listSink struct {
	node     Node
	ref      reflect.Value
	results  reflect.Value
	elemType reflect.Type
	name     string
	isPtr    bool
	idx      int
	scratch  reflect.Value
}

func (l *listSink) slice() reflect.Value {
	return l.results
}

func (l *listSink) setSlice(s reflect.Value) {
	l.results = s
}

func (l *listSink) reset() {
	l.results = reflect.MakeSlice(reflect.Indirect(l.ref).Type(), 0, 0)
}

func (l *listSink) elem() reflect.Value {
	if l.idx < l.results.Len() {
		return l.results.Index(l.idx).Addr()
	}
	// Reuse a scratch value: add copies it into the result slice, so the
	// decoded record never escapes between iterations.
	if !l.scratch.IsValid() {
		l.scratch = reflect.New(l.elemType)
	}
	return l.scratch
}

// prealloc sizes the result slice so records can be decoded directly into it.
// It is a no-op when the slice already has elements or already has capacity.
func (l *listSink) prealloc(n int) {
	if n <= 0 || l.results.Len() != 0 || l.results.Cap() >= n {
		return
	}
	l.results = reflect.MakeSlice(l.results.Type(), n, n)
}

func (l *listSink) bucketName() string {
	return l.name
}

func (l *listSink) add(ctx context.Context, i item) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	if l.idx < l.results.Len() {
		// The record was decoded straight into the pre-sized result slice.
		l.idx++
		return nil
	}

	if l.isPtr {
		// The scratch value is reused for every decoded record. Appending
		// *i.value directly therefore made every element of a []*T result
		// alias the last record: an ordered query with a matcher returned the
		// wrong row when the last visited row did not match. Copy the record
		// into a fresh pointer. See TestOrderedQueryKeepsTheMatcher.
		value := *i.value
		copied := reflect.New(l.elemType)
		if value.Kind() == reflect.Pointer {
			copied.Elem().Set(value.Elem())
		} else {
			copied.Elem().Set(value)
		}
		l.results = reflect.Append(l.results, copied)
	} else {
		l.results = reflect.Append(l.results, reflect.Indirect(*i.value))
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	l.idx++

	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (l *listSink) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	// Drop unused pre-allocated slots (and any slots skipped by Skip/Limit).
	if l.results.Len() > l.idx {
		l.results = l.results.Slice(0, l.idx)
	}

	if l.results.IsValid() && l.results.Len() > 0 {
		reflect.Indirect(l.ref).Set(l.results)
		return nil
	}

	return ErrNotFound
}

func (l *listSink) readOnly() bool {
	return true
}

func (l *listSink) needsValue() bool {
	return true
}

func newFirstSink(node Node, to interface{}) (*firstSink, error) {
	ref := reflect.ValueOf(to)

	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return nil, ErrStructPtrNeeded
	}

	return &firstSink{
		node: node,
		ref:  ref,
	}, nil
}

type firstSink struct {
	node    Node
	ref     reflect.Value
	found   bool
	pending *reflect.Value
}

func (f *firstSink) elem() reflect.Value {
	return reflect.New(reflect.Indirect(f.ref).Type())
}

func (f *firstSink) bucketName() string {
	return bucketName(f.ref.Interface())
}

func (f *firstSink) add(ctx context.Context, i item) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	// Store internally, do not publish to destination yet.
	val := i.value.Elem()
	f.pending = &val
	f.found = true

	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (f *firstSink) flush(ctx context.Context) error {
	if !f.found {
		return ErrNotFound
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	// Publish pending value to caller's destination.
	reflect.Indirect(f.ref).Set(*f.pending)
	return nil
}

func (f *firstSink) readOnly() bool {
	return true
}

func (f *firstSink) needsValue() bool {
	return true
}

func newDeleteSink(node Node, kind interface{}) (*deleteSink, error) {
	ref := reflect.ValueOf(kind)

	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return nil, ErrStructPtrNeeded
	}

	return &deleteSink{
		node: node,
		ref:  ref,
	}, nil
}

type deleteSink struct {
	node Node
	ref  reflect.Value

	// pending holds the matched records. Deletions are applied in flush so the
	// bbolt cursor that produced them is never mutated while being walked.
	pending []item
}

func (d *deleteSink) elem() reflect.Value {
	return reflect.New(reflect.Indirect(d.ref).Type())
}

func (d *deleteSink) bucketName() string {
	return bucketName(d.ref.Interface())
}

func (d *deleteSink) add(ctx context.Context, i item) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	d.pending = append(d.pending, i)
	return nil
}

func (d *deleteSink) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	// A bulk delete that matched nothing is a no-op.
	//
	// This used to return ErrNotFound, which made `Select(matcher).Delete` fail
	// whenever the predicate happened to match zero records — a normal outcome for
	// a cleanup sweep, and inconsistent with DeleteStruct, where deleting an absent
	// key is not an error. Callers that need "how many rows went away" should use
	// Count before deleting.
	if len(d.pending) == 0 {
		return nil
	}

	info, err := extract(&d.ref)
	if err != nil {
		return err
	}

	// The scan is over at this point, so deleting keys no longer mutates a
	// live cursor. Index handles are resolved once per bucket instead of once
	// per record and field.
	indexes := make(map[*bolt.Bucket]map[string]index.Index)
	for _, i := range d.pending {
		if err := checkContext(ctx); err != nil {
			return err
		}

		byField, ok := indexes[i.bucket]
		if !ok {
			var err error
			byField, err = resolveIndexes(i.bucket, info)
			if err != nil {
				return err
			}
			indexes[i.bucket] = byField
		}

		for fieldName, fieldCfg := range info.Fields {
			if err := checkContext(ctx); err != nil {
				return err
			}

			if fieldCfg.Index == "" {
				continue
			}
			idx := byField[fieldName]

			err = idx.RemoveID(ctx, i.k)
			if err != nil {
				if errors.Is(err, index.ErrNotFound) {
					return ErrNotFound
				}
				return err
			}

			if err := checkContext(ctx); err != nil {
				return err
			}
		}

		if err := i.bucket.Delete(i.k); err != nil {
			return err
		}

		if err := checkContext(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *deleteSink) readOnly() bool {
	return false
}

func (d *deleteSink) needsValue() bool {
	return false
}

func newCountSink(node Node, kind interface{}) (*countSink, error) {
	ref := reflect.ValueOf(kind)

	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return nil, ErrStructPtrNeeded
	}

	return &countSink{
		node: node,
		ref:  ref,
	}, nil
}

type countSink struct {
	node    Node
	ref     reflect.Value
	counter int
}

func (c *countSink) elem() reflect.Value {
	return reflect.New(reflect.Indirect(c.ref).Type())
}

func (c *countSink) bucketName() string {
	return bucketName(c.ref.Interface())
}

func (c *countSink) add(ctx context.Context, i item) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.counter++

	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (c *countSink) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (c *countSink) readOnly() bool {
	return true
}

func (c *countSink) needsValue() bool {
	return false
}

// newRawSink builds a raw sink. A non-nil kind makes the sink decode values
// into a copy of that record type so matchers and ordering can be evaluated.
func newRawSink(kind any) (*rawSink, sink, error) {
	r := &rawSink{}
	if kind == nil {
		return r, r, nil
	}
	ref := reflect.ValueOf(kind)
	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return nil, nil, ErrStructPtrNeeded
	}
	return r, &matchedRawSink{
		rawSink:  r,
		elemType: reflect.Indirect(ref).Type(),
		name:     bucketName(kind),
	}, nil
}

type rawSink struct {
	results [][]byte
	execFn  func([]byte, []byte) error
}

// matchedRawSink is a rawSink that decoder-filters values so query matchers
// and ordering see real records.
type matchedRawSink struct {
	*rawSink
	elemType reflect.Type
	name     string
}

func (m *matchedRawSink) elem() reflect.Value {
	return reflect.New(m.elemType)
}

func (m *matchedRawSink) bucketName() string {
	return m.name
}

func (m *matchedRawSink) needsValue() bool {
	return true
}

func (r *rawSink) add(ctx context.Context, i item) error {
	if r.execFn != nil {
		if err := checkContext(ctx); err != nil {
			return err
		}

		err := r.execFn(i.k, i.v)
		if err != nil {
			return err
		}

		if err := checkContext(ctx); err != nil {
			return err
		}

		return nil
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	// Defensive copy: the caller may reuse i.v buffer.
	copied := append([]byte(nil), i.v...)

	if err := checkContext(ctx); err != nil {
		return err
	}

	r.results = append(r.results, copied)

	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (r *rawSink) bucketName() string {
	return ""
}

func (r *rawSink) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (r *rawSink) readOnly() bool {
	return true
}

func (r *rawSink) needsValue() bool {
	return false
}

func newEachSink(to interface{}) (*eachSink, error) {
	ref := reflect.ValueOf(to)

	if !ref.IsValid() || ref.Kind() != reflect.Pointer || ref.Elem().Kind() != reflect.Struct {
		return nil, ErrStructPtrNeeded
	}

	return &eachSink{
		ref: ref,
	}, nil
}

type eachSink struct {
	ref    reflect.Value
	execFn func(interface{}) error
}

func (e *eachSink) elem() reflect.Value {
	return reflect.New(reflect.Indirect(e.ref).Type())
}

func (e *eachSink) bucketName() string {
	return bucketName(e.ref.Interface())
}

func (e *eachSink) add(ctx context.Context, i item) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	err := e.execFn(i.value.Interface())
	if err != nil {
		return err
	}

	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (e *eachSink) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	return nil
}

func (e *eachSink) readOnly() bool {
	return true
}

func (e *eachSink) needsValue() bool {
	return true
}
