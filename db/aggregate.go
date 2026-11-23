package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrInvalidAggregate reports an unusable AggregateSpec.
var ErrInvalidAggregate = errors.New("db: invalid aggregation")

// MeasureType selects how a Measure aggregates a field.
type MeasureType string

const (
	// MeasureCount counts records, or the present values of Field when it is
	// set.
	MeasureCount MeasureType = "count"
	// MeasureSum sums numeric values.
	MeasureSum MeasureType = "sum"
	// MeasureAvg averages numeric values.
	MeasureAvg MeasureType = "avg"
	// MeasureMin and MeasureMax take the smallest and largest numeric value.
	MeasureMin MeasureType = "min"
	MeasureMax MeasureType = "max"
	// MeasureMedian is the 50th percentile.
	MeasureMedian MeasureType = "median"
	// MeasurePercentile needs Measure.P between 0 and 100.
	MeasurePercentile MeasureType = "percentile"
	// MeasureStdDev is the sample standard deviation (n-1).
	MeasureStdDev MeasureType = "stddev"
	// MeasureStdDevPop is the population standard deviation (n).
	MeasureStdDevPop MeasureType = "stddev_pop"
)

// MissingPolicy decides what a numeric measure does with a missing field.
type MissingPolicy int

const (
	// MissingExclude ignores records without the field.
	MissingExclude MissingPolicy = iota
	// MissingZero treats a missing field as zero.
	MissingZero
)

// NonNumericPolicy decides what a numeric measure does with a value that is
// not a number.
type NonNumericPolicy int

const (
	// NonNumericError fails the aggregation.
	NonNumericError NonNumericPolicy = iota
	// NonNumericSkip ignores the value.
	NonNumericSkip
	// NonNumericZero treats the value as zero.
	NonNumericZero
)

// TimeUnit selects the width of a time bucket.
type TimeUnit string

const (
	TimeUnitSecond TimeUnit = "second"
	TimeUnitMinute TimeUnit = "minute"
	TimeUnitHour   TimeUnit = "hour"
	TimeUnitDay    TimeUnit = "day"
	TimeUnitWeek   TimeUnit = "week"
)

// TimeBucket groups records by the start of a fixed time interval taken from
// Field. Field may hold a time.Time, an RFC 3339 string, or a numeric Unix
// timestamp in seconds.
type TimeBucket struct {
	Field string
	Unit  TimeUnit
}

// Measure is one aggregate value in an AggregateSpec.
type Measure struct {
	// Alias names the measure in Group.Measures, Having and OrderBy. It must be
	// unique; an empty alias defaults to the measure type.
	Alias string
	// Type selects the aggregate.
	Type MeasureType
	// Field is the record field to aggregate. Count may leave it empty to
	// count records.
	Field string
	// P is the percentile for MeasurePercentile, in [0, 100].
	P float64
}

// HavingOp compares a measure or group key with a value.
type HavingOp string

const (
	HavingGT  HavingOp = "gt"
	HavingGTE HavingOp = "gte"
	HavingLT  HavingOp = "lt"
	HavingLTE HavingOp = "lte"
	HavingEQ  HavingOp = "eq"
	HavingNEQ HavingOp = "neq"
)

// Having filters groups by a measure alias, "count", or a group key.
type Having struct {
	Alias string
	Op    HavingOp
	Value float64
}

// AggregateSpec describes one aggregation over a bucket.
type AggregateSpec struct {
	// GroupBy lists record fields to group by. An empty list produces one
	// group.
	GroupBy []string
	// Measures are the aggregates to compute.
	Measures []Measure
	// Having filters groups after aggregation.
	Having []Having
	// OrderBy names a measure alias or a group key. An empty OrderBy sorts by
	// the group key.
	OrderBy string
	// Desc reverses the order.
	Desc bool
	// Limit caps the number of groups returned. Zero means no cap.
	Limit int
	// TimeBucket adds a bucketed time field to the group key.
	TimeBucket *TimeBucket
	// Missing and NonNumeric decide how numeric measures treat unusable
	// values. The zero values exclude missing values and reject non-numbers.
	Missing    MissingPolicy
	NonNumeric NonNumericPolicy
}

// Group is one aggregated group.
type Group struct {
	// Keys holds the group-by values and the time bucket (as a time.Time).
	Keys map[string]any
	// Measures holds one value per measure alias.
	Measures map[string]float64
	// Count is the number of records in the group.
	Count int64
}

// AggregateResult is the result of Node.Aggregate.
type AggregateResult struct {
	Groups []Group
	// Skipped counts values that a measure could not aggregate because they
	// were missing (under MissingExclude) or non-numeric (under
	// NonNumericSkip).
	Skipped int64
}

// measureState accumulates one measure for one group.
type measureState struct {
	spec   Measure
	count  int64
	sum    float64
	sumSq  float64
	min    float64
	max    float64
	hasMin bool
	values []float64
}

// groupState accumulates one group.
type groupState struct {
	keys    map[string]any
	keyJSON string
	count   int64
	states  []measureState
}

// Aggregate computes grouped measures over the records of the node's bucket.
//
// Records are decoded with the node's codec into a string-keyed map, so field
// names are the encoded (JSON) field names. Nested fields are not supported in
// v1. The scan holds only the group accumulators; values are retained per
// group only for median and percentile measures.
func (n *node) Aggregate(ctx context.Context, spec AggregateSpec) (AggregateResult, error) {
	if err := checkContext(ctx); err != nil {
		return AggregateResult{}, wrapError("aggregate", err)
	}
	if err := spec.validate(); err != nil {
		return AggregateResult{}, wrapError("aggregate", err)
	}

	groups := make(map[string]*groupState)
	var order []*groupState
	var skipped int64

	err := n.aggregateEach(ctx, func(rec map[string]any) error {
		keys := make(map[string]any, len(spec.GroupBy)+1)
		keyVals := make([]any, 0, len(spec.GroupBy)+1)
		for _, field := range spec.GroupBy {
			v := rec[field]
			keys[field] = v
			keyVals = append(keyVals, v)
		}
		if spec.TimeBucket != nil {
			bucket, ok := timeBucketOf(rec[spec.TimeBucket.Field], spec.TimeBucket.Unit)
			if !ok {
				return fmt.Errorf("%w: time bucket field %q is missing or not a timestamp",
					ErrInvalidAggregate, spec.TimeBucket.Field)
			}
			keys[spec.TimeBucket.Field] = bucket
			keyVals = append(keyVals, bucket.UTC().Format(time.RFC3339Nano))
		}
		keyJSON := canonicalKey(keyVals)

		g := groups[keyJSON]
		if g == nil {
			g = &groupState{
				keys:    keys,
				keyJSON: keyJSON,
				states:  make([]measureState, len(spec.Measures)),
			}
			for i, m := range spec.Measures {
				g.states[i].spec = m
			}
			groups[keyJSON] = g
			order = append(order, g)
		}
		g.count++

		for i := range g.states {
			st := &g.states[i]
			value, present := rec[st.spec.Field]
			if st.spec.Type == MeasureCount && st.spec.Field == "" {
				st.count++
				continue
			}
			if !present {
				switch spec.Missing {
				case MissingZero:
					value = float64(0)
				default:
					skipped++
					continue
				}
			}
			if st.spec.Type == MeasureCount {
				// COUNT(field) counts present values, whatever their type.
				if present {
					st.count++
				}
				continue
			}
			num, ok := numericValue(value)
			if !ok {
				switch spec.NonNumeric {
				case NonNumericZero:
					num = 0
				case NonNumericSkip:
					skipped++
					continue
				default:
					return fmt.Errorf("%w: field %q holds a non-numeric value (%T)",
						ErrInvalidAggregate, st.spec.Field, value)
				}
			}
			st.add(num)
		}
		return nil
	})
	if err != nil {
		return AggregateResult{}, wrapError("aggregate", err)
	}

	out := make([]Group, 0, len(order))
	for _, g := range order {
		measures := make(map[string]float64, len(g.states))
		for i := range g.states {
			st := &g.states[i]
			measures[st.spec.alias()] = st.result()
		}
		group := Group{Keys: g.keys, Measures: measures, Count: g.count}
		if !matchesHaving(group, g, spec.Having) {
			continue
		}
		out = append(out, group)
	}

	if err := sortGroups(out, spec); err != nil {
		return AggregateResult{}, wrapError("aggregate", err)
	}
	if spec.Limit > 0 && len(out) > spec.Limit {
		out = out[:spec.Limit]
	}
	return AggregateResult{Groups: out, Skipped: skipped}, nil
}

// validate checks the spec before any record is read.
func (s AggregateSpec) validate() error {
	if s.Limit < 0 {
		return fmt.Errorf("%w: limit must not be negative", ErrInvalidAggregate)
	}
	switch s.Missing {
	case MissingExclude, MissingZero:
	default:
		return fmt.Errorf("%w: unknown missing policy %d", ErrInvalidAggregate, s.Missing)
	}
	switch s.NonNumeric {
	case NonNumericError, NonNumericSkip, NonNumericZero:
	default:
		return fmt.Errorf("%w: unknown non-numeric policy %d", ErrInvalidAggregate, s.NonNumeric)
	}
	for _, field := range s.GroupBy {
		if field == "" {
			return fmt.Errorf("%w: empty group-by field", ErrInvalidAggregate)
		}
	}
	if s.TimeBucket != nil {
		if s.TimeBucket.Field == "" {
			return fmt.Errorf("%w: time bucket field is required", ErrInvalidAggregate)
		}
		if !validTimeUnit(s.TimeBucket.Unit) {
			return fmt.Errorf("%w: unknown time unit %q", ErrInvalidAggregate, s.TimeBucket.Unit)
		}
	}

	seen := make(map[string]struct{}, len(s.Measures))
	for _, m := range s.Measures {
		alias := m.alias()
		if alias == "" {
			return fmt.Errorf("%w: measure alias is required", ErrInvalidAggregate)
		}
		if _, ok := seen[alias]; ok {
			return fmt.Errorf("%w: duplicate measure alias %q", ErrInvalidAggregate, alias)
		}
		seen[alias] = struct{}{}
		switch m.Type {
		case MeasureCount:
		case MeasureSum, MeasureAvg, MeasureMin, MeasureMax, MeasureMedian, MeasureStdDev, MeasureStdDevPop, MeasurePercentile:
			if m.Field == "" {
				return fmt.Errorf("%w: measure %q needs a field", ErrInvalidAggregate, alias)
			}
		default:
			return fmt.Errorf("%w: unknown measure type %q", ErrInvalidAggregate, m.Type)
		}
		if m.Type == MeasurePercentile && (m.P < 0 || m.P > 100) {
			return fmt.Errorf("%w: percentile P must be between 0 and 100", ErrInvalidAggregate)
		}
	}

	known := map[string]struct{}{"count": {}}
	for alias := range seen {
		known[alias] = struct{}{}
	}
	for _, field := range s.GroupBy {
		known[field] = struct{}{}
	}
	if s.TimeBucket != nil {
		known[s.TimeBucket.Field] = struct{}{}
	}
	for _, h := range s.Having {
		if err := validHavingOp(h.Op); err != nil {
			return err
		}
		if _, ok := known[h.Alias]; !ok {
			return fmt.Errorf("%w: having references unknown alias %q", ErrInvalidAggregate, h.Alias)
		}
	}
	if s.OrderBy != "" {
		if _, ok := known[s.OrderBy]; !ok {
			return fmt.Errorf("%w: order-by references unknown alias %q", ErrInvalidAggregate, s.OrderBy)
		}
	}
	return nil
}

// alias returns the measure's output name.
func (m Measure) alias() string {
	if m.Alias != "" {
		return m.Alias
	}
	return string(m.Type)
}

// add folds one numeric value into the accumulator.
func (st *measureState) add(v float64) {
	st.count++
	st.sum += v
	st.sumSq += v * v
	if !st.hasMin {
		st.min, st.max, st.hasMin = v, v, true
	} else {
		if v < st.min {
			st.min = v
		}
		if v > st.max {
			st.max = v
		}
	}
	switch st.spec.Type {
	case MeasureMedian, MeasurePercentile:
		st.values = append(st.values, v)
	}
}

// result computes the measure value.
func (st *measureState) result() float64 {
	switch st.spec.Type {
	case MeasureCount:
		return float64(st.count)
	case MeasureSum:
		return st.sum
	case MeasureAvg:
		if st.count == 0 {
			return 0
		}
		return st.sum / float64(st.count)
	case MeasureMin:
		if !st.hasMin {
			return 0
		}
		return st.min
	case MeasureMax:
		if !st.hasMin {
			return 0
		}
		return st.max
	case MeasureMedian:
		return percentile(st.values, 50)
	case MeasurePercentile:
		return percentile(st.values, st.spec.P)
	case MeasureStdDev, MeasureStdDevPop:
		if st.count == 0 && st.spec.Type == MeasureStdDevPop {
			return 0
		}
		divisor := float64(st.count)
		if st.spec.Type == MeasureStdDev {
			if st.count < 2 {
				return 0
			}
			divisor = float64(st.count - 1)
		}
		if divisor <= 0 {
			return 0
		}
		mean := st.sum / float64(st.count)
		variance := (st.sumSq / float64(st.count)) - mean*mean
		if variance < 0 {
			variance = 0
		}
		// Convert the population variance to the requested sample estimator.
		pop := variance * float64(st.count) / divisor
		return math.Sqrt(pop)
	}
	return 0
}

// percentile returns the p-th percentile of values using linear interpolation.
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := (p / 100) * float64(len(sorted)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}
	frac := pos - float64(lower)
	return sorted[lower] + (sorted[upper]-sorted[lower])*frac
}

// canonicalKey encodes group-by values into a stable map key.
func canonicalKey(values []any) string {
	raw, err := json.Marshal(values)
	if err != nil {
		// Fall back to a type-tagged rendering for values JSON cannot encode.
		out := make([]string, len(values))
		for i, v := range values {
			out[i] = fmt.Sprintf("%T:%v", v, v)
		}
		return fmt.Sprint(out)
	}
	return string(raw)
}

// numericValue converts the numeric kinds a decoded record can hold.
func numericValue(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int8:
		return float64(t), true
	case int16:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint:
		return float64(t), true
	case uint8:
		return float64(t), true
	case uint16:
		return float64(t), true
	case uint32:
		return float64(t), true
	case uint64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	}
	return 0, false
}

// timeBucketOf parses a timestamp and returns the start of its bucket.
func timeBucketOf(v any, unit TimeUnit) (time.Time, bool) {
	var t time.Time
	switch value := v.(type) {
	case time.Time:
		t = value
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, value)
			if err != nil {
				return time.Time{}, false
			}
		}
		t = parsed
	case float64:
		t = time.Unix(int64(value), 0)
	case int64:
		t = time.Unix(value, 0)
	case int:
		t = time.Unix(int64(value), 0)
	case json.Number:
		secs, err := value.Int64()
		if err != nil {
			return time.Time{}, false
		}
		t = time.Unix(secs, 0)
	default:
		return time.Time{}, false
	}
	return bucketStart(t, unit), true
}

// bucketStart truncates t to the start of its unit in UTC.
func bucketStart(t time.Time, unit TimeUnit) time.Time {
	t = t.UTC()
	switch unit {
	case TimeUnitSecond:
		return t.Truncate(time.Second)
	case TimeUnitMinute:
		return t.Truncate(time.Minute)
	case TimeUnitHour:
		return t.Truncate(time.Hour)
	case TimeUnitDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case TimeUnitWeek:
		// ISO weeks start on Monday.
		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		offset := (int(day.Weekday()) + 6) % 7
		return day.AddDate(0, 0, -offset)
	}
	return t
}

// validTimeUnit reports whether unit is supported.
func validTimeUnit(unit TimeUnit) bool {
	switch unit {
	case TimeUnitSecond, TimeUnitMinute, TimeUnitHour, TimeUnitDay, TimeUnitWeek:
		return true
	}
	return false
}

// matchesHaving applies the having clauses to one group.
func matchesHaving(group Group, state *groupState, clauses []Having) bool {
	for _, h := range clauses {
		value, ok := havingValue(group, state, h.Alias)
		if !ok {
			return false
		}
		switch h.Op {
		case HavingGT:
			if !(value > h.Value) {
				return false
			}
		case HavingGTE:
			if !(value >= h.Value) {
				return false
			}
		case HavingLT:
			if !(value < h.Value) {
				return false
			}
		case HavingLTE:
			if !(value <= h.Value) {
				return false
			}
		case HavingEQ:
			if value != h.Value {
				return false
			}
		case HavingNEQ:
			if value == h.Value {
				return false
			}
		}
	}
	return true
}

// havingValue resolves a having alias to a number.
func havingValue(group Group, state *groupState, alias string) (float64, bool) {
	if alias == "count" {
		return float64(group.Count), true
	}
	if v, ok := group.Measures[alias]; ok {
		return v, true
	}
	v, ok := group.Keys[alias]
	if !ok {
		return 0, false
	}
	return numericValue(v)
}

// validHavingOp checks the comparison operator.
func validHavingOp(op HavingOp) error {
	switch op {
	case HavingGT, HavingGTE, HavingLT, HavingLTE, HavingEQ, HavingNEQ:
		return nil
	}
	return fmt.Errorf("%w: unknown having operator %q", ErrInvalidAggregate, op)
}

// sortGroups orders the result set.
func sortGroups(groups []Group, spec AggregateSpec) error {
	less := func(i, j int) bool {
		a, b := groups[i], groups[j]
		if spec.OrderBy != "" {
			cmp := compareValues(orderValue(a, spec.OrderBy), orderValue(b, spec.OrderBy))
			if cmp == 0 {
				// Ties order by the group key ascending, whatever Desc says, so
				// the result is deterministic.
				return compareValues(groupKeyString(a), groupKeyString(b)) < 0
			}
			if spec.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		cmp := compareValues(groupKeyString(a), groupKeyString(b))
		if spec.Desc {
			return cmp > 0
		}
		return cmp < 0
	}
	sort.SliceStable(groups, less)
	return nil
}

// orderValue returns the value OrderBy names for a group.
func orderValue(g Group, alias string) any {
	if alias == "count" {
		return float64(g.Count)
	}
	if v, ok := g.Measures[alias]; ok {
		return v
	}
	return g.Keys[alias]
}

// groupKeyString renders the group keys for default ordering.
func groupKeyString(g Group) string {
	keys := make([]string, 0, len(g.Keys))
	for k := range g.Keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = g.Keys[k]
	}
	return canonicalKey(out)
}

// compareValues compares two group/measure values.
func compareValues(a, b any) int {
	af, aok := numericValue(a)
	bf, bok := numericValue(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		}
		return 0
	}
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	}
	return 0
}

// aggregateEach streams the decoded records of the node's bucket.
func (n *node) aggregateEach(ctx context.Context, fn func(map[string]any) error) error {
	read := func(tx *bolt.Tx) error {
		b := n.getBucket(tx, "")
		if b == nil {
			return nil
		}
		seen := 0
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil // nested bucket, not a record
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
			return fn(rec)
		})
	}
	if n.tx != nil {
		return read(n.tx)
	}
	return n.s.bolt.View(read)
}
