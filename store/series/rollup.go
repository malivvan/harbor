package series

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
)

const rollupsBucketName = "__rollups__"

// ErrInvalidRollup reports a malformed or unusable rollup specification.
var ErrInvalidRollup = errors.New("series: invalid rollup")

// LabelMatcher selects series by a label key and value. An empty Value matches
// any value for the key; an empty Key matches every series.
type LabelMatcher struct {
	Key   string
	Value string
}

// RollupSpec defines a materialized aggregate: points written to Source are
// folded into Target at Step-sized windows with Func.
type RollupSpec struct {
	Source   string
	Target   string
	Step     int64
	Func     AggregateFunc
	OnInsert bool
	Retain   Retention
	// SharedTarget keeps Target when the rollup is dropped. The zero value
	// means the rollup owns its target and removes it on DropRollup.
	SharedTarget bool
}

func (r RollupSpec) validate() error {
	if r.Source == "" || r.Target == "" {
		return fmt.Errorf("%w: source and target are required", ErrInvalidRollup)
	}
	if r.Source == r.Target {
		return fmt.Errorf("%w: source and target must differ", ErrInvalidRollup)
	}
	if r.Step <= 0 {
		return fmt.Errorf("%w: step must be positive", ErrInvalidRollup)
	}
	switch r.Func {
	case AggMin, AggMax, AggAvg, AggSum, AggCount, AggFirst, AggLast:
	default:
		return fmt.Errorf("%w: unknown aggregate %q", ErrInvalidRollup, r.Func)
	}
	return nil
}

type skipRollupsKey struct{}

func withSkipRollups(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, skipRollupsKey{}, true)
}

func skipRollups(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(skipRollupsKey{}).(bool)
	return v
}

func (s *Store) rollupsBucket(tx *bolt.Tx, create bool) (*bolt.Bucket, error) {
	meta, err := s.metaBucket(tx, create)
	if err != nil || meta == nil {
		return nil, err
	}
	if create {
		return meta.CreateBucketIfNotExists([]byte(rollupsBucketName))
	}
	return meta.Bucket([]byte(rollupsBucketName)), nil
}

// CreateRollup persists a rollup specification. If it already exists it is
// replaced; the target's existing points are not recomputed until RunRollups
// or the next matching insert.
func (s *Store) CreateRollup(ctx context.Context, spec RollupSpec) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := spec.validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if s.encryptsValues() {
		raw, err = s.sealBytes(raw)
		if err != nil {
			return err
		}
	}
	err = s.update(func(tx *bolt.Tx) error {
		if err := s.ensureMarker(tx); err != nil {
			return err
		}
		b, err := s.rollupsBucket(tx, true)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(spec.Target), raw); err != nil {
			return err
		}
		if spec.Retain != (Retention{}) {
			meta, err := s.readMeta(tx, spec.Target)
			if err != nil {
				return err
			}
			meta.Retention = spec.Retain
			if err := s.writeMeta(tx, spec.Target, meta); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return s.traceEntry("create_rollup", spec.Target, digestOf(raw))
}

// DropRollup removes a rollup. When the spec owns its target, the target series
// is deleted too.
func (s *Store) DropRollup(ctx context.Context, target string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	var spec RollupSpec
	var found bool
	err := s.update(func(tx *bolt.Tx) error {
		b, err := s.rollupsBucket(tx, false)
		if err != nil || b == nil {
			return err
		}
		raw := b.Get([]byte(target))
		if raw == nil {
			return nil
		}
		plain := raw
		if s.encryptsValues() {
			plain, err = s.openBytes(raw)
			if err != nil {
				return err
			}
		}
		if err := json.Unmarshal(plain, &spec); err != nil {
			return err
		}
		found = true
		return b.Delete([]byte(target))
	})
	if err != nil {
		return err
	}
	if found && !spec.SharedTarget {
		return s.DeleteSeries(ctx, target)
	}
	return nil
}

// Rollups returns the persisted rollups sorted by target.
func (s *Store) Rollups(ctx context.Context) ([]RollupSpec, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	var out []RollupSpec
	err := s.view(func(tx *bolt.Tx) error {
		b, err := s.rollupsBucket(tx, false)
		if err != nil || b == nil {
			return err
		}
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			plain := v
			if s.encryptsValues() {
				var err error
				plain, err = s.openBytes(v)
				if err != nil {
					return err
				}
			}
			var spec RollupSpec
			if err := json.Unmarshal(plain, &spec); err != nil {
				return err
			}
			out = append(out, spec)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	return out, nil
}

// SeriesMatching returns series whose labels match every matcher.
func (s *Store) SeriesMatching(ctx context.Context, matchers ...LabelMatcher) ([]SeriesInfo, error) {
	infos, err := s.ListSeries(ctx)
	if err != nil {
		return nil, err
	}
	if len(matchers) == 0 {
		return infos, nil
	}
	out := make([]SeriesInfo, 0, len(infos))
	for _, info := range infos {
		if matchesLabels(info.Labels, matchers) {
			out = append(out, info)
		}
	}
	return out, nil
}

func matchesLabels(labels map[string]string, matchers []LabelMatcher) bool {
	for _, m := range matchers {
		if m.Key == "" {
			continue
		}
		value, ok := labels[m.Key]
		if !ok {
			return false
		}
		if m.Value != "" && value != m.Value {
			return false
		}
	}
	return true
}

// RunRollups recomputes every window between from and to (inclusive) for the
// rollups whose Source is source. It is the backfill path for data written
// before a rollup existed or with OnInsert disabled.
func (s *Store) RunRollups(ctx context.Context, source string, from, to int64) (int, error) {
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if from > to {
		return 0, fmt.Errorf("%w: from is after to", ErrInvalidRollup)
	}
	specs, err := s.Rollups(ctx)
	if err != nil {
		return 0, err
	}
	now := s.nowNanos()
	written := 0
	err = s.update(func(tx *bolt.Tx) error {
		for _, spec := range specs {
			if spec.Source != source {
				continue
			}
			for window := bucketStart(from, spec.Step); window <= to; window += spec.Step {
				n, err := s.writeRollupWindow(ctx, tx, spec, window, now)
				if err != nil {
					return err
				}
				written += n
			}
		}
		return nil
	})
	if err != nil {
		return written, err
	}
	return written, nil
}

// applyRollups updates the rollups fed by source after a committed insert in
// the same transaction. The context carries skipRollups for the target write so
// a chain of rollups cannot recurse.
func (s *Store) applyRollups(ctx context.Context, tx *bolt.Tx, source string, points []Point, now int64) error {
	if len(points) == 0 || skipRollups(ctx) {
		return nil
	}
	b, err := s.rollupsBucket(tx, false)
	if err != nil || b == nil {
		return err
	}
	seen := make(map[int64]struct{})
	var specs []RollupSpec
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		plain := v
		if s.encryptsValues() {
			var err error
			plain, err = s.openBytes(v)
			if err != nil {
				return err
			}
		}
		var spec RollupSpec
		if err := json.Unmarshal(plain, &spec); err != nil {
			return err
		}
		if spec.Source == source && spec.OnInsert {
			specs = append(specs, spec)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, spec := range specs {
		for _, point := range points {
			window := bucketStart(point.Timestamp, spec.Step)
			if _, ok := seen[window]; ok {
				continue
			}
			seen[window] = struct{}{}
			if _, err := s.writeRollupWindow(ctx, tx, spec, window, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) writeRollupWindow(ctx context.Context, tx *bolt.Tx, spec RollupSpec, window, now int64) (int, error) {
	bound := s.WithTransaction(tx)
	src := bound.Series(spec.Source)
	points, err := src.Range(ctx, window, window+spec.Step-1)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return 0, err
	}
	if len(points) == 0 {
		target := bound.Series(spec.Target)
		if _, err := target.DeleteRange(ctx, window, window); err != nil && !errors.Is(err, ErrNotFound) {
			return 0, err
		}
		return 0, nil
	}
	value, ok := aggregateWindow(spec.Func, spec.Step, points)
	if !ok {
		return 0, nil
	}
	target := bound.Series(spec.Target)
	if err := target.InsertAt(withSkipRollups(ctx), Point{Timestamp: window, Value: value}, now); err != nil {
		return 0, err
	}
	return 1, nil
}

func aggregateWindow(fn AggregateFunc, step int64, points []Point) (float64, bool) {
	if len(points) == 0 {
		return 0, false
	}
	var agg aggregator
	agg.fn = fn
	agg.step = step
	for _, point := range points {
		agg.add(point)
	}
	agg.flush()
	if len(agg.samples) == 0 {
		return 0, false
	}
	return agg.samples[0].Value, true
}
