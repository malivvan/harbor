package series

import (
	"context"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// AggregateFunc selects an aggregation over a time window.
type AggregateFunc string

const (
	// AggMin returns the minimum value in the window.
	AggMin AggregateFunc = "min"
	// AggMax returns the maximum value in the window.
	AggMax AggregateFunc = "max"
	// AggAvg returns the arithmetic mean of the window.
	AggAvg AggregateFunc = "avg"
	// AggSum returns the sum of the window.
	AggSum AggregateFunc = "sum"
	// AggCount returns the number of points in the window.
	AggCount AggregateFunc = "count"
	// AggFirst returns the earliest value in the window.
	AggFirst AggregateFunc = "first"
	// AggLast returns the latest value in the window.
	AggLast AggregateFunc = "last"
)

// aggregator accumulates points into aligned windows.
type aggregator struct {
	fn      AggregateFunc
	step    int64
	samples []Sample

	have    bool
	current int64
	count   int64
	sum     float64
	min     float64
	max     float64
	first   float64
	last    float64
}

func (a *aggregator) add(point Point) {
	window := bucketStart(point.Timestamp, a.step)
	if !a.have || window != a.current {
		a.flush()
		a.have = true
		a.current = window
		a.count = 0
		a.sum = 0
		a.min = 0
		a.max = 0
		a.first = 0
		a.last = 0
	}

	if a.count == 0 {
		a.first = point.Value
		a.min = point.Value
		a.max = point.Value
	} else {
		if point.Value < a.min {
			a.min = point.Value
		}
		if point.Value > a.max {
			a.max = point.Value
		}
	}
	a.last = point.Value
	a.sum += point.Value
	a.count++
}

func (a *aggregator) flush() {
	if !a.have || a.count == 0 {
		return
	}
	var value float64
	switch a.fn {
	case AggMin:
		value = a.min
	case AggMax:
		value = a.max
	case AggAvg:
		value = a.sum / float64(a.count)
	case AggSum:
		value = a.sum
	case AggCount:
		value = float64(a.count)
	case AggFirst:
		value = a.first
	case AggLast:
		value = a.last
	}
	a.samples = append(a.samples, Sample{Timestamp: a.current, Value: value})
}

// addBlockSummary merges a whole block's summary into the current window. The
// caller guarantees every point in the block falls in window and that blocks
// arrive in ascending time order.
func (a *aggregator) addBlockSummary(window int64, s blockSummary) {
	if s.Count == 0 {
		return
	}
	if !a.have || window != a.current {
		a.flush()
		a.have = true
		a.current = window
		a.count = 0
		a.sum = 0
		a.min = 0
		a.max = 0
		a.first = 0
		a.last = 0
	}
	if a.count == 0 {
		a.first = s.First
		a.min = s.Min
		a.max = s.Max
	} else {
		if s.Min < a.min {
			a.min = s.Min
		}
		if s.Max > a.max {
			a.max = s.Max
		}
	}
	a.last = s.Last
	a.sum += s.Sum
	a.count += s.Count
}

// Aggregate buckets [start, end] into windows of width step and computes fn
// per window. Each returned Sample timestamp is the aligned window start.
func (se *Series) Aggregate(ctx context.Context, start, end int64, step int64, fn AggregateFunc) ([]Sample, error) {
	if se == nil || se.store == nil {
		return nil, ErrNilParam
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if step <= 0 {
		return nil, ErrInvalidStep
	}
	if start > end {
		return nil, ErrInvalidRange
	}
	if !validAggregate(fn) {
		return nil, fmt.Errorf("ts: invalid aggregate %q", fn)
	}

	agg := &aggregator{fn: fn, step: step}
	started := time.Now()
	err := se.store.view(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket, err := se.store.seriesBucket(tx, se.name)
		if err != nil {
			return err
		}
		if bucket == nil {
			return nil
		}

		if se.store.blockDuration() > 0 {
			// Answer from block summaries where the block lies wholly in range
			// and does not cross a window boundary; otherwise decode its points.
			err := se.store.scanBlocksAggregate(ctx, bucket, start, end, step, func(window int64, sum blockSummary, points []Point) error {
				if points == nil {
					agg.addBlockSummary(window, sum)
					return nil
				}
				for _, point := range points {
					if point.Timestamp < start || point.Timestamp > end {
						continue
					}
					agg.add(point)
				}
				return nil
			})
			if err != nil {
				return err
			}
			agg.flush()
			return checkContext(ctx)
		}

		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, raw := cursor.Seek(startKey[:]); key != nil; key, raw = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok, err := se.store.decodePoint(key, raw)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if point.Timestamp > end {
				break
			}
			agg.add(point)
		}
		agg.flush()

		return checkContext(ctx)
	})
	if err != nil {
		return nil, err
	}
	se.store.observeAggregate(time.Since(started), len(agg.samples), nil)
	return agg.samples, nil
}

func validAggregate(fn AggregateFunc) bool {
	switch fn {
	case AggMin, AggMax, AggAvg, AggSum, AggCount, AggFirst, AggLast:
		return true
	default:
		return false
	}
}

func bucketStart(ts, step int64) int64 {
	remainder := ts % step
	if remainder < 0 {
		remainder += step
	}
	return ts - remainder
}
