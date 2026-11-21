package ts

import (
	"context"
	"fmt"

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

	var samples []Sample
	err := se.store.b.View(func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		bucket := se.store.seriesBucket(tx, se.name)
		if bucket == nil {
			return nil
		}

		var (
			have    bool
			current int64
			count   int64
			sum     float64
			min     float64
			max     float64
			first   float64
			last    float64
		)

		flush := func() {
			if !have || count == 0 {
				return
			}

			var value float64
			switch fn {
			case AggMin:
				value = min
			case AggMax:
				value = max
			case AggAvg:
				value = sum / float64(count)
			case AggSum:
				value = sum
			case AggCount:
				value = float64(count)
			case AggFirst:
				value = first
			case AggLast:
				value = last
			}
			samples = append(samples, Sample{Timestamp: current, Value: value})
		}

		startKey := encodeKey(start)
		cursor := bucket.Cursor()
		for key, raw := cursor.Seek(startKey[:]); key != nil; key, raw = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok := decodePoint(key, raw)
			if !ok {
				continue
			}
			if point.Timestamp > end {
				break
			}

			window := bucketStart(point.Timestamp, step)
			if !have || window != current {
				flush()
				have = true
				current = window
				count = 0
				sum = 0
				min = 0
				max = 0
				first = 0
				last = 0
			}

			if count == 0 {
				first = point.Value
				min = point.Value
				max = point.Value
			} else {
				if point.Value < min {
					min = point.Value
				}
				if point.Value > max {
					max = point.Value
				}
			}
			last = point.Value
			sum += point.Value
			count++
		}
		flush()

		return checkContext(ctx)
	})
	if err != nil {
		return nil, err
	}
	return samples, nil
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
