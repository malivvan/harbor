package ts

import (
	"context"
	"encoding/binary"
	"math"
	"sort"

	bolt "go.etcd.io/bbolt"
)

const blockHeaderSize = 4

// blockStart returns the start timestamp of the block containing ts.
func blockStart(ts, duration int64) int64 {
	remainder := ts % duration
	if remainder < 0 {
		remainder += duration
	}
	return ts - remainder
}

// packBlock encodes points as little-endian records.
func packBlock(points []Point) []byte {
	out := make([]byte, blockHeaderSize+len(points)*16)
	binary.LittleEndian.PutUint32(out[:blockHeaderSize], uint32(len(points)))
	offset := blockHeaderSize
	for _, point := range points {
		binary.LittleEndian.PutUint64(out[offset:], uint64(point.Timestamp))
		binary.LittleEndian.PutUint64(out[offset+8:], math.Float64bits(point.Value))
		offset += 16
	}
	return out
}

// unpackBlock decodes points produced by packBlock.
func unpackBlock(data []byte) ([]Point, error) {
	if len(data) < blockHeaderSize {
		return nil, errCorruptPoint
	}
	count := binary.LittleEndian.Uint32(data[:blockHeaderSize])
	// Compare in uint64 so a 32-bit int overflow of count*16 cannot wrap into a
	// bogus "valid" size.
	if uint64(len(data)) != uint64(blockHeaderSize)+uint64(count)*16 {
		return nil, errCorruptPoint
	}

	points := make([]Point, count)
	offset := blockHeaderSize
	for i := range points {
		points[i] = Point{
			Timestamp: int64(binary.LittleEndian.Uint64(data[offset:])),
			Value:     math.Float64frombits(binary.LittleEndian.Uint64(data[offset+8:])),
		}
		offset += 16
	}
	return points, nil
}

// mergePoints combines two point lists, last write winning per timestamp.
// Existing blocks are already sorted; incoming points are sorted here. A
// linear merge replaces the previous map-and-sort approach, which rebuilt and
// re-sorted the whole block on every insert.
func mergePoints(existing, incoming []Point) []Point {
	if len(incoming) == 0 {
		return existing
	}

	// Copy before sorting: callers own their input slices.
	inc := append([]Point(nil), incoming...)
	sort.SliceStable(inc, func(i, j int) bool {
		return inc[i].Timestamp < inc[j].Timestamp
	})

	// Duplicate timestamps in incoming: the last value wins.
	deduped := inc[:0]
	for _, point := range inc {
		if n := len(deduped); n > 0 && deduped[n-1].Timestamp == point.Timestamp {
			deduped[n-1] = point
			continue
		}
		deduped = append(deduped, point)
	}
	inc = deduped

	if len(existing) == 0 {
		return inc
	}

	// Common append case: everything is after the last existing timestamp.
	if inc[0].Timestamp > existing[len(existing)-1].Timestamp {
		out := make([]Point, 0, len(existing)+len(inc))
		out = append(out, existing...)
		return append(out, inc...)
	}

	out := make([]Point, 0, len(existing)+len(inc))
	i, j := 0, 0
	for i < len(existing) && j < len(inc) {
		switch {
		case existing[i].Timestamp < inc[j].Timestamp:
			out = append(out, existing[i])
			i++
		case existing[i].Timestamp > inc[j].Timestamp:
			out = append(out, inc[j])
			j++
		default:
			// Incoming wins for the same timestamp.
			out = append(out, inc[j])
			i++
			j++
		}
	}
	out = append(out, existing[i:]...)
	return append(out, inc[j:]...)
}

func (s *Store) sealBlock(points []Point) ([]byte, error) {
	return s.sealBytes(packBlock(points))
}

func (s *Store) openBlock(raw []byte) ([]Point, error) {
	plain, err := s.openBytes(raw)
	if err != nil {
		return nil, err
	}
	return unpackBlock(plain)
}

func (s *Store) readBlock(bucket *bolt.Bucket, start int64) ([]Point, bool, error) {
	key := encodeKey(start)
	raw := bucket.Get(key[:])
	if raw == nil {
		return nil, false, nil
	}
	points, err := s.openBlock(raw)
	if err != nil {
		return nil, false, err
	}
	return points, true, nil
}

func (s *Store) writeBlock(bucket *bolt.Bucket, start int64, points []Point) error {
	key := encodeKey(start)
	if len(points) == 0 {
		return bucket.Delete(key[:])
	}
	raw, err := s.sealBlock(points)
	if err != nil {
		return err
	}
	return bucket.Put(key[:], raw)
}

// insertBlocks merges points into the encrypted blocks of a bucket.
func (s *Store) insertBlocks(ctx context.Context, bucket *bolt.Bucket, points []Point) (int, error) {
	added := 0
	groups := make(map[int64][]Point)
	for _, point := range points {
		start := blockStart(point.Timestamp, s.blockDuration())
		groups[start] = append(groups[start], point)
	}

	starts := make([]int64, 0, len(groups))
	for start := range groups {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	for _, start := range starts {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		existing, _, err := s.readBlock(bucket, start)
		if err != nil {
			return 0, err
		}
		merged := mergePoints(existing, groups[start])
		added += len(merged) - len(existing)
		if err := s.writeBlock(bucket, start, merged); err != nil {
			return 0, err
		}
	}
	return added, nil
}

// scanBlocks calls fn for every point in [start, end] in time order.
func (s *Store) scanBlocks(ctx context.Context, bucket *bolt.Bucket, start, end int64, fn func(Point) error) error {
	firstKey := encodeKey(blockStart(start, s.blockDuration()))
	cursor := bucket.Cursor()
	for key, raw := cursor.Seek(firstKey[:]); key != nil; key, raw = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if len(key) != recordSize {
			continue
		}
		if decodeKey(key) > end {
			break
		}
		points, err := s.openBlock(raw)
		if err != nil {
			return err
		}
		for _, point := range points {
			if point.Timestamp < start || point.Timestamp > end {
				continue
			}
			if err := fn(point); err != nil {
				return err
			}
		}
	}
	return nil
}

// blockPoint returns the first or last point of the bucket.
func (s *Store) blockPoint(bucket *bolt.Bucket, last bool) (Point, bool, error) {
	cursor := bucket.Cursor()
	key, raw := cursor.First()
	if last {
		key, raw = cursor.Last()
	}
	for key != nil {
		if len(key) == recordSize {
			points, err := s.openBlock(raw)
			if err != nil {
				return Point{}, false, err
			}
			if len(points) > 0 {
				if last {
					return points[len(points)-1], true, nil
				}
				return points[0], true, nil
			}
		}
		if last {
			key, raw = cursor.Prev()
		} else {
			key, raw = cursor.Next()
		}
	}
	return Point{}, false, nil
}

// countBlocks returns the total number of points in all blocks.
func (s *Store) countBlocks(ctx context.Context, bucket *bolt.Bucket) (int, error) {
	total := 0
	cursor := bucket.Cursor()
	for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		if len(key) != recordSize {
			continue
		}
		points, err := s.openBlock(raw)
		if err != nil {
			return 0, err
		}
		total += len(points)
	}
	return total, nil
}

// deleteBlocks removes points in [start, end] from encrypted blocks, returning
// the number deleted.
func (s *Store) deleteBlocks(ctx context.Context, bucket *bolt.Bucket, start, end int64) (int, error) {
	type update struct {
		start  int64
		points []Point
	}

	firstKey := encodeKey(blockStart(start, s.blockDuration()))
	var updates []update
	deleted := 0

	cursor := bucket.Cursor()
	for key, raw := cursor.Seek(firstKey[:]); key != nil; key, raw = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		if len(key) != recordSize {
			continue
		}
		block := decodeKey(key)
		if block > end {
			break
		}
		points, err := s.openBlock(raw)
		if err != nil {
			return 0, err
		}

		kept := make([]Point, 0, len(points))
		for _, point := range points {
			if point.Timestamp >= start && point.Timestamp <= end {
				deleted++
				continue
			}
			kept = append(kept, point)
		}
		updates = append(updates, update{start: block, points: kept})
	}

	for _, u := range updates {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		if err := s.writeBlock(bucket, u.start, u.points); err != nil {
			return 0, err
		}
	}
	return deleted, nil
}

// pruneBlocks applies a retention policy to encrypted blocks. A zero policy
// returns before any block is unsealed.
func (s *Store) pruneBlocks(ctx context.Context, bucket *bolt.Bucket, r Retention, now int64) (int, error) {
	if r.MaxAge <= 0 && r.MaxPoints <= 0 {
		return 0, nil
	}

	var all []Point
	var keys [][]byte

	cursor := bucket.Cursor()
	for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		if len(key) != recordSize {
			continue
		}
		points, err := s.openBlock(raw)
		if err != nil {
			return 0, err
		}
		all = append(all, points...)
		keys = append(keys, append([]byte(nil), key...))
	}

	total := len(all)
	if total == 0 {
		return 0, nil
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Timestamp < all[j].Timestamp })

	remove := 0
	if r.MaxPoints > 0 && total > r.MaxPoints {
		remove = total - r.MaxPoints
	}
	if r.MaxAge > 0 {
		cutoff := retentionCutoff(now, int64(r.MaxAge))
		age := 0
		for _, point := range all {
			if point.Timestamp >= cutoff {
				break
			}
			age++
		}
		if age > remove {
			remove = age
		}
	}
	if remove <= 0 {
		return 0, nil
	}

	for _, key := range keys {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		if err := bucket.Delete(key); err != nil {
			return 0, err
		}
	}

	groups := make(map[int64][]Point)
	for _, point := range all[remove:] {
		start := blockStart(point.Timestamp, s.blockDuration())
		groups[start] = append(groups[start], point)
	}
	for start, points := range groups {
		if err := s.writeBlock(bucket, start, points); err != nil {
			return 0, err
		}
	}
	return remove, nil
}
