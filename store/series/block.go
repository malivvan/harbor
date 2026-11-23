package series

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

	// Fast path: an already sorted, duplicate-free batch needs neither a copy
	// nor a sort; the merge below only reads it.
	inc := incoming
	if !sortedUniqueByTimestamp(incoming) {
		inc = append([]Point(nil), incoming...)
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
	}

	if len(existing) == 0 {
		// Copy so the result does not alias the caller's slice.
		out := make([]Point, len(inc))
		copy(out, inc)
		return out
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

// sortedUniqueByTimestamp reports whether points are strictly increasing by
// timestamp, so an insert batch needs neither a copy nor a sort.
func sortedUniqueByTimestamp(points []Point) bool {
	for i := 1; i < len(points); i++ {
		if points[i].Timestamp <= points[i-1].Timestamp {
			return false
		}
	}
	return true
}

func (s *Store) sealBlock(points []Point) ([]byte, error) {
	return s.encodeBlock(points)
}

func (s *Store) openBlock(raw []byte) ([]Point, error) {
	_, points, err := s.decodeBlock(raw)
	return points, err
}

// blockSummarySize is the encoded size of a blockSummary: an int64 count
// followed by five float64 values.
const blockSummarySize = 8 + 5*8

// blockSummary is the per-block aggregate stored alongside the points. It lets
// a compatible Aggregate answer from the summary instead of decoding every
// point.
type blockSummary struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	First float64
	Last  float64
}

// summarize computes the summary of a time-ordered point list.
func summarize(points []Point) blockSummary {
	if len(points) == 0 {
		return blockSummary{}
	}
	s := blockSummary{
		Count: int64(len(points)),
		Min:   points[0].Value,
		Max:   points[0].Value,
		First: points[0].Value,
	}
	for _, p := range points {
		s.Sum += p.Value
		if p.Value < s.Min {
			s.Min = p.Value
		}
		if p.Value > s.Max {
			s.Max = p.Value
		}
	}
	s.Last = points[len(points)-1].Value
	return s
}

// packSummary encodes a summary in a fixed-size little-endian record.
func packSummary(s blockSummary) []byte {
	out := make([]byte, blockSummarySize)
	binary.LittleEndian.PutUint64(out[0:], uint64(s.Count))
	binary.LittleEndian.PutUint64(out[8:], math.Float64bits(s.Sum))
	binary.LittleEndian.PutUint64(out[16:], math.Float64bits(s.Min))
	binary.LittleEndian.PutUint64(out[24:], math.Float64bits(s.Max))
	binary.LittleEndian.PutUint64(out[32:], math.Float64bits(s.First))
	binary.LittleEndian.PutUint64(out[40:], math.Float64bits(s.Last))
	return out
}

// unpackSummary decodes a summary produced by packSummary.
func unpackSummary(data []byte) (blockSummary, error) {
	if len(data) != blockSummarySize {
		return blockSummary{}, errCorruptPoint
	}
	return blockSummary{
		Count: int64(binary.LittleEndian.Uint64(data[0:])),
		Sum:   math.Float64frombits(binary.LittleEndian.Uint64(data[8:])),
		Min:   math.Float64frombits(binary.LittleEndian.Uint64(data[16:])),
		Max:   math.Float64frombits(binary.LittleEndian.Uint64(data[24:])),
		First: math.Float64frombits(binary.LittleEndian.Uint64(data[32:])),
		Last:  math.Float64frombits(binary.LittleEndian.Uint64(data[40:])),
	}, nil
}

// encodeBlock builds the stored block value: the summary followed by the point
// records, sealed as one blob. Sealing them together keeps tamper detection
// intact: corrupting any byte, including the point payload, fails the AEAD
// open. The summary still lets an aggregate skip per-point parsing.
func (s *Store) encodeBlock(points []Point) ([]byte, error) {
	plain := make([]byte, 0, blockSummarySize+blockHeaderSize+len(points)*16)
	plain = append(plain, packSummary(summarize(points))...)
	plain = append(plain, packBlock(points)...)
	return s.sealBytes(plain)
}

// decodeBlock opens a stored block value into its summary and points.
func (s *Store) decodeBlock(value []byte) (blockSummary, []Point, error) {
	plain, err := s.openBytes(value)
	if err != nil {
		return blockSummary{}, nil, err
	}
	if len(plain) < blockSummarySize {
		return blockSummary{}, nil, errCorruptPoint
	}
	sum, err := unpackSummary(plain[:blockSummarySize])
	if err != nil {
		return blockSummary{}, nil, err
	}
	points, err := unpackBlock(plain[blockSummarySize:])
	if err != nil {
		return blockSummary{}, nil, err
	}
	return sum, points, nil
}

// decodeBlockSummaryValue opens a stored block value and returns only its
// summary, skipping the per-point parse and aggregate work.
func (s *Store) decodeBlockSummaryValue(value []byte) (blockSummary, bool, error) {
	plain, err := s.openBytes(value)
	if err != nil {
		return blockSummary{}, false, err
	}
	if len(plain) < blockSummarySize {
		return blockSummary{}, false, errCorruptPoint
	}
	sum, err := unpackSummary(plain[:blockSummarySize])
	if err != nil {
		return blockSummary{}, false, err
	}
	return sum, true, nil
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
	if len(points) == 0 {
		return 0, nil
	}
	dur := s.blockDuration()

	// Fast path: every point falls in the same block, so skip the group map and
	// the start sort entirely.
	firstStart := blockStart(points[0].Timestamp, dur)
	singleBlock := true
	for i := 1; i < len(points); i++ {
		if blockStart(points[i].Timestamp, dur) != firstStart {
			singleBlock = false
			break
		}
	}
	if singleBlock {
		return s.insertBlockBatch(ctx, bucket, firstStart, points)
	}

	groups := make(map[int64][]Point)
	for _, point := range points {
		start := blockStart(point.Timestamp, dur)
		groups[start] = append(groups[start], point)
	}

	starts := make([]int64, 0, len(groups))
	for start := range groups {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	added := 0
	for _, start := range starts {
		if err := checkContext(ctx); err != nil {
			return 0, err
		}
		n, err := s.insertBlockBatch(ctx, bucket, start, groups[start])
		if err != nil {
			return 0, err
		}
		added += n
	}
	return added, nil
}

// insertBlockBatch merges one block's incoming points into the stored block.
func (s *Store) insertBlockBatch(ctx context.Context, bucket *bolt.Bucket, start int64, incoming []Point) (int, error) {
	existing, _, err := s.readBlock(bucket, start)
	if err != nil {
		return 0, err
	}
	merged := mergePoints(existing, incoming)
	added := len(merged) - len(existing)
	if err := s.writeBlock(bucket, start, merged); err != nil {
		return 0, err
	}
	return added, nil
}

// insertBlockPoint merges a single point into its block without allocating a
// one-element input slice or group map.
func (s *Store) insertBlockPoint(ctx context.Context, bucket *bolt.Bucket, point Point) (int, error) {
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	start := blockStart(point.Timestamp, s.blockDuration())
	existing, _, err := s.readBlock(bucket, start)
	if err != nil {
		return 0, err
	}
	merged := mergePoint(existing, point)
	added := len(merged) - len(existing)
	if err := s.writeBlock(bucket, start, merged); err != nil {
		return 0, err
	}
	return added, nil
}

// mergePoint inserts or replaces one point in a sorted block.
func mergePoint(existing []Point, point Point) []Point {
	n := len(existing)
	if n == 0 {
		return []Point{point}
	}
	// Common append case: the point is newer than everything stored, so skip
	// the binary search.
	if point.Timestamp > existing[n-1].Timestamp {
		out := make([]Point, 0, n+1)
		out = append(out, existing...)
		return append(out, point)
	}
	i := sort.Search(n, func(i int) bool {
		return existing[i].Timestamp >= point.Timestamp
	})
	if i < n && existing[i].Timestamp == point.Timestamp {
		out := append([]Point(nil), existing...)
		out[i] = point
		return out
	}
	out := make([]Point, 0, n+1)
	out = append(out, existing[:i]...)
	out = append(out, point)
	return append(out, existing[i:]...)
}

// scanRangeOrdered walks points in [start, end] in ascending or descending
// time order, calling fn for each until fn returns stop=true. It dispatches to
// the block or plain scanner for the store's mode.
func (s *Store) scanRangeOrdered(ctx context.Context, bucket *bolt.Bucket, start, end int64, descending bool, fn func(Point) (bool, error)) error {
	if s.blockDuration() > 0 {
		return s.scanBlocksOrdered(ctx, bucket, start, end, descending, fn)
	}
	return s.scanPlainOrdered(ctx, bucket, start, end, descending, fn)
}

// scanBlocksOrdered walks the encrypted blocks in either direction, filtering
// to [start, end] and stopping early when fn returns stop=true.
func (s *Store) scanBlocksOrdered(ctx context.Context, bucket *bolt.Bucket, start, end int64, descending bool, fn func(Point) (bool, error)) error {
	dur := s.blockDuration()
	cursor := bucket.Cursor()

	if !descending {
		firstKey := encodeKey(blockStart(start, dur))
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
				stop, err := fn(point)
				if err != nil {
					return err
				}
				if stop {
					return nil
				}
			}
		}
		return nil
	}

	// Descending: start at the last block whose start is <= end.
	endBlock := blockStart(end, dur)
	endKey := encodeKey(endBlock)
	key, raw := cursor.Seek(endKey[:])
	if key == nil {
		key, raw = cursor.Last()
	} else if len(key) == recordSize && decodeKey(key) > endBlock {
		key, raw = cursor.Prev()
	}
	for ; key != nil; key, raw = cursor.Prev() {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if len(key) != recordSize {
			continue
		}
		block := decodeKey(key)
		if block+dur <= start {
			break
		}
		points, err := s.openBlock(raw)
		if err != nil {
			return err
		}
		for i := len(points) - 1; i >= 0; i-- {
			point := points[i]
			if point.Timestamp < start || point.Timestamp > end {
				continue
			}
			stop, err := fn(point)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
	}
	return nil
}

// scanPlainOrdered walks unencrypted point records in either direction,
// filtering to [start, end] and stopping early when fn returns stop=true.
func (s *Store) scanPlainOrdered(ctx context.Context, bucket *bolt.Bucket, start, end int64, descending bool, fn func(Point) (bool, error)) error {
	cursor := bucket.Cursor()

	if !descending {
		startKey := encodeKey(start)
		for key, value := cursor.Seek(startKey[:]); key != nil; key, value = cursor.Next() {
			if err := checkContext(ctx); err != nil {
				return err
			}
			point, ok, err := s.decodePoint(key, value)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if point.Timestamp > end {
				break
			}
			stop, err := fn(point)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		return nil
	}

	endKey := encodeKey(end)
	key, value := cursor.Seek(endKey[:])
	if key == nil {
		key, value = cursor.Last()
	} else if len(key) == recordSize && decodeKey(key) > end {
		key, value = cursor.Prev()
	}
	for ; key != nil; key, value = cursor.Prev() {
		if err := checkContext(ctx); err != nil {
			return err
		}
		point, ok, err := s.decodePoint(key, value)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if point.Timestamp < start {
			break
		}
		stop, err := fn(point)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

// scanBlocksAggregate walks the blocks covering [start, end]. For a block that
// lies wholly inside the range and does not cross an aggregate-window boundary,
// it yields the block's summary (points == nil) so the aggregate can answer
// without decoding every point. Otherwise it yields the decoded points, which
// the caller must still filter to [start, end].
func (s *Store) scanBlocksAggregate(ctx context.Context, bucket *bolt.Bucket, start, end, step int64, fn func(window int64, sum blockSummary, points []Point) error) error {
	dur := s.blockDuration()
	firstKey := encodeKey(blockStart(start, dur))
	cursor := bucket.Cursor()
	for key, raw := cursor.Seek(firstKey[:]); key != nil; key, raw = cursor.Next() {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if len(key) != recordSize {
			continue
		}
		block := decodeKey(key)
		if block > end {
			break
		}

		blockEnd := block + dur - 1
		if block >= start && blockEnd <= end && bucketStart(block, step) == bucketStart(blockEnd, step) {
			sum, ok, err := s.decodeBlockSummaryValue(raw)
			if err != nil {
				return err
			}
			if ok {
				if err := fn(bucketStart(block, step), sum, nil); err != nil {
					return err
				}
				continue
			}
		}

		points, err := s.openBlock(raw)
		if err != nil {
			return err
		}
		if err := fn(0, blockSummary{}, points); err != nil {
			return err
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
