package index

import (
	bolt "go.etcd.io/bbolt"
)

// resultCap returns a capacity hint for an index result slice. KeyN is a cheap
// upper bound (it counts every key, including nested buckets), and Limit
// tightens it when the caller only needs a prefix of the results.
// resultSlice pre-sizes a result slice while preserving the historic nil
// result for an empty index.
func resultSlice(n int) [][]byte {
	if n <= 0 {
		return nil
	}
	return make([][]byte, 0, n)
}

func resultCap(bucket *bolt.Bucket, opts *Options) int {
	if bucket == nil {
		return 0
	}
	n := bucket.Stats().KeyN
	if opts != nil && opts.Limit > 0 && opts.Limit < n {
		n = opts.Limit
	}
	if n < 0 {
		return 0
	}
	return n
}
