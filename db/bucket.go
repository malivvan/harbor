package db

import bolt "go.etcd.io/bbolt"

// createBucketIfNotExists creates the bucket below the current node if it doesn't
// already exist.
func (n *node) createBucketIfNotExists(tx *bolt.Tx, bucket string) (*bolt.Bucket, error) {
	var b *bolt.Bucket
	var err error

	extra := 0
	if bucket != "" {
		extra = 1
	}

	bucketNames := make([][]byte, 0, len(n.root)+extra)
	bucketNames = append(bucketNames, n.root...)
	if bucket != "" {
		bucketNames = append(bucketNames, []byte(bucket))
	}

	for _, bucketName := range bucketNames {
		if len(bucketName) == 0 {
			continue
		}
		if b != nil {
			if b, err = b.CreateBucketIfNotExists(bucketName); err != nil {
				return nil, err
			}

		} else {
			if b, err = tx.CreateBucketIfNotExists(bucketName); err != nil {
				return nil, err
			}
		}
	}

	// If there were no valid bucket names at all, fall back to the tx root
	if b == nil {
		return nil, ErrNoName
	}

	return b, nil
}

// getBucket returns the given bucket below the current node.
func (n *node) getBucket(tx *bolt.Tx, children ...string) *bolt.Bucket {
	var b *bolt.Bucket

	bucketNames := make([][]byte, 0, len(n.root)+len(children))
	bucketNames = append(bucketNames, n.root...)
	for _, child := range children {
		if child != "" {
			bucketNames = append(bucketNames, []byte(child))
		}
	}
	for _, bucketName := range bucketNames {
		if len(bucketName) == 0 {
			continue
		}
		if b != nil {
			if b = b.Bucket(bucketName); b == nil {
				return nil
			}
		} else {
			if b = tx.Bucket(bucketName); b == nil {
				return nil
			}
		}
	}

	return b
}
