package record

import bolt "go.etcd.io/bbolt"

// createBucketIfNotExists creates the bucket below the current node if it doesn't
// already exist.
func (n *node) createBucketIfNotExists(tx *bolt.Tx, bucket string) (*bolt.Bucket, error) {
	var b *bolt.Bucket
	var err error
	named := false

	walk := func(name []byte) error {
		if b == nil {
			b, err = tx.CreateBucketIfNotExists(name)
		} else {
			b, err = b.CreateBucketIfNotExists(name)
		}
		return err
	}

	for _, name := range n.root {
		if len(name) == 0 {
			continue
		}
		if err := walk(name); err != nil {
			return nil, err
		}
		named = true
	}

	if bucket != "" {
		if err := walk([]byte(bucket)); err != nil {
			return nil, err
		}
		named = true
	}

	// If there were no valid bucket names at all, fall back to the tx root
	if !named || b == nil {
		return nil, ErrNoName
	}

	return b, nil
}

// getBucket returns the given bucket below the current node.
func (n *node) getBucket(tx *bolt.Tx, children ...string) *bolt.Bucket {
	var b *bolt.Bucket

	for _, name := range n.root {
		if len(name) == 0 {
			continue
		}
		if b == nil {
			b = tx.Bucket(name)
		} else {
			b = b.Bucket(name)
		}
		if b == nil {
			return nil
		}
	}

	for _, child := range children {
		if child == "" {
			continue
		}
		name := []byte(child)
		if b == nil {
			b = tx.Bucket(name)
		} else {
			b = b.Bucket(name)
		}
		if b == nil {
			return nil
		}
	}

	return b
}
