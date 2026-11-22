package db

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

// Node exposes Storm operations relative to a bbolt bucket path.
type Node interface {
	TypeStore
	KeyValueStore
	BucketScanner

	// From returns a new Storm node with a new bucket root below the current.
	// All Store operations on the new node are executed relative to this bucket.
	From(addend ...string) Node

	// Bucket returns the bucket name as a slice from the root.
	// In the normal, simple case this will be empty.
	Bucket() []string

	// Codec returns the codec selected for this node.
	Codec() Codec

	// WithCodec returns a copy of the node configured to use the given codec.
	WithCodec(codec Codec) Node
}

// Compile-time assertion that *node implements the final Node interface.
var _ Node = (*node)(nil)

// node implements Node for a specific bucket path and optional transaction.
type node struct {
	s *Store

	// The root bucket path. An empty path is the database root; the storm root
	// package configures ["db"] explicitly.
	root [][]byte

	// Transaction object. Nil if not in transaction
	tx *bolt.Tx

	// Codec of this node
	codec Codec
}

// cloneBucketPath returns a defensive copy of the given bucket path.
// nil is preserved as nil; an empty non-nil slice is preserved.
func cloneBucketPath(path [][]byte) [][]byte {
	if path == nil {
		return nil
	}

	cloned := make([][]byte, len(path))
	for i, part := range path {
		cloned[i] = append([]byte(nil), part...)
	}
	return cloned
}

// From returns a new Storm Node with a new bucket root below the current.
// All Store operations on the new node will be executed relative to this bucket.
func (n node) From(addend ...string) Node {
	path := make([][]byte, 0, len(n.root)+len(addend))
	path = append(path, cloneBucketPath(n.root)...)
	for _, part := range addend {
		path = append(path, []byte(part))
	}
	n.root = path
	return &n
}

// withTransaction returns a new Storm Node that will use the given transaction.
func (n node) withTransaction(tx *bolt.Tx) *node {
	n.tx = tx
	return &n
}

// WithCodec returns a new Storm Node that will use the given Codec.
func (n node) WithCodec(codec Codec) Node {
	n.codec = codec
	return &n
}

// Bucket returns the bucket name as a slice from the root.
// In the normal, simple case this will be empty.
func (n *node) Bucket() []string {
	if n.root == nil {
		return nil
	}
	parts := make([]string, len(n.root))
	for i, part := range n.root {
		parts[i] = string(part)
	}
	return parts
}

// Codec returns the EncodeDecoder used by this instance of Storm
func (n *node) Codec() Codec {
	return n.codec
}

// Detects if already in transaction or runs a read write transaction.
func (n *node) readWriteTx(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	run := func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}

		if err := fn(tx); err != nil {
			return err
		}

		return checkContext(ctx)
	}

	if n.tx != nil {
		return run(n.tx)
	}

	return n.s.bolt.Update(run)
}

// Detects if already in transaction or runs a read transaction.
func (n *node) readTx(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	run := func(tx *bolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return checkContext(ctx)
	}

	if n.tx != nil {
		return run(n.tx)
	}

	return n.s.bolt.View(run)
}
