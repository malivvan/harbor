package internal

import (
	"bytes"

	bolt "go.etcd.io/bbolt"
)

const AppID = "com.github.malivvan.harbor"

// Cursor that can be reversed
type Cursor struct {
	C       *bolt.Cursor
	Reverse bool
}

// First element
func (c *Cursor) First() ([]byte, []byte) {
	if c.Reverse {
		return c.C.Last()
	}

	return c.C.First()
}

// Next element
func (c *Cursor) Next() ([]byte, []byte) {
	if c.Reverse {
		return c.C.Prev()
	}

	return c.C.Next()
}

// RangeCursor that can be reversed
type RangeCursor struct {
	C         *bolt.Cursor
	Reverse   bool
	Min       []byte
	Max       []byte
	CompareFn func([]byte, []byte) int

	// SeekMin and SeekMax, when non-nil, are the physical keys used to
	// position the cursor. They let an index whose on-disk key encoding is not
	// the logical value (for example the escaped list-index keys) seek to the
	// right place while CompareFn still compares logical values.
	SeekMin []byte
	SeekMax []byte
}

func (c *RangeCursor) seekMin() []byte {
	if c.SeekMin != nil {
		return c.SeekMin
	}
	return c.Min
}

func (c *RangeCursor) seekMax() []byte {
	if c.SeekMax != nil {
		return c.SeekMax
	}
	return c.Max
}

// First element
func (c *RangeCursor) First() ([]byte, []byte) {
	if c.Reverse {
		if len(c.Max) == 0 {
			return c.C.Last()
		}
		k, v := c.C.Seek(c.seekMax())

		// Seek returns nil when max sorts above every key: fall back to the
		// last key so "everything up to max" still works in reverse.
		if k == nil {
			return c.C.Last()
		}

		// If Seek lands beyond the upper bound, step back so values whose
		// prefix merely starts with max (e.g. "abcd" for max "abc") are not
		// included.
		if c.CompareFn(k, c.Max) > 0 {
			k, v = c.C.Prev()
		}

		return k, v
	}

	return c.C.Seek(c.seekMin())
}

// Next element
func (c *RangeCursor) Next() ([]byte, []byte) {
	if c.Reverse {
		return c.C.Prev()
	}

	return c.C.Next()
}

// Continue tells if the loop needs to continue
func (c *RangeCursor) Continue(val []byte) bool {
	if c.Reverse {
		if len(c.Min) == 0 {
			return val != nil
		}
		return val != nil && c.CompareFn(val, c.Min) >= 0
	}

	if len(c.Max) == 0 {
		return val != nil
	}
	return val != nil && c.CompareFn(val, c.Max) <= 0
}

// PrefixCursor that can be reversed
type PrefixCursor struct {
	C       *bolt.Cursor
	Reverse bool
	Prefix  []byte
}

// First element
func (c *PrefixCursor) First() ([]byte, []byte) {
	var k, v []byte

	for k, v = c.C.First(); k != nil && !bytes.HasPrefix(k, c.Prefix); k, v = c.C.Next() {
	}

	if k == nil {
		return nil, nil
	}

	if c.Reverse {
		kc, vc := k, v
		for ; kc != nil && bytes.HasPrefix(kc, c.Prefix); kc, vc = c.C.Next() {
			k, v = kc, vc
		}
		if kc != nil {
			k, v = c.C.Prev()
		}
	}

	return k, v
}

// Next element
func (c *PrefixCursor) Next() ([]byte, []byte) {
	if c.Reverse {
		return c.C.Prev()
	}

	return c.C.Next()
}

// Continue tells if the loop needs to continue
func (c *PrefixCursor) Continue(val []byte) bool {
	return val != nil && bytes.HasPrefix(val, c.Prefix)
}
