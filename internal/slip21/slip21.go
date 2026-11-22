// Package slip21 implements the SLIP-0021 key derivation scheme for symmetric keys.
// SLIP-0021 implementation according to the https://github.com/satoshilabs/slips/blob/master/slip-0021.md
package slip21

import (
	"crypto/hmac"
	"crypto/sha512"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strings"
)

const (
	// you should use this Prefix for your path
	Prefix = "m/SLIP-0021"

	// As in https://github.com/satoshilabs/slips/blob/master/slip-0021.md
	seedModifier = "Symmetric key seed"
)

var (
	// ErrInvalidPath is returned when a derivation path is malformed.
	ErrInvalidPath = errors.New("invalid derivation path")

	// ErrInvalidSeed is returned when the seed is empty or too long.
	ErrInvalidSeed = errors.New("invalid seed length")

	// ErrDestroyed is returned when a destroyed node is used again.
	ErrDestroyed = errors.New("node has been destroyed")

	pathRegex = regexp.MustCompile(`^m(/.+)*$`)
)

// Node is a SLIP-0021 node. Its symmetric key can be wiped with Destroy.
type Node interface {
	Derive(label []byte) (Node, error)
	Key() []byte
	// Destroy zeroes the node's symmetric key and chain code. It is
	// idempotent, and every method on a destroyed node returns no key
	// material (or ErrDestroyed for Derive).
	Destroy()
}

type node struct {
	key       []byte
	code      []byte
	destroyed bool
}

// Derive derives the key for a path in slip-0021 format and a seed.
func Derive(path string, seed []byte) (Node, error) {
	labels, err := parsePath(path)
	if err != nil {
		return nil, err
	}

	current, err := New(seed)
	if err != nil {
		return nil, err
	}

	for _, label := range labels {
		child, err := current.Derive([]byte(label))
		// The intermediate node was created here and is no longer needed.
		current.Destroy()
		if err != nil {
			return nil, err
		}
		current = child
	}

	return current, nil
}

// New generates a new master key from seed.
//
// SLIP-0021 does not bound the seed length (it is typically the 16-64 byte
// BIP-0039 seed); an empty seed is rejected because it would produce a
// publicly predictable master key.
func New(seed []byte) (Node, error) {
	if err := validateSeed(seed); err != nil {
		return nil, err
	}

	hash := hmac.New(sha512.New, []byte(seedModifier))
	if _, err := hash.Write(seed); err != nil {
		return nil, err
	}
	sum := hash.Sum(nil)
	defer zero(sum)

	return &node{
		key:  append([]byte(nil), sum[32:]...),
		code: append([]byte(nil), sum[0:32]...),
	}, nil
}

// Derive derives a child node with a label.
func (k *node) Derive(label []byte) (Node, error) {
	if k == nil || k.destroyed {
		return nil, ErrDestroyed
	}

	input := append([]byte{0x0}, label...)
	defer zero(input)

	hash := hmac.New(sha512.New, k.code)
	if _, err := hash.Write(input); err != nil {
		return nil, err
	}
	sum := hash.Sum(nil)
	defer zero(sum)

	return &node{
		key:  append([]byte(nil), sum[32:]...),
		code: append([]byte(nil), sum[:32]...),
	}, nil
}

// Key returns the 256-bit symmetric key for the current node. The returned
// slice is a copy, so callers cannot mutate the node's state. It returns nil
// for a nil or destroyed node.
func (k *node) Key() []byte {
	if k == nil || k.destroyed {
		return nil
	}
	return append([]byte(nil), k.key...)
}

// Destroy zeroes the node's symmetric key and chain code. It is safe to call
// multiple times and on a nil receiver.
func (k *node) Destroy() {
	if k == nil || k.destroyed {
		return
	}
	zero(k.key)
	zero(k.code)
	k.key = nil
	k.code = nil
	k.destroyed = true
}

// Destroy zeroes n's key material. It is safe to call with a nil Node.
func Destroy(n Node) {
	if n != nil {
		n.Destroy()
	}
}

// IsValidPath check whether or not the path has valid segments
func IsValidPath(path string) bool {
	_, err := parsePath(path)
	return err == nil
}

// parsePath validates path and returns its labels. Every label must be
// non-empty; the leading "m" segment addresses the master node.
func parsePath(path string) ([]string, error) {
	if !pathRegex.MatchString(path) {
		return nil, ErrInvalidPath
	}

	labels := strings.Split(path, "/")
	out := make([]string, 0, len(labels)-1)
	for _, label := range labels[1:] {
		if label == "" {
			return nil, ErrInvalidPath
		}
		out = append(out, label)
	}
	return out, nil
}

func validateSeed(seed []byte) error {
	if len(seed) == 0 {
		return fmt.Errorf("%w: seed is empty", ErrInvalidSeed)
	}
	return nil
}

// zero overwrites b. KeepAlive prevents the compiler from treating the stores
// as dead once b is no longer otherwise referenced.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}
