// Package slip10 implements SLIP-0010 hierarchical deterministic key derivation for ed25519.
// SLIP-0010 implementation(ed25519 only) according to the https://github.com/satoshilabs/slips/blob/master/slip-0010.md
package slip10

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

const (
	// FirstHardenedIndex is the index of the first hardened key (2^31).
	// https://youtu.be/2HrMlVr1QX8?t=390
	FirstHardenedIndex = uint32(0x80000000)
	// As in https://github.com/satoshilabs/slips/blob/master/slip-0010.md
	seedModifier = "ed25519 seed"

	// MinSeedSize and MaxSeedSize bound the root seed, per SLIP-0010 (128-512
	// bits).
	MinSeedSize = 16
	MaxSeedSize = 64
)

var (
	ErrInvalidPath        = errors.New("invalid derivation path")
	ErrNoPublicDerivation = errors.New("no public derivation for ed25519")
	ErrInvalidSeed        = errors.New("invalid seed length")
	ErrDestroyed          = errors.New("node has been destroyed")

	pathRegex = regexp.MustCompile(`^m(/[0-9]+')*$`)
)

// Node is a SLIP-0010 node. Its private material can be wiped with Destroy.
type Node interface {
	Derive(i uint32) (Node, error)
	KeyPair() (ed25519.PublicKey, ed25519.PrivateKey)
	PrivateKey() []byte
	PublicKeyWithPrefix() []byte
	RawSeed() []byte
	MarshalBinary() ([]byte, error)
	// Destroy zeroes the node's private key and chain code. It is idempotent,
	// and every method on a destroyed node returns no key material (or
	// ErrDestroyed for Derive and MarshalBinary).
	Destroy()
}

type node struct {
	code      []byte
	key       []byte
	destroyed bool
}

// Derive derives the node at `path` starting from a ROOT seed.
//
// Use this when you have the rootSeed(e.g. extracted from the mnemonic) and a derivation path, and you
// want to deterministically re-create the SAME node every time from the root.
//
// Do NOT pass a node's RawSeed() here — that 32-byte value is only the ed25519
// key seed for that node and does not include its chain code. Feeding it here
// creates a NEW unrelated root.
func Derive(path string, rootSeed []byte) (Node, error) {
	indexes, err := parsePath(path)
	if err != nil {
		return nil, err
	}

	current, err := New(rootSeed)
	if err != nil {
		return nil, err
	}

	for _, i := range indexes {
		child, err := current.Derive(i)
		// The intermediate node was created here and is no longer needed.
		current.Destroy()
		if err != nil {
			return nil, err
		}
		current = child
	}

	return current, nil
}

// New constructs the SLIP-0010 *master node* from a ROOT seed.
//
// Use this when you have the original root entropy and want to (re)build a tree
// deterministically from the top (e.g., together with a path like "m/44'/...").
// Security: anyone with rootSeed can derive the entire tree. Treat as highly sensitive.
//
// rootSeed must be between 16 and 64 bytes, as required by SLIP-0010.
func New(rootSeed []byte) (Node, error) {
	if err := validateSeed(rootSeed); err != nil {
		return nil, err
	}

	hash := hmac.New(sha512.New, []byte(seedModifier))
	if _, err := hash.Write(rootSeed); err != nil {
		return nil, err
	}
	sum := hash.Sum(nil)
	defer zero(sum)

	return &node{
		key:  append([]byte(nil), sum[:32]...),
		code: append([]byte(nil), sum[32:]...),
	}, nil
}

func (k *node) Derive(i uint32) (Node, error) {
	if k == nil || k.destroyed {
		return nil, ErrDestroyed
	}
	// no public derivation for ed25519
	if i < FirstHardenedIndex {
		return nil, ErrNoPublicDerivation
	}

	iBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(iBytes, i)
	key := append([]byte{0x0}, k.key...)
	data := append(key, iBytes...)
	defer zero(key)
	defer zero(data)

	hash := hmac.New(sha512.New, k.code)
	if _, err := hash.Write(data); err != nil {
		return nil, err
	}
	sum := hash.Sum(nil)
	defer zero(sum)

	return &node{
		key:  append([]byte(nil), sum[:32]...),
		code: append([]byte(nil), sum[32:]...),
	}, nil
}

// KeyPair returns the Ed25519 key pair for a derived private key.
// It returns nil keys for a nil or destroyed node.
func (k *node) KeyPair() (ed25519.PublicKey, ed25519.PrivateKey) {
	if k == nil || k.destroyed {
		return nil, nil
	}
	priv := ed25519.NewKeyFromSeed(k.key)
	pub, _ := priv.Public().(ed25519.PublicKey)
	return pub, priv
}

// RawSeed returns this node’s 32-byte Ed25519 private key seed (the value you
// would pass to ed25519.NewKeyFromSeed). It is *not* the SLIP-0010 master
// seed and it does *not* include the 32-byte chain code.
//
// Use when:
//   - You need the Ed25519 signing key for this node:
//     priv := ed25519.NewKeyFromSeed(n.RawSeed())
//   - You must export/import a 32-byte Ed25519 seed for compatibility with
//     other libraries or formats.
//
// Do NOT use when:
//   - Rehydrating a node for further derivation — RawSeed() alone is insufficient.
//     To restore a node and derive children, you also need its chain code;
//     use MarshalBinary/UnmarshalNode instead.
//   - Creating a new master/root: passing RawSeed() into New/Derive
//     produces a *new, unrelated* root and changes the blast radius.
//
// The returned slice is a copy, so callers cannot mutate the node's state.
// Security: RawSeed() recovers the node’s signing key.
func (k *node) RawSeed() []byte {
	if k == nil || k.destroyed {
		return nil
	}
	return append([]byte(nil), k.key...)
}

// PrivateKey returns private key seed bytes.
// It returns nil for a nil or destroyed node.
func (k *node) PrivateKey() []byte {
	_, priv := k.KeyPair()
	if priv == nil {
		return nil
	}
	return priv.Seed()
}

// PublicKeyWithPrefix returns public key with 0x00 prefix, as specified in the slip-10
// https://github.com/satoshilabs/slips/blob/master/slip-0010/testvectors.py#L64
// It returns nil for a nil or destroyed node.
func (k *node) PublicKeyWithPrefix() []byte {
	pub, _ := k.KeyPair()
	if pub == nil {
		return nil
	}
	return append([]byte{0x00}, pub...)
}

// MarshalBinary serializes the node's extended private key as key||chainCode (64 bytes).
// Use this to persist a checkpoint so you can restore the node later without the root seed.
// It returns ErrDestroyed for a destroyed node.
// Security: anyone with this blob can derive all descendants of this node.
func (k *node) MarshalBinary() ([]byte, error) {
	if k == nil || k.destroyed {
		return nil, ErrDestroyed
	}
	// [32]key || [32]chainCode
	b := make([]byte, 64)
	copy(b[:32], k.key)
	copy(b[32:], k.code)
	return b, nil
}

// Destroy zeroes the node's private key and chain code. It is safe to call
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

// Destroy zeroes n's private material. It is safe to call with a nil Node.
func Destroy(n Node) {
	if n != nil {
		n.Destroy()
	}
}

// UnmarshalNode restores a node from a 64-byte key||chainCode blob produced by MarshalBinary.
// Use this when you saved a node checkpoint and want to continue deriving BELOW that node.
func UnmarshalNode(b []byte) (Node, error) {
	if len(b) != 64 {
		return nil, fmt.Errorf("invalid node blob length: %d", len(b))
	}
	n := &node{
		key:  append([]byte(nil), b[:32]...),
		code: append([]byte(nil), b[32:]...),
	}
	return n, nil
}

// IsValidPath check whether or not the path has valid segments.
func IsValidPath(path string) bool {
	_, err := parsePath(path)
	return err == nil
}

// parsePath validates path and returns the fully hardened child indexes it
// addresses. Segment values must stay below 2^31 so adding FirstHardenedIndex
// cannot overflow.
func parsePath(path string) ([]uint32, error) {
	if !pathRegex.MatchString(path) {
		return nil, ErrInvalidPath
	}

	segments := strings.Split(path, "/")
	indexes := make([]uint32, 0, len(segments)-1)
	for _, segment := range segments[1:] {
		v, err := strconv.ParseUint(strings.TrimSuffix(segment, "'"), 10, 32)
		if err != nil || v >= uint64(FirstHardenedIndex) {
			return nil, ErrInvalidPath
		}
		indexes = append(indexes, uint32(v)+FirstHardenedIndex)
	}

	return indexes, nil
}

func validateSeed(seed []byte) error {
	if len(seed) < MinSeedSize || len(seed) > MaxSeedSize {
		return fmt.Errorf("%w: got %d bytes, want %d-%d", ErrInvalidSeed, len(seed), MinSeedSize, MaxSeedSize)
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
