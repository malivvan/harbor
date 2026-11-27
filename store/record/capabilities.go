package record

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Capability bits record opt-in, semantics-changing features that an older
// build must not ignore. They are persisted in __database/capabilities as a
// big-endian uint64 and checked when the store opens: a bit this build does
// not implement fails the open instead of silently serving data that should
// be hidden, expired, or otherwise filtered.
//
// Zero is the default and means "no feature beyond format version 1". A store
// that never opts in is byte-identical to one written before this record
// existed.
const (
	// CapScope reserves the bit for context-carried ownership scoping.
	CapScope uint64 = 1 << iota
	// CapExpiry records that some records carry an expiry and reads must
	// exclude expired rows even on a build that did not enable the option.
	CapExpiry
	// CapHistory reserves the bit for per-record revision history.
	CapHistory
	// CapTombstone reserves the bit for soft-delete tombstones.
	CapTombstone
	// CapFieldPolicy reserves the bit for field-level encryption policies.
	CapFieldPolicy
)

// knownCapabilities is the set of bits this build implements. Bits outside it
// are refused on open, which is what makes the fail-closed guarantee real.
// Add a bit here only in the same change that implements its read semantics.
const knownCapabilities = CapExpiry

const capabilitiesKey = "capabilities"

// checkCapabilities reads and validates the persisted capability record.
func (s *Store) checkCapabilities(ctx context.Context) error {
	root, ok := s.Node.(*node)
	if !ok {
		return ErrBadType
	}
	raw, err := root.GetBytes(ctx, databaseInfo, capabilitiesKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	if len(raw) != 8 {
		return fmt.Errorf("%w: malformed capabilities record", ErrIncompatibleVersion)
	}
	caps := binary.BigEndian.Uint64(raw)
	if unknown := caps &^ knownCapabilities; unknown != 0 {
		return fmt.Errorf("%w: unknown capabilities %#x", ErrIncompatibleVersion, unknown)
	}
	s.caps = caps
	return nil
}

// ensureCapability adds bit to the persisted capability record inside tx.
func (n *node) ensureCapability(ctx context.Context, tx *bolt.Tx, bit uint64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	b, err := n.createBucketIfNotExists(tx, databaseInfo)
	if err != nil {
		return err
	}
	id, err := n.keyBytes(capabilitiesKey, keyDomainKV)
	if err != nil {
		return err
	}
	var caps uint64
	if raw := b.Get(id); raw != nil {
		plain, err := n.open(raw)
		if err != nil {
			return err
		}
		if len(plain) != 0 {
			if len(plain) != 8 {
				return fmt.Errorf("%w: malformed capabilities record", ErrIncompatibleVersion)
			}
			caps = binary.BigEndian.Uint64(plain)
		}
	}
	if caps&bit != 0 {
		return nil
	}
	caps |= bit
	plain := make([]byte, 8)
	binary.BigEndian.PutUint64(plain, caps)
	sealed, err := n.seal(plain)
	if err != nil {
		return err
	}
	if err := b.Put(id, sealed); err != nil {
		return err
	}
	n.s.caps = caps
	return checkContext(ctx)
}
