// Package consensus provides cluster-wide replication for a Harbor store using
// the Raft consensus algorithm (github.com/hashicorp/raft).
//
// A Node owns one *harbor.Store replica and the raft machinery that keeps it in
// sync with the rest of the cluster. Writes are proposed through raft on the
// leader and applied, in raft-log order, to the local store on every node;
// reads are served directly from the local store. See PLAN.md for the full
// design and the consistency model.
package consensus

import "errors"

var (
	// ErrClosed is returned by operations on a closed Node.
	ErrClosed = errors.New("consensus: node is closed")

	// ErrNotLeader is returned when a write is attempted on a node that is not
	// the current leader. Callers can redirect using Node.Leader.
	ErrNotLeader = errors.New("consensus: not leader")

	// ErrLeaderNotFound is returned when the cluster has no elected leader.
	ErrLeaderNotFound = errors.New("consensus: leader not found")

	// ErrInvalidConfig reports a nil or unusable Config.
	ErrInvalidConfig = errors.New("consensus: invalid config")
)
