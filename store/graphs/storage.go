package graphs

import (
	"context"
	"errors"
)

// ErrRecordNotFound reports a persisted graph record that is not present. It is
// internal to the storage contract; callers see ErrNodeNotFound/ErrEdgeNotFound.
var ErrRecordNotFound = errors.New("graph: record not found")

// Storage is the persistence contract a graph needs.
//
// Read runs fn against one consistent read view and Write runs fn inside one
// write transaction. The legacy record-backed graph and the independent
// store/graphs backend both implement it, which keeps the graph logic and its
// behaviour identical over either layout.
type Storage interface {
	Read(ctx context.Context, fn func(Reader) error) error
	Write(ctx context.Context, fn func(Writer) error) error
}

// Reader exposes the persisted graph records.
//
// The single-record lookups return ErrRecordNotFound when the id is absent. The
// list methods return an empty slice, not an error, when nothing is stored.
type Reader interface {
	Node(ctx context.Context, id string) (*NodeRecord, error)
	Nodes(ctx context.Context) ([]*NodeRecord, error)
	NodeVersions(ctx context.Context) ([]*NodeVersion, error)
	NodeVersionsByNode(ctx context.Context, nodeID string) ([]*NodeVersion, error)

	Edge(ctx context.Context, id string) (*EdgeRecord, error)
	Edges(ctx context.Context) ([]*EdgeRecord, error)
	EdgeVersions(ctx context.Context) ([]*EdgeVersion, error)
	EdgeVersionsByEdge(ctx context.Context, edgeID string) ([]*EdgeVersion, error)

	// EdgesTouching returns the live edges incident to nodeID on either side.
	EdgesTouching(ctx context.Context, nodeID string) ([]*EdgeRecord, error)
	// EdgeVersionsTouching returns the closed edge versions incident to nodeID.
	EdgeVersionsTouching(ctx context.Context, nodeID string) ([]*EdgeVersion, error)
}

// Writer adds the mutation methods.
type Writer interface {
	Reader

	PutNode(ctx context.Context, record *NodeRecord) error
	DeleteNode(ctx context.Context, id string) error
	PutNodeVersion(ctx context.Context, version *NodeVersion) error
	DeleteNodeVersion(ctx context.Context, versionID string) error

	PutEdge(ctx context.Context, record *EdgeRecord) error
	DeleteEdge(ctx context.Context, id string) error
	PutEdgeVersion(ctx context.Context, version *EdgeVersion) error
	DeleteEdgeVersion(ctx context.Context, versionID string) error
}
