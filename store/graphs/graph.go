package graphs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	hdb "github.com/malivvan/harbor/store/record"
)

var (
	// ErrNodeNotFound reports a node id that is not live.
	ErrNodeNotFound = errors.New("graph: node not found")
	// ErrEdgeNotFound reports an edge id that is not live.
	ErrEdgeNotFound = errors.New("graph: edge not found")
	// ErrAsOfWrite reports a write issued under an as-of context. Writing
	// "now" while the caller believes it is working at another instant would
	// corrupt the record rather than the answer.
	ErrAsOfWrite = errors.New("graph: a write cannot run under an as-of context")
	// ErrInvalidRequest reports a malformed node, edge or traversal.
	ErrInvalidRequest = errors.New("graph: invalid request")
)

// Node is a graph node as callers see it.
type Node struct {
	ID          string         `json:"id"`
	Vector      []float32      `json:"vector,omitempty"`
	Content     string         `json:"content,omitempty"`
	Type        string         `json:"type,omitempty"`
	Properties  map[string]any `json:"properties,omitempty"`
	CreatedAt   time.Time      `json:"createdAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
	ValidFrom   time.Time      `json:"validFrom,omitempty"`
	ValidTo     time.Time      `json:"validTo,omitempty"`
	RecordedAt  time.Time      `json:"recordedAt,omitempty"`
	RetractedAt time.Time      `json:"retractedAt,omitempty"`
}

// Edge is a directed edge as callers see it.
type Edge struct {
	ID          string         `json:"id"`
	From        string         `json:"from"`
	To          string         `json:"to"`
	Type        string         `json:"type,omitempty"`
	Weight      float64        `json:"weight"`
	Properties  map[string]any `json:"properties,omitempty"`
	Vector      []float32      `json:"vector,omitempty"`
	CreatedAt   time.Time      `json:"createdAt"`
	ValidFrom   time.Time      `json:"validFrom,omitempty"`
	ValidTo     time.Time      `json:"validTo,omitempty"`
	RecordedAt  time.Time      `json:"recordedAt,omitempty"`
	RetractedAt time.Time      `json:"retractedAt,omitempty"`
}

// Filter narrows a whole-graph read.
type Filter struct {
	NodeTypes []string `json:"nodeTypes,omitempty"`
	EdgeTypes []string `json:"edgeTypes,omitempty"`
	// Properties keeps nodes whose properties carry every value listed here.
	Properties map[string]string `json:"properties,omitempty"`
	Limit      int               `json:"limit,omitempty"`
}

// TraversalOptions configures a walk.
type TraversalOptions struct {
	MaxDepth  int      `json:"maxDepth"`
	EdgeTypes []string `json:"edgeTypes,omitempty"`
	NodeTypes []string `json:"nodeTypes,omitempty"`
	// Direction is "out", "in" or "both"; empty means "out".
	Direction string `json:"direction,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

// StepFilter is the part of the graph a walk may take: given the node the walk
// stands on and the edges read there, it returns the edges the walk may follow.
type StepFilter func(ctx context.Context, from string, edges []*Edge) ([]*Edge, error)

// Path is a walk from one node to another.
type Path struct {
	Nodes    []*Node `json:"nodes"`
	Edges    []*Edge `json:"edges"`
	Distance int     `json:"distance"`
}

// ReadOptions carries the as-of instant on the context.
type ReadOptions struct {
	AsOf time.Time
}

// IsAsOf reports whether the instant is set.
func (o ReadOptions) IsAsOf() bool { return !o.AsOf.IsZero() }

type readOptionsKey struct{}

// AsOf returns a context whose graph reads answer at the instant. A zero
// instant is "now" and is not stamped.
func AsOf(ctx context.Context, at time.Time) context.Context {
	if at.IsZero() {
		return ctx
	}
	return context.WithValue(ctx, readOptionsKey{}, ReadOptions{AsOf: at.UTC()})
}

// ReadOptionsFrom reads the instant a context carries; the zero value means now.
func ReadOptionsFrom(ctx context.Context) ReadOptions {
	if ctx == nil {
		return ReadOptions{}
	}
	opts, _ := ctx.Value(readOptionsKey{}).(ReadOptions)
	return opts
}

// Invalidation says why a version ended.
type Invalidation struct {
	Reason        string `json:"reason,omitempty"`
	SupersededBy  string `json:"supersededBy,omitempty"`
	Producer      string `json:"producer,omitempty"`
	SchemaID      string `json:"schemaId,omitempty"`
	SchemaVersion int    `json:"schemaVersion,omitempty"`
}

type invalidationKey struct{}

// WithInvalidation returns a context whose writes record why they ended a
// version.
func WithInvalidation(ctx context.Context, inv Invalidation) context.Context {
	return context.WithValue(ctx, invalidationKey{}, inv)
}

// InvalidationFrom reads the invalidation a context carries.
func InvalidationFrom(ctx context.Context) Invalidation {
	if ctx == nil {
		return Invalidation{}
	}
	inv, _ := ctx.Value(invalidationKey{}).(Invalidation)
	return inv
}

// RecordVersion is one closed version of a node or edge, as history returns it.
type RecordVersion struct {
	ID            string
	Kind          string // "node" or "edge"
	From          string // an edge's source, empty for a node
	To            string // an edge's target, empty for a node
	Type          string
	Content       string
	Weight        float64
	Properties    map[string]any
	ValidFrom     time.Time
	ValidTo       time.Time
	RecordedAt    time.Time
	RetractedAt   time.Time
	Reason        string
	SupersededBy  string
	Producer      string
	SchemaID      string
	SchemaVersion int
}

// Status is the version's state: "current" (live and never closed),
// "superseded" (closed by a newer value) or "retracted" (withdrawn).
func (v RecordVersion) Status() string {
	switch {
	case !v.RetractedAt.IsZero():
		return "retracted"
	case !v.ValidTo.IsZero():
		return "superseded"
	default:
		return "current"
	}
}

// PurgeReport is what a purge removed, or would remove under DryRun.
type PurgeReport struct {
	Nodes  int64 `json:"nodes"`
	Edges  int64 `json:"edges"`
	DryRun bool  `json:"dryRun,omitempty"`
}

// BatchResult is the partial-success answer of a batch write.
type BatchResult struct {
	SuccessCount int     `json:"successCount"`
	FailedCount  int     `json:"failedCount"`
	Errors       []error `json:"-"`
}

// BatchOperation is a set of writes applied in the stated order.
type BatchOperation struct {
	NodeUpserts []*Node
	NodeDeletes []string
	EdgeUpserts []*Edge
	EdgeDeletes []string
}

// Statistics is a whole-graph count.
type Statistics struct {
	Nodes     int            `json:"nodes"`
	Edges     int            `json:"edges"`
	NodeTypes map[string]int `json:"nodeTypes,omitempty"`
	EdgeTypes map[string]int `json:"edgeTypes,omitempty"`
}

// ValueCodec encodes and decodes a record's dynamic properties. The default is
// JSON; a caller that already stores properties in another encoding (eidos's
// binco value union, for instance) supplies it here so an existing file keeps
// reading and writing the same bytes.
type ValueCodec interface {
	Encode(map[string]any) ([]byte, error)
	Decode([]byte) (map[string]any, error)
}

// JSONValues is the default property codec.
type JSONValues struct{}

// Encode writes the properties as JSON; nil is no bytes.
func (JSONValues) Encode(properties map[string]any) ([]byte, error) {
	if len(properties) == 0 {
		return nil, nil
	}
	return json.Marshal(properties)
}

// Decode reads JSON properties; empty bytes are nil properties.
func (JSONValues) Decode(blob []byte) (map[string]any, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	var properties map[string]any
	if err := json.Unmarshal(blob, &properties); err != nil {
		return nil, fmt.Errorf("graph: decode properties: %w", err)
	}
	return properties, nil
}

// Option configures a Graph.
type Option func(*Graph)

// WithClock replaces the clock, for deterministic tests.
func WithClock(now func() time.Time) Option {
	return func(g *Graph) {
		if now != nil {
			g.now = now
		}
	}
}

// WithValues replaces the property codec.
func WithValues(codec ValueCodec) Option {
	return func(g *Graph) {
		if codec != nil {
			g.values = codec
		}
	}
}

// WithPrefix namespaces the graph below a bucket path, so several independent
// graphs can share one store.
func WithPrefix(parts ...string) Option {
	return func(g *Graph) {
		g.prefix = append([]string(nil), parts...)
	}
}

// Graph is a property graph over one graph storage.
type Graph struct {
	db      *hdb.Store // legacy record-backed store, nil for an independent graph
	storage Storage
	prefix  []string
	now     func() time.Time
	values  ValueCodec
	// versionSeq keeps two versions recorded in the same nanosecond distinct.
	versionSeq atomic.Uint64
}

// New creates a graph on the legacy record-backed layout. It keeps a file
// written by earlier builds readable. New graphs should use store/graphs and
// NewStorage, which do not depend on the record store's buckets or codec.
func New(db *hdb.Store, opts ...Option) *Graph {
	g := &Graph{
		db:     db,
		now:    func() time.Time { return time.Now().UTC() },
		values: JSONValues{},
	}
	for _, opt := range opts {
		opt(g)
	}
	g.storage = newRecordStorage(db, g.prefix)
	return g
}

// NewStorage creates a graph over an independent Storage implementation.
func NewStorage(storage Storage, opts ...Option) *Graph {
	g := &Graph{
		storage: storage,
		now:     func() time.Time { return time.Now().UTC() },
		values:  JSONValues{},
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// DB returns the legacy record store the graph is built on, or nil for an
// independent graph.
func (g *Graph) RecordStore() *hdb.Store { return g.db }

// Storage returns the graph's persistence backend.
func (g *Graph) Storage() Storage { return g.storage }

// Now is the instant writes are stamped with.
func (g *Graph) Now() time.Time { return g.now() }

// SetClock replaces the clock.
func (g *Graph) SetClock(now func() time.Time) {
	if now != nil {
		g.now = now
	}
}

// Init is a no-op: buckets are created by the first write.
func (g *Graph) Init(context.Context) error { return nil }

// read runs fn against one consistent graph view.
func (g *Graph) read(ctx context.Context, fn func(Reader) error) error {
	return g.storage.Read(ctx, fn)
}

// write runs fn in one write transaction.
func (g *Graph) write(ctx context.Context, fn func(w Writer) error) error {
	if ReadOptionsFrom(ctx).IsAsOf() {
		return ErrAsOfWrite
	}
	return g.storage.Write(ctx, fn)
}

// UpsertNode inserts or updates a node. An unchanged write is a no-op: it
// creates no version and rewrites nothing. CreatedAt is the store's unless the
// caller sets it, which an import does to keep the time a record first existed.
func (g *Graph) UpsertNode(ctx context.Context, node *Node) error {
	if node == nil || node.ID == "" {
		return fmt.Errorf("%w: node id is required", ErrInvalidRequest)
	}
	properties, err := g.values.Encode(node.Properties)
	if err != nil {
		return err
	}
	return g.write(ctx, func(w Writer) error {
		recorded := g.now()
		at := recorded
		if !node.ValidFrom.IsZero() {
			at = node.ValidFrom.UTC()
		}
		var existing NodeRecord
		existingPtr, err := w.Node(ctx, node.ID)
		if err == nil {
			existing = *existingPtr
		}
		switch {
		case err == nil:
			unchanged, cerr := g.nodeUnchanged(&existing, node, properties)
			if cerr != nil {
				return cerr
			}
			if unchanged {
				node.CreatedAt = existing.CreatedAt
				node.UpdatedAt = existing.UpdatedAt
				node.ValidFrom = existing.ValidFrom
				node.RecordedAt = existing.RecordedAt
				node.ValidTo = existing.ValidTo
				node.RetractedAt = existing.RetractedAt
				return nil
			}
			if err := g.archiveNode(ctx, w, &existing, at, recorded, false, InvalidationFrom(ctx)); err != nil {
				return err
			}
		case errors.Is(err, ErrRecordNotFound):
			// New node.
		default:
			return err
		}
		created := recorded
		if !existing.CreatedAt.IsZero() {
			created = existing.CreatedAt
		}
		// A caller-supplied CreatedAt is kept: it is how an import preserves
		// the time the record first existed rather than the time it landed.
		if !node.CreatedAt.IsZero() {
			created = node.CreatedAt.UTC()
		}
		record := &NodeRecord{
			ID: node.ID, Vector: append([]float32(nil), node.Vector...),
			Content: node.Content, NodeType: node.Type, Properties: properties,
			CreatedAt: created, UpdatedAt: recorded, ValidFrom: at, RecordedAt: recorded,
		}
		if err := w.PutNode(ctx, record); err != nil {
			return err
		}
		node.CreatedAt, node.UpdatedAt, node.ValidFrom, node.RecordedAt = created, recorded, at, recorded
		node.ValidTo, node.RetractedAt = time.Time{}, time.Time{}
		return nil
	})
}

// nodeUnchanged reports whether a write would change nothing.
func (g *Graph) nodeUnchanged(existing *NodeRecord, node *Node, properties []byte) (bool, error) {
	if existing.Content != node.Content || existing.NodeType != node.Type {
		return false, nil
	}
	if !equalVectors(existing.Vector, node.Vector) {
		return false, nil
	}
	return equalBlobs(existing.Properties, properties), nil
}

// GetNode returns the live node, or the node in force under an as-of context.
func (g *Graph) GetNode(ctx context.Context, nodeID string) (*Node, error) {
	ro := ReadOptionsFrom(ctx)
	var record *NodeRecord
	err := g.read(ctx, func(r Reader) error {
		var err error
		if ro.IsAsOf() {
			record, err = g.nodeAt(ctx, r, nodeID, ro.AsOf)
			return err
		}
		record, err = r.Node(ctx, nodeID)
		return err
	})
	if err != nil {
		return nil, nodeErr(nodeID, err)
	}
	return g.nodeFromRecord(record)
}

// DeleteNode retracts the live node and every live edge that touches it.
func (g *Graph) DeleteNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("%w: node id is required", ErrInvalidRequest)
	}
	return g.write(ctx, func(w Writer) error {
		record, err := w.Node(ctx, nodeID)
		if err != nil {
			return nodeErr(nodeID, err)
		}
		now := g.now()
		if err := g.archiveNode(ctx, w, record, now, now, true, InvalidationFrom(ctx)); err != nil {
			return err
		}
		edges, err := g.liveEdgesTouching(ctx, w, nodeID)
		if err != nil {
			return err
		}
		for _, edge := range edges {
			if err := g.archiveEdge(ctx, w, edge, now, now, true, InvalidationFrom(ctx)); err != nil {
				return err
			}
		}
		return nil
	})
}

// UpsertEdge inserts or updates an edge. Both endpoints must be live nodes.
func (g *Graph) UpsertEdge(ctx context.Context, edge *Edge) error {
	if edge == nil || edge.ID == "" {
		return fmt.Errorf("%w: edge id is required", ErrInvalidRequest)
	}
	if edge.From == "" || edge.To == "" {
		return fmt.Errorf("%w: edge %q needs both endpoints", ErrInvalidRequest, edge.ID)
	}
	properties, err := g.values.Encode(edge.Properties)
	if err != nil {
		return err
	}
	return g.write(ctx, func(w Writer) error {
		for _, id := range []string{edge.From, edge.To} {
			if _, err := w.Node(ctx, id); err != nil {
				if errors.Is(err, ErrRecordNotFound) {
					return fmt.Errorf("%w: endpoint %q", ErrNodeNotFound, id)
				}
				return err
			}
		}
		recorded := g.now()
		at := recorded
		if !edge.ValidFrom.IsZero() {
			at = edge.ValidFrom.UTC()
		}
		if edge.Weight == 0 {
			edge.Weight = 1
		}
		var existing EdgeRecord
		existingPtr, err := w.Edge(ctx, edge.ID)
		if err == nil {
			existing = *existingPtr
		}
		switch {
		case err == nil:
			unchanged := existing.FromNodeID == edge.From && existing.ToNodeID == edge.To &&
				existing.EdgeType == edge.Type && existing.Weight == edge.Weight &&
				equalVectors(existing.Vector, edge.Vector) && equalBlobs(existing.Properties, properties)
			if unchanged {
				edge.CreatedAt = existing.CreatedAt
				edge.ValidFrom = existing.ValidFrom
				edge.RecordedAt = existing.RecordedAt
				edge.ValidTo = existing.ValidTo
				edge.RetractedAt = existing.Retracted
				return nil
			}
			if err := g.archiveEdge(ctx, w, &existing, at, recorded, false, InvalidationFrom(ctx)); err != nil {
				return err
			}
		case errors.Is(err, ErrRecordNotFound):
			// New edge.
		default:
			return err
		}
		created := recorded
		if !existing.CreatedAt.IsZero() {
			created = existing.CreatedAt
		}
		record := &EdgeRecord{
			ID: edge.ID, FromNodeID: edge.From, ToNodeID: edge.To, EdgeType: edge.Type,
			Weight: edge.Weight, Properties: properties, Vector: append([]float32(nil), edge.Vector...),
			CreatedAt: created, ValidFrom: at, RecordedAt: recorded,
		}
		if err := w.PutEdge(ctx, record); err != nil {
			return err
		}
		edge.CreatedAt, edge.ValidFrom, edge.RecordedAt = created, at, recorded
		edge.ValidTo, edge.RetractedAt = time.Time{}, time.Time{}
		return nil
	})
}

// GetEdges returns the live edges touching nodeID, or those in force at the
// as-of instant. direction is "out", "in" or "both"; empty means "out".
func (g *Graph) GetEdges(ctx context.Context, nodeID, direction string) ([]*Edge, error) {
	if direction == "" {
		direction = "out"
	}
	var out []*EdgeRecord
	err := g.read(ctx, func(r Reader) error {
		for _, side := range edgeSides(direction) {
			records, err := g.edgeRecords(ctx, r, side, nodeID)
			if err != nil {
				return err
			}
			out = append(out, records...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(out))
	edges := make([]*Edge, 0, len(out))
	for _, record := range out {
		if seen[record.ID] {
			continue
		}
		seen[record.ID] = true
		edge, err := g.edgeFromRecord(record)
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return edges, nil
}

// DeleteEdge retracts the live edge.
func (g *Graph) DeleteEdge(ctx context.Context, edgeID string) error {
	if edgeID == "" {
		return fmt.Errorf("%w: edge id is required", ErrInvalidRequest)
	}
	return g.write(ctx, func(w Writer) error {
		record, err := w.Edge(ctx, edgeID)
		if err != nil {
			return edgeErr(edgeID, err)
		}
		now := g.now()
		return g.archiveEdge(ctx, w, record, now, now, true, InvalidationFrom(ctx))
	})
}

// GetNodesBatch returns the live nodes named, in id order, skipping missing ids.
func (g *Graph) GetNodesBatch(ctx context.Context, ids []string) ([]*Node, error) {
	out := make([]*Node, 0, len(ids))
	for _, id := range ids {
		node, err := g.GetNode(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, node)
	}
	return out, nil
}

// GetEdgesBatch returns the live edges named, in id order, skipping missing ids.
func (g *Graph) GetEdgesBatch(ctx context.Context, ids []string) ([]*Edge, error) {
	out := make([]*Edge, 0, len(ids))
	for _, id := range ids {
		var record *EdgeRecord
		err := g.read(ctx, func(r Reader) error {
			var err error
			record, err = r.Edge(ctx, id)
			return err
		})
		if err != nil {
			if errors.Is(err, ErrRecordNotFound) {
				continue
			}
			return nil, err
		}
		edge, err := g.edgeFromRecord(record)
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Neighbors is a breadth-first walk from nodeID.
func (g *Graph) Neighbors(ctx context.Context, nodeID string, opts TraversalOptions) ([]*Node, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	seen := map[string]bool{nodeID: true}
	frontier := []string{nodeID}
	out := make([]*Node, 0)
	for depth := 0; depth < opts.MaxDepth && len(frontier) > 0 && len(out) < limit; depth++ {
		next := make([]string, 0)
		for _, id := range frontier {
			edges, err := g.GetEdges(ctx, id, opts.Direction)
			if err != nil {
				return nil, err
			}
			for _, edge := range edges {
				if !containsString(opts.EdgeTypes, edge.Type) {
					continue
				}
				other := edge.To
				if other == id {
					other = edge.From
				}
				if seen[other] {
					continue
				}
				node, err := g.GetNode(ctx, other)
				if err != nil {
					if errors.Is(err, ErrNodeNotFound) {
						continue
					}
					return nil, err
				}
				if !containsString(opts.NodeTypes, node.Type) {
					continue
				}
				seen[other] = true
				next = append(next, other)
				out = append(out, node)
				if len(out) >= limit {
					return out, nil
				}
			}
		}
		frontier = next
	}
	return out, nil
}

// ShortestPath finds a breadth-first path from one live node to another.
func (g *Graph) ShortestPath(ctx context.Context, fromID, toID string) (*Path, error) {
	return g.ShortestPathAdmitting(ctx, fromID, toID, nil)
}

// ShortestPathAdmitting is ShortestPath over the part of the graph admit keeps.
func (g *Graph) ShortestPathAdmitting(ctx context.Context, fromID, toID string, admit StepFilter) (*Path, error) {
	if fromID == toID {
		node, err := g.GetNode(ctx, fromID)
		if err != nil {
			return nil, err
		}
		return &Path{Nodes: []*Node{node}}, nil
	}
	parent := map[string]string{}
	parentEdge := map[string]*Edge{}
	seen := map[string]bool{fromID: true}
	queue := []string{fromID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		edges, err := g.GetEdges(ctx, id, "out")
		if err != nil {
			return nil, err
		}
		if admit != nil {
			edges, err = admit(ctx, id, edges)
			if err != nil {
				return nil, err
			}
		}
		for _, edge := range edges {
			if seen[edge.To] {
				continue
			}
			seen[edge.To] = true
			parent[edge.To] = id
			parentEdge[edge.To] = edge
			if edge.To == toID {
				return g.pathFrom(ctx, fromID, toID, parent, parentEdge)
			}
			queue = append(queue, edge.To)
		}
	}
	return nil, ErrNodeNotFound
}

// pathFrom walks the parent chain back into a Path.
func (g *Graph) pathFrom(ctx context.Context, fromID, toID string, parent map[string]string, parentEdge map[string]*Edge) (*Path, error) {
	ids := []string{toID}
	edges := []*Edge{}
	for id := toID; id != fromID; {
		previous, ok := parent[id]
		if !ok {
			return nil, ErrNodeNotFound
		}
		edges = append([]*Edge{parentEdge[id]}, edges...)
		ids = append([]string{previous}, ids...)
		id = previous
	}
	nodes := make([]*Node, 0, len(ids))
	for _, id := range ids {
		node, err := g.GetNode(ctx, id)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return &Path{Nodes: nodes, Edges: edges, Distance: len(edges)}, nil
}

// Connected reports whether one node reaches another within maxDepth.
func (g *Graph) Connected(ctx context.Context, fromID, toID string, maxDepth int) (bool, error) {
	path, err := g.ShortestPath(ctx, fromID, toID)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	if maxDepth > 0 && path.Distance > maxDepth {
		return false, nil
	}
	return true, nil
}

// CountNodes counts the live nodes matching filter, or those in force as of.
func (g *Graph) CountNodes(ctx context.Context, filter *Filter) (int, error) {
	nodes, err := g.GetAllNodes(ctx, filter)
	return len(nodes), err
}

// GetAllNodes returns the live nodes matching filter, in id order.
func (g *Graph) GetAllNodes(ctx context.Context, filter *Filter) ([]*Node, error) {
	var records []*NodeRecord
	err := g.read(ctx, func(r Reader) error {
		var err error
		records, err = g.nodeRecords(ctx, r)
		return err
	})
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(records))
	for _, record := range records {
		node, err := g.nodeFromRecord(record)
		if err != nil {
			return nil, err
		}
		if matchesNodeFilter(node, filter) {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	if filter != nil && filter.Limit > 0 && len(nodes) > filter.Limit {
		nodes = nodes[:filter.Limit]
	}
	return nodes, nil
}

// GetAllEdges returns the live edges, in id order.
func (g *Graph) GetAllEdges(ctx context.Context) ([]*Edge, error) {
	var records []*EdgeRecord
	err := g.read(ctx, func(r Reader) error {
		var err error
		records, err = g.edgeRecordsAll(ctx, r)
		return err
	})
	if err != nil {
		return nil, err
	}
	edges := make([]*Edge, 0, len(records))
	for _, record := range records {
		edge, err := g.edgeFromRecord(record)
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return edges, nil
}

// NodeTypeCounts counts the live nodes by type.
func (g *Graph) NodeTypeCounts(ctx context.Context) (map[string]int, error) {
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, node := range nodes {
		counts[node.Type]++
	}
	return counts, nil
}

// EdgeTypeCounts counts the live edges by type.
func (g *Graph) EdgeTypeCounts(ctx context.Context) (map[string]int, error) {
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, edge := range edges {
		counts[edge.Type]++
	}
	return counts, nil
}

// Statistics is the whole-graph count.
func (g *Graph) Statistics(ctx context.Context) (*Statistics, error) {
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, err
	}
	stats := &Statistics{Nodes: len(nodes), Edges: len(edges), NodeTypes: map[string]int{}, EdgeTypes: map[string]int{}}
	for _, node := range nodes {
		stats.NodeTypes[node.Type]++
	}
	for _, edge := range edges {
		stats.EdgeTypes[edge.Type]++
	}
	return stats, nil
}

// ExistingNodeIDs reports which ids are live nodes.
func (g *Graph) ExistingNodeIDs(ctx context.Context, ids []string) (map[string]bool, error) {
	out := make(map[string]bool, len(ids))
	err := g.read(ctx, func(r Reader) error {
		for _, id := range ids {
			if _, err := r.Node(ctx, id); err == nil {
				out[id] = true
			} else if !errors.Is(err, ErrRecordNotFound) {
				return err
			}
		}
		return nil
	})
	return out, err
}

// UpsertNodesBatch applies node upserts, reporting per-node failures.
func (g *Graph) UpsertNodesBatch(ctx context.Context, nodes []*Node) (*BatchResult, error) {
	result := &BatchResult{Errors: make([]error, 0)}
	for _, node := range nodes {
		if err := g.UpsertNode(ctx, node); err != nil {
			result.Errors = append(result.Errors, err)
			result.FailedCount++
			continue
		}
		result.SuccessCount++
	}
	return result, nil
}

// DeleteNodesBatch retracts each node.
func (g *Graph) DeleteNodesBatch(ctx context.Context, ids []string) (*BatchResult, error) {
	result := &BatchResult{Errors: make([]error, 0)}
	for _, id := range ids {
		if err := g.DeleteNode(ctx, id); err != nil {
			result.Errors = append(result.Errors, err)
			result.FailedCount++
			continue
		}
		result.SuccessCount++
	}
	return result, nil
}

// UpsertEdgesBatch applies edge upserts, reporting per-edge failures.
func (g *Graph) UpsertEdgesBatch(ctx context.Context, edges []*Edge) (*BatchResult, error) {
	result := &BatchResult{Errors: make([]error, 0)}
	for _, edge := range edges {
		if err := g.UpsertEdge(ctx, edge); err != nil {
			result.Errors = append(result.Errors, err)
			result.FailedCount++
			continue
		}
		result.SuccessCount++
	}
	return result, nil
}

// DeleteEdgesBatch retracts each edge.
func (g *Graph) DeleteEdgesBatch(ctx context.Context, ids []string) (*BatchResult, error) {
	result := &BatchResult{Errors: make([]error, 0)}
	for _, id := range ids {
		if err := g.DeleteEdge(ctx, id); err != nil {
			result.Errors = append(result.Errors, err)
			result.FailedCount++
			continue
		}
		result.SuccessCount++
	}
	return result, nil
}

// ExecuteBatch applies the four operation lists in order.
func (g *Graph) ExecuteBatch(ctx context.Context, ops *BatchOperation) (*BatchResult, error) {
	result := &BatchResult{Errors: make([]error, 0)}
	if ops == nil {
		return result, nil
	}
	for _, run := range []func() (*BatchResult, error){
		func() (*BatchResult, error) { return g.UpsertNodesBatch(ctx, ops.NodeUpserts) },
		func() (*BatchResult, error) { return g.DeleteNodesBatch(ctx, ops.NodeDeletes) },
		func() (*BatchResult, error) { return g.UpsertEdgesBatch(ctx, ops.EdgeUpserts) },
		func() (*BatchResult, error) { return g.DeleteEdgesBatch(ctx, ops.EdgeDeletes) },
	} {
		part, err := run()
		if err != nil {
			return nil, err
		}
		result.SuccessCount += part.SuccessCount
		result.FailedCount += part.FailedCount
		result.Errors = append(result.Errors, part.Errors...)
	}
	return result, nil
}

// helpers

func nodeErr(id string, err error) error {
	if errors.Is(err, ErrRecordNotFound) {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	return err
}

func edgeErr(id string, err error) error {
	if errors.Is(err, ErrRecordNotFound) {
		return fmt.Errorf("%w: %s", ErrEdgeNotFound, id)
	}
	return err
}

func edgeSides(direction string) []string {
	switch direction {
	case "in":
		return []string{"in"}
	case "both":
		return []string{"out", "in"}
	default:
		return []string{"out"}
	}
}

func containsString(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func equalVectors(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalBlobs(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func matchesNodeFilter(node *Node, filter *Filter) bool {
	if filter == nil {
		return true
	}
	if len(filter.NodeTypes) > 0 && !containsString(filter.NodeTypes, node.Type) {
		return false
	}
	for key, want := range filter.Properties {
		if fmt.Sprint(node.Properties[key]) != want {
			return false
		}
	}
	return true
}

func coversAt(validFrom, validTo, at time.Time) bool {
	if !validFrom.IsZero() && validFrom.After(at) {
		return false
	}
	if !validTo.IsZero() && !validTo.After(at) {
		return false
	}
	return true
}
