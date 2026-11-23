package graphs

// Diff, vocabulary and whole-graph interchange: the read surfaces built on the
// core's as-of reads.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// ChangeKind is what happened to a record between two instants.
type ChangeKind string

const (
	ChangeAdded   ChangeKind = "added"
	ChangeRemoved ChangeKind = "removed"
	ChangeChanged ChangeKind = "changed"
)

// NodeChange is one node's difference between two instants.
type NodeChange struct {
	ID     string     `json:"id"`
	Kind   ChangeKind `json:"kind"`
	Before *Node      `json:"before,omitempty"`
	After  *Node      `json:"after,omitempty"`
}

// EdgeChange is one edge's difference between two instants.
type EdgeChange struct {
	ID     string     `json:"id"`
	Kind   ChangeKind `json:"kind"`
	Before *Edge      `json:"before,omitempty"`
	After  *Edge      `json:"after,omitempty"`
}

// DiffResult is what changed between two valid-time instants.
type DiffResult struct {
	From  time.Time    `json:"from"`
	To    time.Time    `json:"to"`
	Nodes []NodeChange `json:"nodes,omitempty"`
	Edges []EdgeChange `json:"edges,omitempty"`
}

// Diff reports what was added, removed or changed between two instants.
//
// It is a valid-time diff: both sides are read as the graph stood then, so a
// record rewritten in between is "changed" even though its id never moved. A
// node or edge whose properties differ only in an ignored field still counts as
// changed; the answer says which records moved, not which bytes differ.
func (g *Graph) Diff(ctx context.Context, from, to time.Time) (*DiffResult, error) {
	if from.IsZero() {
		from = g.now()
	}
	if to.IsZero() {
		to = g.now()
	}
	beforeNodes, err := g.GetAllNodes(AsOf(ctx, from), nil)
	if err != nil {
		return nil, err
	}
	afterNodes, err := g.GetAllNodes(AsOf(ctx, to), nil)
	if err != nil {
		return nil, err
	}
	beforeEdges, err := g.GetAllEdges(AsOf(ctx, from))
	if err != nil {
		return nil, err
	}
	afterEdges, err := g.GetAllEdges(AsOf(ctx, to))
	if err != nil {
		return nil, err
	}
	result := &DiffResult{From: from, To: to}

	beforeByID := make(map[string]*Node, len(beforeNodes))
	for _, node := range beforeNodes {
		beforeByID[node.ID] = node
	}
	afterByID := make(map[string]*Node, len(afterNodes))
	for _, node := range afterNodes {
		afterByID[node.ID] = node
	}
	for id, after := range afterByID {
		before, had := beforeByID[id]
		switch {
		case !had:
			result.Nodes = append(result.Nodes, NodeChange{ID: id, Kind: ChangeAdded, After: after})
		case nodeChanged(before, after):
			result.Nodes = append(result.Nodes, NodeChange{ID: id, Kind: ChangeChanged, Before: before, After: after})
		}
	}
	for id, before := range beforeByID {
		if _, still := afterByID[id]; !still {
			result.Nodes = append(result.Nodes, NodeChange{ID: id, Kind: ChangeRemoved, Before: before})
		}
	}

	beforeEdgesByID := make(map[string]*Edge, len(beforeEdges))
	for _, edge := range beforeEdges {
		beforeEdgesByID[edge.ID] = edge
	}
	afterEdgesByID := make(map[string]*Edge, len(afterEdges))
	for _, edge := range afterEdges {
		afterEdgesByID[edge.ID] = edge
	}
	for id, after := range afterEdgesByID {
		before, had := beforeEdgesByID[id]
		switch {
		case !had:
			result.Edges = append(result.Edges, EdgeChange{ID: id, Kind: ChangeAdded, After: after})
		case edgeChanged(before, after):
			result.Edges = append(result.Edges, EdgeChange{ID: id, Kind: ChangeChanged, Before: before, After: after})
		}
	}
	for id, before := range beforeEdgesByID {
		if _, still := afterEdgesByID[id]; !still {
			result.Edges = append(result.Edges, EdgeChange{ID: id, Kind: ChangeRemoved, Before: before})
		}
	}

	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].ID < result.Nodes[j].ID })
	sort.Slice(result.Edges, func(i, j int) bool { return result.Edges[i].ID < result.Edges[j].ID })
	return result, nil
}

// nodeChanged reports whether two values of the same node differ in what a
// caller can observe.
func nodeChanged(a, b *Node) bool {
	if a.Type != b.Type || a.Content != b.Content || !equalVectors(a.Vector, b.Vector) {
		return true
	}
	return !equalMaps(a.Properties, b.Properties)
}

// edgeChanged reports whether two values of the same edge differ.
func edgeChanged(a, b *Edge) bool {
	if a.From != b.From || a.To != b.To || a.Type != b.Type || a.Weight != b.Weight || !equalVectors(a.Vector, b.Vector) {
		return true
	}
	return !equalMaps(a.Properties, b.Properties)
}

// equalMaps compares the JSON value model two properties decode into.
func equalMaps(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		other, ok := b[key]
		if !ok || fmt.Sprint(value) != fmt.Sprint(other) {
			return false
		}
	}
	return true
}

// Label is one node's readable identity.
type Label struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// NodeLabels returns the live nodes as id/name/type labels. The name is the
// node's name property when it carries one, and its content otherwise.
func (g *Graph) NodeLabels(ctx context.Context, filter *Filter) ([]Label, error) {
	nodes, err := g.GetAllNodes(ctx, filter)
	if err != nil {
		return nil, err
	}
	labels := make([]Label, 0, len(nodes))
	for _, node := range nodes {
		name, _ := node.Properties["name"].(string)
		if name == "" {
			name = node.Content
		}
		labels = append(labels, Label{ID: node.ID, Name: name, Type: node.Type})
	}
	return labels, nil
}

// EdgeShape is the endpoint shape of one edge type.
type EdgeShape struct {
	Type      string   `json:"type"`
	FromTypes []string `json:"fromTypes,omitempty"`
	ToTypes   []string `json:"toTypes,omitempty"`
	Count     int      `json:"count"`
}

// EdgeShapes describes the edge types a graph holds, optionally only those
// named: which node types each connects and how many edges it has.
func (g *Graph) EdgeShapes(ctx context.Context, edgeTypes ...string) ([]EdgeShape, error) {
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	nodeType := make(map[string]string, len(nodes))
	for _, node := range nodes {
		nodeType[node.ID] = node.Type
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, err
	}
	shapes := make(map[string]*EdgeShape)
	for _, edge := range edges {
		if !containsString(edgeTypes, edge.Type) {
			continue
		}
		shape, ok := shapes[edge.Type]
		if !ok {
			shape = &EdgeShape{Type: edge.Type}
			shapes[edge.Type] = shape
		}
		shape.Count++
		shape.FromTypes = appendUnique(shape.FromTypes, nodeType[edge.From])
		shape.ToTypes = appendUnique(shape.ToTypes, nodeType[edge.To])
	}
	out := make([]EdgeShape, 0, len(shapes))
	for _, shape := range shapes {
		sort.Strings(shape.FromTypes)
		sort.Strings(shape.ToTypes)
		out = append(out, *shape)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out, nil
}

func appendUnique(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// PropertyCounts counts the live nodes carrying each value of a property.
func (g *Graph) PropertyCounts(ctx context.Context, key string) (map[string]int, error) {
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, node := range nodes {
		value, ok := node.Properties[key]
		if !ok {
			continue
		}
		counts[fmt.Sprint(value)]++
	}
	return counts, nil
}

// RecordRef names one record carrying a property.
type RecordRef struct {
	ID         string         `json:"id"`
	Kind       string         `json:"kind"`
	Type       string         `json:"type,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

// RecordsWithProperties returns the live nodes and edges whose properties carry
// key with the given value. An empty value matches any non-empty value; an
// empty key matches every record carrying any property.
func (g *Graph) RecordsWithProperties(ctx context.Context, key, value string) ([]RecordRef, error) {
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]RecordRef, 0)
	match := func(properties map[string]any) bool {
		if key == "" {
			return len(properties) > 0
		}
		found, ok := properties[key]
		if !ok {
			return false
		}
		return value == "" || fmt.Sprint(found) == value
	}
	for _, node := range nodes {
		if match(node.Properties) {
			out = append(out, RecordRef{ID: node.ID, Kind: "node", Type: node.Type, Properties: node.Properties})
		}
	}
	for _, edge := range edges {
		if match(edge.Properties) {
			out = append(out, RecordRef{ID: edge.ID, Kind: "edge", Type: edge.Type, Properties: edge.Properties})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// Document is a whole graph in the interchange shape.
type Document struct {
	Nodes []*Node `json:"nodes"`
	Edges []*Edge `json:"edges"`
}

// ExportDocument returns the live graph.
func (g *Graph) ExportDocument(ctx context.Context) (*Document, error) {
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, err
	}
	return &Document{Nodes: nodes, Edges: edges}, nil
}

// ExportJSON writes the live graph as JSON.
func (g *Graph) ExportJSON(ctx context.Context, w io.Writer) error {
	doc, err := g.ExportDocument(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(doc)
}

// ImportDocument adds a document's nodes and edges, reporting per-record
// failures. Nodes are written before edges, so an edge naming a node in the
// document always finds it.
func (g *Graph) ImportDocument(ctx context.Context, doc *Document) (*BatchResult, error) {
	if doc == nil {
		return &BatchResult{}, nil
	}
	result, err := g.UpsertNodesBatch(ctx, doc.Nodes)
	if err != nil {
		return nil, err
	}
	edges, err := g.UpsertEdgesBatch(ctx, doc.Edges)
	if err != nil {
		return nil, err
	}
	result.SuccessCount += edges.SuccessCount
	result.FailedCount += edges.FailedCount
	result.Errors = append(result.Errors, edges.Errors...)
	return result, nil
}

// ImportJSON reads a document from JSON and imports it.
func (g *Graph) ImportJSON(ctx context.Context, r io.Reader) (*BatchResult, error) {
	var doc Document
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("graph: decode document: %w", err)
	}
	return g.ImportDocument(ctx, &doc)
}
