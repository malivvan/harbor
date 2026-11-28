package graphs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

// ErrSchemaUnsupported reports SetSchema on a storage backend that cannot
// persist a schema (the legacy record-backed layout).
var ErrSchemaUnsupported = errors.New("graph: schema is not supported by this storage")

// SchemaViolation describes one record that does not satisfy the graph schema.
type SchemaViolation struct {
	Kind    string // "node" or "edge"
	ID      string
	Field   string
	Message string
}

func (v SchemaViolation) Error() string {
	if v.Field != "" {
		return fmt.Sprintf("graph: schema violation on %s %q field %q: %s", v.Kind, v.ID, v.Field, v.Message)
	}
	return fmt.Sprintf("graph: schema violation on %s %q: %s", v.Kind, v.ID, v.Message)
}

// ValidationReport lists the violations found by Validate.
type ValidationReport struct {
	Violations []SchemaViolation
}

// OK reports whether the graph satisfies its schema.
func (r *ValidationReport) OK() bool { return r == nil || len(r.Violations) == 0 }

// Schema is an opt-in graph contract. A zero Schema is valid and disables
// validation, preserving the permissive default.
type Schema struct {
	NodeTypes map[string]NodeTypeSpec `json:"node_types,omitempty"`
	EdgeTypes map[string]EdgeTypeSpec `json:"edge_types,omitempty"`
}

// NodeTypeSpec constrains one node type.
type NodeTypeSpec struct {
	Required   []string                `json:"required,omitempty"`
	Properties map[string]PropertySpec `json:"properties,omitempty"`
	Unique     []string                `json:"unique,omitempty"`
}

// PropertySpec constrains one property value.
type PropertySpec struct {
	// Type is "string", "number", "bool", "array", "object" or "any".
	Type string `json:"type,omitempty"`
	// Enum restricts a string property to the listed values.
	Enum []string `json:"enum,omitempty"`
}

// EdgeTypeSpec constrains one edge type.
type EdgeTypeSpec struct {
	From       []string `json:"from,omitempty"`
	To         []string `json:"to,omitempty"`
	Required   []string `json:"required,omitempty"`
	MaxPerPair int      `json:"max_per_pair,omitempty"`
}

// schemaStore is implemented by storage backends that can persist a schema.
type schemaStore interface {
	LoadSchema(ctx context.Context) ([]byte, error)
	SaveSchema(ctx context.Context, raw []byte) error
}

const schemaKey = "schema"

func (g *graphStorage) LoadSchema(ctx context.Context) ([]byte, error) {
	var raw []byte
	err := g.store.view(ctx, func(tx *bolt.Tx) error {
		b, err := g.buckets(tx, false)
		if err != nil {
			return err
		}
		if b.schema == nil {
			return nil
		}
		sealed := b.schema.Get([]byte(schemaKey))
		if sealed == nil {
			return nil
		}
		plain, err := g.store.openValue(sealed)
		if err != nil {
			return err
		}
		raw = append([]byte(nil), plain...)
		return nil
	})
	return raw, err
}

func (g *graphStorage) SaveSchema(ctx context.Context, raw []byte) error {
	var entries []ledger.Entry
	err := g.store.update(ctx, func(tx *bolt.Tx) error {
		b, err := g.buckets(tx, true)
		if err != nil {
			return err
		}
		sealed, err := g.store.seal(raw)
		if err != nil {
			return err
		}
		if err := b.schema.Put([]byte(schemaKey), sealed); err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		entries = append(entries, ledger.Entry{
			Store:  ledger.StoreGraphs,
			Op:     "schema",
			Entity: g.name + ":" + schemaKey,
			Digest: sum[:],
		})
		return nil
	})
	if err != nil {
		return err
	}
	if g.store.trace != nil {
		for _, entry := range entries {
			if err := g.store.trace.Record(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetSchema validates and persists a schema. It is available on the
// independent graph store; the legacy record-backed layout returns
// ErrSchemaUnsupported.
func (g *Graph) SetSchema(ctx context.Context, schema Schema) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if err := validateSchemaDefinition(schema); err != nil {
		return err
	}
	store, ok := g.storage.(schemaStore)
	if !ok {
		return ErrSchemaUnsupported
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	if err := store.SaveSchema(ctx, raw); err != nil {
		return err
	}
	g.schemaMu.Lock()
	g.schema = &schema
	g.schemaLoaded = true
	g.schemaMu.Unlock()
	return nil
}

// Schema returns the persisted schema, or nil when none is set.
func (g *Graph) Schema(ctx context.Context) (*Schema, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	g.schemaMu.RLock()
	if g.schemaLoaded {
		schema := g.schema
		g.schemaMu.RUnlock()
		return schema, nil
	}
	g.schemaMu.RUnlock()

	store, ok := g.storage.(schemaStore)
	if !ok {
		g.schemaMu.Lock()
		g.schemaLoaded = true
		g.schemaMu.Unlock()
		return nil, nil
	}
	raw, err := store.LoadSchema(ctx)
	if err != nil {
		return nil, err
	}
	var schema *Schema
	if len(raw) > 0 {
		var decoded Schema
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("graph: decode schema: %w", err)
		}
		schema = &decoded
	}
	g.schemaMu.Lock()
	g.schema = schema
	g.schemaLoaded = true
	g.schemaMu.Unlock()
	return schema, nil
}

// Validate checks every live node and edge against the schema. dryRun is
// accepted for API symmetry; validation never mutates the graph.
func (g *Graph) Validate(ctx context.Context, dryRun bool) (*ValidationReport, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	schema, err := g.Schema(ctx)
	if err != nil {
		return nil, err
	}
	report := &ValidationReport{}
	if schema == nil {
		return report, nil
	}
	nodes, err := g.GetAllNodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if err := validateNodeAgainst(schema, node, nodes); err != nil {
			appendViolation(report, err)
		}
	}
	for _, edge := range edges {
		if err := validateEdgeAgainst(schema, edge, nodes, edges); err != nil {
			appendViolation(report, err)
		}
	}
	return report, nil
}

func (g *Graph) validateNode(ctx context.Context, node *Node) error {
	schema, err := g.Schema(ctx)
	if err != nil || schema == nil {
		return err
	}
	var nodes []*Node
	if spec, ok := schema.NodeTypes[node.Type]; ok && len(spec.Unique) > 0 {
		nodes, err = g.GetAllNodes(ctx, nil)
		if err != nil {
			return err
		}
	}
	return validateNodeAgainst(schema, node, nodes)
}

func (g *Graph) validateEdge(ctx context.Context, edge *Edge) error {
	schema, err := g.Schema(ctx)
	if err != nil || schema == nil {
		return err
	}
	var nodes []*Node
	spec, ok := schema.EdgeTypes[edge.Type]
	if ok && (len(spec.From) > 0 || len(spec.To) > 0) {
		nodes, err = g.GetAllNodes(ctx, nil)
		if err != nil {
			return err
		}
	}
	edges, err := g.GetAllEdges(ctx)
	if err != nil {
		return err
	}
	return validateEdgeAgainst(schema, edge, nodes, edges)
}

func validateSchemaDefinition(schema Schema) error {
	if len(schema.NodeTypes) == 0 && len(schema.EdgeTypes) == 0 {
		return nil
	}
	for name, spec := range schema.NodeTypes {
		if name == "" {
			return fmt.Errorf("%w: empty node type", ErrInvalidRequest)
		}
		for key, property := range spec.Properties {
			switch property.Type {
			case "", "any", "string", "number", "bool", "array", "object":
			default:
				return fmt.Errorf("%w: node type %q property %q has unknown type %q", ErrInvalidRequest, name, key, property.Type)
			}
		}
	}
	for name, spec := range schema.EdgeTypes {
		if name == "" {
			return fmt.Errorf("%w: empty edge type", ErrInvalidRequest)
		}
		if len(schema.NodeTypes) > 0 {
			for _, from := range spec.From {
				if _, ok := schema.NodeTypes[from]; !ok {
					return fmt.Errorf("%w: edge type %q names unknown node type %q", ErrInvalidRequest, name, from)
				}
			}
			for _, to := range spec.To {
				if _, ok := schema.NodeTypes[to]; !ok {
					return fmt.Errorf("%w: edge type %q names unknown node type %q", ErrInvalidRequest, name, to)
				}
			}
		}
	}
	return nil
}

func validateNodeAgainst(schema *Schema, node *Node, all []*Node) error {
	if schema == nil || node == nil {
		return nil
	}
	if len(schema.NodeTypes) > 0 {
		spec, ok := schema.NodeTypes[node.Type]
		if !ok {
			return SchemaViolation{Kind: "node", ID: node.ID, Field: "type", Message: fmt.Sprintf("unknown node type %q", node.Type)}
		}
		for _, field := range spec.Required {
			if _, ok := node.Properties[field]; !ok {
				return SchemaViolation{Kind: "node", ID: node.ID, Field: field, Message: "required property is missing"}
			}
		}
		for field, property := range spec.Properties {
			value, ok := node.Properties[field]
			if !ok {
				continue
			}
			if err := checkProperty(field, value, property); err != nil {
				return SchemaViolation{Kind: "node", ID: node.ID, Field: field, Message: err.Error()}
			}
		}
		for _, field := range spec.Unique {
			value, ok := node.Properties[field]
			if !ok {
				continue
			}
			for _, other := range all {
				if other == nil || other.ID == node.ID || other.Type != node.Type {
					continue
				}
				if otherValue, ok := other.Properties[field]; ok && fmt.Sprint(otherValue) == fmt.Sprint(value) {
					return SchemaViolation{Kind: "node", ID: node.ID, Field: field, Message: fmt.Sprintf("value %v is already used by %q", value, other.ID)}
				}
			}
		}
	}
	return nil
}

func validateEdgeAgainst(schema *Schema, edge *Edge, nodes []*Node, edges []*Edge) error {
	if schema == nil || edge == nil {
		return nil
	}
	if len(schema.EdgeTypes) > 0 {
		spec, ok := schema.EdgeTypes[edge.Type]
		if !ok {
			return SchemaViolation{Kind: "edge", ID: edge.ID, Field: "type", Message: fmt.Sprintf("unknown edge type %q", edge.Type)}
		}
		for _, field := range spec.Required {
			if _, ok := edge.Properties[field]; !ok {
				return SchemaViolation{Kind: "edge", ID: edge.ID, Field: field, Message: "required property is missing"}
			}
		}
		nodeType := make(map[string]string, len(nodes))
		for _, node := range nodes {
			if node != nil {
				nodeType[node.ID] = node.Type
			}
		}
		if len(spec.From) > 0 {
			if typ, ok := nodeType[edge.From]; ok && !containsString(spec.From, typ) {
				return SchemaViolation{Kind: "edge", ID: edge.ID, Field: "from", Message: fmt.Sprintf("node %q has type %q, allowed %v", edge.From, typ, spec.From)}
			}
		}
		if len(spec.To) > 0 {
			if typ, ok := nodeType[edge.To]; ok && !containsString(spec.To, typ) {
				return SchemaViolation{Kind: "edge", ID: edge.ID, Field: "to", Message: fmt.Sprintf("node %q has type %q, allowed %v", edge.To, typ, spec.To)}
			}
		}
		if spec.MaxPerPair > 0 {
			count := 0
			for _, other := range edges {
				if other == nil || other.ID == edge.ID {
					continue
				}
				if other.Type == edge.Type && other.From == edge.From && other.To == edge.To {
					count++
				}
			}
			if count >= spec.MaxPerPair {
				return SchemaViolation{Kind: "edge", ID: edge.ID, Field: "type", Message: fmt.Sprintf("edge type %q already has %d edges between %q and %q (max %d)", edge.Type, count, edge.From, edge.To, spec.MaxPerPair)}
			}
		}
	}
	return nil
}

func checkProperty(field string, value any, spec PropertySpec) error {
	if len(spec.Enum) > 0 {
		if !containsString(spec.Enum, fmt.Sprint(value)) {
			return fmt.Errorf("value %v is not in %v", value, spec.Enum)
		}
	}
	switch spec.Type {
	case "", "any":
		return nil
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("value %v is not a string", value)
		}
	case "number":
		switch value.(type) {
		case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		default:
			return fmt.Errorf("value %v is not a number", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("value %v is not a bool", value)
		}
	case "array":
		switch value.(type) {
		case []any, []string, []float64, []int:
		default:
			return fmt.Errorf("value %v is not an array", value)
		}
	case "object":
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("value %v is not an object", value)
		}
	}
	return nil
}

func appendViolation(report *ValidationReport, err error) {
	var violation SchemaViolation
	if errors.As(err, &violation) {
		report.Violations = append(report.Violations, violation)
	}
	sort.SliceStable(report.Violations, func(i, j int) bool {
		if report.Violations[i].Kind != report.Violations[j].Kind {
			return report.Violations[i].Kind < report.Violations[j].Kind
		}
		if report.Violations[i].ID != report.Violations[j].ID {
			return report.Violations[i].ID < report.Violations[j].ID
		}
		return report.Violations[i].Field < report.Violations[j].Field
	})
}

// ctxErr is a small local alias so this file does not import context twice in
// call sites that already have the helper.
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
