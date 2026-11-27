package graphs

// The persisted records and the bitemporal reads and writes over them.
//
// The records keep the field names and order of the graph eidos previously kept
// in its own package, and pin the bucket names the earlier build used
// (EidosBucketName). That is what lets a file written by an earlier build stay
// readable: the record store derives a bucket from the type name, and a
// schema-driven codec derives a record's fingerprint from its field names and
// order.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// NodeRecord is one live node.
type NodeRecord struct {
	ID          string    `db:"id"`
	Vector      []float32 `json:"vector,omitempty"`
	Content     string    `json:"content,omitempty"`
	NodeType    string    `db:"index" json:"nodeType"`
	Properties  []byte    `json:"properties,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	ValidFrom   time.Time `json:"validFrom"`
	ValidTo     time.Time `json:"validTo,omitempty"`
	RecordedAt  time.Time `json:"recordedAt"`
	RetractedAt time.Time `json:"retractedAt,omitempty"`
}

// EidosBucketName pins the bucket the earlier graph implementation used.
func (NodeRecord) EidosBucketName() string { return "eidosNodeRecord" }

// NodeVersion is one closed version of a node.
type NodeVersion struct {
	VersionID     string    `db:"id"`
	NodeID        string    `db:"index" json:"nodeId"`
	Vector        []float32 `json:"vector,omitempty"`
	Content       string    `json:"content,omitempty"`
	NodeType      string    `json:"nodeType"`
	Properties    []byte    `json:"properties,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	ValidFrom     time.Time `json:"validFrom"`
	ValidTo       time.Time `json:"validTo"`
	RecordedAt    time.Time `json:"recordedAt"`
	RetractedAt   time.Time `json:"retractedAt,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	Superseded    string    `json:"supersededBy,omitempty"`
	Producer      string    `json:"producer,omitempty"`
	SchemaID      string    `json:"schemaId,omitempty"`
	SchemaVersion int       `json:"schemaVersion,omitempty"`
}

// EidosBucketName pins the bucket the earlier graph implementation used.
func (NodeVersion) EidosBucketName() string { return "eidosNodeVersion" }

// EdgeRecord is one live directed edge.
type EdgeRecord struct {
	ID         string    `db:"id"`
	FromNodeID string    `db:"index" json:"fromNodeId"`
	ToNodeID   string    `db:"index" json:"toNodeId"`
	EdgeType   string    `db:"index" json:"edgeType"`
	Weight     float64   `json:"weight"`
	Properties []byte    `json:"properties,omitempty"`
	Vector     []float32 `json:"vector,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	ValidFrom  time.Time `json:"validFrom"`
	ValidTo    time.Time `json:"validTo,omitempty"`
	RecordedAt time.Time `json:"recordedAt"`
	Retracted  time.Time `json:"retractedAt,omitempty"`
}

// EidosBucketName pins the bucket the earlier graph implementation used.
func (EdgeRecord) EidosBucketName() string { return "eidosEdgeRecord" }

// EdgeVersion is one closed version of an edge.
type EdgeVersion struct {
	VersionID     string    `db:"id"`
	EdgeID        string    `db:"index" json:"edgeId"`
	FromNodeID    string    `db:"index" json:"fromNodeId"`
	ToNodeID      string    `db:"index" json:"toNodeId"`
	EdgeType      string    `db:"index" json:"edgeType"`
	Weight        float64   `json:"weight"`
	Properties    []byte    `json:"properties,omitempty"`
	Vector        []float32 `json:"vector,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	ValidFrom     time.Time `json:"validFrom"`
	ValidTo       time.Time `json:"validTo"`
	RecordedAt    time.Time `json:"recordedAt"`
	Retracted     time.Time `json:"retractedAt,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	Superseded    string    `json:"supersededBy,omitempty"`
	Producer      string    `json:"producer,omitempty"`
	SchemaID      string    `json:"schemaId,omitempty"`
	SchemaVersion int       `json:"schemaVersion,omitempty"`
}

// EidosBucketName pins the bucket the earlier graph implementation used.
func (EdgeVersion) EidosBucketName() string { return "eidosEdgeVersion" }

// nodeFromRecord converts a stored node to its caller-facing value.
func (g *Graph) nodeFromRecord(record *NodeRecord) (*Node, error) {
	properties, err := g.values.Decode(record.Properties)
	if err != nil {
		return nil, err
	}
	return &Node{
		ID: record.ID, Vector: append([]float32(nil), record.Vector...),
		Content: record.Content, Type: record.NodeType, Properties: properties,
		CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		ValidFrom: record.ValidFrom, ValidTo: record.ValidTo,
		RecordedAt: record.RecordedAt, RetractedAt: record.RetractedAt,
	}, nil
}

// edgeFromRecord converts a stored edge to its caller-facing value.
func (g *Graph) edgeFromRecord(record *EdgeRecord) (*Edge, error) {
	properties, err := g.values.Decode(record.Properties)
	if err != nil {
		return nil, err
	}
	return &Edge{
		ID: record.ID, From: record.FromNodeID, To: record.ToNodeID, Type: record.EdgeType,
		Weight: record.Weight, Properties: properties, Vector: append([]float32(nil), record.Vector...),
		CreatedAt: record.CreatedAt, ValidFrom: record.ValidFrom, ValidTo: record.ValidTo,
		RecordedAt: record.RecordedAt, RetractedAt: record.Retracted,
	}, nil
}

// versionID names a version record. The id is not the record's identity to a
// reader (NodeID/EdgeID are); it only has to be unique in the version bucket.
func (g *Graph) versionID(id string) string {
	return fmt.Sprintf("%s@%d", id, g.versionSeq.Add(1))
}

// recordedAtOf keeps a version's own recording instant: a row that lived from
// T0 to T2 was recorded at T0, and the closure is the interval's end, not a
// second recording. An as-of read before T2 must still see it.
func recordedAtOf(recorded, fallback time.Time) time.Time {
	if recorded.IsZero() {
		return fallback
	}
	return recorded
}

// archiveNode writes a closed node version and removes the live record.
func (g *Graph) archiveNode(ctx context.Context, w Writer, record *NodeRecord, validTo, recorded time.Time, retracted bool, inv Invalidation) error {
	version := &NodeVersion{
		VersionID: g.versionID(record.ID), NodeID: record.ID,
		Vector: append([]float32(nil), record.Vector...), Content: record.Content,
		NodeType: record.NodeType, Properties: record.Properties,
		CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		ValidFrom: record.ValidFrom, ValidTo: validTo,
		RecordedAt: recordedAtOf(record.RecordedAt, recorded),
		Reason:     inv.Reason, Superseded: inv.SupersededBy, Producer: inv.Producer,
		SchemaID: inv.SchemaID, SchemaVersion: inv.SchemaVersion,
	}
	if retracted {
		version.RetractedAt = recorded
	}
	if err := w.PutNodeVersion(ctx, version); err != nil {
		return err
	}
	return w.DeleteNode(ctx, record.ID)
}

// archiveEdge writes a closed edge version and removes the live record.
func (g *Graph) archiveEdge(ctx context.Context, w Writer, record *EdgeRecord, validTo, recorded time.Time, retracted bool, inv Invalidation) error {
	version := &EdgeVersion{
		VersionID: g.versionID(record.ID), EdgeID: record.ID,
		FromNodeID: record.FromNodeID, ToNodeID: record.ToNodeID, EdgeType: record.EdgeType,
		Weight: record.Weight, Properties: record.Properties,
		Vector: append([]float32(nil), record.Vector...), CreatedAt: record.CreatedAt,
		ValidFrom: record.ValidFrom, ValidTo: validTo,
		RecordedAt: recordedAtOf(record.RecordedAt, recorded),
		Reason:     inv.Reason, Superseded: inv.SupersededBy, Producer: inv.Producer,
		SchemaID: inv.SchemaID, SchemaVersion: inv.SchemaVersion,
	}
	if retracted {
		version.Retracted = recorded
	}
	if err := w.PutEdgeVersion(ctx, version); err != nil {
		return err
	}
	return w.DeleteEdge(ctx, record.ID)
}

// nodeAt returns the node in force at an instant.
func (g *Graph) nodeAt(ctx context.Context, r Reader, id string, at time.Time) (*NodeRecord, error) {
	live, err := r.Node(ctx, id)
	haveLive := err == nil
	if err != nil && !errors.Is(err, ErrRecordNotFound) {
		return nil, err
	}
	versions, err := r.NodeVersionsByNode(ctx, id)
	if err != nil {
		return nil, err
	}
	var chosen *NodeRecord
	consider := func(record *NodeRecord) {
		if !coversAt(record.ValidFrom, record.ValidTo, at) {
			return
		}
		if !record.RecordedAt.IsZero() && record.RecordedAt.After(at) {
			return
		}
		if chosen == nil || record.ValidFrom.After(chosen.ValidFrom) {
			chosen = record
		}
	}
	if haveLive {
		consider(live)
	}
	for _, version := range versions {
		consider(nodeRecordFromVersion(version))
	}
	if chosen == nil {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	return chosen, nil
}

// nodeRecordFromVersion projects a version back to the live-record shape.
func nodeRecordFromVersion(version *NodeVersion) *NodeRecord {
	return &NodeRecord{
		ID: version.NodeID, Vector: version.Vector, Content: version.Content,
		NodeType: version.NodeType, Properties: version.Properties,
		CreatedAt: version.CreatedAt, UpdatedAt: version.UpdatedAt,
		ValidFrom: version.ValidFrom, ValidTo: version.ValidTo,
		RecordedAt: version.RecordedAt, RetractedAt: version.RetractedAt,
	}
}

// edgeRecordFromVersion projects a version back to the live-record shape.
func edgeRecordFromVersion(version *EdgeVersion) *EdgeRecord {
	return &EdgeRecord{
		ID: version.EdgeID, FromNodeID: version.FromNodeID, ToNodeID: version.ToNodeID,
		EdgeType: version.EdgeType, Weight: version.Weight, Properties: version.Properties,
		Vector: version.Vector, CreatedAt: version.CreatedAt,
		ValidFrom: version.ValidFrom, ValidTo: version.ValidTo,
		RecordedAt: version.RecordedAt, Retracted: version.Retracted,
	}
}

// nodeRecords returns the live nodes, or those in force at the as-of instant.
func (g *Graph) nodeRecords(ctx context.Context, r Reader) ([]*NodeRecord, error) {
	live, err := r.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	ro := ReadOptionsFrom(ctx)
	if !ro.IsAsOf() {
		return live, nil
	}
	versions, err := r.NodeVersions(ctx)
	if err != nil {
		return nil, err
	}
	chosen := make(map[string]*NodeRecord, len(live))
	consider := func(record *NodeRecord) {
		if !coversAt(record.ValidFrom, record.ValidTo, ro.AsOf) {
			return
		}
		if !record.RecordedAt.IsZero() && record.RecordedAt.After(ro.AsOf) {
			return
		}
		if existing, ok := chosen[record.ID]; !ok || record.ValidFrom.After(existing.ValidFrom) {
			chosen[record.ID] = record
		}
	}
	for _, record := range live {
		consider(record)
	}
	for _, version := range versions {
		consider(nodeRecordFromVersion(version))
	}
	out := make([]*NodeRecord, 0, len(chosen))
	for _, record := range chosen {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// edgeRecordsAll returns the live edges, or those in force at the as-of instant.
func (g *Graph) edgeRecordsAll(ctx context.Context, r Reader) ([]*EdgeRecord, error) {
	live, err := r.Edges(ctx)
	if err != nil {
		return nil, err
	}
	ro := ReadOptionsFrom(ctx)
	if !ro.IsAsOf() {
		return live, nil
	}
	versions, err := r.EdgeVersions(ctx)
	if err != nil {
		return nil, err
	}
	chosen := make(map[string]*EdgeRecord, len(live))
	consider := func(record *EdgeRecord) {
		if !coversAt(record.ValidFrom, record.ValidTo, ro.AsOf) {
			return
		}
		if !record.RecordedAt.IsZero() && record.RecordedAt.After(ro.AsOf) {
			return
		}
		if existing, ok := chosen[record.ID]; !ok || record.ValidFrom.After(existing.ValidFrom) {
			chosen[record.ID] = record
		}
	}
	for _, record := range live {
		consider(record)
	}
	for _, version := range versions {
		consider(edgeRecordFromVersion(version))
	}
	out := make([]*EdgeRecord, 0, len(chosen))
	for _, record := range chosen {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// edgeRecords returns the edges on one side of a node.
func (g *Graph) edgeRecords(ctx context.Context, r Reader, side, nodeID string) ([]*EdgeRecord, error) {
	if ReadOptionsFrom(ctx).IsAsOf() {
		all, err := g.edgeRecordsAll(ctx, r)
		if err != nil {
			return nil, err
		}
		out := make([]*EdgeRecord, 0)
		for _, record := range all {
			if (side == "out" && record.FromNodeID == nodeID) || (side == "in" && record.ToNodeID == nodeID) {
				out = append(out, record)
			}
		}
		return out, nil
	}
	all, err := r.EdgesTouching(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	out := make([]*EdgeRecord, 0, len(all))
	for _, record := range all {
		if (side == "out" && record.FromNodeID == nodeID) || (side == "in" && record.ToNodeID == nodeID) {
			out = append(out, record)
		}
	}
	return out, nil
}

// liveEdgesTouching returns the live edges incident to a node on either side.
func (g *Graph) liveEdgesTouching(ctx context.Context, r Reader, nodeID string) ([]*EdgeRecord, error) {
	out, err := g.edgeRecords(ctx, r, "out", nodeID)
	if err != nil {
		return nil, err
	}
	in, err := g.edgeRecords(ctx, r, "in", nodeID)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(out)+len(in))
	all := make([]*EdgeRecord, 0, len(out)+len(in))
	for _, record := range append(out, in...) {
		if record.ID == "" || seen[record.ID] {
			continue
		}
		seen[record.ID] = true
		all = append(all, record)
	}
	return all, nil
}

// NodeHistory returns every closed version of a node, oldest first.
func (g *Graph) NodeHistory(ctx context.Context, nodeID string) ([]RecordVersion, error) {
	var versions []*NodeVersion
	err := g.read(ctx, func(r Reader) error {
		var err error
		versions, err = r.NodeVersionsByNode(ctx, nodeID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return g.nodeVersions(ctx, versions)
}

// AllNodeVersions returns every closed node version in the graph, oldest first:
// the whole-history companion of NodeHistory, for the questions that ask what
// churn a store has seen without naming an id first.
func (g *Graph) AllNodeVersions(ctx context.Context) ([]RecordVersion, error) {
	var versions []*NodeVersion
	err := g.read(ctx, func(r Reader) error {
		var err error
		versions, err = r.NodeVersions(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return g.nodeVersions(ctx, versions)
}

// AllEdgeVersions returns every closed edge version in the graph, oldest first.
func (g *Graph) AllEdgeVersions(ctx context.Context) ([]RecordVersion, error) {
	var versions []*EdgeVersion
	err := g.read(ctx, func(r Reader) error {
		var err error
		versions, err = r.EdgeVersions(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return g.edgeVersions(ctx, versions)
}

// nodeVersions decodes and orders node versions.
func (g *Graph) nodeVersions(ctx context.Context, versions []*NodeVersion) ([]RecordVersion, error) {
	sort.SliceStable(versions, func(i, j int) bool {
		if !versions[i].RecordedAt.Equal(versions[j].RecordedAt) {
			return versions[i].RecordedAt.Before(versions[j].RecordedAt)
		}
		if !versions[i].ValidFrom.Equal(versions[j].ValidFrom) {
			return versions[i].ValidFrom.Before(versions[j].ValidFrom)
		}
		return versions[i].VersionID < versions[j].VersionID
	})
	out := make([]RecordVersion, 0, len(versions))
	for _, version := range versions {
		properties, err := g.values.Decode(version.Properties)
		if err != nil {
			return nil, err
		}
		out = append(out, RecordVersion{
			ID: version.NodeID, Kind: "node", Type: version.NodeType, Content: version.Content,
			Properties: properties, ValidFrom: version.ValidFrom, ValidTo: version.ValidTo,
			RecordedAt: version.RecordedAt, RetractedAt: version.RetractedAt,
			Reason: version.Reason, SupersededBy: version.Superseded,
			Producer: version.Producer, SchemaID: version.SchemaID, SchemaVersion: version.SchemaVersion,
		})
	}
	return out, nil
}

// EdgeHistory returns every closed version of an edge, oldest first.
func (g *Graph) EdgeHistory(ctx context.Context, edgeID string) ([]RecordVersion, error) {
	var versions []*EdgeVersion
	err := g.read(ctx, func(r Reader) error {
		var err error
		versions, err = r.EdgeVersionsByEdge(ctx, edgeID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return g.edgeVersions(ctx, versions)
}

// NodeEdgeHistory returns every closed version of the edges touching a node.
func (g *Graph) NodeEdgeHistory(ctx context.Context, nodeID string) ([]RecordVersion, error) {
	var out []*EdgeVersion
	err := g.read(ctx, func(r Reader) error {
		var err error
		out, err = r.EdgeVersionsTouching(ctx, nodeID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return g.edgeVersions(ctx, out)
}

// edgeVersions decodes and orders edge versions.
func (g *Graph) edgeVersions(ctx context.Context, versions []*EdgeVersion) ([]RecordVersion, error) {
	sort.SliceStable(versions, func(i, j int) bool {
		if !versions[i].RecordedAt.Equal(versions[j].RecordedAt) {
			return versions[i].RecordedAt.Before(versions[j].RecordedAt)
		}
		if !versions[i].ValidFrom.Equal(versions[j].ValidFrom) {
			return versions[i].ValidFrom.Before(versions[j].ValidFrom)
		}
		return versions[i].VersionID < versions[j].VersionID
	})
	out := make([]RecordVersion, 0, len(versions))
	for _, version := range versions {
		properties, err := g.values.Decode(version.Properties)
		if err != nil {
			return nil, err
		}
		out = append(out, RecordVersion{
			ID: version.EdgeID, Kind: "edge", From: version.FromNodeID, To: version.ToNodeID,
			Type: version.EdgeType, Weight: version.Weight, Properties: properties,
			ValidFrom: version.ValidFrom, ValidTo: version.ValidTo,
			RecordedAt: version.RecordedAt, RetractedAt: version.Retracted,
			Reason: version.Reason, SupersededBy: version.Superseded,
			Producer: version.Producer, SchemaID: version.SchemaID, SchemaVersion: version.SchemaVersion,
		})
	}
	return out, nil
}

// EndedEdgeVersions returns the closed edge versions that ended at or after
// since; a zero since means the whole history.
func (g *Graph) EndedEdgeVersions(ctx context.Context, since time.Time) ([]RecordVersion, error) {
	var versions []*EdgeVersion
	err := g.read(ctx, func(r Reader) error {
		var err error
		versions, err = r.EdgeVersions(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	kept := make([]*EdgeVersion, 0, len(versions))
	for _, version := range versions {
		end := version.Retracted
		if end.IsZero() {
			end = version.ValidTo
		}
		if end.IsZero() {
			continue
		}
		if !since.IsZero() && end.Before(since) {
			continue
		}
		kept = append(kept, version)
	}
	return g.edgeVersions(ctx, kept)
}

// Purge removes the versions that ended before a cutoff, or reports what it
// would remove under dryRun. An unset cutoff is refused: it would delete the
// whole history.
func (g *Graph) Purge(ctx context.Context, before time.Time, dryRun bool) (*PurgeReport, error) {
	if before.IsZero() {
		return nil, fmt.Errorf("%w: purge needs a cutoff", ErrInvalidRequest)
	}
	report := &PurgeReport{DryRun: dryRun}
	var nodeVersions []*NodeVersion
	var edgeVersions []*EdgeVersion
	if err := g.read(ctx, func(r Reader) error {
		var err error
		nodeVersions, err = r.NodeVersions(ctx)
		if err != nil {
			return err
		}
		edgeVersions, err = r.EdgeVersions(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	return report, g.write(ctx, func(w Writer) error {
		for _, version := range nodeVersions {
			end := version.RetractedAt
			if end.IsZero() {
				end = version.ValidTo
			}
			if end.IsZero() || !end.Before(before) {
				continue
			}
			report.Nodes++
			if !dryRun {
				if err := w.DeleteNodeVersion(ctx, version.VersionID); err != nil {
					return err
				}
			}
		}
		for _, version := range edgeVersions {
			end := version.Retracted
			if end.IsZero() {
				end = version.ValidTo
			}
			if end.IsZero() || !end.Before(before) {
				continue
			}
			report.Edges++
			if !dryRun {
				if err := w.DeleteEdgeVersion(ctx, version.VersionID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// RetractNodeAt withdraws a live node as of an instant, and every live edge
// touching it with it: an edge left behind would answer a traversal with an
// endpoint that is no longer there. DeleteNode is this at the store clock.
func (g *Graph) RetractNodeAt(ctx context.Context, nodeID string, at time.Time) error {
	if at.IsZero() {
		at = g.now()
	}
	return g.write(ctx, func(w Writer) error {
		record, err := w.Node(ctx, nodeID)
		if err != nil {
			return nodeErr(nodeID, err)
		}
		edges, err := g.liveEdgesTouching(ctx, w, nodeID)
		if err != nil {
			return err
		}
		for _, edge := range edges {
			if err := g.archiveEdge(ctx, w, edge, at.UTC(), at.UTC(), true, InvalidationFrom(ctx)); err != nil {
				return err
			}
		}
		return g.archiveNode(ctx, w, record, at.UTC(), at.UTC(), true, InvalidationFrom(ctx))
	})
}

// RetractEdgeAt withdraws a live edge as of an instant.
func (g *Graph) RetractEdgeAt(ctx context.Context, edgeID string, at time.Time) error {
	if at.IsZero() {
		at = g.now()
	}
	return g.write(ctx, func(w Writer) error {
		record, err := w.Edge(ctx, edgeID)
		if err != nil {
			return edgeErr(edgeID, err)
		}
		return g.archiveEdge(ctx, w, record, at.UTC(), at.UTC(), true, InvalidationFrom(ctx))
	})
}

// EndEdge closes a live edge's validity at an instant without retracting it:
// the version stays readable as of every instant before at, and the record says
// why it ended (InvalidationFrom). It is the correction path behind a
// single-valued link, where the old value was replaced rather than withdrawn.
func (g *Graph) EndEdge(ctx context.Context, edgeID string, at time.Time) error {
	if edgeID == "" {
		return fmt.Errorf("%w: edge id is required", ErrInvalidRequest)
	}
	if at.IsZero() {
		at = g.now()
	}
	return g.write(ctx, func(w Writer) error {
		record, err := w.Edge(ctx, edgeID)
		if err != nil {
			return edgeErr(edgeID, err)
		}
		return g.archiveEdge(ctx, w, record, at.UTC(), g.now(), false, InvalidationFrom(ctx))
	})
}

// RetractNodes withdraws each live node and its edges at the store clock,
// reporting how many live nodes and edges it found.
func (g *Graph) RetractNodes(ctx context.Context, nodeIDs []string) (nodes int, edges int, err error) {
	for _, id := range nodeIDs {
		var out, in []*EdgeRecord
		if err = g.read(ctx, func(r Reader) error {
			var err error
			if out, err = g.edgeRecords(ctx, r, "out", id); err != nil {
				return err
			}
			in, err = g.edgeRecords(ctx, r, "in", id)
			return err
		}); err != nil {
			return nodes, edges, err
		}
		if err = g.RetractNodeAt(ctx, id, g.now()); err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nodes, edges, err
		}
		nodes++
		edges += len(out) + len(in)
	}
	return nodes, edges, nil
}
