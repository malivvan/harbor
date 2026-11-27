package graphs

import (
	"context"
	"errors"

	hdb "github.com/malivvan/eidos-db/store/record"
	"github.com/malivvan/eidos-db/store/record/q"
)

// recordStorage is the legacy adapter: it keeps graph records in the
// record-backed layout the graph package used before the independent graphs
// store existed. The bucket names and field order are pinned by the record
// structs' EidosBucketName methods and tags, so existing files stay readable.
type recordStorage struct {
	db     *hdb.Store
	prefix []string
}

func newRecordStorage(db *hdb.Store, prefix []string) *recordStorage {
	return &recordStorage{db: db, prefix: prefix}
}

// scoped names the graph's node under the record store.
func (s *recordStorage) scoped(n hdb.Node) hdb.Node {
	if len(s.prefix) == 0 {
		return n
	}
	return n.From(s.prefix...)
}

// Read runs fn in one record read transaction.
func (s *recordStorage) Read(ctx context.Context, fn func(Reader) error) error {
	return s.db.ReadTransaction(ctx, func(n hdb.Node) error {
		return fn(&recordReader{n: s.scoped(n)})
	})
}

// Write runs fn in one record write transaction.
func (s *recordStorage) Write(ctx context.Context, fn func(Writer) error) error {
	return s.db.WriteTransaction(ctx, func(n hdb.Node) error {
		return fn(&recordWriter{recordReader{n: s.scoped(n)}})
	})
}

type recordReader struct {
	n hdb.Node
}

func notFound(err error) error {
	if errors.Is(err, hdb.ErrNotFound) {
		return ErrRecordNotFound
	}
	return err
}

func ignoreNotFound(err error) error {
	if errors.Is(err, hdb.ErrNotFound) {
		return nil
	}
	return err
}

func (r *recordReader) Node(ctx context.Context, id string) (*NodeRecord, error) {
	var rec NodeRecord
	if err := r.n.One(ctx, "ID", id, &rec); err != nil {
		return nil, notFound(err)
	}
	return &rec, nil
}

func (r *recordReader) Nodes(ctx context.Context) ([]*NodeRecord, error) {
	var recs []*NodeRecord
	if err := r.n.All(ctx, &recs); err != nil {
		return nil, notFound(err)
	}
	return recs, nil
}

func (r *recordReader) NodeVersions(ctx context.Context) ([]*NodeVersion, error) {
	var versions []*NodeVersion
	if err := r.n.All(ctx, &versions); err != nil {
		return nil, notFound(err)
	}
	return versions, nil
}

func (r *recordReader) NodeVersionsByNode(ctx context.Context, nodeID string) ([]*NodeVersion, error) {
	var versions []*NodeVersion
	if err := r.n.Select(q.Eq("NodeID", nodeID)).Find(ctx, &versions); err != nil {
		return nil, ignoreNotFound(err)
	}
	return versions, nil
}

func (r *recordReader) Edge(ctx context.Context, id string) (*EdgeRecord, error) {
	var rec EdgeRecord
	if err := r.n.One(ctx, "ID", id, &rec); err != nil {
		return nil, notFound(err)
	}
	return &rec, nil
}

func (r *recordReader) Edges(ctx context.Context) ([]*EdgeRecord, error) {
	var recs []*EdgeRecord
	if err := r.n.All(ctx, &recs); err != nil {
		return nil, notFound(err)
	}
	return recs, nil
}

func (r *recordReader) EdgeVersions(ctx context.Context) ([]*EdgeVersion, error) {
	var versions []*EdgeVersion
	if err := r.n.All(ctx, &versions); err != nil {
		return nil, notFound(err)
	}
	return versions, nil
}

func (r *recordReader) EdgeVersionsByEdge(ctx context.Context, edgeID string) ([]*EdgeVersion, error) {
	var versions []*EdgeVersion
	if err := r.n.Select(q.Eq("EdgeID", edgeID)).Find(ctx, &versions); err != nil {
		return nil, ignoreNotFound(err)
	}
	return versions, nil
}

func (r *recordReader) EdgesTouching(ctx context.Context, nodeID string) ([]*EdgeRecord, error) {
	var out, in []*EdgeRecord
	if err := r.n.Select(q.Eq("FromNodeID", nodeID)).Find(ctx, &out); err != nil {
		if !errors.Is(err, hdb.ErrNotFound) {
			return nil, err
		}
	}
	if err := r.n.Select(q.Eq("ToNodeID", nodeID)).Find(ctx, &in); err != nil {
		if !errors.Is(err, hdb.ErrNotFound) {
			return nil, err
		}
	}
	seen := make(map[string]bool, len(out)+len(in))
	merged := make([]*EdgeRecord, 0, len(out)+len(in))
	for _, rec := range append(out, in...) {
		if rec.ID == "" || seen[rec.ID] {
			continue
		}
		seen[rec.ID] = true
		merged = append(merged, rec)
	}
	return merged, nil
}

func (r *recordReader) EdgeVersionsTouching(ctx context.Context, nodeID string) ([]*EdgeVersion, error) {
	var out, in []*EdgeVersion
	if err := r.n.Select(q.Eq("FromNodeID", nodeID)).Find(ctx, &out); err != nil {
		if !errors.Is(err, hdb.ErrNotFound) {
			return nil, err
		}
	}
	if err := r.n.Select(q.Eq("ToNodeID", nodeID)).Find(ctx, &in); err != nil {
		if !errors.Is(err, hdb.ErrNotFound) {
			return nil, err
		}
	}
	seen := make(map[string]bool, len(out)+len(in))
	merged := make([]*EdgeVersion, 0, len(out)+len(in))
	for _, version := range append(out, in...) {
		if version.VersionID == "" || seen[version.VersionID] {
			continue
		}
		seen[version.VersionID] = true
		merged = append(merged, version)
	}
	return merged, nil
}

type recordWriter struct {
	recordReader
}

func (w *recordWriter) PutNode(ctx context.Context, rec *NodeRecord) error {
	return w.n.Save(ctx, rec)
}

func (w *recordWriter) DeleteNode(ctx context.Context, id string) error {
	return notFound(w.n.DeleteStruct(ctx, &NodeRecord{ID: id}))
}

func (w *recordWriter) PutNodeVersion(ctx context.Context, version *NodeVersion) error {
	return w.n.Save(ctx, version)
}

func (w *recordWriter) DeleteNodeVersion(ctx context.Context, versionID string) error {
	return notFound(w.n.DeleteStruct(ctx, &NodeVersion{VersionID: versionID}))
}

func (w *recordWriter) PutEdge(ctx context.Context, rec *EdgeRecord) error {
	return w.n.Save(ctx, rec)
}

func (w *recordWriter) DeleteEdge(ctx context.Context, id string) error {
	return notFound(w.n.DeleteStruct(ctx, &EdgeRecord{ID: id}))
}

func (w *recordWriter) PutEdgeVersion(ctx context.Context, version *EdgeVersion) error {
	return w.n.Save(ctx, version)
}

func (w *recordWriter) DeleteEdgeVersion(ctx context.Context, versionID string) error {
	return notFound(w.n.DeleteStruct(ctx, &EdgeVersion{VersionID: versionID}))
}
