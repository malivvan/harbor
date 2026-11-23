package graphs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/malivvan/harbor/graph"
	"github.com/malivvan/harbor/pkg/ledger"
	bolt "go.etcd.io/bbolt"
)

const (
	nodesBucket        = "nodes"
	nodeVersionsBucket = "node_versions"
	edgesBucket        = "edges"
	edgeVersionsBucket = "edge_versions"
)

// graphStorage adapts one named graph to graph.Storage.
type graphStorage struct {
	store *Store
	name  string
}

func (g *graphStorage) Read(ctx context.Context, fn func(graph.Reader) error) error {
	return g.store.view(ctx, func(tx *bolt.Tx) error {
		b, err := g.buckets(tx, false)
		if err != nil {
			return err
		}
		return fn(&reader{store: g.store, b: b, ctx: ctx})
	})
}

func (g *graphStorage) Write(ctx context.Context, fn func(graph.Writer) error) error {
	var entries []ledger.Entry
	err := g.store.update(ctx, func(tx *bolt.Tx) error {
		b, err := g.buckets(tx, true)
		if err != nil {
			return err
		}
		w := &writer{
			reader:  reader{store: g.store, b: b, ctx: ctx},
			entries: &entries,
			graph:   g.name,
		}
		return fn(w)
	})
	if err != nil {
		return err
	}
	if g.store.audit != nil {
		for _, entry := range entries {
			if err := g.store.audit.Record(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

type buckets struct {
	nodes        *bolt.Bucket
	nodeVersions *bolt.Bucket
	edges        *bolt.Bucket
	edgeVersions *bolt.Bucket
}

func (g *graphStorage) buckets(tx *bolt.Tx, create bool) (*buckets, error) {
	root := g.store.rootBucket(tx)
	if root == nil {
		if create {
			var err error
			root, err = g.store.createRootBucket(tx)
			if err != nil {
				return nil, err
			}
		} else {
			return &buckets{}, nil
		}
	}
	var gb *bolt.Bucket
	if create {
		var err error
		gb, err = root.CreateBucketIfNotExists([]byte(g.name))
		if err != nil {
			return nil, err
		}
	} else {
		gb = root.Bucket([]byte(g.name))
	}
	if gb == nil {
		return &buckets{}, nil
	}
	get := func(name string) *bolt.Bucket {
		if create {
			return nil
		}
		return gb.Bucket([]byte(name))
	}
	if create {
		out := &buckets{}
		for name, dst := range map[string]**bolt.Bucket{
			nodesBucket:        &out.nodes,
			nodeVersionsBucket: &out.nodeVersions,
			edgesBucket:        &out.edges,
			edgeVersionsBucket: &out.edgeVersions,
		} {
			b, err := gb.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return nil, err
			}
			*dst = b
		}
		return out, nil
	}
	return &buckets{
		nodes:        get(nodesBucket),
		nodeVersions: get(nodeVersionsBucket),
		edges:        get(edgesBucket),
		edgeVersions: get(edgeVersionsBucket),
	}, nil
}

type reader struct {
	store *Store
	b     *buckets
	ctx   context.Context
}

func (r *reader) Node(ctx context.Context, id string) (*graph.NodeRecord, error) {
	var rec graph.NodeRecord
	if err := r.get(r.b.nodes, id, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (r *reader) Nodes(ctx context.Context) ([]*graph.NodeRecord, error) {
	var out []*graph.NodeRecord
	err := r.list(r.b.nodes, func(raw []byte) error {
		var rec graph.NodeRecord
		if err := r.decode(raw, &rec); err != nil {
			return err
		}
		out = append(out, &rec)
		return nil
	})
	return out, err
}

func (r *reader) NodeVersions(ctx context.Context) ([]*graph.NodeVersion, error) {
	var out []*graph.NodeVersion
	err := r.list(r.b.nodeVersions, func(raw []byte) error {
		var version graph.NodeVersion
		if err := r.decode(raw, &version); err != nil {
			return err
		}
		out = append(out, &version)
		return nil
	})
	return out, err
}

func (r *reader) NodeVersionsByNode(ctx context.Context, nodeID string) ([]*graph.NodeVersion, error) {
	var out []*graph.NodeVersion
	err := r.list(r.b.nodeVersions, func(raw []byte) error {
		var version graph.NodeVersion
		if err := r.decode(raw, &version); err != nil {
			return err
		}
		if version.NodeID == nodeID {
			out = append(out, &version)
		}
		return nil
	})
	return out, err
}

func (r *reader) Edge(ctx context.Context, id string) (*graph.EdgeRecord, error) {
	var rec graph.EdgeRecord
	if err := r.get(r.b.edges, id, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (r *reader) Edges(ctx context.Context) ([]*graph.EdgeRecord, error) {
	var out []*graph.EdgeRecord
	err := r.list(r.b.edges, func(raw []byte) error {
		var rec graph.EdgeRecord
		if err := r.decode(raw, &rec); err != nil {
			return err
		}
		out = append(out, &rec)
		return nil
	})
	return out, err
}

func (r *reader) EdgeVersions(ctx context.Context) ([]*graph.EdgeVersion, error) {
	var out []*graph.EdgeVersion
	err := r.list(r.b.edgeVersions, func(raw []byte) error {
		var version graph.EdgeVersion
		if err := r.decode(raw, &version); err != nil {
			return err
		}
		out = append(out, &version)
		return nil
	})
	return out, err
}

func (r *reader) EdgeVersionsByEdge(ctx context.Context, edgeID string) ([]*graph.EdgeVersion, error) {
	var out []*graph.EdgeVersion
	err := r.list(r.b.edgeVersions, func(raw []byte) error {
		var version graph.EdgeVersion
		if err := r.decode(raw, &version); err != nil {
			return err
		}
		if version.EdgeID == edgeID {
			out = append(out, &version)
		}
		return nil
	})
	return out, err
}

func (r *reader) EdgesTouching(ctx context.Context, nodeID string) ([]*graph.EdgeRecord, error) {
	var out []*graph.EdgeRecord
	err := r.list(r.b.edges, func(raw []byte) error {
		var rec graph.EdgeRecord
		if err := r.decode(raw, &rec); err != nil {
			return err
		}
		if rec.FromNodeID == nodeID || rec.ToNodeID == nodeID {
			out = append(out, &rec)
		}
		return nil
	})
	return out, err
}

func (r *reader) EdgeVersionsTouching(ctx context.Context, nodeID string) ([]*graph.EdgeVersion, error) {
	var out []*graph.EdgeVersion
	err := r.list(r.b.edgeVersions, func(raw []byte) error {
		var version graph.EdgeVersion
		if err := r.decode(raw, &version); err != nil {
			return err
		}
		if version.FromNodeID == nodeID || version.ToNodeID == nodeID {
			out = append(out, &version)
		}
		return nil
	})
	return out, err
}

func (r *reader) get(b *bolt.Bucket, key string, dest any) error {
	if b == nil {
		return graph.ErrRecordNotFound
	}
	raw := b.Get([]byte(key))
	if raw == nil {
		return graph.ErrRecordNotFound
	}
	return r.decode(raw, dest)
}

func (r *reader) list(b *bolt.Bucket, fn func([]byte) error) error {
	if b == nil {
		return nil
	}
	i := 0
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		if i%256 == 0 {
			if err := r.ctx.Err(); err != nil {
				return err
			}
		}
		i++
		return fn(v)
	})
}

func (r *reader) decode(raw []byte, dest any) error {
	plain, err := r.store.openValue(raw)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(plain, dest); err != nil {
		return fmt.Errorf("graphs: decode record: %w", err)
	}
	return nil
}

type writer struct {
	reader
	entries *[]ledger.Entry
	graph   string
}

func (w *writer) PutNode(ctx context.Context, rec *graph.NodeRecord) error {
	return w.put(w.b.nodes, rec.ID, rec, "upsert_node")
}

func (w *writer) DeleteNode(ctx context.Context, id string) error {
	return w.del(w.b.nodes, id, "delete_node")
}

func (w *writer) PutNodeVersion(ctx context.Context, version *graph.NodeVersion) error {
	return w.put(w.b.nodeVersions, version.VersionID, version, "upsert_node_version")
}

func (w *writer) DeleteNodeVersion(ctx context.Context, versionID string) error {
	return w.del(w.b.nodeVersions, versionID, "delete_node_version")
}

func (w *writer) PutEdge(ctx context.Context, rec *graph.EdgeRecord) error {
	return w.put(w.b.edges, rec.ID, rec, "upsert_edge")
}

func (w *writer) DeleteEdge(ctx context.Context, id string) error {
	return w.del(w.b.edges, id, "delete_edge")
}

func (w *writer) PutEdgeVersion(ctx context.Context, version *graph.EdgeVersion) error {
	return w.put(w.b.edgeVersions, version.VersionID, version, "upsert_edge_version")
}

func (w *writer) DeleteEdgeVersion(ctx context.Context, versionID string) error {
	return w.del(w.b.edgeVersions, versionID, "delete_edge_version")
}

func (w *writer) put(b *bolt.Bucket, key string, value any, op string) error {
	if b == nil {
		return graph.ErrRecordNotFound
	}
	plain, err := json.Marshal(value)
	if err != nil {
		return err
	}
	sealed, err := w.store.seal(plain)
	if err != nil {
		return err
	}
	if err := b.Put([]byte(key), sealed); err != nil {
		return err
	}
	if w.entries != nil {
		*w.entries = append(*w.entries, ledger.Entry{
			Store:  ledger.StoreGraphs,
			Op:     op,
			Entity: w.graph + ":" + key,
			Digest: digestOf(plain),
		})
	}
	return nil
}

func (w *writer) del(b *bolt.Bucket, key, op string) error {
	if b == nil {
		return graph.ErrRecordNotFound
	}
	if b.Get([]byte(key)) == nil {
		return graph.ErrRecordNotFound
	}
	if err := b.Delete([]byte(key)); err != nil {
		return err
	}
	if w.entries != nil {
		*w.entries = append(*w.entries, ledger.Entry{
			Store:  ledger.StoreGraphs,
			Op:     op,
			Entity: w.graph + ":" + key,
		})
	}
	return nil
}

// ensure graph.Storage is implemented.
var _ graph.Storage = (*graphStorage)(nil)
