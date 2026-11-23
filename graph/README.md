# graph — a reusable bitemporal property graph

`graph` is the portable core of a property graph over one Harbour `db` store:
nodes and directed edges with typed labels and free-form properties, adjacency
reads, breadth-first traversal, counts, and the bitemporal history that makes an
as-of question answerable. It is deliberately **not** an application model: it
does not know what a node type means, which labels are bookkeeping, or how an
ontology resolves a single-valued link. Those stay with the caller.

```go
records, err := db.Open(ctx, bdb, db.Config{})
g := graph.New(records)

_ = g.UpsertNode(ctx, &graph.Node{
    ID: "leo", Type: "person",
    Properties: map[string]any{"name": "Leo"},
})
_ = g.UpsertNode(ctx, &graph.Node{ID: "beijing", Type: "city"})
_ = g.UpsertEdge(ctx, &graph.Edge{ID: "lives", From: "leo", To: "beijing", Type: "lives_in"})

node, _ := g.GetNode(ctx, "leo")
edges, _ := g.GetEdges(ctx, "leo", "out")

// What did this store believe a year ago?
old, _ := g.GetNode(graph.AsOf(ctx, time.Now().Add(-365*24*time.Hour)), "leo")
```

## What it owns

| Concern | API |
|---|---|
| Nodes and edges | `UpsertNode`, `GetNode`, `DeleteNode`, `UpsertEdge`, `GetEdges`, `DeleteEdge`, `GetNodesBatch`, `GetEdgesBatch` |
| Whole-graph reads | `GetAllNodes`, `GetAllEdges`, `CountNodes`, `NodeTypeCounts`, `EdgeTypeCounts`, `Statistics`, `ExistingNodeIDs` |
| Batches | `UpsertNodesBatch`, `DeleteNodesBatch`, `UpsertEdgesBatch`, `DeleteEdgesBatch`, `ExecuteBatch` (partial success, per-record errors) |
| Traversal | `Neighbors`, `ShortestPath`, `ShortestPathAdmitting`, `Connected` |
| Bitemporal history | `AsOf`, `ReadOptionsFrom`, `NodeHistory`, `EdgeHistory`, `NodeEdgeHistory`, `AllNodeVersions`, `AllEdgeVersions`, `EndedEdgeVersions`, `Diff` |
| Retention and withdrawal | `Purge`, `RetractNodeAt`, `RetractEdgeAt`, `RetractNodes`, `EndEdge` |
| Vocabulary | `NodeLabels`, `EdgeShapes`, `PropertyCounts`, `RecordsWithProperties` |
| Interchange | `ExportDocument`, `ExportJSON`, `ImportDocument`, `ImportJSON` |

Everything a batch can partially fail on reports both a count and the errors;
nothing is silently dropped.

## Storage and compatibility

Records use the names the graph kept in eidos's earlier implementation and pin
their buckets with `HarborBucketName` (`harborNodeRecord`, `harborNodeVersion`,
`harborEdgeRecord`, `harborEdgeVersion`); their field names and order are
identical. Harbour's `db` derives a bucket from the type name, so the pin is what
keeps an existing file readable.

The store's codec encodes records. A record's dynamic properties are encoded by
a `ValueCodec` (`WithValues`); the default is JSON, and a caller whose existing
files use another byte encoding supplies it so an adoption does not reinterpret
stored properties. `Graph` borrows the `db.Store` and never closes it; several
graphs can share one file through `WithPrefix`.

## Time

A live record is one row. A version that ended, was replaced, or was retracted
moves to a history bucket with its interval closed:

- **ValidFrom / ValidTo** — valid time: when the fact was true in the world.
- **RecordedAt** — transaction time: when this store believed it.

An as-of read returns the version whose interval covers the instant **and**
which had been recorded by then. A version learned later is never reported as
having been known earlier. Writing under an as-of context is refused
(`ErrAsOfWrite`): writing "now" while the caller believes it is working at
another instant corrupts the record rather than the answer.

A node delete cascades: its live edges are retracted with it, and so does a
backdated `RetractNodeAt`. Retraction closes a version at an instant the caller
names, so a fact found wrong can be withdrawn as of when it was wrong.

`EndEdge` is the other way a version can end: a correction rather than a
withdrawal. It closes a live edge's interval at an instant without retracting
it, and the history row says why (`InvalidationFrom`), which is what an
application's single-valued link — one current value per subject — writes when a
newer value replaces an older one. Whether a link is single-valued is the
application's rule, not this package's.

## Lifecycle and limits

- `New` borrows the store; the caller closes it. `Init` is a no-op — buckets are
  created by the first write.
- Reads and writes honour `context.Context` cancellation through Harbour.
- `CreatedAt` is the store's unless the caller sets it, which an import does to
  keep the time a record first existed.
- Ids are not generated: a caller names records. Results are returned in id
  order; `Diff` and history are ordered by recorded time, then valid time.
- Upserts, deletes (with their cascade) and archives run in one write
  transaction. Batches are per-record: a failed element does not roll back the
  ones before it.
- `Filter.Limit` bounds a whole-graph read; a traversal bounds by `Limit` and
  `MaxDepth`.

## Not in this package (yet)

The application-facing surfaces of a graph store stay in the application or are
later optional packages: RDF/SPARQL and its projections, rule engines, RDFS/OWL
and SHACL reasoning, Cypher, ontology-driven single-valued supersession,
PageRank and community detection, merge-entities, and GraphML/GEXF export. The
core exposes the reads and writes those need; moving one of them into Harbour is
a separate decision, not a side effect of this package.
