# graphs

`graphs` is both Harbor's bitemporal property-graph store and the reusable graph
core that operates on it. A `Store` owns a dedicated root bucket (`graphs/`), its
own record encoding and its own encryption settings, so graph data does not
depend on the record store's buckets or codec. The core — nodes, directed edges,
adjacency, traversal and the bitemporal history — is the `Graph` type, and it can
run over a `Store` (the default) or over a `record.Store` (the pre-existing
record-backed layout).

```go
g, err := store.Graphs().Open("social")
if err != nil {
    log.Fatal(err)
}

err = g.UpsertNode(ctx, &graphs.Node{ID: "alice", Type: "person"})
err = g.UpsertEdge(ctx, &graphs.Edge{ID: "e1", From: "alice", To: "bob", Type: "knows"})
neighbors, err := g.Neighbors(ctx, "alice", graphs.TraversalOptions{MaxDepth: 2})
```

The core over a record store, which is what the legacy layout uses:

```go
records, err := record.Open(ctx, bdb, record.Config{})
g := graphs.New(records)

_ = g.UpsertNode(ctx, &graphs.Node{
    ID: "leo", Type: "person",
    Properties: map[string]any{"name": "Leo"},
})
_ = g.UpsertNode(ctx, &graphs.Node{ID: "beijing", Type: "city"})
_ = g.UpsertEdge(ctx, &graphs.Edge{ID: "lives", From: "leo", To: "beijing", Type: "lives_in"})

node, _ := g.GetNode(ctx, "leo")
edges, _ := g.GetEdges(ctx, "leo", "out")

// What did this store believe a year ago?
old, _ := g.GetNode(graphs.AsOf(ctx, time.Now().Add(-365*24*time.Hour)), "leo")
```

## What the core owns

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

The core is deliberately **not** an application model: it does not know what a
node type means, which labels are bookkeeping, or how an ontology resolves a
single-valued link. Those stay with the caller. Everything a batch can partially
fail on reports both a count and the errors; nothing is silently dropped.

## Storage

Each named graph has its own bucket under the configured root:

```text
<graph>/
  nodes/<id>                    live node record
  node_versions/<version id>    closed node version
  edges/<id>                    live edge record
  edge_versions/<version id>    closed edge version
```

Records are JSON and are sealed when `Config.EncryptValues` is enabled. Keys
stay readable. The store writes its own format version, encryption-mode marker
and canary; an unknown version or wrong mode fails the open.

The record-backed layout keeps the record names of eidos's earlier graph
implementation and pins their buckets with `HarborBucketName`
(`harborNodeRecord`, `harborNodeVersion`, `harborEdgeRecord`,
`harborEdgeVersion`); their field names and order are identical. Harbor's
`record` derives a bucket from the type name, so the pin is what keeps an
existing file readable.

A record's dynamic properties are encoded by a `ValueCodec` (`WithValues`); the
default is JSON, and a caller whose existing files use another byte encoding
supplies it so an adoption does not reinterpret stored properties. `Graph`
borrows the store it is built on and never closes it; several graphs can share
one file through `WithPrefix`.

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

## Lifecycle

`graphs.Open` borrows a caller-owned `*bolt.DB` and never closes it. The root
`harbor.Store` opens the conventional store under `graphs/` and exposes it as
`Graphs()`. Extra accessor segments select a rooted view
(`store.Graphs("tenant")`). A transaction-bound view is available as
`tx.Graphs()`.

`New` borrows the store it wraps; the caller closes it. `Init` is a no-op —
buckets are created by the first write. Reads and writes honour
`context.Context` cancellation through Harbor. `CreatedAt` is the store's unless
the caller sets it, which an import does to keep the time a record first existed.
Ids are not generated: a caller names records. Results are returned in id order;
`Diff` and history are ordered by recorded time, then valid time. Upserts,
deletes (with their cascade) and archives run in one write transaction. Batches
are per-record: a failed element does not roll back the ones before it.
`Filter.Limit` bounds a whole-graph read; a traversal bounds by `Limit` and
`MaxDepth`.

## Migration from the record-backed layout

Earlier graph builds stored records in the record store's buckets
(`harborNodeRecord`, `harborNodeVersion`, `harborEdgeRecord`,
`harborEdgeVersion`). That layout is still readable through the legacy
constructor:

```go
legacy := graphs.New(recordStore, graphs.WithPrefix("old"))
```

It is never reinterpreted in place. To move a graph, including its closed
version history, into the independent store:

```go
err := store.Graphs().MigrateFromRecord(ctx, "social", recordStore, "old")
```

`MigrateFromRecord` copies live records and every closed version in one write
transaction and is idempotent. The legacy data is left untouched.

## Encryption, audit and backups

- Value encryption follows `Config.EncryptValues`; the root store enables it
  with `EncryptGraphValues` (or the db value flag for stores created before
  that flag existed).
- `Config.Audit` receives one event per committed graph record write under the
  `graphs` store identifier.
- `VerifyBackup` validates the graph store's version and mode, and lists every
  graph in the copy. `BackupFile` records the graph format version in its
  manifest.

## Not in this package (yet)

The application-facing surfaces of a graph store stay in the application or are
later optional packages: RDF/SPARQL and its projections, rule engines, RDFS/OWL
and SHACL reasoning, Cypher, ontology-driven single-valued supersession,
PageRank and community detection, merge-entities, and GraphML/GEXF export. The
core exposes the reads and writes those need; moving one of them into Harbor is
a separate decision, not a side effect of this package.
