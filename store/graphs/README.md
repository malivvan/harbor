# graphs

`graphs` is Harbor's independent property-graph store. It owns a dedicated root
bucket (`graphs/`), its own record encoding and its own encryption settings, so
graph data does not depend on the record store's buckets or codec. The graph
API itself lives in the `graph` package.

```go
g, err := store.Graphs().Open("social")
if err != nil {
    log.Fatal(err)
}

err = g.UpsertNode(ctx, &graph.Node{ID: "alice", Type: "person"})
err = g.UpsertEdge(ctx, &graph.Edge{ID: "e1", From: "alice", To: "bob", Type: "knows"})
neighbors, err := g.Neighbors(ctx, "alice", graph.TraversalOptions{MaxDepth: 2})
```

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

## Lifecycle

`graphs.Open` borrows a caller-owned `*bolt.DB` and never closes it. The root
`harbor.Store` opens the conventional store under `graphs/` and exposes it as
`Graphs()`. Extra accessor segments select a rooted view
(`store.Graphs("tenant")`). A transaction-bound view is available as
`tx.Graphs()`.

## Migration from the record-backed layout

Earlier graph builds stored records in the record store's buckets
(`harborNodeRecord`, `harborNodeVersion`, `harborEdgeRecord`,
`harborEdgeVersion`). That layout is still readable through the legacy
constructor:

```go
legacy := graph.New(recordStore, graph.WithPrefix("old"))
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
