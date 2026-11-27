# PLAN — Close implementation gaps in tempest

This document is a complete, executable implementation plan. It is written for a
coding agent (e.g. Reasonix + deepseek-flash) to follow step by step. Every step
names the file, the function/signature, and the test that proves it. Verify with
the commands in [Verification](#verification) before marking any phase done.

Scope: close the eight gaps below without changing the public on-disk formats
(invariants in `AGENTS.md` still hold). No format-version bump is required for
any of this work; where a wire/container format is extended (export), the change
is additive and old readers already fail closed on the new record kinds.

---

## Table of contents

1. [Gap summary and priority order](#gap-summary-and-priority-order)
2. [Decisions and invariants](#decisions-and-invariants)
3. [Phase 0 — shared prerequisites](#phase-0--shared-prerequisites)
4. [Phase 1 — database-backup restore API](#phase-1--database-backup-restore-api) *(most urgent)*
5. [Phase 2 — complete logical export/import](#phase-2--complete-logical-exportimport) *(most urgent)*
6. [Phase 3 — cluster coverage for search/graphs/objects](#phase-3--cluster-coverage-for-searchgraphsobjects) *(most urgent)*
7. [Phase 4 — vector cluster writes refresh in-memory indexes](#phase-4--vector-cluster-writes-refresh-in-memory-indexes)
8. [Phase 5 — search audit flag and implementation](#phase-5--search-audit-flag-and-implementation)
9. [Phase 6 — search key rotation and VerifyAll](#phase-6--search-key-rotation-and-verifyall)
10. [Phase 7 — encryption/rotation consolidation (graphs, object)](#phase-7--encryptionrotation-consolidation-graphs-object)
11. [Phase 8 — audit/ledger consolidation](#phase-8--auditledger-consolidation)
12. [Documentation updates](#documentation-updates)
13. [Verification](#verification)
14. [Risks, trade-offs, open decisions](#risks-trade-offs-open-decisions)

---

## Gap summary and priority order

| # | Gap | Urgency | Phase |
|---|-----|---------|-------|
| 2 | No database-backup restore API | urgent | 1 |
| 1 | Logical export/import omits search, graphs, objects | urgent | 2 |
| 3 | Cluster omits search, graphs, objects | urgent | 3 |
| 6 | Vector cluster writes do not refresh in-memory indexes | high | 4 |
| 4 | AuditAll omits search; no search audit implementation | medium | 5 |
| 5 | Search lacks rotation / VerifyAll | medium | 6 |
| 7 | Encryption (incl. Rotate) not consolidated (graphs, object) | medium | 7 |
| 8 | Audit/Ledger not consolidated (search) | medium | 8 |

Phases 1–3 are the most urgent and are independent of each other. Phases 4–8
build on the shared prerequisites (Phase 0) and can be done in the order below.

---

## Decisions and invariants

These decisions are made up front so the implementing agent does not re-derive
them. Where a choice is genuinely two-sided it is marked **[DECISION]** with the
recommended default stated; otherwise the default is safe and reversible.

1. **Export container version stays `1`.** Adding `fts.*`, `graph.*`, `object.*`
   record kinds is additive; the NDJSON framing, header, footer and checksum are
   unchanged. A build that does not know the new kinds already returns
   `ErrExportFormat` ("unknown record kind"), i.e. it fails closed. No
   `exportVersion` bump. If strict signalling is ever wanted, bump to `2` later.
2. **Graphs export is live-only.** `ExportDocument`/`ImportDocument` carry live
   nodes and edges, not the closed bitemporal version history. History
   migration between layouts stays available in-process via
   `graphs.Copy` / `Store.MigrateFromRecord`. Cross-encryption logical migration
   of graphs is therefore live-only, and this limitation is documented in
   `store/graphs/README.md` and `README.md`. *(This is the least-surprise
   default; exporting history would require a new public history-interchange
   type that does not exist yet.)*
3. **Search export is postings-level, not source-text.** The search index never
   stores the original document text — only term frequencies, postings and
   corpus stats. Export therefore carries `{docs (id,len,tf)}, {terms (term,
   docs)}, {meta (k1,b,tokenizer,docCount,totalLen)}`. Import reconstructs the
   index byte-equivalently under the destination's own encryption. Search
   behavior (BM25 ranking, `Count`, `Search`) is preserved exactly; the original
   free text is not recoverable (it was never stored). Document this in
   `store/search/README.md`.
4. **Search tokenizer on import:** built-in tokenizers (`words`, `words+cjk`)
   round-trip by name. A `custom` tokenizer cannot be reconstructed from a name;
   import of such an index fails with a clear error unless the caller supplies a
   matching `Tokenizer` via a new import option (see Phase 2). **[DECISION]**
   Default: fail closed with a clear error for `custom`; do not silently drop
   or approximate the tokenizer.
5. **RestoreBackup is package-level, not a `*Store` method.** A backup is a
   byte-identical encrypted image of the whole database with the *same*
   password; it cannot be restored over a store that is currently open (bbolt
   holds the file lock). Therefore `RestoreBackup` is a standalone function that
   verifies `src` and atomically writes `dst`, and the caller must not point
   `dst` at a live store. Restoring between encryption modes is NOT possible via
   backup — that is Export/Import's job (see `README.md`).
6. **No root-level rotation entry point.** `AGENTS.md` documents that the root
   package derives keys from the password and does not expose rotation. Keep
   that contract. Rotation consolidation in Phases 6–7 is entirely at the
   sub-store level (`search`, `graphs`, `object`), matching `record`, `fstree`,
   `series`, `vector`.
7. **New audit/encryption bits are appended, never renumbered.** `AuditSearch`
   is appended after `AuditObject` (value `1 << 6`), and its SLIP-10 path is
   `m/6'`. Audit flags are a per-open runtime choice (not persisted), so adding
   a bit changes no on-disk data.
8. **Vector cluster fix = index + durable write via the collection handle.** The
   follower must update the *live* in-memory HNSW/Flat index for replicated
   upserts/deletes. The recommended approach reuses `Collection.Upsert` /
   `Collection.Delete` (Phase 4); the durability nuance is flagged there.

---

## Phase 0 — shared prerequisites

These are additive public APIs that later phases depend on. Implement them all
first, with focused unit tests in the owning packages.

### 0.1 `object.Store.Buckets` (and `DropBucket`)

File: `store/object/object.go`

```go
// Buckets lists the object bucket names under the store's root, sorted.
// The store's own metadata keys (any name starting "__") are skipped.
func (s *Store) Buckets(ctx context.Context) ([]string, error)

// DropBucket removes an entire object bucket. A missing bucket is a no-op.
// It audits one "drop_bucket" entry when auditing is enabled.
func (s *Store) DropBucket(ctx context.Context, bucket string) error
```

- Implement `Buckets` with `s.view` over `s.rootBucket(tx)`, collecting keys
  where `v == nil && !strings.HasPrefix(string(k), "__")`, then `sort.Strings`.
- Implement `DropBucket` with `s.update`, deleting the bucket via the root
  bucket; audit via `s.replay([]ledger.Entry{{Store: ledger.StoreObject, Op:
  "drop_bucket", Entity: bucket}})`.
- Tests: `TestObjectBuckets`, `TestObjectDropBucket` in `object_test.go`.

### 0.2 `graphs.Store.Drop`

File: `store/graphs/graphs.go`

```go
// Drop removes a named graph and all its buckets. A missing graph is a no-op.
func (s *Store) Drop(ctx context.Context, name string) error
```

- Implement with `s.update`, delete the graph bucket under the root. Audit is
  optional (mirror `object` if a sink exists).
- Test: `TestGraphsDrop` in `graphs_test.go`.

### 0.3 `search.Store` export/restore primitives

File: `store/search/search.go` (types) and `store/search/index.go` (methods).

Add to `store/search/index.go`:

```go
// DocEntry is one indexed document's logical record.
type DocEntry struct {
    ID     string         `json:"id"`
    Length int            `json:"len"`
    TF     map[string]int `json:"tf"`
}

// TermEntry is one posting list.
type TermEntry struct {
    Term string   `json:"term"`
    Docs []string `json:"docs"`
}

// IndexMeta is the index configuration plus corpus statistics.
type IndexMeta struct {
    K1        float64 `json:"k1"`
    B         float64 `json:"b"`
    Tokenizer string  `json:"tokenizer"`
    DocCount  int64   `json:"docs"`
    TotalLen  int64   `json:"total_len"`
}
```

Methods on `*Index` (all read under `s.view`):

```go
// Meta returns the persisted index metadata.
func (idx *Index) Meta(ctx context.Context) (IndexMeta, error)

// Docs returns every indexed document, sorted by id.
func (idx *Index) Docs(ctx context.Context) ([]DocEntry, error)

// Terms returns every posting list, sorted by term.
func (idx *Index) Terms(ctx context.Context) ([]TermEntry, error)
```

Methods on `*Store`:

```go
// RestoreIndex creates (or replaces) the named index from a logical snapshot in
// ONE write transaction: version/mode records, metadata, documents and posting
// lists. cfg supplies the tokenizer when meta.Tokenizer is "custom"; K1/B in
// meta always win. It writes everything under the store's current encryption.
func (s *Store) RestoreIndex(ctx context.Context, name string, cfg Config, meta IndexMeta, docs []DocEntry, terms []TermEntry) error

// Drop removes a named index and all its data. A missing index is a no-op.
func (s *Store) Drop(ctx context.Context, name string) error
```

Implementation notes for `RestoreIndex`:
- Resolve the tokenizer: `tokenizerName(cfg.Tokenizer)`; if `meta.Tokenizer ==
  "custom"` require the caller to pass a non-nil `cfg.Tokenizer`, otherwise
  return `ErrInvalidConfig`; if `meta.Tokenizer` is `words`/`words+cjk` select
  the built-in and reject a conflicting non-nil custom tokenizer (same rule as
  `Open.adoptMeta`).
- In one `s.update`: create the index bucket + `docs`/`terms` sub-buckets; write
  `versionKey` and `modeKey` exactly as `Open` does; write `metaKey` via
  `saveMeta`; write each `DocEntry` via `saveDoc`; write each `TermEntry` via
  `saveTerm` (which sorts `Docs` and handles SIV term keys).
- `Drop`: in one `s.update`, delete the index bucket; reuse the same root
  traversal as `Open`.

Tests: `TestSearchMetaDocsTerms`, `TestSearchRestoreIndex`, `TestSearchDrop` in
`search_test.go` (including an encrypted-store round trip).

### 0.4 `vector.Store.Collection` (needed by Phase 4)

File: `store/vector/store.go`

```go
// Collection returns the live, in-memory collection for name, loading it from
// its persisted CollectionMeta and points if it is not already tracked. The
// returned *Collection is the one the store keeps for search/flush.
func (s *Store) Collection(name string) (*Collection, error)
```

- If `s.collections[name]` exists (under `s.mu`), return it.
- Otherwise `LoadCollectionMeta(name)`; on `ErrMetadataNotFound` return
  `ErrCollectionNotFound`; build `CollectionOptions{Name, VectorLen, Metric,
  UseHNSW, HNSWParams}` from the meta and call `NewCollectionWithOptions`
  (which loads points into the index and calls `trackCollection`).
- Test: `TestCollectionReload` in `store_test.go`.

---

## Phase 1 — database-backup restore API

Goal: add `RestoreBackup` so a verified backup can be atomically restored to a
path, with manifest/checksum guarantees.

File: `backup.go` (package `tempest`).

### 1.1 Add `RestoreOptions`

```go
// RestoreOptions tunes RestoreBackup.
type RestoreOptions struct {
    // Overwrite replaces an existing file at dst. When false, an existing dst
    // fails with os.ErrExist.
    Overwrite bool
}
```

### 1.2 Add `RestoreBackup`

```go
// RestoreBackup verifies src (a backup written by BackupFile/BackupTo) with
// VerifyBackup and atomically restores it to dst: the bytes go to a temporary
// file in dst's directory, are synced and closed, and only then replace dst by
// rename. dst must not be the path of a store that is currently open.
//
// The backup is the encrypted on-disk image, so the restore is byte-identical
// and opens with the same password and flags. Cross-mode migration uses
// Export/Import, not RestoreBackup.
func RestoreBackup(ctx context.Context, src, dst, password string, opts *Options, rest RestoreOptions) (BackupInfo, error)
```

Implementation:
1. Validate args: `src`, `dst`, `password` non-empty; `ctx` nil → Background;
   check `ctx.Err()`.
2. `info, err := VerifyBackup(ctx, src, password, opts)` — proves the password,
   flags, every sub-store marker, and the manifest/checksum before touching
   `dst`.
3. If `!rest.Overwrite`, `os.Stat(dst)` → `os.ErrExist` on success (mirror
   `BackupFile`).
4. `os.CreateTemp(filepath.Dir(dst), ".tempest-restore-*")`; `tmp.Sync()`;
   `tmp.Close()`; `os.Rename(tmpPath, dst)`; on any error remove the temp file.
   Copy with `io.Copy` through `ctxWriter{ctx, tmp}` (reuse the existing
   `ctxWriter` in `backup.go`) so cancellation aborts the copy.
5. Hash the stream while copying (reuse `countingWriter` + `sha256`), and return
   `BackupInfo{Path: dst, Size, SHA256, CreatedAt: time.Now().UTC(), Flags:
   info.Flags, Version: record.Version}`.
6. Do **not** rewrite the manifest sidecar (the backup's sidecar describes the
   source bytes; if desired it can be copied as an optional follow-up — not
   required here).

### 1.3 Tests

File: `backup_test.go`
- `TestRestoreBackupRoundTrip`: backup a store, restore to a fresh path, `Open`
  it with the same password, and assert all sub-store data is present.
- `TestRestoreBackupWrongPassword`: fails with `ErrInvalidPassword`.
- `TestRestoreBackupCorruptSrc`: flip a byte in the source; fails with
  `ErrInvalidBackup`/checksum error.
- `TestRestoreBackupOverwrite`: existing `dst` fails without `Overwrite`, and
  succeeds with it.
- `TestRestoreBackupCanceledContext`: canceled context aborts and leaves no
  partial `dst`.

---

## Phase 2 — complete logical export/import

Goal: extend `Export`/`Import` to cover search, graphs, and objects, so logical
encryption migration works for every store.

### 2.1 New record kinds

File: `export.go` — extend the `const` block:

```go
exportKindSearchIndex   = "fts.index"
exportKindSearchDocs    = "fts.docs"
exportKindSearchTerms   = "fts.terms"
exportKindGraphDocument = "graph.document"
exportKindObject        = "object.put"
```

Extend the `exportRecord` wire struct with fields used by the new kinds:

```go
Name      string             `json:"name,omitempty"`
Bucket    string             `json:"bucket,omitempty"`
Key       string             `json:"key,omitempty"`
ETag      string             `json:"etag,omitempty"`
K1        float64            `json:"k1,omitempty"`
B         float64            `json:"b,omitempty"`
Tokenizer string             `json:"tokenizer,omitempty"`
DocCount  int64              `json:"doc_count,omitempty"`
TotalLen  int64              `json:"total_len,omitempty"`
Docs      []search.DocEntry  `json:"docs,omitempty"`
Terms     []search.TermEntry `json:"terms,omitempty"`
Nodes     []*graphs.Node     `json:"nodes,omitempty"`
Edges     []*graphs.Edge     `json:"edges,omitempty"`
```

Add imports `github.com/malivvan/tempest/store/graphs` and
`github.com/malivvan/tempest/store/object` and
`github.com/malivvan/tempest/store/search`.

### 2.2 Exporters

Add to the `[]func() error` in `Export` (the loop currently `e.exportDB,
e.exportFS, e.exportTS, e.exportVec`):

```go
for _, export := range []func() error{
    e.exportDB, e.exportFS, e.exportTS, e.exportVec,
    e.exportSearch, e.exportGraphs, e.exportObjects,
} { ... }
```

Implement in `export.go`:

```go
func (e *exporter) exportSearch() error
```

- `e.src.Search().Indexes(e.ctx)`; sort names.
- For each name, `idx, err := e.src.Search().Open(name, search.Config{})`.
- `meta, _ := idx.Meta(e.ctx)`; write `exportKindSearchIndex` with
  `Name`, `K1`, `B`, `Tokenizer`, `DocCount`, `TotalLen`.
- `docs, _ := idx.Docs(e.ctx)`; write in batches of `exportBatch` as
  `exportKindSearchDocs` (`Name`, `Docs`).
- `terms, _ := idx.Terms(e.ctx)`; write in batches of `exportBatch` as
  `exportKindSearchTerms` (`Name`, `Terms`).
- Order: `fts.index` before `fts.docs`/`fts.terms` so import creates the index
  first.

```go
func (e *exporter) exportGraphs() error
```

- `e.src.Graphs().Names(e.ctx)`; sort names.
- For each name, `g, err := e.src.Graphs().Open(name)`.
- `doc, err := g.ExportDocument(e.ctx)`; write `exportKindGraphDocument` with
  `Name`, `Nodes`, `Edges` (the live graph).

```go
func (e *exporter) exportObjects() error
```

- `e.src.Objects().Buckets(e.ctx)`; sort.
- For each bucket, `infos, _ := e.src.Objects().List(e.ctx, bucket, "")`.
- For each `Info`, `data, _ := e.src.Objects().ReadAll(e.ctx, bucket, info.Key)`;
  write `exportKindObject` with `Bucket`, `Key`, `Data`, `ETag: info.ETag`.

### 2.3 Importers

File: `import.go`.

Extend the `importer` struct with a search/graphs object registry if needed
(maps are optional; `Open`/`RestoreIndex` are idempotent per record):

```go
importOptions Tokenizer // see ImportOptions below (search custom tokenizer)
```

Extend `ImportOptions`:

```go
// SearchTokenizer, when non-nil, is used to import an index whose persisted
// tokenizer is "custom". It is ignored for built-in tokenizers.
SearchTokenizer search.Tokenizer
```

Extend `apply` switch with the new cases:

```go
case exportKindSearchIndex:   return im.applySearchIndex(rec)
case exportKindSearchDocs:    return im.applySearchDocs(rec)
case exportKindSearchTerms:   return im.applySearchTerms(rec)
case exportKindGraphDocument: return im.applyGraphDocument(rec)
case exportKindObject:        return im.applyObject(rec)
```

Implementations:

- `applySearchIndex`: validate `Name`; in dry-run return nil; otherwise
  accumulate `Name → IndexMeta` in a new `im.indexMeta map[string]search.IndexMeta`
  (add the field) and return nil. The index is materialized in `finish` (below)
  because docs/terms arrive after the meta line.
- `applySearchDocs`: validate `Name` + non-empty `Docs`; dry-run → nil; append
  to `im.indexDocs map[string][]search.DocEntry` (new field).
- `applySearchTerms`: validate `Name` + non-empty `Terms`; dry-run → nil; append
  to `im.indexTerms map[string][]search.TermEntry` (new field).
- `applyGraphDocument`: validate `Name`, `Nodes`/`Edges` non-nil; dry-run →
  nil; `g, err := im.dest.Graphs().Open(rec.Name)`; `res, err :=
  g.ImportDocument(im.ctx, &graphs.Document{Nodes: rec.Nodes, Edges:
  rec.Edges})`; if `res.FailedCount > 0` return a descriptive error (strict
  import).
- `applyObject`: validate `Bucket`, `Key`, `Data`; dry-run → nil; `_, err :=
  im.dest.Objects().Put(im.ctx, rec.Bucket, rec.Key, bytes.NewReader(rec.Data),
  int64(len(rec.Data)), "")`; optionally verify the recomputed ETag against
  `rec.ETag` (read it back with `Stat`; mismatch → error). **[DECISION]**
  Default: skip the ETag re-check to keep import O(1) per object; the container
  checksum already guarantees integrity. Document this.

Extend `finish`:

```go
func (im *importer) finish() error {
    // existing vec Flush loop ...
    for name, meta := range im.indexMeta {
        if err := im.dest.Search().RestoreIndex(im.ctx, name, search.Config{
            Tokenizer: im.opts.SearchTokenizer,
        }, meta, im.indexDocs[name], im.indexTerms[name]); err != nil {
            return fmt.Errorf("tempest: import search %q: %w", name, err)
        }
    }
    return nil
}
```

Add the new fields (`indexMeta`, `indexDocs`, `indexTerms`) to `importer`.

### 2.4 Tests

File: `export_test.go` (and/or `import_test.go`):
- `TestExportImportSearch`: build an index (with default and CJK tokenizers),
  export, import into a fresh store with a *different* encryption mode, assert
  `Search`/`Count` parity.
- `TestExportImportGraphs`: build a graph (nodes + edges), export, import,
  assert `Statistics`/`GetAllNodes`/`GetAllEdges` parity. Assert closed-version
  history is *not* carried (documents the live-only decision).
- `TestExportImportObjects`: put objects across buckets, export, import, assert
  `List`/`ReadAll` parity and ETag equality.
- `TestExportImportAllStores`: one store with every kind, plaintext → encrypted
  and encrypted → plaintext, full parity.
- `TestImportSearchCustomTokenizerFails`: an index with `custom` tokenizer and
  no `SearchTokenizer` fails cleanly.

---

## Phase 3 — cluster coverage for search/graphs/objects

Goal: replicated write facades + FSM dispatch for the three missing stores.

### 3.1 Command ops

File: `cluster/command.go` — append:

```go
// search store
opSearchCreate = "search.create"
opSearchUpsert = "search.upsert"
opSearchDelete = "search.delete"
opSearchDrop   = "search.drop"

// graphs store
opGraphCreate     = "graph.create"
opGraphDrop       = "graph.drop"
opGraphUpsertNode = "graph.upsert_node"
opGraphDeleteNode = "graph.delete_node"
opGraphUpsertEdge = "graph.upsert_edge"
opGraphDeleteEdge = "graph.delete_edge"

// object store
opObjectPut    = "object.put"
opObjectDelete = "object.delete"
opObjectMove   = "object.move"
opObjectDrop   = "object.drop"
```

### 3.2 New facade files

#### `cluster/search.go`

```go
type Search struct{ n *Node }

type searchCreateArg struct {
    Name      string  `json:"name"`
    K1        float64 `json:"k1"`
    B         float64 `json:"b"`
    Tokenizer string  `json:"tokenizer"`
}
type searchUpsertArg struct {
    Name string `json:"name"`
    ID   string `json:"id"`
    Text string `json:"text"`
}
type searchDeleteArg struct {
    Name string   `json:"name"`
    IDs  []string `json:"ids"`
}
type searchDropArg struct{ Name string `json:"name"` }

func (n *Node) Search() *Search
func (s *Search) Create(name string, cfg search.Config) error   // opSearchCreate
func (s *Search) Upsert(name, id, text string) error            // opSearchUpsert
func (s *Search) Delete(name string, ids ...string) error       // opSearchDelete
func (s *Search) Drop(name string) error                        // opSearchDrop
```

- `Create` encodes `{Name, K1, B, Tokenizer: tokenizerName(cfg.Tokenizer)}`
  (export the unexported `tokenizerName` via a small helper or pass the name
  string; the FSM resolves the built-in by name). The FSM apply runs
  `st.Search().Open(name, search.Config{K1, B, Tokenizer: <resolved>})`.

#### `cluster/graphs.go`

```go
type Graphs struct{ n *Node }

type graphCreateArg struct{ Name string `json:"name"` }
type graphDropArg   struct{ Name string `json:"name"` }
type graphNodeArg   struct {
    Name string     `json:"name"`
    Node *graphs.Node `json:"node"`
}
type graphNodeIDArg struct {
    Name string `json:"name"`
    ID   string `json:"id"`
}
type graphEdgeArg struct {
    Name string     `json:"name"`
    Edge *graphs.Edge `json:"edge"`
}
type graphEdgeIDArg struct {
    Name string `json:"name"`
    ID   string `json:"id"`
}

func (n *Node) Graphs() *Graphs
func (g *Graphs) Create(name string) error                       // opGraphCreate
func (g *Graphs) Drop(name string) error                         // opGraphDrop
func (g *Graphs) UpsertNode(name string, node *graphs.Node) error // opGraphUpsertNode
func (g *Graphs) DeleteNode(name, id string) error               // opGraphDeleteNode
func (g *Graphs) UpsertEdge(name string, edge *graphs.Edge) error // opGraphUpsertEdge
func (g *Graphs) DeleteEdge(name, id string) error               // opGraphDeleteEdge
```

- FSM apply: `st.Graphs().Open(name)` then `UpsertNode`/`DeleteNode`/
  `UpsertEdge`/`DeleteEdge`; `Create` = `Open(name)` (idempotent); `Drop` =
  `Store.Drop(name)`.

#### `cluster/object.go`

```go
type Objects struct{ n *Node }

type objectPutArg struct {
    Bucket string `json:"bucket"`
    Key    string `json:"key"`
    Data   []byte `json:"data"`
}
type objectKeyArg struct {
    Bucket string `json:"bucket"`
    Key    string `json:"key"`
}
type objectMoveArg struct {
    Bucket string `json:"bucket"`
    Src    string `json:"src"`
    Dst    string `json:"dst"`
}
type objectBucketArg struct{ Bucket string `json:"bucket"` }

func (n *Node) Objects() *Objects
func (o *Objects) Put(bucket, key string, data []byte) error      // opObjectPut
func (o *Objects) Delete(bucket, key string) error                // opObjectDelete
func (o *Objects) Move(bucket, src, dst string) error             // opObjectMove
func (o *Objects) DropBucket(bucket string) error                 // opObjectDrop
```

- FSM apply: `st.Objects().Put(ctx, bucket, key, bytes.NewReader(data), len,
  "")`, `Delete`, `Move`, `DropBucket`.

### 3.3 FSM dispatch

File: `cluster/fsm.go` — add cases in `applyCommand` for every new op, decoding
the corresponding arg struct and calling the apply helper on
`f.node.storeHandle()` (`st.Search()`, `st.Graphs()`, `st.Objects()`).

Add the `applySearch*`, `applyGraph*`, `applyObject*` helpers in the respective
facade files (mirror `applyVector*` in `vector.go`).

### 3.4 Tests

File: `cluster/replication_test.go` / `coverage_facades_test.go`:
- `TestClusterSearchReplication`: leader `Search().Create` + `Upsert`, wait for
  follower `Barrier`, assert follower `Store().Search()` returns the same hits.
- `TestClusterGraphsReplication`: nodes + edges converge on the follower.
- `TestClusterObjectsReplication`: put/delete/move/drop converge.
- `TestClusterFacadeNotLeader`: off-leader facade calls return
  `cluster.ErrNotLeader`.

### 3.5 Update `cluster/README.md`

- Add search/graphs/object blocks to "Replicated writes".
- Note the new ops.

---

## Phase 4 — vector cluster writes refresh in-memory indexes

Goal: after a replicated vector write, the live HNSW/Flat index on every node
(leader and follower) reflects the change — no reload required.

### 4.1 Change the FSM apply functions

File: `cluster/vector.go` — replace the body of the apply helpers:

```go
func (f *fsm) applyVectorUpsert(ctx context.Context, a vectorUpsertArg) any {
    st := f.node.storeHandle()
    if st == nil { return ErrClosed }
    col, err := st.Vector().Collection(a.Collection)   // loads from meta if needed
    if err != nil { return err }
    return col.Upsert(a.Points)
}

func (f *fsm) applyVectorDelete(ctx context.Context, a vectorDeleteArg) any {
    st := f.node.storeHandle()
    if st == nil { return ErrClosed }
    col, err := st.Vector().Collection(a.Collection)
    if err != nil { return err }
    _, err = col.Delete(a.IDs, nil)
    return err
}
```

`applyVectorCreate*` already registers the collection via
`NewCollectionWithOptions` (which calls `trackCollection`); `applyVectorFlush`
already calls `st.Vector().Flush()` (which drains every tracked collection's
buffer). No change there.

### 4.2 Durability nuance (flag, do not silently change)

`Collection.Upsert` updates the in-memory index immediately and buffers the
points; they become durable after `node.Vector().Flush`, the flush threshold, or
`Close`. This means `node.Vector().Upsert` now returns once the point is in the
live index (and buffered), **not** once it is durable — which is *the same
semantics as the direct `vector` store* and is consistent with the existing
`node.Vector().Flush(ctx)` facade and the `serializeStore` pre-flush. Callers
that require durability call `node.Vector().Flush`.

If a future requirement insists on durable-per-Upsert, the alternative is a
`vector.Store.UpsertPointsLive`/`DeletePointsLive` pair that (1) writes durably
via the existing `UpsertPoints`/`DeletePoints` and (2) applies the delta to the
tracked collection's index under `col.mu`. The recommended approach above is
preferred for consistency; note this trade-off in the change description.

### 4.3 Tests

File: `cluster/replication_test.go`:
- `TestClusterVectorIndexRefresh`: leader creates a collection and upserts
  points; wait for follower `Barrier`; on the follower, call
  `node.Store().Vector().Collection(name).Search(...)` (or the tracked
  collection's search) and assert the new points are returned **without** a
  reload/`LoadCollection`.
- `TestClusterVectorDeleteRefresh`: replicated delete removes the point from the
  follower's live index.
- `TestClusterVectorSnapshotDrainsBuffer`: after upsert, force `Snapshot()` and
  confirm a restored follower sees the points (existing behavior, kept green).

### 4.4 Update `cluster/README.md`

- Remove/replace the "Limitations (v1)" bullet about vec index refresh.

---

## Phase 5 — search audit flag and implementation

Goal: `AuditAll` actually audits search; search has a real audit sink.

### 5.1 `store/system/system.go`

- Append the audit bit:

```go
// AuditSearch records full-text writes.
AuditSearch // value 1 << 6, appended after AuditObject
```

- Update `AuditAll`:

```go
AuditAll = AuditRecord | AuditFile | AuditSeries | AuditVector | AuditGraphs | AuditObject | AuditSearch
```

- Append `AuditPathSearch = "m/6'"`.
- Add `Keys.AuditSearch ed25519.PrivateKey`.
- Derive it in `Derive` (append `{AuditPathSearch, &keys.AuditSearch}` to
  `auditPaths`).
- Wipe it in `Wipe` (append `k.AuditSearch` to the wipe list).

### 5.2 `store/ledger/audit.go`

- Append `StoreSearch = bucket.SEARCH`.

### 5.3 `store/search` audit wiring

Files: `store/search/search.go`, `store/search/index.go`.

- `search.Config`: add `Audit ledger.Sink`.
- `Store`: add `audit ledger.Sink`; set it from `cfg.Audit` in `Open`; carry it
  through `WithRoot`/`WithTransaction` (like `obs`).
- Add helper (mirror `object.replay`):

```go
func (s *Store) auditEntry(op, entity string, digest []byte) error {
    if s == nil || s.audit == nil { return nil }
    return s.audit.Record(ledger.Entry{
        Store: ledger.StoreSearch, Op: op, Entity: entity, Digest: digest,
    })
}
```

- Emit entries:
  - `Index.Upsert`: after the successful write transaction, `op: "upsert"`,
    `Entity: idx.name + ":" + id`, `Digest: sha256(id + "\x00" + text)`.
  - `Index.Delete`: after the write, `op: "delete"`, `Entity: idx.name` (or
    `name + ":" + joined ids`), `Digest: nil`.
  - `Store.Open` creating a new index (`meta` absent): `op: "create_index"`,
    `Entity: name`.
  - `Store.Drop` (Phase 0): `op: "drop_index"`, `Entity: name`.

Import `github.com/malivvan/tempest/store/ledger` and `crypto/sha256` as needed.

### 5.4 Root `db.go` wiring

- Re-export `AuditSearch = system.AuditSearch` in the root `const` block.
- `auditPublicKeys`: add `auditSearch ed25519.PublicKey`.
- `Open`: when `HasFlag(options.Audit, AuditSearch)`, add
  `recKeys[ledger.StoreSearch] = append(..., keys.AuditSearch...)` and set
  `s.auditPubs.auditSearch = publicOf(keys.AuditSearch)`.
- `search.Config` in `Open`: add `Audit: auditSink(AuditSearch)`.
- `VerifyAudit`: when `HasFlag(s.auditFlags, AuditSearch)`, add
  `keys[ledger.StoreSearch] = s.auditPubs.auditSearch`.

### 5.5 Tests

- `store/system`: `TestAuditAllIncludesSearch` (bit math).
- `db_test.go`: open a store with `Audit: AuditAll`, write a search doc, call
  `VerifyAudit()` and assert the count includes the search event.
- `store/search`: `TestSearchAudit` — open with a sink-backed recorder, upsert +
  delete, assert two events with correct store/op.

---

## Phase 6 — search key rotation and VerifyAll

Goal: search reaches lifecycle parity (`RotateAll`, `VerifyAll`, optional
`ReEncryptIndex`).

File: new `store/search/rotate.go`.

### 6.1 Encryption-state helpers

```go
func (s *Store) withKey(newKey []byte) (*encState, error)          // drop old keys
func (s *Store) withRotationKey(newKey []byte) (*encState, error)  // keep fallbacks
func (s *Store) adoptEnc(next *encState)
```

- `withKey`: `cur := s.encState()`; nil → `ErrInvalidConfig` ("store is not
  encrypted"); `len(newKey) != 32` → `ErrInvalidKey`; rebuild via `resolveEnc`
  with `Secret: newKey, EncryptValues: cur.encValues, EncryptTokens:
  cur.encTokens`.
- `withRotationKey`: build a `Keychain` via `cur.kc.WithKeyKeepingFallbacks
  (newKey)` and a new `SIVNameCipher` from `newKey` (label `"tempest/fts/v1"`),
  preserving `encValues`/`encTokens`.

Add `rotMu sync.RWMutex` to `Store` (init in `Open`).

### 6.2 `RotateAll`

```go
// RotateAll re-encrypts every index value and any SIV-encrypted term key under
// newKey, then adopts newKey and drops the previous keys.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error
```

- `s.tx != nil` → `ErrInTransaction`; `s.base != nil` → delegate to base.
- Build `next := s.withKey(newKey)`; take `rotMu.Lock()`.
- In one `s.db.Update`: enumerate indexes (`Indexes`); for each, run the
  `rotation` walker below with `old` and `next`; adopt via `adoptEnc(next)`.

### 6.3 `VerifyAll`

```go
// VerifyAll re-reads the version/mode records and every sealed value and SIV
// term key to confirm they decrypt with the configured keys. Any value that
// fails authentication is reported as ErrAuthFailed.
func (s *Store) VerifyAll(ctx context.Context) error
```

- `s.tx != nil` → `ErrInTransaction`; delegate base if set.
- In one `s.db.View`: confirm `encState != nil` (else `ErrInvalidConfig`); for
  each index, run the `rotation` walker with `next == nil` (verify-only).

### 6.4 `ReEncryptIndex` (optional parity)

```go
// ReEncryptIndex rewrites one index under newKey, keeping the previous key as a
// fallback. Rejected with ErrInvalidConfig when EncryptTokens is enabled: the
// SIV term keys are derived from one master key, so a partial rotation would
// leave the not-yet-rewritten indexes unreadable.
func (s *Store) ReEncryptIndex(ctx context.Context, name string, newKey []byte) error
```

- Mirror `vector.ReEncryptCollection`: reject when `enc.encTokens`; use
  `withRotationKey`; rewrite only that index; keep the old marker (there is none
  in search, so just adopt).

### 6.5 The `rotation` walker

```go
type rotation struct {
    ctx   context.Context
    store *Store
    old   *encState
    next  *encState
}
func (r *rotation) verify() bool { return r.next == nil }
```

Per index bucket:
- `metaKey`: if `old.encValues`, `openValue` then `next.seal`; verify-only just
  `openValue`.
- `docs/*` values: if `old.encValues`, `openValue` then `next.seal`.
- `terms/*`: key is the SIV-encrypted term key when `old.encTokens` — `openName`
  via `old.siv`, then re-`SealName` via `next.siv` (rename the key in a new
  bucket or use `ApplyEntries`); value is sealed when `old.encValues`.
- `versionKey`/`modeKey` are plaintext — untouched.

Use `crypto.ApplyEntries` (same pattern as `record`/`vector`) for key renames,
and the search `bucket`/`rootBucket`/`createRootBucket` helpers for traversal.
Note bbolt cannot rename buckets; for the term-keys rename use per-key
delete+put inside the same `terms` bucket (keys are leaf entries, so no bucket
rename is needed).

### 6.6 Tests

File: `store/search/rotate_test.go`:
- `TestSearchRotateAll`: encrypted index → `RotateAll(newKey)` → values still
  search correctly with the store reopened under the new key (via a store
  opened with `newKey`), and old-key decryption no longer works.
- `TestSearchVerifyAll`: tamper a sealed doc value → `VerifyAll` returns
  `ErrAuthFailed`.
- `TestSearchReEncryptIndexRejectsTokens`: `EncryptTokens` + `ReEncryptIndex`
  → `ErrInvalidConfig`.

---

## Phase 7 — encryption/rotation consolidation (graphs, object)

Goal: graphs and object reach `RotateAll` + `VerifyAll` parity. (Search is Phase
6; record/fstree/series/vector already have it.)

### 7.1 `store/graphs/rotate.go`

```go
// RotateAll re-encrypts every node/edge/version record and the canary under
// newKey, then adopts newKey.
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error

// VerifyAll re-reads the canary and every record to confirm they decrypt.
func (s *Store) VerifyAll(ctx context.Context) error

// ReEncryptGraph rewrites one graph under newKey, keeping the previous key as a
// fallback until RotateAll. (Optional; safe because graph keys are plaintext.)
func (s *Store) ReEncryptGraph(ctx context.Context, name string, newKey []byte) error
```

- `graphs.encState` holds only a `*crypto.Keychain`; `resolveEnc` builds it from
  `Secret`. Add `withKey`/`withRotationKey`/`adoptEnc` (mirror search/vector).
- `RotateAll`: in one `s.db.Update`, for each graph in `Names`, re-seal all
  live records and closed versions (`nodes`, `node_versions`, `edges`,
  `edge_versions` buckets) with `next`, then re-seal the canary (`canaryKey`)
  with `next`; adopt.
- `VerifyAll`: in one `s.db.View`, open the canary with `old`, then open every
  record value.
- `ReEncryptGraph`: rewrite only `name`'s buckets with `withRotationKey`;
  keep the canary under the old key until `RotateAll` (mirror vector).

### 7.2 `store/object/rotate.go`

```go
func (s *Store) RotateAll(ctx context.Context, newKey []byte) error
func (s *Store) VerifyAll(ctx context.Context) error
func (s *Store) ReEncryptBucket(ctx context.Context, bucket string, newKey []byte) error // optional
```

- Same pattern: re-seal every object `record` and the canary; `VerifyAll`
  re-reads them.

### 7.3 Tests

- `store/graphs/rotate_test.go`: `TestGraphsRotateAll`, `TestGraphsVerifyAll`,
  `TestGraphsReEncryptGraph`.
- `store/object/rotate_test.go`: `TestObjectRotateAll`, `TestObjectVerifyAll`,
  `TestObjectReEncryptBucket`.

### 7.4 Notes

- Graphs/object have no deterministic names, so full `RotateAll` is always
  sufficient; partial rotation is included only for parity and can be deferred
  without losing the consolidated guarantee. **[DECISION]** Default: implement
  `RotateAll` + `VerifyAll` now; implement `ReEncryptGraph`/`ReEncryptBucket`
  as follow-up if per-entity rotation is actually needed.
- `VerifyAll` on a non-encrypted store returns `ErrInvalidConfig`/`ErrWrongMode`
  (same as other stores).

---

## Phase 8 — audit/ledger consolidation

This is the umbrella check that everything is consistent after Phases 5 and the
pre-existing wiring. Concretely:

- Confirm `AuditAll` now includes **every** sub-store: record, file, series,
  vector, graphs, object, search.
- Confirm `ledger.Store*` constants cover all seven (`StoreSearch` added in
  Phase 5).
- Confirm `system.Derive` derives audit keys for all seven paths (`m/0'..m/6'`).
- Confirm `db.go` wires every sink and every public key in `Open`/`VerifyAudit`.
- Confirm every sub-store that mutates data calls its audit sink after a
  committed write (record/fstree/series/vector/graphs/object already do; search
  after Phase 5).

Add one cross-cutting test in `db_test.go`:

```go
func TestAuditAllCoversEveryStore(t *testing.T)
```

- Open a store with `Audit: AuditAll`, perform one representative write in each
  of the seven stores, `VerifyAudit()`, and assert at least one event per store
  name is present in the chain.

---

## Documentation updates

Update these in the same change, per `AGENTS.md` documentation conventions.

1. **README.md**
   - Encryption section: note search/graphs/object now support rotation +
     `VerifyAll` (sub-store level), and search audit is available.
   - Audit table/flags: add `AuditSearch`; `AuditAll` description updated.
   - Export/Import: state that search, graphs and objects are now exported;
     graphs are live-only; search is postings-level.
   - Backup: add `RestoreBackup` usage and the "not over a live store" caveat.
   - Cluster: mention the new facades.
2. **AGENTS.md**
   - Repository map / encryption internals / audit internals: reflect the new
     capabilities (search rotation + audit, graphs/object rotation).
   - `cluster/README.md` already lives in-repo; keep the audit/rotation sections
     accurate.
3. **cluster/README.md**
   - Replicated writes: add search/graphs/objects.
   - Limitations: remove the vec-index-refresh bullet; note search/graphs/object
     facades and any remaining limitations (e.g. search custom tokenizer).
4. **Package guides** (`store/search/README.md`, `store/graphs/README.md`,
   `store/object/README.md`, `store/system/README.md`, `store/ledger/README.md`,
   `store/vector/README.md`):
   - Document new APIs (`RestoreIndex`, `Drop`, `Buckets`, `DropBucket`,
     `Collection`, `RotateAll`, `VerifyAll`, `AuditSearch`, `StoreSearch`).

---

## Verification

Run after each phase, and once more at the end (from `AGENTS.md`):

```sh
test -z "$(gofmt -l .)"
go vet ./...
go build ./...
go test -count=1 -timeout 300s ./...
CGO_ENABLED=0 go test -count=1 -timeout 300s ./...
go test -race -count=1 -timeout 600s ./...          # for Phases 3, 4 (concurrency)
CGO_ENABLED=0 GOARCH=386 go vet ./...
CGO_ENABLED=0 GOARCH=386 go test -count=1 -timeout 600s ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...
go mod tidy -diff
```

New tests must be focused regression tests (per `AGENTS.md`): one per new
function, race detector for the concurrency changes (Phases 3 and 4).

---

## Risks, trade-offs, open decisions

1. **Search export is not source-text.** It exports derived postings. This is
   the only faithful option because source text is not retained; it is flagged
   clearly so callers do not assume the original documents are recoverable.
2. **Graphs export is live-only.** Closed bitemporal versions are not carried by
   Export/Import. In-process `graphs.Copy`/`MigrateFromRecord` still migrates
   history between layouts. If cross-encryption history migration is later
   required, add a `Document`-style history interchange type (out of scope).
3. **Vector cluster durability shifts.** Switching the FSM to `Collection.Upsert`
   makes replicated vector upserts buffered (durable after `Flush`/threshold),
   matching the direct store. Callers needing durability call
   `node.Vector().Flush`. The plan keeps the existing `applyVectorFlush` and
   snapshot pre-flush, so snapshots remain complete.
4. **RestoreBackup cannot target a live store.** bbolt holds the file lock;
   document and test that `dst` must be a fresh/closed path.
5. **Search custom tokenizers** do not round-trip through export; import fails
   closed unless the caller supplies the tokenizer.
6. **`ReEncryptIndex`/`ReEncryptGraph`/`ReEncryptBucket`** are optional parity
   items; `RotateAll` + `VerifyAll` are the required consolidation surface.

### Open decisions for the user (defaults chosen, confirm if different)

- Confirm the vector-cluster durability trade-off in Phase 4 (buffered vs
  durable-per-upsert). Recommended: buffered (consistent with direct store).
- Confirm graphs export is live-only (recommended), vs. adding a history
  interchange type (larger scope).
- Confirm `RestoreBackup` signature (`RestoreBackup(ctx, src, dst, password,
  opts, rest)` package-level function). Recommended.
