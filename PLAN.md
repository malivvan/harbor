# Harbor-wide consensus support — implementation plan

This document is the complete, detailed plan for adding cluster-wide consensus
to Harbor using [`github.com/hashicorp/raft`](https://github.com/hashicorp/raft).
It was produced by studying two reference implementations placed in `tmp/` for
this exercise — [`hraftd`](https://github.com/otoolep/hraftd) (the canonical
Hashicorp raft key/value example) and [`rqlite`](https://github.com/rqlite/rqlite)
(the closest production analog: a local single-file B-tree database replicated
by raft) — and engineering the approach that best fits Harbor's actual
architecture rather than copying either one.

---

## 1. Goal and non-goals

**Goal.** Let several Harbor processes form a cluster in which the four stores
(`db`, `fs`, `ts`, `vec`) stay in sync: a write accepted by the leader is
durably replicated to a quorum and applied, in raft-log order, to the same
logical state on every node. Reads may be served locally from any node's
replica. Cluster membership can grow and shrink without downtime.

**Non-goals** (deliberately out of scope, documented so they are not assumed):

- Multi-primary / active-active writes. Writes are leader-serialized.
- Cross-datacenter WAN optimization, TLS transport, or a discovery service.
  (TCP transport + manual `Join` is the v1 surface; these are listed as
  follow-ups.)
- Replacing Harbor's single-file storage. The raft log and the harbor database
  remain two separate, independent on-disk stores, exactly as rqlite keeps its
  SQLite database separate from its raft log.
- In-place plaintext ↔ encrypted migration (unchanged from today).

---

## 2. What we learn from the references (and what we reject)

### 2.1 hraftd — the shape to reuse

hraftd is a ~350-line example that shows the *correct* scaffolding:

- `raft.DefaultConfig()` + `LocalID`.
- `raft.NewTCPTransport(bindAddr, addr, maxPool, timeout, logOutput)`.
- `raft.NewFileSnapshotStore(dir, retainCount, logOutput)`.
- Durable raft log/stable store via `raft-boltdb` (a separate bbolt file
  `raft.db`).
- `raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)`.
- Single-node bootstrap with `BootstrapCluster` when `enableSingle`.
- `Join` that re-uses an ID/address by `RemoveServer` + `AddVoter`.
- A JSON `command` envelope (`Op`, plus fields), an `fsm.Apply` switch, and an
  `fsmSnapshot` whose `Persist` writes a serialized state to the sink.

Harbor reuses *this shape* (transport, storage wiring, membership, command
envelope, FSM, snapshot) but not hraftd's in-memory `map[string]string` state
machine.

### 2.2 rqlite — the proof that local-file replication works

rqlite replicates a local single-writer B-tree (SQLite) via raft:

- Writes flow through `raft.Apply` on the leader; the FSM applies the command
  to the local database on every node.
- The snapshot is a byte-copy of the database file (SQLite backup API); restore
  replaces the local database file and reopens.
- Raft's own log/stable store lives in a separate `raft-boltdb` file.
- `ErrNotLeader` is returned by write paths; reads can be served locally with an
  explicit freshness model (leader vs. stale).

This validates the central decision for Harbor: **replicate logical operations
through the raft log, keep the local bbolt database as the materialized replica,
and snapshot it as a byte stream.**

### 2.3 What we reject from both

- **hraftd's in-memory state machine** — Harbor state is durable and shared by
  four heterogeneous stores; the state machine is the harbor database itself.
- **rqlite's SQL/text-wire protocol and its cluster service/HTTP mux** — Harbor
  is a *library*, not a server. v1 exposes a Go API, not a wire protocol.
- **rqlite's statement-level determinism hazards** (SQL `time('now')`, random)
  — handled explicitly for Harbor in §6.

---

## 3. Architectural decision

### 3.1 The problem, stated precisely

Harbor's entire state is **one bbolt file** (`*bolt.DB`) with four stores on
top, each opened under its own top-level bucket (`db/`, `fs/`, `ts/`, `vec/`)
plus the unencrypted `harbor.Metadata` record. There is no intermediate
"statement" layer: `db.Save` performs bbolt puts (records, indexes,
auto-increment counters); `fs.Write` merges inode and content writes; `ts`
writes point records; `vec` buffers point writes and flushes.

To replicate, we must choose a **replication unit** and a **capture point**.

### 3.2 Options considered

| Option | Capture point | Verdict |
|---|---|---|
| A. Replicate raw bbolt pages | bbolt mmap / page cache | Rejected. No public diff API; leaks encryption layout; fragile across bbolt versions. |
| B. Replicate the whole bbolt file per write | `Tx.WriteTo` | Rejected as the *log* unit: O(database size) per write; correct only as the *snapshot* unit. |
| C. Capture bbolt write transactions as `(bucket,key,value)` ops via a `*bolt.DB` wrapper | `DB.Update/Batch` | Rejected for v1. bbolt exposes concrete `*bolt.Tx`/`*bolt.Bucket` types with value-returning methods; Go has no covariance, so a wrapper cannot satisfy the stores' call sites without rewriting every `func(tx *bolt.Tx)` closure in all four stores. Too invasive/risky. |
| D. **Replicate logical operations (command log)** | Harbor store API | **Chosen.** Matches rqlite's proven model; the apply path re-uses the exact store code, so it cannot diverge from the direct path. |

### 3.3 The chosen model — command-log replication at the store-API level

```text
                        ┌──────────────────────────────┐
caller ── write ───────▶│ consensus.Node (leader only) │
                        │  encode command              │
                        │  raft.Apply(cmd)             │
                        └──────────────┬───────────────┘
                                       │ replicated log
                        ┌──────────────▼───────────────┐
                        │ raft.FSM.Apply  (all nodes)  │
                        │  dispatch to db/fs/ts/vec    │
                        │  call the SAME store method  │
                        └──────────────┬───────────────┘
                        ┌──────────────▼───────────────┐
                        │ local *harbor.Store (bbolt)  │
                        └──────────────────────────────┘

caller ── read ────────▶ local *harbor.Store  (any node, no raft round-trip)
```

- **Leader** is the only writer. A write API on the leader encodes the
  operation into a `Command`, calls `raft.Apply`, and returns the `ApplyFuture`
  error.
- **Every node** (leader included) applies the command through the FSM, which
  dispatches to the *same* `db/fs/ts/vec` method the caller would have invoked
  directly. There is exactly one implementation of each operation — the store's
  own — so direct and replicated behavior cannot drift.
- **Reads** bypass raft entirely and hit the local `*harbor.Store` (eventual on
  followers; linearizable on the leader). The consistency contract is spelled
  out in §7.

This satisfies "complete and harbor wide": because the apply path is the store
path, *every* mutation the stores can perform is replicable; no per-store state
machine is duplicated.

---

## 4. Package and module layout

```
harbor/
  consensus/            # NEW — raft-backed cluster over a *harbor.Store
    config.go           #   Config, defaults, validation
    node.go             #   Node: lifecycle, leader/state, Apply, membership
    errors.go           #   sentinel errors
    command.go          #   Command envelope, Op constants, marshal/unmarshal
    fsm.go              #   raft.FSM implementation (Apply/Snapshot/Restore)
    snapshot.go         #   snapshot format + Persist/Release
    db.go               #   db store command dispatch + write wrappers
    ts.go               #   ts store command dispatch + write wrappers
    vec.go              #   vec store command dispatch + write wrappers
    fs.go               #   fs store command dispatch + write wrappers
    *_test.go
  harbor.go             # unchanged (consensus imports harbor, not vice versa)
  README.md             # new "Clustering" section
```

Dependencies added to `go.mod`:

```text
github.com/hashicorp/raft           v1.8.0
github.com/hashicorp/raft-boltdb/v2 v2.3.0   # durable raft log/stable store
github.com/hashicorp/go-hclog       (transitive, for raft's logger)
```

`raft-boltdb/v2` depends on `go.etcd.io/bbolt` (already a direct dependency),
so it does not pull a second storage engine. `consensus` imports `harbor` (and
the four store packages) one-way; the root package does **not** import
`consensus`, so there is no cycle and no coupling for non-clustered users.

---

## 5. Public API (draft)

```go
package consensus

// Config configures one cluster node.
type Config struct {
    NodeID      raft.ServerID      // required, stable across restarts
    BindAddr    string             // "host:port" raft transport binds
    Dir         string             // raft log/stable/snapshot directory
    Bootstrap   bool               // true on the first node to form the cluster
    StorePath   string             // harbor database file path
    StoreMode   os.FileMode
    StoreOpts   *harbor.Options    // password, KDF, encryption, codec
    ApplyTimeout time.Duration     // raft.Apply timeout (default 10s)
}

func Open(ctx context.Context, cfg Config) (*Node, error)
func (n *Node) Close() error

// Replicated writes (leader only; each returns ErrNotLeader off-leader).
func (n *Node) DB() *DBFacade
func (n *Node) TS() *TSFacade
func (n *Node) Vec() *VecFacade
func (n *Node) FS() *FSFacade

// Local reads (any node).
func (n *Node) Store() *harbor.Store

// Cluster management.
func (n *Node) Join(id raft.ServerID, addr raft.ServerAddress) error
func (n *Node) Leave(id raft.ServerID) error
func (n *Node) Leader() (raft.ServerAddress, raft.ServerID)
func (n *Node) State() raft.RaftState
func (n *Node) Stats() map[string]string
func (n *Node) Snapshot() error              // force a raft snapshot now
func (n *Node) Barrier(timeout time.Duration) error
```

The four `*Facade` types expose **write** methods mirroring the stores' write
APIs; `Store()` returns the live `*harbor.Store` for **reads** (and is
documented as read-only — direct writes to it do not replicate).

---

## 6. Command model

### 6.1 Envelope

```go
type Command struct {
    Op  string          // one of the Op constants below
    Arg json.RawMessage // operation-specific, deterministic, JSON
}
```

`Command` marshals to a single length-prefixed byte slice for the raft log.
JSON is chosen for the *envelope* because the four stores' default codec is
JSON; the command layer is independent of any store's at-rest codec (a store
may still store the record with a custom codec — the command carries the
logical record, the store re-encodes it). This mirrors how hraftd uses JSON
for its command envelope.

### 6.2 Determinism contract

Raft applies commands in identical order on every node, so the state converges
iff `Apply` is a **pure function of (current state, command)**. Audit results:

- **db** — deterministic under ordered replay. `NextSequence`-based
  auto-increment counters advance identically because every prior command was
  applied identically.
- **ts** — deterministic. `Insert`/`InsertMany` use the caller's explicit
  `Point.Timestamp`; the pluggable `now` is only used by retention maintenance
  (`EnforceRetention`), which is not a replicated write (see §6.7).
- **vec** — replicated through the **durable** store path. `Store.UpsertPoints`
  / `Store.DeletePoints` are deterministic (they encode the caller's points
  verbatim and write them to bbolt; no wall clock is consulted), so the facade
  uses those rather than `Collection.Upsert`, which stamps a `time.Now()`-based
  `Version` and therefore cannot be replayed deterministically. Trade-off: the
  in-memory search index (Flat/HNSW) is not updated by replicated writes — a
  node's search index reflects the state at collection-load time and is
  refreshed by reloading the collection. A deterministic-version
  `Collection.Upsert` (e.g. carrying the version in the command, or a `SetNow`
  clock) is the documented follow-up for index-aware replication.
- **fs** — **the one non-deterministic store**: `inode.go` calls `time.Now()`
  for atime/mtime on create/write/chmod/chtimes. To keep apply pure, fs
  commands carry the leader's chosen times explicitly (the leader records them
  during the operation and encodes them in the command; followers use the
  carried values). Alternatively fs gains a `SetNow` clock hook (like ts
  already has); the plan recommends **carrying explicit times in the command**
  because it also removes the need to change fs internals.

Rule for future commands: **any value that would differ across nodes (wall
time, random nonces, process-local IDs) must be carried in the command**, not
computed during apply.

### 6.3 db command set

| Op | Args | Store call applied |
|---|---|---|
| `db.save` | `{Bucket, From, Data}` | `store.DB().From(...).Save(ctx, data)` |
| `db.update` | `{Bucket, From, Data}` | `Update` |
| `db.update_field` | `{Bucket, From, Data, Field, Value}` | `UpdateField` |
| `db.delete_struct` | `{Bucket, From, Data}` | `DeleteStruct` |
| `db.drop` | `{Bucket, From, Data}` | `Drop` |
| `db.set` | `{Bucket, From, KV, Key, Value}` | `Set` |
| `db.set_bytes` | `{Bucket, From, KV, Key, Value}` | `SetBytes` |
| `db.delete` | `{Bucket, From, KV, Key}` | `Delete` |
| `db.init` | `{Bucket, From, Data}` | `Init` |
| `db.reindex` | `{Bucket, From, Data}` | `ReIndex` |

`Data`/`Value` are JSON-marshaled in the leader wrapper and unmarshaled into the
caller's struct type in `Apply` (the facade remembers the destination type via
generics or a decode hook). Because the default codec is JSON, this round-trips
losslessly for the default configuration.

### 6.4 ts command set

| Op | Args | Store call applied |
|---|---|---|
| `ts.insert` | `{Series, Point}` | `store.TS().Series(name).Insert(ctx, p)` |
| `ts.insert_many` | `{Series, Points}` | `InsertMany` |
| `ts.delete_range` | `{Series, Start, End}` | `DeleteRange` |
| `ts.drop_series` | `{Series}` | `Drop` |
| `ts.set_labels` | `{Series, Labels}` | `SetLabels` |
| `ts.set_retention` | `{Series, Retention}` | `SetRetention` |
| `ts.delete_series` | `{Name}` | `Store.DeleteSeries` |

### 6.5 vec command set

| Op | Args | Store call applied |
|---|---|---|
| `vec.create_collection` | `{Name, VectorLen, Metric, UseHNSW, Params}` | `NewCollection(WithParams)` |
| `vec.upsert` | `{Collection, Points}` | `Collection.Upsert` |
| `vec.delete` | `{Collection, IDs, Filter}` | `Collection.Delete` |
| `vec.drop_collection` | `{Name}` | `DropCollection` |

### 6.6 fs command set

| Op | Args | Store call applied |
|---|---|---|
| `fs.create` | `{FS, Name, Perm, Atime, Mtime}` | `OpenFile(O_CREATE)` + carried times |
| `fs.write` | `{FS, Name, Offset, Data, Mtime}` | `OpenFile`+`Write` + carried mtime |
| `fs.truncate` | `{FS, Name, Size, Mtime}` | `Truncate` |
| `fs.remove` | `{FS, Name}` | `Remove` |
| `fs.rename` | `{FS, Old, New}` | `Rename` |
| `fs.mkdir` | `{FS, Name, Perm, Times}` | `Mkdir` |
| `fs.chmod` | `{FS, Name, Mode}` | `Chmod` |
| `fs.chtimes` | `{FS, Name, Atime, Mtime}` | `Chtimes` |
| `fs.chown` | `{FS, Name, UID, GID}` | `Chown` |
| `fs.symlink` | `{FS, Old, New}` | `Symlink` |
| `fs.write_at` | `{FS, Name, Off, Data, Mtime}` | `WriteAt` (chunked path) |
| `fs.delete_fs` | `{Name}` | `Store.DeleteFS` |

Content (`Data`) is carried inline; for large blobs the facade can stream a
single command as one raft entry (v1) with a documented size ceiling, and a
future "chunked command" extension splits large writes (see §12).

### 6.7 Explicitly NOT replicated (maintenance, run on the leader only)

- `db.ReEncrypt`/`RotateAll`/`VerifyAll`, `fs.RotateAll`, `ts.RotateAll`,
  `vec.RotateAll`: key rotation is a per-node, key-material operation and stays
  outside the log (documented; requires the operator to run it on every node or
  rotate keys out-of-band).
- `ts.EnforceRetention`, `Store.SetNow`: local maintenance/clock plumbing.
- `vec.Flush`/`SetOwnsDB`/`Close`: in-memory buffer lifecycle, managed by the
  node.

---

## 7. Consistency model

- **Writes** are linearizable at the leader: `raft.Apply` returns only after the
  entry is committed to a quorum and applied.
- **Leader reads** are linearizable: the leader serves its own committed state.
- **Follower reads** are eventually consistent (last applied index). `Node`
  exposes `Barrier` and the FSM index so callers can implement read-your-writes
  by waiting for an applied index. Strong/linearizable reads from a follower
  require a `raft.Barrier`-style round-trip to the leader; this is documented
  and left as an explicit opt-in, matching rqlite's `freshness` semantics.
- **Not leader** writes return `ErrNotLeader` (with the leader's address via
  `Leader()` for client-side redirect). Harbor does **not** auto-forward; the
  caller redirects, which keeps the library honest and transport-free.

---

## 8. Encryption interaction

Encryption is **per-node at-rest** and orthogonal to replication:

- The command log carries **logical plaintext** (records, points, vectors,
  file content). Each node's `*harbor.Store` seals it at rest with its **own**
  derived keys.
- The Argon2id **salt is generated per store creation** and stored in
  `harbor.Metadata`; two nodes therefore have different salts and different
  ciphertexts for the same logical record. That is fine: each node decrypts its
  own bytes, and raft guarantees the logical state converges.
- **Deployment requirement:** every node in a cluster must be opened with the
  **same password** (and the same `Encrypt` flags for a *new* store), so each
  node can open its own replica. This is the only cross-node secret; the raft
  transport itself is unauthenticated in v1 (documented in §13).
- A node that joins late receives a snapshot of the harbor database's
  **at-rest bytes** (see below).

**Snapshot format decision.** The FSM snapshot serializes the harbor database
via `bbolt.Tx.WriteTo`, i.e. the *encrypted-at-rest bytes* (or plaintext bytes
for a plaintext store). A raw byte snapshot **is** restorable across nodes
because it is self-describing:

1. The snapshot carries the leader's `harbor.Metadata` record — salt, KDF
   parameters, flags and canary — so a follower that restores it adopts the
   leader's salt. With the shared password it derives the leader's keys and
   verifies the canary, then reads the leader's ciphertext correctly.
2. After restore the node reopens its store, which rebuilds the in-memory
   caches (fs inode cache, vec search indexes) from disk.

**Chosen:** the snapshot is the **raw `Tx.WriteTo` byte stream + reopen**,
exactly the rqlite "swap the database file and reopen" model. It needs no
logical export/import, no per-store serialization, and is byte-accurate for
both plaintext and encrypted stores. The one requirement is the shared
password (§8 above). This is detailed in §9.4.

---

## 9. FSM, snapshot and restore

### 9.1 `Apply`

```go
func (f *fsm) Apply(l *raft.Log) any {
    var c Command
    if err := json.Unmarshal(l.Data, &c); err != nil {
        panic(err) // log corruption is unrecoverable, like hraftd
    }
    switch c.Op {
    case "db.save": /* decode args, call f.node.Store().DB()...Save */
    ...
    }
    return nil
}
```

The FSM holds a reference to the node's `*harbor.Store`. Dispatching calls the
store's own method, so index maintenance, unique enforcement, and counters are
identical to the direct path. **The db store's change hooks are the one
replication recursion risk** (§9.3).

### 9.2 `Snapshot` / `Persist`

`Snapshot()` acquires the node's apply lock, serializes the harbor database
with `bbolt.Tx.WriteTo` (via `*harbor.Store.View`), and returns an
`fsmSnapshot`. `Persist` writes the bytes to the `raft.SnapshotSink` and
`Close`s it; on error it `Cancel`s — the exact hraftd contract. `Release` is a
no-op (the snapshot holds no locks).

### 9.3 Recursion guard

The `db` store emits `ChangeHook`s on committed writes. If the FSM applies a
`db.save` command by calling `store.DB().Save`, that would emit a hook, and if
the node also uses the hook to *drive* replication, it would recurse. v1 avoids
the whole problem: **replication is driven by the explicit facade API, never by
change hooks.** The FSM applies via the store methods and ignores hooks (it
registers none). Change hooks remain a caller convenience and are simply not
used for consensus. (A future "transparent" mode may use them; that is a
separate, opt-in layer.)

### 9.4 Restore and reopen

`Restore(rc)` reads the snapshot bytes, writes them to a sibling temp file, and
reopens the store over them:

1. Flush and close the current `*harbor.Store` (which closes the bbolt handle).
2. Replace the store file with the restored bytes.
3. Reopen `*harbor.Store` with the remembered `Config.StoreOpts`.

Because `vec` buffers points and `fs` caches inodes in memory, restore must
reopen (not patch) so those caches rebuild from disk. `reopen` also re-sets
`vec.SetOwnsDB(false)`, exactly as the root package does today. This is the
rqlite "swap the database file and reopen" model.

---

## 10. Raft lifecycle

Mirrors hraftd §"Open" with Harbor specifics:

1. `raft.DefaultConfig()`; set `LocalID`, `ApplyTimeout`, and a logger via
   `hclog` wired to a configurable `io.Writer`.
2. `raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10s, logWriter)`.
3. `raft.NewFileSnapshotStore(filepath.Join(cfg.Dir, "snapshots"), 2, logWriter)`.
4. `raftboltdb.New(Options{Path: filepath.Join(cfg.Dir, "raft.db")})` for both
   `LogStore` and `StableStore`.
5. `raft.NewRaft(...)`.
6. If `cfg.Bootstrap` (and no existing configuration), `BootstrapCluster` with
   `{ID: LocalID, Address: transport.LocalAddr()}`. Use `GetConfiguration` and
   bootstrap only when empty (idempotent restart-safe bootstrap).
7. Membership: `Join` (`AddVoter`, with hraftd's re-use-ID/address handling),
   `Leave` (`RemoveServer`), `Leader`/`LeaderWithID`, `State`, `Stats`.

`Close` shuts down raft (`raft.Shutdown()`), the transport, and the local
`*harbor.Store` (flushing `vec` first, per the root package's contract).

---

## 11. Root-package integration

The root `harbor` package is unchanged. `consensus.Open` constructs the
`*harbor.Store` it needs by calling the **exported** `harbor.Open` with the
caller's `harbor.Options`, so:

- password/KDF/canary logic stays in `harbor`,
- `consensus` never touches `harbor.Metadata` or key derivation,
- the clustered and non-clustered paths share one `harbor.Open`.

No import cycle results because only `consensus` imports `harbor`.

---

## 12. Implementation phases and commit points

Each phase is one commit; the tree builds and tests green after each.

1. **`build: add hashicorp/raft and raft-boltdb`** — update `go.mod`/`go.sum`,
   `go mod tidy`.
2. **`consensus: add Config, errors and the command envelope`** —
   `consensus/config.go`, `errors.go`, `command.go` (+ `command_test.go`).
3. **`consensus: add the raft FSM and snapshot format`** —
   `fsm.go`, `snapshot.go` (+ round-trip tests against an in-memory store).
4. **`consensus: add node lifecycle and membership`** —
   `node.go`: `Open`/`Close`/`Leader`/`State`/`Stats`/`Join`/`Leave`/`Snapshot`
   (+ a two-node in-memory-transport test: elect leader, replicate a barrier).
5. **`consensus: replicate db writes`** — `db.go`: facade + `db.*` commands +
   apply dispatch (+ a two-node db replication test: save on leader, read on
   follower after barrier).
6. **`consensus: replicate ts writes`** — `ts.go` (+ replication test).
7. **`consensus: replicate vec writes`** — `vec.go` (+ replication test).
8. **`consensus: replicate fs writes`** — `fs.go` with carried-times commands
   (+ replication test).
9. **`consensus: document clustering`** — README section + `consensus` package
   doc; verify snippets compile.
10. **`consensus: add an example cluster`** — `examples/consensus/main.go`
    (three-node demo with join/status/put/get), wired into `Makefile`.

Commit messages are prefixed `consensus:` (or `build:`/`docs:`) following the
repository's existing convention (`db:`, `fix(db):`, `refactor!:`).

---

## 13. Security and threat model notes

- The raft transport is **unauthenticated TCP** in v1: it must be deployed on a
  private network/VPN. TLS via `raft.NetworkTransport` + a `tls.Config` is a
  documented follow-up (the transport already supports it).
- The **password is shared across the cluster** so every node can open its
  replica. This is the single cross-node secret and is unchanged from Harbor's
  existing single-node model (one password per store).
- The **command log carries plaintext** operations. Anyone who can read the
  raft log directory can read the operations; this is the same trust boundary
  as the raft log in hraftd/rqlite and is documented, not hidden.
- `Apply` panics on a malformed command (unrecoverable log corruption), the
  same stance hraftd takes. Commands are produced only by our own encoder, so
  a panic indicates tampering/corruption, not a recoverable condition.
- No new claim is added to README's threat model beyond: *consensus replicates
  operations to a quorum; encryption at rest remains per-node and unchanged.*

---

## 14. Testing strategy

- **Unit:** command encode/decode round-trips; snapshot export/import
  round-trips (including encrypted and plaintext stores); FSM dispatch per op;
  determinism (apply the same command twice from identical state → identical
  logical state).
- **Integration:** a helper that starts N nodes on `raft.NewInmemTransport`
  (loopback) so tests run without ports, mirroring raft's own test style:
  - leader election with 1 and 3 nodes,
  - write on leader → barrier → read on follower (each of db/ts/vec/fs),
  - leader step-down / `Leave` → new leader → write succeeds,
  - snapshot + `Restore` → a fresh node catches up from the snapshot alone,
  - `ErrNotLeader` returned from a follower's write facade.
- **Determinism:** a test that runs the same command sequence on two
  independently-bootstrapped nodes and asserts byte-identical *logical* exports
  (accounting for per-node ciphertext).
- Run the repository's standard gate: `gofmt`, `go vet`, `go build`, `go test`,
  `go test -race`, plus `CGO_ENABLED=0 go test` (raft is pure Go).

---

## 15. Risks, trade-offs and open questions

- **fs content size.** A large file write is one raft entry. v1 accepts this
  with a documented ceiling; a chunked-command extension (split a write into
  N sequential commands) is the follow-up. Chunking must be visible in the
  command stream so replay is order-stable.
- **fs determinism** requires carried times; if that proves noisy, fall back to
  adding `SetNow` to `fs` (mirroring `ts.SetNow`).
- **Snapshot cost.** Logical export/import is O(database size) but runs only on
  snapshot/restore (rare), matching bbolt's own `Tx.WriteTo` cost.
- **Read-your-writes on followers** needs a `Barrier`; the facade API exposes
  it but does not implicitly add a raft round-trip to every read.
- **Auto-forwarding** of `ErrNotLeader` is deliberately omitted (library
  philosophy); a caller-provided redirect is trivial with `Leader()`.
- **Transparent mode** (auto-replicate existing `harbor.Store` calls via change
  hooks, no facade) is a possible later layer; it is not v1 because it requires
  suppression hooks in all four stores and re-entrancy guards.
