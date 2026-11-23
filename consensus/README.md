# consensus

Raft-backed cluster replication for a Harbor store. Each node owns its own
`*harbor.Store`; writes are proposed through
[`hashicorp/raft`](https://github.com/hashicorp/raft) on the leader and applied,
in raft-log order, to the same logical state on every node. Reads are served
directly from the local replica.

## Model

```text
write ──▶ leader encodes command ──▶ raft.Apply ──▶ committed to quorum
                                                       │
                 every node's FSM applies the command  ▼
                 to its own *harbor.Store (db/fs/ts/vec)

read  ──▶ local *harbor.Store  (no raft round-trip)
```

- The **leader is the only writer**. A write facade encodes the operation into
  a `Command` and calls `raft.Apply`; the FSM dispatches the command to the
  *same* store method the caller would have used, so replicated behavior cannot
  drift from direct behavior.
- **Reads** hit the local store. Leader reads are linearizable; follower reads
  are eventually consistent and can be made read-your-writes-safe with
  `Barrier` (see [Consistency](#consistency)).
- **Encryption is per-node at rest.** Commands carry logical plaintext; each
  node seals it with its own derived keys. The salt is generated per store and
  is carried inside snapshots, so the only cross-node requirement is a shared
  password.

## Setup

```go
ctx := context.Background()

node, err := consensus.Open(ctx, consensus.Config{
    NodeID:    "node-1",
    BindAddr:  "127.0.0.1:7001",          // raft transport address
    Dir:       "./data/node-1",           // raft log/stable/snapshot store
    Bootstrap: true,                      // first node forms the cluster
    StorePath: "./data/node-1/harbor.db",
    StoreMode: 0o600,
    StoreOpts: &harbor.Options{Password: "shared-secret"},
})
if err != nil { /* ... */ }
defer node.Close()
```

A second node opens with `Bootstrap: false` and joins the cluster through the
leader:

```go
// on node-2
n2, _ := consensus.Open(ctx, consensus.Config{ /* NodeID node-2, no Bootstrap */ })

// on the leader
leader.Join("node-2", "127.0.0.1:7002")
```

## Replicated writes

Each store has a write facade; the local store remains available for reads via
`Node.Store()`. Off-leader writes return `consensus.ErrNotLeader`.

```go
// db (records) — register the record type so followers can apply it.
consensus.Register((*User)(nil)) // call on every node
err := node.DB().Save(ctx, &User{ID: "u1", Name: "alice"})
err = node.DB().From("actor").Save(ctx, &Actor{ID: 1})
err = node.DB().UpdateField(ctx, &User{ID: "u1"}, "Name", "alicia")
err = node.DB().DeleteStruct(ctx, &User{ID: "u1"})

// db (raw key/value)
err = node.DB().Set(ctx, "settings", "theme", "dark")  // value encoded on the leader
err = node.DB().SetBytes(ctx, "blobs", "raw", raw)     // caller-encoded bytes
err = node.DB().Delete(ctx, "settings", "theme")

// ts (time series)
err = node.TS().Insert(ctx, "cpu", ts.Point{Timestamp: now, Value: 42.0})
err = node.TS().InsertMany(ctx, "cpu", pts)
n, err := node.TS().DeleteRange(ctx, "cpu", start, end) // returns removed count
err = node.TS().SetLabels(ctx, "cpu", map[string]string{"host": "a"})
err = node.TS().SetRetention(ctx, "cpu", ts.Retention{MaxPoints: 1000})

// vec (vectors)
err = node.Vec().CreateCollection("colors", 2, vec.Dot, false)
err = node.Vec().CreateCollectionWithOptions(ctx, opts)
err = node.Vec().EnsureCollection(ctx, "colors")
err = node.Vec().Upsert("colors", points)
err = node.Vec().Delete("colors", []string{"red"})
err = node.Vec().Flush(ctx) // drain every replica's write buffer

// fs (files)
fsys := node.FS().Open("data")
err = fsys.Mkdir(ctx, "/docs", 0o755)
err = fsys.MkdirAll(ctx, "/docs/2026/q1", 0o755)
err = fsys.WriteFile(ctx, "/docs/readme.txt", []byte("hello"), 0o644)
err = fsys.Rename(ctx, "/docs/readme.txt", "/docs/README.txt")
err = fsys.Truncate(ctx, "/docs/README.txt", 0)
err = fsys.Chmod(ctx, "/docs/README.txt", 0o600)
err = fsys.Chtimes(ctx, "/docs/README.txt", atime, mtime)
err = fsys.Chown(ctx, "/docs/README.txt", uid, gid)
err = fsys.Symlink(ctx, "/docs/README.txt", "/docs/latest")
```

Streaming writes buffer in memory and are replicated once, on `Close`, so a
partial file never enters the raft log:

```go
f, err := fsys.Create(ctx, "/docs/report.txt", 0o644)
if err != nil {
	return err
}
if _, err := io.Copy(f, source); err != nil { // many Write calls
	return err
}
err = f.Close() // one command with the complete content
```

`OpenWriter` opens the leader's current content for appending; `WriteAt` and
`Truncate` edit the buffer before `Close` publishes it. Writes made elsewhere
between `OpenWriter` and `Close` are not observed: the buffer replaces the
content.

Records and raw key/value writes are encoded on the leader and applied
verbatim, so all nodes must run the same codec. Keys use the same
type-sensitive encoding as direct access, so an int key and a string key never
collide. Directory times are pinned into `Mkdir`/`MkdirAll` commands, and
snapshots drain the vector buffer first, so buffered points survive a restore.

## Reading

```go
var u User
err := node.Store().DB().One(ctx, "ID", "u1", &u)

p, err := node.Store().TS().Series("cpu").Get(ctx, ts)

fsys, _ := node.Store().FS().Open("data")
b, err := fsys.ReadFile("/docs/README.txt")
```

## Cluster management

```go
addr, id := node.Leader()          // current leader address and ID
state := node.State()              // raft.Leader / Follower / Candidate / Shutdown
stats := node.Stats()              // raft statistics
err := node.Barrier(10*time.Second) // block until prior entries are applied here
err := node.Snapshot()             // force an immediate raft snapshot
err := leader.Join(id, addr)       // add a voter (leader only)
err := leader.Leave(id)            // remove a voter (leader only)
```

## Consistency

- Writes are linearizable at the leader: `raft.Apply` returns only after the
  entry is committed to a quorum and applied.
- Leader reads are linearizable; follower reads are eventually consistent.
  To read your own write from a follower, wait for its applied index to reach
  the leader's, or call `Barrier` on the leader first.
- `consensus.ErrNotLeader` carries no redirect; callers fetch the leader with
  `Leader` and retry there. Auto-forwarding is deliberately not provided.

## Deployment notes

- Every node must use the **same password** in `StoreOpts`. Encryption flags
  matter only when a store is first created.
- The raft transport is **unauthenticated TCP**: run the cluster on a private
  network/VPN.
- `NodeID` must be stable across restarts. `Dir` (raft state) and `StorePath`
  (harbor data) must be local to each node and not shared.
- The raft log carries plaintext commands; treat `Dir` with the same care as
  the raft log of any raft deployment.
- A node that is far behind catches up from a snapshot automatically: the FSM
  serializes the harbor database with `bbolt.Tx.WriteTo` and a joining node
  reopens over the restored bytes.

## Limitations (v1)

- fs directory mtimes set by `Remove`/`Rename` use each node's wall clock; file
  content, names and explicit `Chtimes` converge. Full determinism needs a
  `SetNow` clock in fs (see PLAN.md).
- vec replicated writes update the durable point store but not the in-memory
  search index; reload the collection to refresh search results.
- Large file writes are single raft entries; a chunked command is a follow-up.
- Streaming `OpenFile`+`Write` and `db` raw key/value (`Set`/`SetBytes`) writes
  are not yet covered by the facades; use the path-level and record-level
  facades above.

Full design: [../PLAN.md](../PLAN.md).
