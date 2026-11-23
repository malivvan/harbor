# snapshot

`snapshot` takes point-in-time, content-addressed, incremental snapshots of a set of
objects and restores them.

The core is written against a narrow `ObjectStore` interface (List/Get/Put/Delete/
Stat over prefix-keyed objects), so it has no opinion about where bytes live. The
`blobstore` subpackage adapts it to [`blob`](../blob).

```go
import (
    "github.com/malivvan/harbor/pkg/snapshot"
    "github.com/malivvan/harbor/pkg/snapshot/blobstore"
)

st := blobstore.New(myBlobStore, "app-data")
snap := snapshot.New(st, snapshot.Config{DataPrefix: "os/"})

idx, err := snap.Create(ctx, snapshot.KindManual, "")   // capture
err      = snap.Restore(ctx, idx.ID)                    // roll back
```

## How it works

- **Capture** walks every live object under `DataPrefix`, hashes the content, and
  uploads only blobs that are not already present — so a snapshot is incremental
  and deduplicated, and unchanged objects cost no bytes.
- Artifacts live under `DataPrefix + "_snapshots/"`, which is **excluded from
  capture**, so snapshots never snapshot themselves.
- Each snapshot carries a complete, gzip-compressed manifest of its objects, so it
  is independently restorable even though blob *data* is shared across snapshots.
- The manifest is hashed into the index; **restore verifies the hash before doing
  anything**, and refuses (fail-closed) on a mismatch.
- Manifest keys are validated segment by segment, so a crafted manifest cannot
  write outside the data prefix or into the reserved artifact area.
- `Restore` takes a `pre-restore` snapshot first, so a bad restore is reversible.

## Snapshot kinds

| Constant | Meaning |
|---|---|
| `KindManual` | taken on request |
| `KindScheduled` | taken by the `Scheduler` |
| `KindPreRestore` | taken automatically before a restore |

## Retention

`Prune(ctx, policy)` applies `Policy` (daily/weekly retention). `DefaultPolicy` is
used by `NewScheduler` when the policy is zero.

## Metering

Metering is an interface with **no implementation here**, because what "bytes
stored" means for billing is the application's business:

```go
type Meterer interface {
    MeterSnapshotBytes(ctx context.Context, accountID string, bytes int64)
}
```

Wire one with `Snapshotter.WithMeter`; a standalone deployment wires none and
nothing is reported. `StorageUsage` reports the bytes held, broken down by kind.

## Testing

`objectstore.go` is the whole contract, so the suite ships an in-memory
implementation (`memstore_test.go`) and never needs a database.
