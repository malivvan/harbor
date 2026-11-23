# blob

`blob` is a flat object store on top of a Harbor file system.

Harbor's `fs` is hierarchical; most applications want a flat blob namespace
instead — "put these bytes at `tenant/2024/invoice.pdf` and give them back". A
bucket is a top-level directory, a key is the path beneath it, and the pair maps to
exactly one file.

```go
import "github.com/malivvan/harbor/blob"

s, err := blob.Open(st.FS(), "objects") // st is a *harbor.Store
if err != nil {
    log.Fatal(err)
}

_, err = s.Put(ctx, "canopy-u1", "drive/notes.txt", strings.NewReader("hello"), 5, "text/plain")
data, err := s.ReadAll(ctx, "canopy-u1", "drive/notes.txt")
```

## Containment

A key is always interpreted relative to its bucket, and **a key containing a `..`
segment is rejected** rather than rewritten. That makes the bucket a real boundary:
no key can address a file in another bucket, however it is spelled. Bucket names
must be a single path element.

## Operations

| Method | Behaviour |
|---|---|
| `Put` | Creates parent directories. `size`/`contentType` are accepted and ignored — the file system needs no length and carries no content type. |
| `Get` / `ReadAll` | `Get` returns a reader the caller closes plus the size (`-1` if unknown). |
| `Delete` | A missing object is a **no-op**, so cleanup sweeps do not fail. |
| `Move` | Creates the destination's parents. A **missing source is a no-op** — an index record can exist before its bytes are uploaded. |
| `Stat` | Returns `(Info, exists, err)`; a missing object is not an error. |
| `List` | Recursive under a key prefix; empty prefix lists the whole bucket; a prefix matching nothing returns an empty slice. |
| `Exists` | Convenience over `Stat`. |

`Info.ETag` is a **version marker** derived from the stored file's size and
modification time, not a content hash: Harbor's file system has no content ETag, so
it distinguishes versions of one key rather than naming the content. `Put` returns
an empty ETag; call `Stat` if you need the marker.

## Grants

`Grant` and `Minter` are bookkeeping only — they record *which* object was
authorized, for which method, and until when:

```go
m := blob.NewMinter(15 * time.Minute)
g := m.Mint(bucket, key, "GET", 0) // g.ExpiresAt = now + ttl
```

Deciding *whether* a caller may be granted stays in the application, because that is
where the access-control data lives. A grant is not a credential and carries no
secret.
