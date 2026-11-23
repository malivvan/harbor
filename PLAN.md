# Harbor-wide cryptographically sound audit trail — implementation plan

This document is the complete plan for adding a **tamper-evident, append-only
audit trail** to Harbor. Audit events are signed with Ed25519 keys derived from
the store's own key material using the newly re-added `internal/slip10`
(SLIP-0010) package — in the same way the data-store sub-keys are derived today
with `internal/slip21` (SLIP-0021). The feature is configurable per store,
mirroring how encryption flags are managed.

---

## 1. Goal and non-goals

**Goal.** Give Harbor an optional, cryptographically sound record of every
mutating operation across the four stores (`db`, `fs`, `ts`, `vec`):

- **Authenticity** — each event is signed with an Ed25519 key derived from the
  store password, so events cannot be forged or silently altered without the
  key material.
- **Order and completeness of the interior** — events form a hash chain, so an
  event cannot be deleted, reordered, or inserted in the middle without
  breaking the chain.
- **Harbor wide** — one trail covering all four stores, each signed with its
  own per-store key.

**Non-goals** (documented so they are not assumed):

- **Detecting tail truncation** by itself. Deleting the *last* N events leaves a
  self-consistent prefix. Detection requires anchoring the chain tip outside
  the store (see §9); the recorder exposes `Head` for exactly that.
- **Non-repudiation against the password holder.** The signing keys derive from
  the store password, so anyone who holds the password can produce valid
  events. The trail proves integrity *against an attacker without the key*, and
  provides an ordered, verifiable history — the same trust boundary as the
  encryption itself.
- **Per-user identity.** Events record *what* happened, not *which human* did
  it; there is no authentication layer.
- **Crash-atomic coupling of data and audit writes.** Audit events are appended
  immediately after a committed store write, in their own transaction (see §8).

---

## 2. Background: SLIP-10 vs SLIP-21

Harbor's key pipeline today (see `harbor.go` / `meta.go`):

```text
password ──Argon2id──▶ 32-byte seed ──SLIP-0021 master──▶ per-store symmetric keys
                                                          (db, fs, ts, vec, canary)
```

- **`internal/slip21` (SLIP-0021)** derives *symmetric* keys from the seed via
  HMAC-SHA512 with a per-label derivation. This is what the four sub-stores use
  to encrypt at rest.
- **`internal/slip10` (SLIP-0010)** derives *Ed25519* key pairs from the same
  kind of root seed, using hardened integer indices (`m/0'`, `m/1'`, …) and a
  per-node 32-byte chain code. Its public surface used here:

  ```go
  slip10.NewMasterNode(seed)          // Node
  slip10.DeriveForPath("m/N'", seed)  // Node
  node.KeyPair() (ed25519.PublicKey, ed25519.PrivateKey)
  node.RawSeed() []byte               // 32-byte ed25519 seed
  ```

Both schemes consume the **same 32-byte Argon2id seed**. SLIP-10 and SLIP-21
are domain-separated (different HMAC keys: `"ed25519 seed"` vs
`"Symmetric key seed"`), so deriving signing keys and encryption keys from one
seed does not couple them. This is the "similar way than the datastore sub
keys" requirement: the audit Ed25519 keys hang off the exact same seed that
already produces the per-store encryption keys.

---

## 3. Architectural decision

### 3.1 What is captured

Harbor state is one bbolt file with four stores on top. As established for the
consensus feature, there is no safe way to capture "a bbolt write" generically:
`*bolt.Tx`/`*bolt.Bucket` are concrete types and cannot be wrapped without
rewriting every closure in the stores. The audit trail therefore captures
**at the store-API level**, exactly like consensus: each store emits a
normalized event on every committed write.

### 3.2 Where the trail lives

The trail is a new top-level bucket `audit/` in the **same bbolt file**, next to
`db/`, `fs/`, `ts/`, `vec/` and the unencrypted `harbor.Metadata` record:

```text
audit/
  head        record: current event count + chain tip hash + format version
  events/     one record per event, keyed by 8-byte big-endian sequence number
```

Storing it in the same file keeps the single-file model intact and makes the
trail travel with snapshots/backups automatically.

### 3.3 Data model

```go
// Entry is what a store reports; it is the raw material, unsigned.
type Entry struct {
    Store  string    // "db" | "fs" | "ts" | "vec"
    Op     string    // e.g. "save", "write", "insert", "upsert"
    Entity string    // bucket path / file path / series / collection
    Digest []byte    // SHA-256 of the written payload; nil for deletes
    At     time.Time // when the write committed
}

// Event is the signed, hash-linked unit persisted to disk.
type Event struct {
    Seq      uint64   // monotonic, 0-based
    Store    string
    Op       string
    Entity   string
    Digest   []byte   // SHA-256 payload digest
    At       int64    // unix nanos (canonical, not time.Time)
    PrevHash []byte   // SHA-256 of the previous encoded Event (32 bytes)
}
```

The stored record is `encode(Event) ‖ signature` where `encode` is a canonical,
deterministic encoding (JSON with a fixed field order), `signature =
ed25519.Sign(key, encode(Event))`, and `PrevHash = SHA-256(encode(prevEvent))`.
Signing the *encoded event including `PrevHash` and `Seq`* binds an event to its
position and predecessor.

### 3.4 Why this is sound

- **Forge resistance:** `signature` can only be produced by the Ed25519 key
  derived from the password; there is no way to inject or alter an event.
- **Interior integrity:** `PrevHash` chains every event to its predecessor;
  deleting, reordering, or splicing events breaks a hash and thus verification.
- **Start anchoring:** the first event links to a fixed genesis hash
  (`SHA-256("harbor/audit/v1/genesis")`), so the beginning of the chain is
  anchored.
- **End anchoring (deployment):** the recorder exposes `Head()` returning the
  tip; operators periodically record the tip outside the store to detect tail
  truncation (§9).

---

## 4. Key derivation

`harbor.deriveKeys` is extended so that, from the same `seed`, it also derives
one Ed25519 key per store via SLIP-10. The mapping is positional (SLIP-10 only
supports hardened integer indices; there is no string-label API):

| Store | SLIP-10 path | Purpose |
|---|---|---|
| db  | `m/0'` | signs db audit events |
| fs  | `m/1'` | signs fs audit events |
| ts  | `m/2'` | signs ts audit events |
| vec | `m/3'` | signs vec audit events |

```go
node, _ := slip10.DeriveForPath("m/0'", seed)
pub, priv := node.KeyPair()   // ed25519.PublicKey / ed25519.PrivateKey
```

The resulting private keys are added to the existing `storeKeys` struct and
wiped alongside the symmetric keys in `storeKeys.wipe`. The `storeKeys` fields:

```go
type storeKeys struct {
    canary, db, fs, ts, vec []byte           // SLIP-0021 symmetric (existing)
    auditDB, auditFS, auditTS, auditVec ed25519.PrivateKey // SLIP-0010 (new)
}
```

No separate audit canary is needed: the existing canary already proves the seed
(and therefore the derived audit keys) is correct — a wrong password yields a
wrong seed and thus wrong audit keys, which verification then rejects.

---

## 5. The `audit` package

New public package `github.com/malivvan/harbor/audit`:

```go
package audit

type Entry struct { Store, Op, Entity string; Digest []byte; At time.Time }

type Sink interface { Record(Entry) error }
type SinkFunc func(Entry) error

// Recorder appends signed, hash-linked events to a bbolt database.
type Recorder struct { /* b *bolt.DB, keys map[string]ed25519.PrivateKey, now func() time.Time */ }
func NewRecorder(b *bolt.DB, keys map[string]ed25519.PrivateKey) (*Recorder, error)
func (r *Recorder) Record(entry Entry) error
func (r *Recorder) Head() (count uint64, tip []byte, err error)

// Verifier replays the chain and checks signatures and hashes.
func Verify(b *bolt.DB, keys map[string]ed25519.PublicKey) (Result, error)
type Result struct { Count uint64; Tip []byte }

// DeriveKey derives an Ed25519 key pair from the Argon2id seed at a path.
func DeriveKey(seed []byte, path string) (ed25519.PublicKey, ed25519.PrivateKey, error)
```

`Record` opens its own short bbolt write transaction: it reads the head,
computes the next `Seq` and `PrevHash`, encodes, signs, stores the event, and
updates the head. It is safe for concurrent use because bbolt serializes
writers, and `Record` is only invoked from store write paths.

`Verify` replays every event in sequence and returns the count and tip hash; it
fails on the first forged signature, broken link, or non-monotonic sequence.

---

## 6. Configuration (mirrors the crypto flags)

```go
// in the harbor root package (meta.go)
const (
    AuditNone uint16 = 0
    AuditDB   uint16 = 1 << iota // 1
    AuditFS                      // 2
    AuditTS                      // 4
    AuditVec                     // 8
    AuditAll = AuditDB | AuditFS | AuditTS | AuditVec
)

// harbor.Options gains one field:
type Options struct {
    // ... existing fields ...
    Audit uint16 // which stores record audit events; 0 disables
}
```

Differences from encryption flags, and why:

- **Not persisted.** Encryption flags *must* be persisted because reopening has
  to know the mode before it can decrypt. Audit flags are a per-open runtime
  choice: enabling them records events, disabling them simply stops recording.
  There is no "open in the wrong mode" hazard. Keeping them out of `Metadata`
  avoids a format change; the caller re-passes `Audit` on each `Open`.
- **No canary binding** for the same reason. The audit keys are bound to the
  seed indirectly through the existing encryption canary.
- **Default `AuditNone`.** A store is audited only when the caller opts in.

The root `harbor.Open` builds a `map[string]ed25519.PrivateKey` for the enabled
stores, constructs one `audit.Recorder` over the shared `*bolt.DB`, and passes
the recorder (as an `audit.Sink`) to the stores whose flag is set.

---

## 7. Store integration

Each store's `Config` gains an optional `Audit audit.Sink`. After a write
transaction commits, the store calls `Audit.Record(Entry{...})` when `Audit`
is non-nil. The mapping:

### 7.1 db

| Op | Entity | Digest |
|---|---|---|
| `save`, `update`, `update_field` | `path/bucket:ID` | SHA-256 of the codec-encoded record |
| `delete_struct`, `drop` | `path/bucket:ID` | nil |
| `set`, `set_bytes` | `path/bucket:key` | SHA-256 of the value |
| `delete` (kv) | `path/bucket:key` | nil |

db already has a post-commit `ChangeHook`; the audit sink is a **parallel**
path that also covers the key/value and drop operations the change hook does not
emit. The existing `ChangeHook` semantics are untouched.

### 7.2 ts

| Op | Entity | Digest |
|---|---|---|
| `insert`, `insert_many` | `series` | SHA-256 of the encoded points |
| `delete_range` | `series` | nil |
| `drop`, `delete_series` | `series` | nil |
| `set_labels`, `set_retention` | `series` | SHA-256 of the new labels/retention |

### 7.3 vec

| Op | Entity | Digest |
|---|---|---|
| `create_collection` | `collection` | SHA-256 of the collection meta |
| `upsert` | `collection` | SHA-256 of the encoded points |
| `delete` | `collection` | nil |
| `drop_collection` | `collection` | nil |

### 7.4 fs

| Op | Entity | Digest |
|---|---|---|
| `create`, `write`, `truncate` | path | SHA-256 of the written bytes (streaming: of the resulting file content where cheap) |
| `mkdir` | path | nil |
| `remove`, `remove_all`, `rename` | path | nil |
| `chmod`, `chtimes`, `chown` | path | the new mode/times/uid·gid |
| `symlink` | path | SHA-256 of the target |

fs is the broadest surface (an `absfs.Filer`). v1 instruments the path-level
operations (`Create`/`Write`/`Truncate` via `OpenFile`+`Write`, `Mkdir`,
`Remove`, `RemoveAll`, `Rename`, `Chmod`, `Chtimes`, `Chown`, `Symlink`,
`DeleteFS`); large streaming writes are digested once at `Close` (documented).

### 7.5 Not recorded

- Reads (`Get`, `Find`, `Range`, `Search`, `Stat`, `ReadFile`, …).
- Rotation/maintenance (`ReEncrypt`, `RotateAll`, `VerifyAll`,
  `EnforceRetention`, `Flush`) — these are key/material lifecycle, not
  application writes, and are listed in the docs as out of scope.

---

## 8. Concurrency and atomicity notes

- `Record` runs after the store's write transaction has committed, in its own
  transaction. bbolt's single-writer serialization makes the sequence numbers
  correct under concurrent store writes.
- A crash between the data write and the audit append loses the *trailing*
  audit event (the data is present, the event is not). This is the same model
  as the existing `db.ChangeHook` and is documented; full crash-atomic coupling
  would require in-transaction recording and is a follow-up.
- The recorder must never be called inside a store transaction; the stores call
  it strictly post-commit.

---

## 9. Threat model

Guarantees (for a verifier who derives keys from the correct password):

- Every event is signed → no forging, no altering an existing event.
- Hash chain → no deleting/reordering/splicing interior events.
- Genesis anchor → the chain start is fixed.

Not guaranteed (documented, not hidden):

- **Tail truncation** — delete the last N events and the prefix still verifies.
  Mitigation: record `Head().tip` in an external tamper-resistant place (log,
  DB, ledger) on a schedule; the `audit` package provides the tip for this.
- **Password-holder forgery** — the keys derive from the password, so the
  password holder can sign arbitrary events. This matches the encryption
  trust model.
- **Payload-content tampering** beyond what the digest allows: an attacker
  without the key cannot change a signed digest, so re-hashing current data
  against `Digest` detects content changes. Full data re-verification is a
  per-store, opt-in follow-up (`VerifyData`); v1 verifies the *chain*.
- The raft/consensus layer is unaffected: audit is a local, single-store
  concern. In a cluster each node records its own trail (documented).

---

## 10. Implementation phases and commit points

Each phase compiles and its tests pass; one commit per phase.

1. **`internal/slip10: add SLIP-0010 ed25519 derivation`** — already staged by
   the user; commit as-is (`internal/slip10/slip10.go`, `_test.go`).
2. **`audit: add the entry, event, recorder and verifier`** — `audit/audit.go`
   (+ `audit_test.go`): event encode/sign, chain append, `Verify`, genesis
   anchor, `Head`, `DeriveKey`.
3. **`harbor: derive per-store audit keys and expose audit flags`** —
   `meta.go` flag constants, `harbor.go` `storeKeys` + `deriveKeys` extension,
   `Options.Audit`, and recorder wiring in `Open` (behind the flags).
4. **`db: record audit events on writes`** — `db.Config.Audit` + emit on
   `Save`/`Update`/`UpdateField`/`DeleteStruct`/`Drop`/`Set`/`SetBytes`/`Delete`.
5. **`ts: record audit events on writes`** — `ts.Config.Audit` + emit on the
   series write operations.
6. **`vec: record audit events on writes`** — `vec.Config.Audit` + emit on
   collection operations.
7. **`fs: record audit events on writes`** — `fs.Config.Audit` + emit on the
   path-level operations.
8. **`harbor: expose verification`** — a `Store.VerifyAudit()` (or equivalent)
   plus root-level tests and the `Audit()` accessor.
9. **`docs: audit trail guide`** — README section + `audit/README.md`, threat
   model, flag table; verify snippets compile.

Commit messages are prefixed `audit:` (or `harbor:`/`db:`/`ts:`/`vec:`/`fs:`/
`docs:`) following the repo convention.

---

## 11. Testing strategy

- **Unit (audit):** encode/sign round-trip; `Record` + `Verify` on a fresh
  store; forged signature is rejected; a deleted middle event is rejected; a
  truncated tail is (by design) *not* rejected but `Head` reports the reduced
  tip; unknown store key is rejected; concurrent `Record` keeps a monotonic
  sequence.
- **Unit (keys):** the same `seed` + path yields the same Ed25519 pair
  (deterministic); different store paths yield different keys; a changed seed
  yields a different key.
- **Integration (per store):** open a store with `Audit` enabled, perform
  writes, then `Verify` over the derived public keys and assert the event count
  and contents; open with `Audit` disabled and assert no events.
- **Wrong password:** verification with a different password's keys fails.
- Run the repository gate: `gofmt`, `go vet`, `go build`, `go test`, and
  `go test -race` (audit touches shared bbolt state).

---

## 12. Risks and open questions

- **SLIP-10 node wiping:** `slip10` does not expose a `Destroy`/wipe like
  `slip21`. Intermediate SLIP-10 nodes (and the derived private keys) live in
  memory until GC. The `audit` keys are wiped by `storeKeys.wipe`; the
  intermediate `slip10.Node` values are not. Documented; adding a wipe to
  `slip10` is a small follow-up.
- **fs digest cost:** hashing large file content at `Close` adds work on the
  write path; acceptable for v1 and documented (the digest is what makes
  content tampering detectable).
- **Audit flags are runtime-only:** if this surprises users we can later persist
  them in `Metadata` with a version bump; deliberately deferred.
- **Per-store key map:** `map[string]ed25519.PrivateKey` keyed by the `Store`
  string; a typo in a store's `Entry.Store` fails closed (unknown store).
- **Tail-truncation anchoring** is a deployment practice, not built-in; the
  `Head()` API is the seam.
