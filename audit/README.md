# audit

Tamper-evident, append-only audit trail for Harbor. Every mutating operation in
the `db`, `fs`, `ts` and `vec` stores can be recorded as a signed, hash-linked
event, so the trail proves *what* changed and *in which order* — and cannot be
forged, altered, reordered or spliced without the store's key material.

## Model

```text
store write ──commit──▶ audit.Record(Entry)
                             │
                             ▼
              encode(Event) ──▶ ed25519.Sign(key, …) ──▶ events/<seq>
                             │                              │
                    PrevHash = SHA-256(encode(prev)) ───────┘
```

- **Authenticity.** Each event is signed with an Ed25519 key derived from the
  store password (SLIP-0010, one key per sub-store). An event cannot be forged
  or silently altered without the key.
- **Interior integrity.** Every event carries `PrevHash`, the SHA-256 of the
  previous encoded event. Deleting, reordering or inserting an interior event
  breaks a hash and fails verification.
- **Start anchoring.** The first event links to a fixed genesis hash,
  `SHA-256("harbor/audit/v1/genesis")`, so the beginning of the chain is fixed.

The trail lives in a top-level `audit/` bucket in the same bbolt file as the
four stores, so it travels with snapshots and backups automatically.

## Quick start

Auditing is enabled per open with `harbor.Options.Audit`, mirroring the
encryption flags:

```go
store, err := harbor.Open(ctx, "app.db", 0o600, &harbor.Options{
	Password: "correct horse battery staple",
	Audit:    harbor.AuditAll, // or AuditDB | AuditTS, …
})
if err != nil {
	log.Fatal(err)
}

// … perform writes through the sub-stores …

res, err := store.VerifyAudit()
if err != nil {
	log.Fatal(err) // forged signature, broken link, or no trail
}
log.Printf("audit: %d events, tip %x", res.Count, res.Tip)
```

| Flag | Records |
|------|---------|
| `AuditNone` | nothing (default) |
| `AuditDB` | `db` writes |
| `AuditFS` | `fs` writes |
| `AuditTS` | `ts` writes |
| `AuditVec` | `vec` writes |
| `AuditAll` | all four |

Unlike encryption flags, audit flags are **not persisted**: they are a per-open
runtime choice. Opening with a flag set records events; opening without it
simply stops recording. There is no "open in the wrong mode" hazard, so the
flags stay out of the on-disk metadata and the format is unchanged.

## Anchoring the tip

The trail detects interior tampering but **cannot by itself detect tail
truncation**: deleting the last N events leaves a self-consistent prefix. To
detect that, record the chain tip somewhere the store's writer cannot reach —
a log, a separate database, a ledger:

```go
count, tip, err := store.Audit().Head()
// persist (count, tip) out of band on a schedule
```

Later, compare the anchored tip against `VerifyAudit`'s `Result.Tip`. A tip that
is no longer a prefix of the current chain means events were removed from the
end.

## Using the package directly

The `audit` package works on any `*bolt.DB`, so a store opened without the root
package can use it too:

```go
pub, priv, err := audit.DeriveKey(seed, "m/0'") // seed = 32-byte Argon2id output
rec, err := audit.NewRecorder(bdb, map[string]ed25519.PrivateKey{audit.StoreDB: priv})

err = rec.Record(audit.Entry{
	Store:  audit.StoreDB,
	Op:     "save",
	Entity: "users:u1",
	Digest: sha256sum(payload),
})

res, err := audit.Verify(bdb, map[string]ed25519.PublicKey{audit.StoreDB: pub})
```

`Record` opens its own short write transaction: it reads the head, computes the
next sequence number and `PrevHash`, encodes, signs, stores the event and
updates the head. bbolt serializes writers, so the sequence stays correct under
concurrent store writes. `Record` must be called **after** the store's write
has committed, never inside a store transaction.

## Key derivation

The signing keys hang off the same Argon2id seed that produces the encryption
keys, using SLIP-0010 hardened paths:

| Store | Path |
|-------|------|
| `db`  | `m/0'` |
| `fs`  | `m/1'` |
| `ts`  | `m/2'` |
| `vec` | `m/3'` |

SLIP-0010 and SLIP-0021 are domain-separated (different HMAC keys), so deriving
signing keys and encryption keys from one seed does not couple them. No separate
audit canary is needed: the existing encryption canary already proves the seed,
and therefore the derived audit keys, is correct.

## What is recorded

| Store | Operations |
|-------|------------|
| `db`  | `save`, `update`, `update_field`, `delete_struct`, `drop`, `set`, `set_bytes`, `delete` |
| `fs`  | `mkdir`, `write`, `truncate`, `remove`, `remove_all`, `rename`, `chmod`, `chtimes`, `chown`, `symlink`, `delete_fs` |
| `ts`  | `insert`, `insert_many`, `delete_range`, `set_labels`, `set_retention`, `delete_series` |
| `vec` | `create_collection`, `upsert`, `delete`, `drop_collection` |

Each event carries a SHA-256 `Digest` of the written payload (nil for deletes),
so re-hashing current data against the recorded digest detects content changes.

**Not recorded:** reads (`Get`, `Find`, `Range`, `Search`, `Stat`, `ReadFile`,
…), and key/material lifecycle operations (`ReEncrypt`, `RotateAll`,
`VerifyAll`, `EnforceRetention`, `Flush`).

## Threat model

Guarantees, for a verifier holding the correct password:

- Every event is signed — no forging, no altering an existing event.
- The hash chain — no deleting, reordering or splicing interior events.
- The genesis anchor — the chain start is fixed.

Not guaranteed (by design, not by omission):

- **Tail truncation.** Delete the last N events and the prefix still verifies.
  Mitigation: anchor the tip out of band (see [Anchoring the tip](#anchoring-the-tip)).
- **Non-repudiation against the password holder.** The keys derive from the
  password, so anyone holding it can produce valid events. The trail proves
  integrity *against an attacker without the key*, the same trust boundary as
  the encryption itself.
- **Per-user identity.** Events record what happened, not which human did it.
- **Crash-atomic coupling.** An event is appended in its own transaction after
  the data write commits. A crash in between loses the *trailing* audit event
  (the data is present, the event is not) — the same model as `db.ChangeHook`.

## Verification

`audit.Verify` replays every event in sequence and returns the count and tip
hash. It fails on the first forged signature, broken link, or non-monotonic
sequence, and returns `audit.ErrNoLog` when no trail exists. At the root,
`Store.VerifyAudit()` does the same using the public keys derived at open.
