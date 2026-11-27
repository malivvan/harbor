# ledger

Tamper-evident, append-only audit trail for tempest. Every mutating operation in
the `record`, `file`, `series` and `vector` stores can be recorded as a signed, hash-linked
event, so the trail proves *what* changed and *in which order* — and cannot be
forged, altered, reordered or spliced without the store's key material.

## Model

```text
store write ──commit──▶ ledger.Record(Entry)
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
  `SHA-256("tempest/audit/v1/genesis")`, so the beginning of the chain is fixed.

The trail lives in a top-level `ledger/` bucket (`bucket.LEDGER`) in the same
bbolt file as the stores, so it travels with snapshots and backups
automatically.

## Quick start

Auditing is enabled per open with `tempest.Options.Audit`, mirroring the
encryption flags:

```go
store, err := tempest.Open(ctx, "app.db", 0o600, &tempest.Options{
	Password: "correct horse battery staple",
	Audit:    tempest.AuditAll, // or AuditRecord | AuditSeries, …
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
| `AuditRecord` | `record` writes |
| `AuditFile` | `file` writes |
| `AuditSeries` | `series` writes |
| `AuditVector` | `vector` writes |
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
end. A scheduler that calls `Head` every few minutes and writes the pair to an
append-only log outside the store is enough; there is no helper in tempest
because the destination is the operator's choice.

## Using the package directly

The `ledger` package works on any `*bolt.DB`, so a store opened without the root
package can use it too:

```go
pub, priv, err := ledger.DeriveKey(seed, "m/0'") // seed = 32-byte Argon2id output
rec, err := ledger.NewRecorder(bdb, map[string]ed25519.PrivateKey{ledger.StoreRecord: priv})

err = rec.Record(ledger.Entry{
	Store:  ledger.StoreRecord,
	Op:     "save",
	Entity: "users:u1",
	Digest: sha256sum(payload),
})

res, err := ledger.Verify(bdb, map[string]ed25519.PublicKey{ledger.StoreRecord: pub})
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
| record (`record`) | `m/0'` |
| file (`fstree`) | `m/1'` |
| series (`series`) | `m/2'` |
| vector (`vector`) | `m/3'` |

SLIP-0010 and SLIP-0021 are domain-separated (different HMAC keys), so deriving
signing keys and encryption keys from one seed does not couple them. No separate
audit canary is needed: the existing encryption canary already proves the seed,
and therefore the derived audit keys, is correct.

## What is recorded

| Store | Operations |
|-------|------------|
| record (`record`) | `save`, `update`, `update_field`, `delete_struct`, `drop`, `set`, `set_bytes`, `delete` |
| file (`fstree`) | `mkdir`, `write`, `truncate`, `remove`, `remove_all`, `rename`, `copy`, `chmod`, `chtimes`, `chown`, `symlink`, `delete_fs` |
| series (`series`) | `insert`, `insert_many`, `delete_range`, `set_labels`, `set_retention`, `delete_series` |
| vector (`vector`) | `create_collection`, `upsert`, `delete`, `drop_collection` |

Each event carries a SHA-256 `Digest` of the written payload (nil for deletes),
so re-hashing current data against the recorded digest detects content changes.

## Event encoding

An event is encoded with `compact` — deterministic JSON with keys
sorted at every depth and no insignificant whitespace — and **those exact bytes
are what is signed and stored** (with the Ed25519 signature appended). The hash
chain is SHA-256 over the same bytes, so verifiers do not need a
canonicalisation step and a future event field holding a map still has one
signature per logical value. `TestEventEncodingIsCanonical` pins the stored
bytes to the canonical form.

`record.Set` records `set` and `record.SetBytes` records `set_bytes`: both write the same
encoded value, but the operation name preserves the caller's intent. Higher-level
packages are audited at the layer that performs the write: `backup` writes
through the object store, so its writes appear as `object` events; the `store`
manager only opens stores and records nothing itself.

**Not recorded:** reads (`Get`, `Find`, `Range`, `Search`, `Stat`, `ReadFile`,
…), key/material lifecycle operations (`ReEncrypt`, `RotateAll`, `VerifyAll`,
`EnforceRetention`, `Flush`) and higher-level copy paths beyond the single
`copy` event. Re-hashing current records and comparing them to the recorded
digests (`VerifyData`) is out of scope for v1; the digest is present for a
future tool.

Audit flags are a per-open runtime choice and are **not persisted**. If they
must survive a restart, the application has to open the store with the same
flags; changing that would require a metadata version bump and a canary
binding.

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
  (the data is present, the event is not) — the same model as `record.ChangeHook`.

## Verification

`ledger.Verify` replays every event in sequence and returns the count and tip
hash. It fails on the first forged signature, broken link, or non-monotonic
sequence, and returns `ledger.ErrNoLog` when no trail exists. At the root,
`Store.VerifyAudit()` does the same using the public keys derived at open.
