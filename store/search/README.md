# search

`search` is a full-text index for tempest: one document per id, BM25 ranking, and
tokenizers for word text and CJK scripts. The package was historically named
`fts`; its key label (`tempest/fts/v1`) keeps that name, so existing indexes stay
readable. It lives under the `search` root bucket (`bucket.SEARCH`) in the same
bbolt file and
participates in [cross-store transactions](../../README.md#cross-store-transactions).

```go
idx, err := store.Search().Open("articles", search.Config{})
if err != nil {
    log.Fatal(err)
}

err = idx.Upsert(ctx, "article-42", "tempest keeps records, files, series and vectors")
hits, err := idx.Search(ctx, "vectors", 10) // []search.Hit, best first
```

## How it works

- Text is tokenized into terms. The index stores a posting list per term
  (`terms/`) and a record per document (`docs/`) holding its length and term
  frequencies.
- `Upsert` replaces any previous version of the document atomically: old
  postings are removed before new ones are written, and the corpus count and
  total length are adjusted in the same transaction.
- `Delete` removes the postings and the document, and updates the statistics.
  Deleting an unknown id is a no-op.
- `Search` scores candidates with BM25 (`DefaultK1` 1.2, `DefaultB` 0.75),
  orders by score descending and then id, and returns at most `limit` hits. A
  non-positive limit returns every match.
- `Count` reads the persisted document count.

## Tokenizers

| Tokenizer | Behaviour |
|---|---|
| `search.Words()` (default) | Maximal runs of Unicode letters and digits, lowercased. |
| `search.WordsWithCJK()` | `Words` plus overlapping trigrams for runs of Han, Hiragana, Katakana and Hangul; a run shorter than three characters contributes the run itself. |

The tokenizer and the BM25 parameters are recorded in the index metadata and
checked on reopen: opening an index with a different tokenizer, or with a
non-zero `K1`/`B` that differs, returns `search.ErrInvalidConfig`. A custom
`Tokenizer` has no persisted identity; the caller is responsible for using the
same one.

## Export, restore and rotation

- `Index.Meta`, `Index.Docs` and `Index.Terms` read the persisted index as data:
  the corpus statistics, one record per document (id, length, term frequencies)
  and one posting list per term. The original text is never stored, so it is not
  recoverable.
- `Store.RestoreIndex` creates or replaces an index from those three pieces in
  one write transaction, under the destination store's own encryption. Built-in
  tokenizers round-trip by name; an index created with a custom tokenizer needs
  the caller to supply it (`ImportOptions.SearchTokenizer` in the root package)
  and fails with `ErrInvalidConfig` otherwise.
- `Store.Drop` removes an index and all its data; a missing index is a no-op.
- `Store.RotateAll` re-encrypts the metadata, every document, every posting list
  and any SIV-encrypted term key under a new 32-byte key, then adopts it.
  `Store.VerifyAll` re-reads them and reports `crypto.ErrAuthFailed` on any value
  that does not authenticate. `Store.ReEncryptIndex` rotates one index and keeps
  the previous key as a fallback, but it is rejected when `EncryptTokens` is
  enabled: the deterministic term keys are derived from one master key, so the
  indexes that were not rewritten could no longer be found.
- `Config.Trace` receives one event per committed index write (`create_index`,
  `upsert`, `delete`, `drop_index`) under the `search` store identifier.
- Opening an index without supplying a tokenizer adopts the persisted built-in
  tokenizer, so export and verification do not need to know how the index was
  created.

## Compression

`Config.Compress` Zstd-compresses documents, posting lists and index metadata
before sealing; `CompressLevel` and `MinCompress` tune it. Term keys (plaintext
or deterministic SIV) are never compressed. Reads auto-detect the envelope, so a
compressed index stays readable with `Compress` disabled, and import/rotation
preserve the setting.

## Errors and format

- The format version is `search.FormatVersion` (1). An index written by another
  version fails with `search.ErrIncompatibleVersion`.
- Values are sealed when `Config.EncryptValues` is set; the root store enables
  it with the `EncryptRecordValues` flag. `Config.EncryptTokens` additionally
  encrypts term keys deterministically, hiding the vocabulary while still
  leaking term equality and result sizes. It is off by default.
- The encryption mode is a plaintext record, so opening an index with the wrong
  mode fails with `search.ErrWrongMode` instead of returning decoding errors.

## Layout

```text
fts/                          root configured by the root package
  <index>/
    __version__               8-byte format revision
    __mode__                  plaintext encryption-mode bits
    __meta__                  document count, total length, K1/B, tokenizer
    docs/<id>                 length and term frequencies
    terms/<term>              posting list (term keys may be encrypted)
```

`Store.Indexes` lists the index names. `Store.Close` marks a standalone store
closed and never closes the shared bbolt handle.
