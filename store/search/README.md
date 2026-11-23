# search

`search` is a full-text index for Harbor: one document per id, BM25 ranking, and
tokenizers for word text and CJK scripts. The package was historically named
`fts`; its key label (`harbor/fts/v1`) keeps that name, so existing indexes stay
readable. It lives under the `search` root bucket (`store.SEARCH`) in the same
bbolt file and
participates in [cross-store transactions](../../README.md#cross-store-transactions).

```go
idx, err := store.Search().Open("articles", search.Config{})
if err != nil {
    log.Fatal(err)
}

err = idx.Upsert(ctx, "article-42", "Harbor keeps records, files, series and vectors")
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
