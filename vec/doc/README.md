# vec/doc

`vec/doc` is a document-oriented facade over `vec`.

`vec` is point-oriented: a point has an ID, a vector and a payload of
`map[string]any`. Embedding callers usually want something simpler — an id, the
text that was embedded, the embedding, and a map of **string** metadata — and they
do not know the collection's dimension until the first embedding arrives.

```go
import (
    "github.com/malivvan/harbor/vec/doc"
)

idx, err := doc.Open(st.Vec(), "recall")
if err != nil {
    log.Fatal(err)
}

err = idx.Upsert(&doc.Document{
    ID:        "file-42",
    Content:   "quarterly report",
    Embedding: embedding,          // []float32
    Metadata:  map[string]string{"path": "docs/q3.md"},
})

hits, err := idx.Search(queryEmbedding, 10) // []doc.SearchResult, best first
```

## Lazy dimension

A `vec` collection needs a fixed dimension when it is created, but an embedding
caller does not know it until the first embedding exists. `Index` therefore creates
the collection from the **first `Upsert`'s vector length** and afterwards reopens it
with the configuration stored in its metadata. A later vector of the wrong length is
rejected by `vec` instead of being silently reinterpreted.

Opening an `Index` touches no data, so it is cheap to create one per request.

## API

| Method | Notes |
|---|---|
| `Upsert` | Creates the collection on first write; replaces an existing ID. |
| `Get` | `(*Document, exists, error)`. Reads the in-memory index, so it is O(1) and safe inside a scan loop. |
| `Search` | Top-k by cosine similarity, best first. An empty index returns no results, not an error. |
| `Delete` | Deleting an absent ID — or from an index never written — is a no-op. |
| `All` | Every document, unspecified order. |
| `Count` | Number of documents. |
| `Flush` | Persists buffered writes. |

## Payload layout

A document is one point: the embedding is the vector, and `{content, metadata}`
ride in the payload. Metadata is carried as a JSON string because Harbor payloads
hold arbitrary JSON while this model is string-to-string. A corrupt metadata blob
keeps the document with empty metadata rather than dropping the hit.
