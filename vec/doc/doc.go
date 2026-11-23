// Package doc is a document-oriented facade over Harbor's vector store.
//
// `vec` is point-oriented: a point has an ID, a vector and a payload of
// map[string]any. Callers modelling text embeddings usually want something
// simpler — an id, the text that was embedded, the embedding, and a map of string
// metadata — and they do not want to know the collection's dimension in advance.
// This package provides exactly that.
//
// # Lazy dimension
//
// A vector collection has a fixed dimension that must be given when it is
// created, but an embedding caller does not know it until the first embedding
// arrives. Index therefore creates the collection from the first Upsert's vector
// length and, from then on, reopens it with the configuration stored in its
// metadata. A mismatched later vector is rejected by `vec` rather than silently
// reinterpreted.
//
// # Payload layout
//
// A document is stored as one point: the embedding is the vector, and
// {content, metadata} ride in the payload. Metadata is carried as a JSON string
// because Harbor payloads hold arbitrary JSON while this model is
// string-to-string.
package doc

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/malivvan/harbor/vec"
)

// Payload keys used for a document's two non-vector fields.
const (
	payloadContent = "content"
	payloadMeta    = "meta"
)

// Document is one indexed item.
type Document struct {
	ID        string            `json:"id"`
	Content   string            `json:"content"`
	Embedding []float32         `json:"embedding"`
	Metadata  map[string]string `json:"metadata"`
}

// SearchResult is a document with its similarity score, best first.
type SearchResult struct {
	Document
	Score float64 `json:"score"`
}

// Index is one named document collection in the Harbor vector store.
//
// An Index is safe for concurrent use. Its collection is created on first write
// and reopened from stored metadata afterwards, so opening an Index is cheap and
// touches no data.
type Index struct {
	store *vec.Store
	name  string

	mu  sync.Mutex
	col *vec.Collection
}

// Open binds to the named collection in store. It does not create the collection;
// the first Upsert does.
func Open(store *vec.Store, name string) (*Index, error) {
	if store == nil {
		return nil, errors.New("doc: nil vector store")
	}
	if name == "" {
		return nil, errors.New("doc: empty collection name")
	}
	return &Index{store: store, name: name}, nil
}

// Name reports the collection name.
func (i *Index) Name() string { return i.name }

// collection returns the open collection, creating it for dim on first use.
func (i *Index) collection(dim int) (*vec.Collection, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.col != nil {
		return i.col, nil
	}
	if dim <= 0 {
		return nil, fmt.Errorf("doc: %q has no vectors yet", i.name)
	}

	if meta, err := i.store.LoadCollectionMeta(i.name); err == nil {
		col, cerr := i.store.NewCollection(i.name, meta.VectorLen, meta.Metric, meta.UseHNSW)
		if cerr != nil {
			return nil, fmt.Errorf("doc: reopen collection %q: %w", i.name, cerr)
		}
		i.col = col
		return col, nil
	}

	col, err := i.store.NewCollection(i.name, dim, vec.Cosine, false)
	if err != nil {
		return nil, fmt.Errorf("doc: create collection %q: %w", i.name, err)
	}
	i.col = col
	return col, nil
}

// existing is collection() without requiring a dimension: it reports (nil, nil)
// when the collection has never been written, which callers treat as "nothing
// indexed".
func (i *Index) existing() (*vec.Collection, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.col != nil {
		return i.col, nil
	}
	meta, err := i.store.LoadCollectionMeta(i.name)
	if err != nil {
		return nil, nil // not created yet
	}
	col, cerr := i.store.NewCollection(i.name, meta.VectorLen, meta.Metric, meta.UseHNSW)
	if cerr != nil {
		return nil, fmt.Errorf("doc: reopen collection %q: %w", i.name, cerr)
	}
	i.col = col
	return col, nil
}

// Upsert adds or replaces a document. Its embedding dimension fixes the
// collection's dimension on the first write.
func (i *Index) Upsert(d *Document) error {
	if d == nil || d.ID == "" {
		return errors.New("doc: document has no id")
	}
	col, err := i.collection(len(d.Embedding))
	if err != nil {
		return err
	}
	meta := d.Metadata
	if meta == nil {
		meta = map[string]string{}
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("doc: encode metadata for %q: %w", d.ID, err)
	}
	return col.Upsert([]vec.PointStruct{{
		ID:     d.ID,
		Vector: d.Embedding,
		Payload: vec.Payload{
			payloadContent: d.Content,
			payloadMeta:    string(raw),
		},
	}})
}

// Delete removes a document by ID. Deleting an absent ID is not an error, and
// deleting from an index that was never written is a no-op.
func (i *Index) Delete(id string) error {
	col, err := i.existing()
	if err != nil || col == nil {
		return err
	}
	_, err = col.Delete([]string{id}, nil)
	return err
}

// Get returns the document with the given id. The bool reports presence: a
// missing document is not an error.
//
// This reads the in-memory index, so it is O(1) and safe to call inside a scan
// loop as a "have I already indexed this?" check.
func (i *Index) Get(id string) (*Document, bool, error) {
	col, err := i.existing()
	if err != nil || col == nil {
		return nil, false, err
	}
	point, err := col.Get(id)
	if err != nil {
		if errors.Is(err, vec.ErrPointNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("doc: get %q: %w", id, err)
	}
	d, ok := fromPayload(point.ID, point.Payload)
	if !ok {
		return nil, false, nil
	}
	d.Embedding = point.Vector
	return &d, true, nil
}

// Search returns the topK documents most similar to emb, best first. An empty
// index or an empty query vector returns no results rather than an error.
func (i *Index) Search(emb []float32, topK int) ([]SearchResult, error) {
	col, err := i.existing()
	if err != nil || col == nil {
		return nil, err
	}
	if len(emb) == 0 {
		return nil, nil
	}
	scored, err := col.Search(emb, nil, topK)
	if err != nil {
		return nil, fmt.Errorf("doc: search %q: %w", i.name, err)
	}
	out := make([]SearchResult, 0, len(scored))
	for _, sp := range scored {
		d, ok := fromPayload(sp.ID, sp.Payload)
		if !ok {
			continue
		}
		out = append(out, SearchResult{Document: d, Score: float64(sp.Score)})
	}
	return out, nil
}

// All returns every stored document in unspecified order.
func (i *Index) All() ([]*Document, error) {
	col, err := i.existing()
	if err != nil || col == nil {
		return nil, err
	}
	points, err := col.GetPointsByFilter(nil)
	if err != nil {
		return nil, fmt.Errorf("doc: list %q: %w", i.name, err)
	}
	out := make([]*Document, 0, len(points))
	for _, p := range points {
		if d, ok := fromPayload(p.ID, p.Payload); ok {
			d.Embedding = p.Vector
			out = append(out, &d)
		}
	}
	return out, nil
}

// Count reports how many documents are indexed.
func (i *Index) Count() int {
	col, err := i.existing()
	if err != nil || col == nil {
		return 0
	}
	return col.Count()
}

// Flush writes buffered points to the database. `vec` batches writes, so a
// process that stops between passes should call this. It is a no-op when nothing
// is buffered or the collection has never been written.
func (i *Index) Flush() error {
	col, err := i.existing()
	if err != nil || col == nil {
		return err
	}
	return col.Flush()
}

// fromPayload rebuilds a Document from a point's payload.
func fromPayload(id string, p vec.Payload) (Document, bool) {
	d := Document{ID: id, Metadata: map[string]string{}}
	if p == nil {
		return d, true
	}
	if c, ok := p[payloadContent].(string); ok {
		d.Content = c
	}
	if raw, ok := p[payloadMeta].(string); ok && raw != "" {
		if err := json.Unmarshal([]byte(raw), &d.Metadata); err != nil {
			// A corrupt metadata blob must not lose the content: keep the
			// document with empty metadata rather than dropping the hit.
			d.Metadata = map[string]string{}
		}
	}
	return d, true
}
