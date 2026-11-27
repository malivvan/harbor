package search

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// Reserved record and bucket names inside one index bucket.
const (
	metaKey     = "__meta__"
	versionKey  = "__version__"
	modeKey     = "__mode__"
	docsBucket  = "docs"
	termsBucket = "terms"

	// Mode bits stored in modeKey. They are plaintext so an open with the wrong
	// encryption configuration fails closed before any value is decoded.
	modeValues byte = 1 << 0
	modeTokens byte = 1 << 1
)

// indexMeta is the persisted index metadata.
type indexMeta struct {
	DocCount  int64   `json:"docs"`
	TotalLen  int64   `json:"total_len"`
	K1        float64 `json:"k1"`
	B         float64 `json:"b"`
	Tokenizer string  `json:"tokenizer"`
}

// docRecord is one indexed document.
type docRecord struct {
	Length    int            `json:"len"`
	TF        map[string]int `json:"tf"`
	ExpiresAt int64          `json:"expires_at,omitempty"`
}

// termRecord is one posting list.
type termRecord struct {
	Docs []string `json:"docs"`
}

// Index is one named full-text index.
type Index struct {
	store     *Store
	name      string
	tokenizer Tokenizer
	k1        float64
	b         float64
}

// Open opens or creates the named index. Settings not supplied (a nil
// Tokenizer, zero K1/B) take the defaults for a new index and the persisted
// values for an existing one. A non-zero setting that differs from what the
// index was created with returns ErrInvalidConfig: the tokenizer and BM25
// parameters are part of the on-disk format.
func (s *Store) Open(name string, cfg Config) (*Index, error) {
	if s == nil {
		return nil, ErrNilParam
	}
	if name == "" {
		return nil, fmt.Errorf("%w: empty", ErrInvalidName)
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	if err := validateBM25(cfg.K1, cfg.B); err != nil {
		return nil, err
	}

	idx := &Index{
		store:     s,
		name:      name,
		tokenizer: resolveTokenizer(cfg.Tokenizer),
		k1:        cfg.K1,
		b:         cfg.B,
	}
	if idx.k1 == 0 {
		idx.k1 = DefaultK1
	}
	if idx.b == 0 {
		idx.b = DefaultB
	}

	readOnly := s.readOnly || (s.tx != nil && !s.tx.Writable())
	run := s.update
	if readOnly {
		run = s.view
	}
	created := false
	err := run(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		var err error
		if readOnly {
			b, err = s.indexBucket(tx, name)
		} else {
			b, err = s.createIndexBucket(tx, name)
		}
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, name)
		}
		if !readOnly {
			if _, err := b.CreateBucketIfNotExists([]byte(docsBucket)); err != nil {
				return err
			}
			if _, err := b.CreateBucketIfNotExists([]byte(termsBucket)); err != nil {
				return err
			}
		} else if b.Bucket([]byte(docsBucket)) == nil || b.Bucket([]byte(termsBucket)) == nil {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, name)
		}
		if err := s.checkVersion(b, readOnly); err != nil {
			return err
		}
		if err := s.checkMode(b, readOnly); err != nil {
			return err
		}
		meta, found, err := s.loadMeta(b)
		if err != nil {
			return err
		}
		if !found {
			if readOnly {
				return fmt.Errorf("%w: %q", ErrIndexNotFound, name)
			}
			meta = indexMeta{
				K1:        idx.k1,
				B:         idx.b,
				Tokenizer: tokenizerName(cfg.Tokenizer),
			}
			created = true
			return s.saveMeta(b, meta)
		}
		// A caller that did not ask for a tokenizer gets the persisted built-in
		// one, so opening an index for export or verification does not need to
		// know how it was created. A custom tokenizer cannot be reconstructed
		// by name and keeps the default until the caller supplies it.
		if cfg.Tokenizer == nil {
			if tok, ok := tokenizerByName(meta.Tokenizer); ok {
				idx.tokenizer = tok
			}
		}
		return idx.adoptMeta(meta)
	})
	if err != nil {
		return nil, err
	}
	if created {
		if err := s.traceEntry("create_index", name, nil); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

// adoptMeta validates persisted settings against the requested ones.
func (idx *Index) adoptMeta(meta indexMeta) error {
	if meta.K1 > 0 {
		idx.k1 = meta.K1
	}
	if meta.B > 0 {
		idx.b = meta.B
	}
	if meta.Tokenizer != "" && meta.Tokenizer != "custom" {
		if got := tokenizerName(idx.tokenizer); got != "custom" && got != meta.Tokenizer {
			return fmt.Errorf("%w: index %q uses tokenizer %q, not %q",
				ErrInvalidConfig, idx.name, meta.Tokenizer, got)
		}
	}
	return nil
}

// validateBM25 checks the BM25 parameter range.
func validateBM25(k1, b float64) error {
	if k1 < 0 {
		return fmt.Errorf("%w: K1 must not be negative", ErrInvalidConfig)
	}
	if b < 0 || b > 1 {
		return fmt.Errorf("%w: B must be between 0 and 1", ErrInvalidConfig)
	}
	return nil
}

// checkVersion validates the persisted format version, writing version 1 for a
// new index.
func (s *Store) checkVersion(b *bolt.Bucket, readOnly bool) error {
	raw := b.Get([]byte(versionKey))
	if raw == nil {
		if readOnly {
			return fmt.Errorf("%w: missing version record", ErrIncompatibleVersion)
		}
		var buf [8]byte
		for i := range buf {
			buf[i] = byte(FormatVersion >> (8 * i))
		}
		return b.Put([]byte(versionKey), buf[:])
	}
	if len(raw) != 8 {
		return fmt.Errorf("%w: malformed version record", ErrIncompatibleVersion)
	}
	var v uint64
	for i := len(raw) - 1; i >= 0; i-- {
		v = v<<8 | uint64(raw[i])
	}
	if v != FormatVersion && v != FormatVersionExpiry {
		return fmt.Errorf("%w: index format %d, this build supports %d", ErrIncompatibleVersion, v, FormatVersionExpiry)
	}
	return nil
}

// setVersion writes the on-disk format revision.
func (s *Store) setVersion(b *bolt.Bucket, version uint64) error {
	var buf [8]byte
	for i := range buf {
		buf[i] = byte(version >> (8 * i))
	}
	return b.Put([]byte(versionKey), buf[:])
}

// checkMode validates the plaintext encryption-mode record.
func (s *Store) checkMode(b *bolt.Bucket, readOnly bool) error {
	want := byte(0)
	if enc := s.encState(); enc != nil {
		if enc.encValues {
			want |= modeValues
		}
		if enc.encTokens {
			want |= modeTokens
		}
	}
	raw := b.Get([]byte(modeKey))
	if raw == nil {
		if readOnly {
			return ErrWrongMode
		}
		return b.Put([]byte(modeKey), []byte{want})
	}
	if len(raw) != 1 {
		return fmt.Errorf("%w: malformed mode record (%d bytes)", ErrWrongMode, len(raw))
	}
	if raw[0] != want {
		return fmt.Errorf("%w: stored mode %#x, configured %#x", ErrWrongMode, raw[0], want)
	}
	return nil
}

// loadMeta reads the index metadata. found is false when the record is absent.
func (s *Store) loadMeta(b *bolt.Bucket) (indexMeta, bool, error) {
	var meta indexMeta
	raw := b.Get([]byte(metaKey))
	if raw == nil {
		return meta, false, nil
	}
	plain, err := s.openValue(raw)
	if err != nil {
		return meta, false, err
	}
	if err := json.Unmarshal(plain, &meta); err != nil {
		return meta, false, fmt.Errorf("search: decode metadata: %w", err)
	}
	return meta, true, nil
}

// saveMeta writes the index metadata.
func (s *Store) saveMeta(b *bolt.Bucket, meta indexMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("search: encode metadata: %w", err)
	}
	sealed, err := s.seal(raw)
	if err != nil {
		return err
	}
	return b.Put([]byte(metaKey), sealed)
}

// bucket returns the index bucket and its sub-buckets.
func (idx *Index) bucket(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, *bolt.Bucket, error) {
	b, err := idx.store.indexBucket(tx, idx.name)
	if err != nil {
		return nil, nil, nil, err
	}
	if b == nil {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
	}
	docs := b.Bucket([]byte(docsBucket))
	terms := b.Bucket([]byte(termsBucket))
	if docs == nil || terms == nil {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
	}
	return b, docs, terms, nil
}

// loadDoc reads a document record. found is false when the id is absent.
func (idx *Index) loadDoc(docs *bolt.Bucket, id string) (docRecord, bool, error) {
	var doc docRecord
	raw := docs.Get([]byte(id))
	if raw == nil {
		return doc, false, nil
	}
	plain, err := idx.store.openValue(raw)
	if err != nil {
		return doc, false, err
	}
	if err := json.Unmarshal(plain, &doc); err != nil {
		return doc, false, fmt.Errorf("search: decode document %q: %w", id, err)
	}
	if doc.TF == nil {
		doc.TF = map[string]int{}
	}
	return doc, true, nil
}

// saveDoc writes a document record.
func (idx *Index) saveDoc(docs *bolt.Bucket, id string, doc docRecord) error {
	raw, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("search: encode document %q: %w", id, err)
	}
	sealed, err := idx.store.seal(raw)
	if err != nil {
		return err
	}
	return docs.Put([]byte(id), sealed)
}

// loadTerm reads a posting list. found is false when the term is absent.
func (idx *Index) loadTerm(terms *bolt.Bucket, term string) (termRecord, bool, error) {
	var rec termRecord
	key, err := idx.store.termKey(term)
	if err != nil {
		return rec, false, err
	}
	raw := terms.Get(key)
	if raw == nil {
		return rec, false, nil
	}
	plain, err := idx.store.openValue(raw)
	if err != nil {
		return rec, false, err
	}
	if err := json.Unmarshal(plain, &rec); err != nil {
		return rec, false, fmt.Errorf("search: decode posting list: %w", err)
	}
	return rec, true, nil
}

// saveTerm writes a posting list, deleting the record when it becomes empty.
func (idx *Index) saveTerm(terms *bolt.Bucket, term string, rec termRecord) error {
	key, err := idx.store.termKey(term)
	if err != nil {
		return err
	}
	if len(rec.Docs) == 0 {
		return terms.Delete(key)
	}
	sort.Strings(rec.Docs)
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("search: encode posting list: %w", err)
	}
	sealed, err := idx.store.seal(raw)
	if err != nil {
		return err
	}
	return terms.Put(key, sealed)
}

// addPosting inserts id into the term's posting list.
func (idx *Index) addPosting(terms *bolt.Bucket, term, id string) error {
	rec, found, err := idx.loadTerm(terms, term)
	if err != nil {
		return err
	}
	if !found {
		rec = termRecord{}
	}
	if !containsID(rec.Docs, id) {
		rec.Docs = append(rec.Docs, id)
	}
	return idx.saveTerm(terms, term, rec)
}

// removePosting deletes id from the term's posting list.
func (idx *Index) removePosting(terms *bolt.Bucket, term, id string) error {
	rec, found, err := idx.loadTerm(terms, term)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	out := rec.Docs[:0]
	for _, d := range rec.Docs {
		if d != id {
			out = append(out, d)
		}
	}
	rec.Docs = out
	return idx.saveTerm(terms, term, rec)
}

func containsID(ids []string, id string) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

// Upsert indexes text under id, replacing any previous version of the document.
// An empty text indexes an empty document: it counts toward the corpus but
// matches no query.
func (idx *Index) Upsert(ctx context.Context, id, text string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("%w: document id is required", ErrNilParam)
	}

	tokens := idx.tokenizer.Tokenize(text)
	tf := make(map[string]int, len(tokens))
	for _, term := range tokens {
		tf[term]++
	}

	if err := idx.store.update(func(tx *bolt.Tx) error {
		b, docs, terms, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		meta, found, err := idx.store.loadMeta(b)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
		}

		old, exists, err := idx.loadDoc(docs, id)
		if err != nil {
			return err
		}
		if exists {
			for term := range old.TF {
				if err := idx.removePosting(terms, term, id); err != nil {
					return err
				}
			}
			meta.DocCount--
			meta.TotalLen -= int64(old.Length)
		}

		for term := range tf {
			if err := idx.addPosting(terms, term, id); err != nil {
				return err
			}
		}
		if err := idx.clearExpiry(tx, b, id); err != nil {
			return err
		}
		if err := idx.saveDoc(docs, id, docRecord{Length: len(tokens), TF: tf}); err != nil {
			return err
		}
		meta.DocCount++
		meta.TotalLen += int64(len(tokens))
		return idx.store.saveMeta(b, meta)
	}); err != nil {
		return err
	}
	return idx.store.traceEntry("upsert", idx.name+":"+id, digestEvent(id, text))
}

// Delete removes documents from the index. Unknown ids are ignored.
func (idx *Index) Delete(ctx context.Context, ids ...string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}

	if err := idx.store.update(func(tx *bolt.Tx) error {
		b, docs, terms, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		meta, found, err := idx.store.loadMeta(b)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
		}
		changed := false
		for i, id := range ids {
			if i%256 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			doc, exists, err := idx.loadDoc(docs, id)
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			for term := range doc.TF {
				if err := idx.removePosting(terms, term, id); err != nil {
					return err
				}
			}
			if err := docs.Delete([]byte(id)); err != nil {
				return err
			}
			if err := idx.clearExpiry(tx, b, id); err != nil {
				return err
			}
			meta.DocCount--
			meta.TotalLen -= int64(doc.Length)
			changed = true
		}
		if !changed {
			return nil
		}
		return idx.store.saveMeta(b, meta)
	}); err != nil {
		return err
	}
	return idx.store.traceEntry("delete", idx.name, nil)
}

// Hit is one search result.
type Hit struct {
	// ID is the indexed document id.
	ID string
	// Score is the BM25 score; larger is more relevant.
	Score float64
}

// Search returns at most limit documents matching query, best first. A
// non-positive limit returns every match. Ties are ordered by id.
func (idx *Index) Search(ctx context.Context, query string, limit int) ([]Hit, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	terms := uniqueTerms(idx.tokenizer.Tokenize(query))
	if len(terms) == 0 {
		return nil, nil
	}

	var hits []Hit
	err := idx.store.view(func(tx *bolt.Tx) error {
		b, docs, termsBucket, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		meta, found, err := idx.store.loadMeta(b)
		if err != nil {
			return err
		}
		if !found || meta.DocCount == 0 {
			return nil
		}

		n := float64(meta.DocCount)
		avgdl := float64(meta.TotalLen) / n
		nowNanos := idx.store.now().UnixNano()
		if avgdl <= 0 {
			avgdl = 1
		}

		scores := make(map[string]float64)
		cache := make(map[string]docRecord)
		for i, term := range terms {
			if i%64 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			posting, found, err := idx.loadTerm(termsBucket, term)
			if err != nil {
				return err
			}
			if !found || len(posting.Docs) == 0 {
				continue
			}
			df := float64(len(posting.Docs))
			idf := math.Log(1 + (n-df+0.5)/(df+0.5))
			for _, id := range posting.Docs {
				doc, ok := cache[id]
				if !ok {
					loaded, exists, err := idx.loadDoc(docs, id)
					if err != nil {
						return err
					}
					if !exists {
						continue
					}
					doc = loaded
					cache[id] = doc
				}
				if doc.ExpiresAt > 0 && doc.ExpiresAt <= nowNanos {
					continue
				}
				tf := float64(doc.TF[term])
				if tf == 0 {
					continue
				}
				denom := tf + idx.k1*(1-idx.b+idx.b*float64(doc.Length)/avgdl)
				if denom <= 0 {
					continue
				}
				scores[id] += idf * (tf * (idx.k1 + 1)) / denom
			}
		}

		hits = make([]Hit, 0, len(scores))
		for id, score := range scores {
			hits = append(hits, Hit{ID: id, Score: score})
		}
		sort.Slice(hits, func(i, j int) bool {
			if hits[i].Score != hits[j].Score {
				return hits[i].Score > hits[j].Score
			}
			return hits[i].ID < hits[j].ID
		})
		if limit > 0 && len(hits) > limit {
			hits = hits[:limit]
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return hits, nil
}

// uniqueTerms returns the distinct terms in order of first appearance.
func uniqueTerms(terms []string) []string {
	seen := make(map[string]struct{}, len(terms))
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		out = append(out, term)
	}
	return out
}

// Count returns the number of indexed documents.
func (idx *Index) Count(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var count int
	err := idx.store.view(func(tx *bolt.Tx) error {
		b, err := idx.store.indexBucket(tx, idx.name)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
		}
		meta, found, err := idx.store.loadMeta(b)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		count = int(meta.DocCount)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Name returns the index name.
func (idx *Index) Name() string {
	if idx == nil {
		return ""
	}
	return idx.name
}

// Indexes lists the index names under the store's root.
func (s *Store) Indexes(ctx context.Context) ([]string, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var names []string
	err := s.view(func(tx *bolt.Tx) error {
		var parent *bolt.Bucket
		if len(s.root) > 0 {
			parent = s.rootBucket(tx)
			if parent == nil {
				return nil
			}
			names = names[:0]
			return parent.ForEach(func(k, v []byte) error {
				if v == nil {
					names = append(names, string(k))
				}
				return nil
			})
		}
		return tx.ForEach(func(k []byte, b *bolt.Bucket) error {
			if len(k) > 0 && k[0] == '_' {
				return nil
			}
			names = append(names, string(k))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// Close marks a standalone store closed. It never closes the shared bbolt
// handle. A bound view is never closed through this method.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	return nil
}

// DocEntry is one indexed document's logical record: its length in tokens and
// its term frequencies. The original text is never stored, so it cannot be
// recovered from an index or an export.
type DocEntry struct {
	ID        string         `json:"id"`
	Length    int            `json:"len"`
	TF        map[string]int `json:"tf"`
	ExpiresAt int64          `json:"expires_at,omitempty"`
}

// TermEntry is one posting list, sorted by term.
type TermEntry struct {
	Term string   `json:"term"`
	Docs []string `json:"docs"`
}

// IndexMeta is the persisted index configuration plus its corpus statistics.
type IndexMeta struct {
	K1        float64 `json:"k1"`
	B         float64 `json:"b"`
	Tokenizer string  `json:"tokenizer"`
	DocCount  int64   `json:"docs"`
	TotalLen  int64   `json:"total_len"`
}

// Meta returns the persisted index metadata.
func (idx *Index) Meta(ctx context.Context) (IndexMeta, error) {
	if ctx == nil {
		return IndexMeta{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return IndexMeta{}, err
	}
	var out IndexMeta
	err := idx.store.view(func(tx *bolt.Tx) error {
		b, err := idx.store.indexBucket(tx, idx.name)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
		}
		meta, found, err := idx.store.loadMeta(b)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %q", ErrIndexNotFound, idx.name)
		}
		out = IndexMeta{
			K1:        meta.K1,
			B:         meta.B,
			Tokenizer: meta.Tokenizer,
			DocCount:  meta.DocCount,
			TotalLen:  meta.TotalLen,
		}
		return nil
	})
	if err != nil {
		return IndexMeta{}, err
	}
	return out, nil
}

// Docs returns every indexed document, sorted by id.
func (idx *Index) Docs(ctx context.Context) ([]DocEntry, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []DocEntry
	err := idx.store.view(func(tx *bolt.Tx) error {
		_, docs, _, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		return docs.ForEach(func(k, v []byte) error {
			doc, found, err := idx.loadDoc(docs, string(k))
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			out = append(out, DocEntry{ID: string(k), Length: doc.Length, TF: doc.TF, ExpiresAt: doc.ExpiresAt})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Terms returns every posting list, sorted by term. When term keys are
// encrypted they are decrypted through the store's name cipher.
func (idx *Index) Terms(ctx context.Context) ([]TermEntry, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []TermEntry
	err := idx.store.view(func(tx *bolt.Tx) error {
		_, _, terms, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		return terms.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			term, err := idx.store.termName(k)
			if err != nil {
				return err
			}
			rec, found, err := idx.loadTerm(terms, term)
			if err != nil {
				return err
			}
			if !found || len(rec.Docs) == 0 {
				return nil
			}
			out = append(out, TermEntry{Term: term, Docs: rec.Docs})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Term < out[j].Term })
	return out, nil
}

// RestoreIndex creates or replaces the named index from a logical snapshot in
// one write transaction: the version and mode records, the metadata, every
// document and every posting list. cfg supplies the tokenizer when the snapshot
// was taken from an index with a custom tokenizer; built-in tokenizers are
// resolved by name and a conflicting custom tokenizer is rejected. K1 and B in
// meta always win. Everything is written under the store's current encryption.
func (s *Store) RestoreIndex(ctx context.Context, name string, cfg Config, meta IndexMeta, docs []DocEntry, terms []TermEntry) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return ErrNilParam
	}
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidName)
	}
	if err := validateName(name); err != nil {
		return err
	}
	if err := validateBM25(meta.K1, meta.B); err != nil {
		return err
	}
	tokenizer, err := restoreTokenizer(cfg.Tokenizer, meta.Tokenizer)
	if err != nil {
		return err
	}
	idx := &Index{store: s, name: name, tokenizer: tokenizer, k1: meta.K1, b: meta.B}
	if idx.k1 == 0 {
		idx.k1 = DefaultK1
	}
	if idx.b == 0 {
		idx.b = DefaultB
	}
	return s.update(func(tx *bolt.Tx) error {
		b, err := s.createIndexBucket(tx, name)
		if err != nil {
			return err
		}
		if _, err := b.CreateBucketIfNotExists([]byte(docsBucket)); err != nil {
			return err
		}
		if _, err := b.CreateBucketIfNotExists([]byte(termsBucket)); err != nil {
			return err
		}
		if err := s.checkVersion(b, false); err != nil {
			return err
		}
		if err := s.checkMode(b, false); err != nil {
			return err
		}
		if err := s.saveMeta(b, indexMeta{
			DocCount:  meta.DocCount,
			TotalLen:  meta.TotalLen,
			K1:        idx.k1,
			B:         idx.b,
			Tokenizer: tokenizerName(tokenizer),
		}); err != nil {
			return err
		}
		docsB := b.Bucket([]byte(docsBucket))
		termsB := b.Bucket([]byte(termsBucket))
		for _, doc := range docs {
			if doc.ID == "" {
				return fmt.Errorf("%w: document id is required", ErrNilParam)
			}
			tf := doc.TF
			if tf == nil {
				tf = map[string]int{}
			}
			if err := idx.saveDoc(docsB, doc.ID, docRecord{Length: doc.Length, TF: tf, ExpiresAt: doc.ExpiresAt}); err != nil {
				return err
			}
			if doc.ExpiresAt != 0 {
				if err := idx.setExpiry(tx, b, doc.ID, doc.ExpiresAt); err != nil {
					return err
				}
			}
		}
		for _, term := range terms {
			if term.Term == "" {
				continue
			}
			if err := idx.saveTerm(termsB, term.Term, termRecord{Docs: term.Docs}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Drop removes a named index and every document and posting list in it. A
// missing index is a no-op.
func (s *Store) Drop(ctx context.Context, name string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return ErrNilParam
	}
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidName)
	}
	if err := validateName(name); err != nil {
		return err
	}
	dropped := false
	if err := s.update(func(tx *bolt.Tx) error {
		b, err := s.indexBucket(tx, name)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		dropped = true
		if len(s.root) > 0 {
			root := s.rootBucket(tx)
			if root == nil {
				return nil
			}
			return root.DeleteBucket([]byte(name))
		}
		return tx.DeleteBucket([]byte(name))
	}); err != nil {
		return err
	}
	if !dropped {
		return nil
	}
	return s.traceEntry("drop_index", name, nil)
}

// digestEvent is the SHA-256 of the document as it was written. The separator
// keeps ("ab", "c") and ("a", "bc") distinct.
func digestEvent(id, text string) []byte {
	sum := sha256.Sum256([]byte(id + "\x00" + text))
	return sum[:]
}

// restoreTokenizer resolves the tokenizer for a restored index. Built-in
// tokenizers round-trip by name; "custom" requires the caller to supply one.
func restoreTokenizer(supplied Tokenizer, name string) (Tokenizer, error) {
	switch name {
	case "", "words":
		if supplied != nil && tokenizerName(supplied) == "custom" {
			return nil, fmt.Errorf("%w: index uses the built-in words tokenizer, not a custom one", ErrInvalidConfig)
		}
		return Words(), nil
	case "words+cjk":
		if supplied != nil && tokenizerName(supplied) == "custom" {
			return nil, fmt.Errorf("%w: index uses the built-in words+cjk tokenizer, not a custom one", ErrInvalidConfig)
		}
		return WordsWithCJK(), nil
	case "custom":
		if supplied == nil {
			return nil, fmt.Errorf("%w: index uses a custom tokenizer; supply Tokenizer to restore it", ErrInvalidConfig)
		}
		return supplied, nil
	default:
		return nil, fmt.Errorf("%w: unknown tokenizer %q", ErrInvalidConfig, name)
	}
}
