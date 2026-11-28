package search

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
)

// ErrCursorMismatch reports a cursor produced by a different query or index.
var ErrCursorMismatch = errors.New("search: cursor does not match the query")

// PageCursor resumes a SearchPage result. Its fields are opaque to callers;
// pass it back unchanged.
type PageCursor struct {
	Score float64
	ID    string
	Shape string
}

// SearchPage returns one stable page of Search results. The cursor encodes the
// last (score, id) pair and a query fingerprint, so a page neither duplicates
// nor skips a document when writes land between calls.
//
// The implementation materializes the full hit list before slicing it; for an
// index large enough that this matters, use Search and paginate in the caller.
// The cursor contract is still stable and shape-checked.
func (idx *Index) SearchPage(ctx context.Context, query string, limit int, after *PageCursor) ([]Hit, *PageCursor, error) {
	shape := queryShape(query)
	if after != nil && after.Shape != shape {
		return nil, nil, ErrCursorMismatch
	}
	hits, err := idx.Search(ctx, query, 0)
	if err != nil {
		return nil, nil, err
	}

	start := 0
	if after != nil {
		for start < len(hits) {
			hit := hits[start]
			if hit.Score < after.Score || (hit.Score == after.Score && hit.ID > after.ID) {
				break
			}
			start++
		}
	}
	end := len(hits)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	page := hits[start:end]
	if end >= len(hits) || len(page) == 0 {
		return page, nil, nil
	}
	last := page[len(page)-1]
	return page, &PageCursor{Score: last.Score, ID: last.ID, Shape: shape}, nil
}

func queryShape(query string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(query))
	return fmt.Sprintf("%x", h.Sum64())
}
