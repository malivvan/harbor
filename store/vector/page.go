package vector

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
)

// ErrCursorMismatch reports a cursor produced by a different filter.
var ErrCursorMismatch = errors.New("vector: cursor does not match the filter")

// ListCursor resumes ListPointsPage. Its fields are opaque to callers.
type ListCursor struct {
	AfterID string
	Shape   string
}

// ListPointsPage returns one stable page of points matching filter in
// ascending ID order. A cursor from a different filter is rejected with
// ErrCursorMismatch, and inserts or deletes between pages neither duplicate
// nor skip an ID.
func (c *Collection) ListPointsPage(filter *Filter, limit int, after *ListCursor) ([]PointStruct, *ListCursor, error) {
	shape, err := filterShape(filter)
	if err != nil {
		return nil, nil, err
	}
	if after != nil && after.Shape != shape {
		return nil, nil, ErrCursorMismatch
	}

	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	ids := c.sortedFilterIDs(filter)
	c.mu.RUnlock()

	want := limit
	if want > 0 {
		want++ // read one past the page to know whether a cursor is needed
	}
	out := make([]PointStruct, 0, want)
	for _, id := range ids {
		if after != nil && id <= after.AfterID {
			continue
		}
		point, err := c.index.Get(id)
		if err != nil {
			return nil, nil, err
		}
		if point == nil || pointExpired(point, c.now()) {
			continue
		}
		out = append(out, *point)
		if want > 0 && len(out) >= want {
			break
		}
	}
	var cursor *ListCursor
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		cursor = &ListCursor{AfterID: out[limit-1].ID, Shape: shape}
	}
	if len(out) == 0 {
		return out, nil, nil
	}
	return out, cursor, nil
}

func filterShape(filter *Filter) (string, error) {
	raw, err := json.Marshal(filter)
	if err != nil {
		return "", fmt.Errorf("vector: encode filter for cursor: %w", err)
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return fmt.Sprintf("%x", h.Sum64()), nil
}
