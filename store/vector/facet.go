package vector

import (
	"fmt"
	"sort"
)

// FacetRequest asks for payload-value counts over the points of a collection.
type FacetRequest struct {
	// Fields are the payload keys to count. Only string values are counted;
	// missing keys and values of other types are skipped.
	Fields []string
	// Filter restricts the points considered. A nil filter counts the whole
	// collection.
	Filter *Filter
	// MaxValues caps the values returned per field, keeping the most frequent
	// (ties by value). Zero means no cap.
	MaxValues int
}

// FacetResult is the count of one payload field.
type FacetResult struct {
	// Field is the payload key.
	Field string
	// Values maps each string value to the number of matching points that
	// carry it.
	Values map[string]int
	// Total is the number of points that matched the filter, before value
	// truncation.
	Total int
}

// FacetCounts counts the string values of payload fields across the points that
// match req.Filter. Values are ordered by count descending and value ascending;
// the ordering is applied before MaxValues truncates, so a truncated map keeps
// the most frequent values.
//
// Counts cover the filtered collection, not a prior search result. To facet a
// result set, count the returned ids in the caller or pass a filter that
// selects them.
func (c *Collection) FacetCounts(req FacetRequest) ([]FacetResult, error) {
	if req.MaxValues < 0 {
		return nil, fmt.Errorf("%w: MaxValues must not be negative, got %d", ErrInvalidOptions, req.MaxValues)
	}
	fields := uniqueFields(req.Fields)
	if len(fields) == 0 {
		return []FacetResult{}, nil
	}
	for _, field := range fields {
		if field == "" {
			return nil, fmt.Errorf("%w: facet field name is empty", ErrInvalidOptions)
		}
	}

	counts := make([]map[string]int, len(fields))
	for i := range counts {
		counts[i] = map[string]int{}
	}

	total := 0
	err := c.ForEachPoint(req.Filter, func(point PointStruct) error {
		total++
		for i, field := range fields {
			value, ok := point.Payload[field]
			if !ok {
				continue
			}
			if s, ok := value.(string); ok {
				counts[i][s]++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make([]FacetResult, len(fields))
	for i, field := range fields {
		out[i] = FacetResult{
			Field:  field,
			Values: truncateFacet(counts[i], req.MaxValues),
			Total:  total,
		}
	}
	return out, nil
}

// uniqueFields removes duplicates and empty entries while preserving order.
func uniqueFields(fields []string) []string {
	seen := make(map[string]struct{}, len(fields))
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "" {
			// An empty name is reported as an error by the caller; keep it so
			// the error points at the request rather than being silently
			// dropped.
			out = append(out, field)
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

// truncateFacet keeps the most frequent values, up to max. Zero means all.
func truncateFacet(counts map[string]int, max int) map[string]int {
	type entry struct {
		value string
		count int
	}
	entries := make([]entry, 0, len(counts))
	for value, count := range counts {
		entries = append(entries, entry{value: value, count: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].value < entries[j].value
	})
	if max > 0 && len(entries) > max {
		entries = entries[:max]
	}
	out := make(map[string]int, len(entries))
	for _, e := range entries {
		out[e.value] = e.count
	}
	return out
}
