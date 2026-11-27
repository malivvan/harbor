package tempest

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/malivvan/tempest/ranking"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/vector"
)

// Hybrid retrieval errors.
var (
	// ErrHybridQuery reports a request with neither text nor a query vector.
	ErrHybridQuery = errors.New("tempest: hybrid search needs text or a vector")
	// ErrHybridConfig reports a request missing the index or collection name
	// required by an arm it selected.
	ErrHybridConfig = errors.New("tempest: hybrid search is missing a text index or vector collection")
	// ErrHybridFilter reports a payload filter without a vector collection to
	// evaluate it against.
	ErrHybridFilter = errors.New("tempest: hybrid filter requires a vector collection")
)

// HybridRequest is one hybrid retrieval query: a lexical arm, a dense arm, or
// both, fused into one ranked list.
//
// The two arms share an ID namespace. A document indexed in only one arm is
// still returned by that arm; with a filter it must also exist in the vector
// collection, because the filter is evaluated against the payload.
type HybridRequest struct {
	// Text is the lexical query. Empty disables the text arm.
	Text string
	// Vector is the dense query. Empty disables the dense arm.
	Vector []float32
	// Filter is an optional payload filter. It must be non-nil only when
	// VectorCol names a collection.
	Filter *vector.Filter
	// TopK caps the returned hits. Zero selects 10; values above 1000 are
	// clamped to 1000.
	TopK int
	// TextIndex names the full-text index for the text arm.
	TextIndex string
	// VectorCol names the vector collection for the dense arm.
	VectorCol string
	// Fusion selects RRF (the zero value) or weighted fusion.
	Fusion ranking.Fusion
	// Weights applies to weighted fusion: text arm first, dense arm second.
	// Missing weights are 1.
	Weights []float64
	// Rerank, when non-nil, replaces the fused score with a normalized blend of
	// the two raw arm scores.
	Rerank *HybridRerank
	// Cursor resumes after a previous page.
	Cursor *HybridCursor
}

// HybridRerank blends the two arm scores after fusion. Zero weights select
// 0.6 for text and 0.4 for dense.
type HybridRerank struct {
	TextWeight   float64
	VectorWeight float64
}

// HybridCursor marks the end of a returned page. It is opaque to callers;
// pass it back to HybridRequest.Cursor to continue.
type HybridCursor struct {
	Score float64
	ID    string
}

// HybridHit is one fused result.
type HybridHit struct {
	// ID is the shared identifier.
	ID string
	// Score is the fused (or reranked) score, best first.
	Score float64
	// Text is the raw lexical score, zero when the text arm did not return it.
	Text float64
	// Dense is the raw vector score, zero when the dense arm did not return it.
	Dense float64
	// Rank is the 1-based position in the full fused list.
	Rank int
	// Why names the arms and stages that produced the hit.
	Why []string
}

// HybridSearch runs the lexical and dense arms, fuses their rankings and
// returns one page. It is the store-level retrieval primitive: policy such as
// decay, importance or prompt budgeting stays in the caller.
func (s *Store) HybridSearch(ctx context.Context, req HybridRequest) ([]HybridHit, *HybridCursor, error) {
	if ctx == nil {
		return nil, nil, search.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s == nil || s.Search() == nil || s.Vector() == nil {
		return nil, nil, ErrClosed
	}
	if req.Text == "" && len(req.Vector) == 0 {
		return nil, nil, ErrHybridQuery
	}
	if req.Text != "" && req.TextIndex == "" {
		return nil, nil, ErrHybridConfig
	}
	if len(req.Vector) > 0 && req.VectorCol == "" {
		return nil, nil, ErrHybridConfig
	}
	if req.Filter != nil && req.VectorCol == "" {
		return nil, nil, ErrHybridFilter
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	if req.TopK > 1000 {
		req.TopK = 1000
	}
	candidate := req.TopK * 4
	if candidate < 64 {
		candidate = 64
	}

	textScores := make(map[string]float64)
	var textList []ranking.Result
	if req.Text != "" {
		idx, err := s.Search().Open(req.TextIndex, search.Config{})
		if err != nil {
			return nil, nil, fmt.Errorf("tempest: hybrid text arm: %w", err)
		}
		hits, err := idx.Search(ctx, req.Text, candidate)
		if err != nil {
			return nil, nil, fmt.Errorf("tempest: hybrid text arm: %w", err)
		}
		for _, hit := range hits {
			textList = append(textList, ranking.Result{ID: hit.ID, Score: hit.Score})
			textScores[hit.ID] = hit.Score
		}
	}

	denseScores := make(map[string]float64)
	var denseList []ranking.Result
	var col *vector.Collection
	if len(req.Vector) > 0 || req.Filter != nil {
		var err error
		col, err = s.Vector().Collection(req.VectorCol)
		if err != nil {
			return nil, nil, fmt.Errorf("tempest: hybrid dense arm: %w", err)
		}
	}
	if len(req.Vector) > 0 {
		points, err := col.Search(req.Vector, req.Filter, candidate)
		if err != nil {
			return nil, nil, fmt.Errorf("tempest: hybrid dense arm: %w", err)
		}
		for _, point := range points {
			denseList = append(denseList, ranking.Result{ID: point.ID, Score: float64(point.Score)})
			denseScores[point.ID] = float64(point.Score)
		}
	}

	// A filter is a payload predicate. The dense arm already applied it; the
	// text arm has no payload, so intersect it with the filtered collection.
	if req.Filter != nil && col != nil && len(textList) > 0 {
		live := textList[:0]
		for _, item := range textList {
			point, err := col.Get(item.ID)
			if err != nil || point == nil || !vector.MatchFilter(point.Payload, req.Filter) {
				delete(textScores, item.ID)
				continue
			}
			live = append(live, item)
		}
		textList = live
	}

	lists := make([][]ranking.Result, 0, 2)
	if req.Text != "" {
		lists = append(lists, textList)
	}
	if len(req.Vector) > 0 {
		lists = append(lists, denseList)
	}
	weights := req.Weights
	if len(weights) == 0 {
		weights = []float64{1, 1}
	}
	fused := ranking.Fuse(req.Fusion, 0, weights, lists...)

	if req.Rerank != nil && req.Text != "" && len(req.Vector) > 0 {
		tw, vw := req.Rerank.TextWeight, req.Rerank.VectorWeight
		if tw == 0 && vw == 0 {
			tw, vw = 0.6, 0.4
		}
		nt := normalizeScores(textScores)
		nd := normalizeScores(denseScores)
		for i := range fused {
			fused[i].Score = tw*nt[fused[i].ID] + vw*nd[fused[i].ID]
		}
		sort.Slice(fused, func(i, j int) bool {
			if fused[i].Score != fused[j].Score {
				return fused[i].Score > fused[j].Score
			}
			return fused[i].ID < fused[j].ID
		})
	}

	start := 0
	if req.Cursor != nil {
		for start < len(fused) {
			item := fused[start]
			if item.Score < req.Cursor.Score || (item.Score == req.Cursor.Score && item.ID > req.Cursor.ID) {
				break
			}
			start++
		}
	}
	end := start + req.TopK
	if end > len(fused) {
		end = len(fused)
	}
	page := fused[start:end]

	hits := make([]HybridHit, 0, len(page))
	for i, item := range page {
		why := make([]string, 0, 4)
		if _, ok := textScores[item.ID]; ok {
			why = append(why, "text")
		}
		if _, ok := denseScores[item.ID]; ok {
			why = append(why, "dense")
		}
		if len(lists) > 1 {
			why = append(why, req.Fusion.String())
		}
		if req.Rerank != nil && len(lists) > 1 {
			why = append(why, "rerank")
		}
		hits = append(hits, HybridHit{
			ID:    item.ID,
			Score: item.Score,
			Text:  textScores[item.ID],
			Dense: denseScores[item.ID],
			Rank:  start + i + 1,
			Why:   why,
		})
	}

	if end < len(fused) && len(page) > 0 {
		last := page[len(page)-1]
		return hits, &HybridCursor{Score: last.Score, ID: last.ID}, nil
	}
	return hits, nil, nil
}

// normalizeScores maps raw arm scores to [0,1]. An empty map stays empty; a
// single candidate or an all-equal arm is 1, so it still contributes.
func normalizeScores(scores map[string]float64) map[string]float64 {
	if len(scores) == 0 {
		return nil
	}
	min, max := 0.0, 0.0
	first := true
	for _, score := range scores {
		if first {
			min, max, first = score, score, false
			continue
		}
		if score < min {
			min = score
		}
		if score > max {
			max = score
		}
	}
	out := make(map[string]float64, len(scores))
	for id, score := range scores {
		if max == min {
			out[id] = 1
			continue
		}
		out[id] = (score - min) / (max - min)
	}
	return out
}
