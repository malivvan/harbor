package vector

// ExplainedPoint is a search hit with the metric that produced its score.
type ExplainedPoint struct {
	ScoredPoint
	Metric Distance
	// Formula names the metric's score convention. Cosine and Dot are higher
	// is better; Euclid is lower, but ScoredPoint.Score keeps the collection's
	// own sign and ordering.
	Formula string
}

// SearchExplain returns the same points as Search with their metric metadata.
// The vector score is a single distance, so the explanation is the metric
// rather than a per-term breakdown.
func (c *Collection) SearchExplain(query []float32, filter *Filter, topK int) ([]ExplainedPoint, error) {
	points, err := c.Search(query, filter, topK)
	if err != nil {
		return nil, err
	}
	formula := "similarity"
	if c.Metric == Euclid {
		formula = "distance"
	}
	out := make([]ExplainedPoint, 0, len(points))
	for _, point := range points {
		out = append(out, ExplainedPoint{
			ScoredPoint: point,
			Metric:      c.Metric,
			Formula:     formula,
		})
	}
	return out, nil
}
