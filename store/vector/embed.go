package vector

import (
	"context"
	"errors"
	"fmt"
)

// ModelPayloadKey is the reserved payload key that stamps a point with the
// embedding model that produced it. Points without a stamp inherit the
// collection's Model.
const ModelPayloadKey = "__model__"

// ReembedOptions configures Reembed.
type ReembedOptions struct {
	// Model is the target model id. It is required.
	Model string
	// Normalized and Revision are recorded on the collection model when the
	// pass completes.
	Normalized bool
	Revision   string
	// BatchSize flushes every BatchSize points. Zero selects 64.
	BatchSize int
	// Progress, when non-nil, is called after each re-embedded point.
	Progress func(done, total int)
}

// ReembedReport describes one re-embedding pass.
type ReembedReport struct {
	// Done is the number of points embedded by this call.
	Done int
	// Total is the number of stale points when the call started.
	Total int
}

// pointModel returns the model stamp for a point, falling back to the
// collection model for points written before stamps existed.
func pointModel(point *PointStruct, fallback string) string {
	if point == nil || point.Payload == nil {
		return fallback
	}
	if value, ok := point.Payload[ModelPayloadKey]; ok {
		if id, ok := value.(string); ok && id != "" {
			return id
		}
	}
	return fallback
}

// SetModel records the collection's embedding provenance. The dimensions and
// metric must match the collection when set. Existing points are not rewritten;
// use Reembed first, then call SetModel to declare the collection consistent.
func (c *Collection) SetModel(model EmbeddingModel) error {
	if c == nil || c.storage == nil {
		return ErrInvalidConfig
	}
	if model.ID == "" {
		return fmt.Errorf("%w: embedding model id is required", ErrInvalidConfig)
	}
	if model.Dimensions == 0 {
		model.Dimensions = c.VectorLen
	}
	if model.Dimensions != c.VectorLen {
		return fmt.Errorf("%w: model dimensions %d do not match collection dimension %d", ErrInvalidConfig, model.Dimensions, c.VectorLen)
	}
	if model.Metric == "" {
		model.Metric = c.Metric
	}
	if model.Metric != c.Metric {
		return fmt.Errorf("%w: model metric %q does not match collection metric %q", ErrInvalidConfig, model.Metric, c.Metric)
	}
	meta, err := c.storage.LoadCollectionMeta(c.Name)
	if err != nil {
		return err
	}
	meta.Model = model
	if err := c.storage.SaveCollectionMeta(c.Name, *meta); err != nil {
		return err
	}
	c.mu.Lock()
	c.mixed = false
	c.mu.Unlock()
	return nil
}

// StalePoints returns up to limit point IDs whose model stamp differs from
// model. A non-positive limit returns every stale point, in ID order. A point
// with no stamp inherits the collection model, so it is stale exactly when the
// collection model differs from model.
func (c *Collection) StalePoints(model string, limit int) ([]string, error) {
	if c == nil || c.storage == nil {
		return nil, ErrInvalidConfig
	}
	if model == "" {
		return nil, fmt.Errorf("%w: embedding model id is required", ErrInvalidConfig)
	}
	meta, err := c.storage.LoadCollectionMeta(c.Name)
	if err != nil {
		return nil, err
	}
	current := meta.Model.ID
	var out []string
	err = c.ForEachPoint(nil, func(point PointStruct) error {
		if pointModel(&point, current) == model {
			return nil
		}
		out = append(out, point.ID)
		if limit > 0 && len(out) >= limit {
			return ErrStopIteration
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Reembed replaces the vectors of points whose model stamp differs from
// opts.Model. It is resumable: each point is stamped as it is written, so a
// canceled call leaves the completed points current and a later call skips
// them.
func (c *Collection) Reembed(ctx context.Context, fn func(id string, old []float32) ([]float32, error), opts ReembedOptions) (*ReembedReport, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, fmt.Errorf("%w: re-embed function is required", ErrInvalidOptions)
	}
	if opts.Model == "" {
		return nil, fmt.Errorf("%w: embedding model id is required", ErrInvalidConfig)
	}
	if c == nil || c.storage == nil {
		return nil, ErrInvalidConfig
	}
	stale, err := c.StalePoints(opts.Model, 0)
	if err != nil {
		return nil, err
	}
	report := &ReembedReport{Total: len(stale)}
	if len(stale) == 0 {
		if err := c.SetModel(EmbeddingModel{
			ID:         opts.Model,
			Dimensions: c.VectorLen,
			Metric:     c.Metric,
			Normalized: opts.Normalized,
			Revision:   opts.Revision,
		}); err != nil {
			return report, err
		}
		return report, nil
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = DefaultFlushThreshold
	}
	for _, id := range stale {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		point, err := c.Get(id)
		if err != nil {
			if errors.Is(err, ErrPointNotFound) {
				continue
			}
			return report, err
		}
		vec, err := fn(id, point.Vector)
		if err != nil {
			return report, err
		}
		if len(vec) != c.VectorLen {
			return report, fmt.Errorf("%w: model returned dimension %d, expected %d", ErrInvalidConfig, len(vec), c.VectorLen)
		}
		payload := make(Payload, len(point.Payload)+1)
		for k, v := range point.Payload {
			payload[k] = v
		}
		payload[ModelPayloadKey] = opts.Model
		if err := c.Upsert([]PointStruct{{ID: point.ID, Vector: vec, Payload: payload}}); err != nil {
			return report, err
		}
		report.Done++
		if opts.Progress != nil {
			opts.Progress(report.Done, report.Total)
		}
		if report.Done%batch == 0 {
			if err := c.Flush(); err != nil {
				return report, err
			}
		}
	}
	if err := c.Flush(); err != nil {
		return report, err
	}
	if err := c.SetModel(EmbeddingModel{
		ID:         opts.Model,
		Dimensions: c.VectorLen,
		Metric:     c.Metric,
		Normalized: opts.Normalized,
		Revision:   opts.Revision,
	}); err != nil {
		return report, err
	}
	return report, nil
}
