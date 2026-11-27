package vector

import "time"

// ExpiryPayloadKey is the reserved payload key holding a point's expiry as
// Unix nanoseconds. It is visible in payload filters like any other key; do
// not use it for application data.
const ExpiryPayloadKey = "__expires_at__"

// SetNow replaces the clock used by expiry checks and sweeps. Passing nil
// restores time.Now.
func (s *Store) SetNow(fn func() time.Time) {
	if s == nil {
		return
	}
	base := s.rootStore()
	if base == nil {
		return
	}
	base.nowMu.Lock()
	base.nowFn = fn
	base.nowMu.Unlock()
}

func (s *Store) now() time.Time {
	base := s.rootStore()
	if base == nil {
		return time.Now()
	}
	base.nowMu.RLock()
	fn := base.nowFn
	base.nowMu.RUnlock()
	if fn != nil {
		return fn()
	}
	return time.Now()
}

func (c *Collection) now() time.Time {
	if c != nil && c.storage != nil {
		return c.storage.now()
	}
	return time.Now()
}

// SetExpiry records an expiry for one point. A zero time clears it. The point
// is rewritten with the reserved payload key, so the expiry is durable and
// travels with the point through backup and export.
func (c *Collection) SetExpiry(id string, at time.Time) error {
	if c == nil {
		return ErrClosed
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	point, err := c.index.Get(id)
	if err != nil {
		c.mu.RUnlock()
		return err
	}
	if point == nil {
		c.mu.RUnlock()
		return ErrPointNotFound
	}
	payload := make(Payload, len(point.Payload)+1)
	for k, v := range point.Payload {
		payload[k] = v
	}
	updated := *point
	c.mu.RUnlock()

	if at.IsZero() {
		delete(payload, ExpiryPayloadKey)
	} else {
		payload[ExpiryPayloadKey] = at.UnixNano()
	}
	updated.Payload = payload
	if err := c.Upsert([]PointStruct{updated}); err != nil {
		return err
	}
	return c.ensureExpiryFormat()
}

// Expiry returns the stored expiry for one point.
func (c *Collection) Expiry(id string) (time.Time, bool, error) {
	if c == nil {
		return time.Time{}, false, ErrClosed
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return time.Time{}, false, ErrClosed
	}
	point, err := c.index.Get(id)
	if err != nil {
		return time.Time{}, false, err
	}
	if point == nil {
		return time.Time{}, false, ErrPointNotFound
	}
	at, ok := expiryOf(point.Payload)
	return at, ok, nil
}

// SweepExpired deletes up to limit expired points. A limit <= 0 removes every
// expired point. It is idempotent.
func (c *Collection) SweepExpired(now time.Time, limit int) (int, error) {
	if c == nil {
		return 0, ErrClosed
	}
	if now.IsZero() {
		now = c.now()
	}
	var ids []string
	err := c.ForEachPoint(nil, func(point PointStruct) error {
		if !pointExpired(&point, now) {
			return nil
		}
		ids = append(ids, point.ID)
		if limit > 0 && len(ids) >= limit {
			return ErrStopIteration
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return c.Delete(ids, nil)
}

// ensureExpiryFormat upgrades the collection metadata to the version that
// makes an older build refuse to open it. A build that does not know the
// expiry payload key must not serve expired points.
func (c *Collection) ensureExpiryFormat() error {
	if c == nil || c.storage == nil {
		return nil
	}
	meta, err := c.storage.LoadCollectionMeta(c.Name)
	if err != nil {
		return err
	}
	if meta.FormatVersion >= CollectionFormatVersionExpiry {
		return nil
	}
	meta.FormatVersion = CollectionFormatVersionExpiry
	return c.storage.SaveCollectionMeta(c.Name, *meta)
}

// filterExpiredScored removes expired hits from a search result.
func (c *Collection) filterExpiredScored(results []ScoredPoint) []ScoredPoint {
	now := c.now()
	out := results[:0]
	for _, result := range results {
		if expiredPayload(result.Payload, now) {
			continue
		}
		out = append(out, result)
	}
	return out
}

// filterExpiredPoints removes expired points from a list result.
func (c *Collection) filterExpiredPoints(points []PointStruct) []PointStruct {
	now := c.now()
	out := points[:0]
	for i := range points {
		if pointExpired(&points[i], now) {
			continue
		}
		out = append(out, points[i])
	}
	return out
}

func pointExpired(point *PointStruct, now time.Time) bool {
	if point == nil {
		return false
	}
	return expiredPayload(point.Payload, now)
}

func expiredPayload(payload Payload, now time.Time) bool {
	at, ok := expiryOf(payload)
	if !ok {
		return false
	}
	return !at.After(now)
}

func expiryOf(payload Payload) (time.Time, bool) {
	if payload == nil {
		return time.Time{}, false
	}
	value, ok := payload[ExpiryPayloadKey]
	if !ok {
		return time.Time{}, false
	}
	switch v := value.(type) {
	case int64:
		return time.Unix(0, v).UTC(), true
	case int:
		return time.Unix(0, int64(v)).UTC(), true
	case float64:
		return time.Unix(0, int64(v)).UTC(), true
	case time.Time:
		return v.UTC(), true
	case string:
		at, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, false
		}
		return at.UTC(), true
	default:
		return time.Time{}, false
	}
}
