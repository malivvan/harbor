package vector

import (
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// Payload mimics Qdrant's payload structure, storing metadata as a map of string keys to any values.
// It is used for filtering points based on their associated metadata.
type Payload map[string]any

// PointStruct represents a single vector data point, compatible with Qdrant's data model.
// It contains a unique identifier, the vector embedding, and optional metadata.
type PointStruct struct {
	ID      string    `json:"id"`                // UUID or uint64 (using string for now)
	Version uint64    `json:"version"`           // Incremental or timestamp-based version
	Vector  []float32 `json:"vector"`            // The actual embeddings
	Payload Payload   `json:"payload,omitempty"` // Metadata for filtering
}

// ScoredPoint is returned by search queries, containing the distance score and point data.
// The Score field represents the computed similarity/distance based on the collection's metric.
type ScoredPoint struct {
	ID      string  `json:"id"`
	Version uint64  `json:"version"`
	Score   float32 `json:"score"`
	Payload Payload `json:"payload,omitempty"`
	// Vector is the stored embedding, populated only by SearchWith with
	// WithVectors. It is nil otherwise so a normal search allocates nothing
	// extra.
	Vector []float32 `json:"vector,omitempty"`
}

// Filter defines conditions for querying or deleting points based on their payload.
// It supports Must (all conditions must match) and MustNot (all conditions must not match) clauses.
type Filter struct {
	Must    []Condition `json:"must,omitempty"`
	MustNot []Condition `json:"must_not,omitempty"`
}

// ConditionType defines the type of filter condition
type ConditionType string

const (
	// MatchTypeExact exact value match
	MatchTypeExact ConditionType = "exact"
	// MatchTypeRange range match (greater than, less than, etc.)
	MatchTypeRange ConditionType = "range"
	// MatchTypePrefix prefix match
	MatchTypePrefix ConditionType = "prefix"
	// MatchTypeContains contains match (for arrays)
	MatchTypeContains ConditionType = "contains"
	// MatchTypeRegex regex match
	MatchTypeRegex ConditionType = "regex"
)

// Condition represents a single filter condition on a specific payload key.
type Condition struct {
	Key   string        `json:"key"`
	Type  ConditionType `json:"type"`
	Match MatchValue    `json:"match,omitempty"`
	Range *RangeValue   `json:"range,omitempty"`
}

// MatchValue holds the value to match against in a filter condition.
type MatchValue struct {
	Value any `json:"value"`
}

// RangeValue holds range values for range conditions
type RangeValue struct {
	GT  any `json:"gt,omitempty"`  // Greater than
	GTE any `json:"gte,omitempty"` // Greater than or equal
	LT  any `json:"lt,omitempty"`  // Less than
	LTE any `json:"lte,omitempty"` // Less than or equal
}

// CollectionMeta stores the configuration and metadata for a vector collection.
// It is used for persisting collection settings and reloading them on server restart.
// EmbeddingModel identifies the model that produced a collection's vectors.
// It is provenance, not a credential: an empty ID means "unknown".
type EmbeddingModel struct {
	ID         string   `json:"id,omitempty"`
	Dimensions int      `json:"dimensions,omitempty"`
	Metric     Distance `json:"metric,omitempty"`
	Normalized bool     `json:"normalized,omitempty"`
	Revision   string   `json:"revision,omitempty"`
}

type CollectionMeta struct {
	Name       string     `json:"name"`
	VectorLen  int        `json:"vector_size"`
	Metric     Distance   `json:"distance"`
	UseHNSW    bool       `json:"hnsw"`
	HNSWParams HNSWParams `json:"parameters"`

	// UseQuant records whether the points are stored quantized. Reopening a
	// quantized collection without the quantizer would return nil vectors, so
	// the mode is validated on load.
	UseQuant bool `json:"quantized,omitempty"`

	// FormatVersion is the persisted-format revision, and SchemaFingerprint is
	// the generated StoredPoint wire-layout hash. Both are checked on reopen so
	// a reordered or otherwise incompatible generated codec is detected instead
	// of silently reinterpreting old blobs.
	FormatVersion     uint32 `json:"format_version,omitempty"`
	SchemaFingerprint string `json:"schema_fingerprint,omitempty"`

	// Model is the embedding provenance for the collection. Points re-embedded
	// under a newer model carry the model in their payload; points without a
	// stamp inherit this value.
	Model EmbeddingModel `json:"model,omitempty"`

	// AllowMixedModels lets Search run over a collection whose points carry
	// different model stamps. It is saved so reopening keeps the choice.
	AllowMixedModels bool `json:"allow_mixed_models,omitempty"`
}

// CollectionFormatVersion is the on-disk collection metadata revision this
// build writes and accepts. The first release has a single version 1; a newer
// value fails the load with ErrInvalidConfig.
const CollectionFormatVersion uint32 = 1

// CollectionFormatVersionExpiry is written when a collection carries point
// expiry metadata. Version 1 collections still open; version 2 is refused by
// older builds, which is what keeps expired points from being served by a
// build that does not implement the filter.
const CollectionFormatVersionExpiry uint32 = 2

// MatchFilter evaluates whether a given payload matches the filter criteria.
// It returns true if:
//   - The filter is nil (no filtering)
//   - All Must conditions are satisfied
//   - No MustNot conditions are satisfied
func MatchFilter(payload Payload, filter *Filter) bool {
	if filter == nil {
		return true // No filter means match all
	}

	// Must conditions (all must match)
	for _, cond := range filter.Must {
		if !matchCondition(payload, cond) {
			return false
		}
	}

	// MustNot conditions (none must match)
	for _, cond := range filter.MustNot {
		if matchCondition(payload, cond) {
			return false
		}
	}

	return true
}

// matchCondition evaluates a single condition against a payload
func matchCondition(payload Payload, cond Condition) bool {
	val, exists := payload[cond.Key]
	if !exists {
		return false
	}

	switch cond.Type {
	case MatchTypeExact:
		return matchExact(val, cond.Match.Value)
	case MatchTypeRange:
		return matchRange(val, cond.Range)
	case MatchTypePrefix:
		return matchPrefix(val, cond.Match.Value)
	case MatchTypeContains:
		return matchContains(val, cond.Match.Value)
	case MatchTypeRegex:
		return matchRegex(val, cond.Match.Value)
	default:
		return matchExact(val, cond.Match.Value)
	}
}

// matchExact reports whether the payload value equals the filter target.
//
// Numbers are compared across the concrete types a payload can hold (int,
// int64, float32, float64, ...): a filter built with MatchValue{Value: 5} keeps
// matching after a round-trip through storage, which returns integers as int64.
func matchExact(val, target any) bool {
	if val == nil || target == nil {
		return val == target
	}

	if fv, ok := asFloat64(val); ok {
		ft, ok := asFloat64(target)
		if !ok {
			return false
		}
		// Compare integral values exactly to avoid losing precision above 2^53.
		if iv, ok := asInt64(val); ok {
			if it, ok := asInt64(target); ok {
				return iv == it
			}
		}
		return fv == ft
	}

	if vs, ok := val.(string); ok {
		if ts, ok := target.(string); ok {
			return vs == ts
		}
	}

	valType := reflect.TypeOf(val)
	if valType != reflect.TypeOf(target) {
		// Different, incomparable/opaque dynamic types; fall back to a deep
		// comparison so that maps and slices do not panic.
		return reflect.DeepEqual(val, target)
	}
	if !valType.Comparable() {
		return reflect.DeepEqual(val, target)
	}
	return val == target
}

// asFloat64 converts a numeric payload value to float64.
// It reports false for non-numeric values.
func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

// asInt64 converts an integral payload value to int64 without losing precision.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > uint64(1<<63-1) {
			return 0, false
		}
		return int64(n), true
	default:
		return 0, false
	}
}

// matchRange evaluates a range condition.
//
// Comparison is numeric and type-agnostic: any numeric payload value (int,
// int64, float32, float64, ...) can be ranged against any numeric bound.
// Non-numeric payload values, and conditions whose bound is not numeric, never
// match: a range filter must not silently accept values it cannot compare.
func matchRange(val any, r *RangeValue) bool {
	if r == nil {
		return false
	}

	v, ok := asFloat64(val)
	if !ok {
		return false
	}

	if r.GT != nil {
		gt, ok := asFloat64(r.GT)
		if !ok || v <= gt {
			return false
		}
	}
	if r.GTE != nil {
		gte, ok := asFloat64(r.GTE)
		if !ok || v < gte {
			return false
		}
	}
	if r.LT != nil {
		lt, ok := asFloat64(r.LT)
		if !ok || v >= lt {
			return false
		}
	}
	if r.LTE != nil {
		lte, ok := asFloat64(r.LTE)
		if !ok || v > lte {
			return false
		}
	}

	return true
}

// matchPrefix evaluates a prefix condition
func matchPrefix(val any, prefix any) bool {
	strVal, ok := val.(string)
	if !ok {
		return false
	}

	strPrefix, ok := prefix.(string)
	if !ok {
		return false
	}

	if len(strPrefix) > len(strVal) {
		return false
	}

	return strVal[:len(strPrefix)] == strPrefix
}

// matchContains evaluates a contains condition
func matchContains(val any, target any) bool {
	switch v := val.(type) {
	case []any:
		// Reloaded JSON arrays hold int64/float64/nested maps. Compare with the
		// same numeric-coercing, panic-free logic as an exact match.
		for _, item := range v {
			if matchExact(item, target) {
				return true
			}
		}
	case []string:
		if targetStr, ok := target.(string); ok {
			for _, item := range v {
				if item == targetStr {
					return true
				}
			}
		}
	case []int:
		for _, item := range v {
			if matchExact(item, target) {
				return true
			}
		}
	case []int64:
		for _, item := range v {
			if matchExact(item, target) {
				return true
			}
		}
	case []float64:
		for _, item := range v {
			if matchExact(item, target) {
				return true
			}
		}
	case string:
		if targetStr, ok := target.(string); ok {
			return len(targetStr) > 0 && strings.Contains(v, targetStr)
		}
	}

	return false
}

// containsString is kept for compatibility with internal callers.
func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}

// regexCache memoizes compiled patterns: MatchFilter is evaluated once per
// point, so recompiling the same pattern would dominate a filtered scan.
var regexCache sync.Map // map[string]*regexp.Regexp

// regexCacheCount tracks the number of memoized patterns without ranging the
// whole cache on every insert.
var regexCacheCount atomic.Int64

// regexCacheSize returns the tracked number of memoized patterns.
func regexCacheSize() int {
	return int(regexCacheCount.Load())
}

// compileRegex compiles pattern once, caching both success and failure. An
// invalid pattern is stored as nil and never matches.
func compileRegex(pattern string) *regexp.Regexp {
	if cached, ok := regexCache.Load(pattern); ok {
		if cached == nil {
			return nil
		}
		return cached.(*regexp.Regexp)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		regexCache.Store(pattern, nil)
		return nil
	}
	if cached, ok := regexCache.Load(pattern); ok && cached != nil {
		return cached.(*regexp.Regexp)
	}
	if regexCacheCount.Load() > 1024 {
		regexCache.Clear()
		regexCacheCount.Store(0)
	}
	regexCache.Store(pattern, re)
	regexCacheCount.Add(1)
	return re
}

// matchRegex evaluates a regex condition
func matchRegex(val any, pattern any) bool {
	strVal, ok := val.(string)
	if !ok {
		return false
	}

	strPattern, ok := pattern.(string)
	if !ok {
		return false
	}

	re := compileRegex(strPattern)
	if re == nil {
		return false
	}
	return re.MatchString(strVal)
}
