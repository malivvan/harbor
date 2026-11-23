package vector

// ValueKind identifies the active field in a Value.
type ValueKind uint8

const (
	// KindNone represents a nil or unset value.
	KindNone ValueKind = iota
	// KindString stores a string.
	KindString
	// KindInt stores an int64.
	KindInt
	// KindDouble stores a float64.
	KindDouble
	// KindBool stores a bool.
	KindBool
	// KindBytes stores a raw byte slice.
	KindBytes
	// KindJSON stores a JSON document (maps, slices and other complex payload
	// values).
	KindJSON
)

// Value is a payload value. Only the field selected by Kind is meaningful.
type Value struct {
	Kind        ValueKind
	StringValue string
	IntValue    int64
	DoubleValue float64
	BoolValue   bool
	BytesValue  []byte
}

// GetStringValue returns the string value when Kind is KindString.
func (v *Value) GetStringValue() string {
	if v == nil || v.Kind != KindString {
		return ""
	}
	return v.StringValue
}

// GetIntValue returns the integer value when Kind is KindInt.
func (v *Value) GetIntValue() int64 {
	if v == nil || v.Kind != KindInt {
		return 0
	}
	return v.IntValue
}

// GetDoubleValue returns the float value when Kind is KindDouble.
func (v *Value) GetDoubleValue() float64 {
	if v == nil || v.Kind != KindDouble {
		return 0
	}
	return v.DoubleValue
}

// GetBoolValue returns the bool value when Kind is KindBool.
func (v *Value) GetBoolValue() bool {
	if v == nil || v.Kind != KindBool {
		return false
	}
	return v.BoolValue
}

// GetBytesValue returns the bytes value when Kind is KindBytes.
func (v *Value) GetBytesValue() []byte {
	if v == nil || v.Kind != KindBytes {
		return nil
	}
	return v.BytesValue
}

// GetJSONValue returns the JSON document when Kind is KindJSON.
func (v *Value) GetJSONValue() []byte {
	if v == nil || v.Kind != KindJSON {
		return nil
	}
	return v.BytesValue
}

// StoredPoint is the persisted representation of a vector point.
type StoredPoint struct {
	ID      string
	Vector  []float32
	Payload map[string]*Value
	Version uint64
}

// clonePoint returns a copy of p that shares nothing mutable with it: the
// vector slice and the payload (including nested maps and slices) are deep
// duplicated so callers mutating a returned point cannot corrupt the index.
func clonePoint(p *PointStruct) PointStruct {
	cp := *p

	if p.Vector != nil {
		cp.Vector = make([]float32, len(p.Vector))
		copy(cp.Vector, p.Vector)
	}

	cp.Payload = clonePayload(p.Payload)
	return cp
}

// clonePayload deep-copies a payload map.
func clonePayload(p Payload) Payload {
	if p == nil {
		return nil
	}
	out := make(Payload, len(p))
	for k, v := range p {
		out[k] = deepCopyValue(v)
	}
	return out
}

// deepCopyValue copies the mutable container types a payload can hold. Scalar
// values are returned as-is.
func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[k] = deepCopyValue(item)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = deepCopyValue(item)
		}
		return out
	case []string:
		return append([]string(nil), t...)
	case []int:
		return append([]int(nil), t...)
	case []int64:
		return append([]int64(nil), t...)
	case []float32:
		return append([]float32(nil), t...)
	case []float64:
		return append([]float64(nil), t...)
	case []byte:
		return append([]byte(nil), t...)
	default:
		return v
	}
}
