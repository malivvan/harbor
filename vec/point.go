package vec

//go:generate go run ./gen

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
	// KindBytes stores a byte slice.
	KindBytes
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

// StoredPoint is the persisted representation of a vector point.
type StoredPoint struct {
	ID      string
	Vector  []float32
	Payload map[string]*Value
}
