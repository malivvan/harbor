// Package codec contains sub-packages with different codecs that can be used
// to encode and decode entities in Storm.
package codec

// MarshalUnmarshaler represents a codec used to marshal and unmarshal entities.
type MarshalUnmarshaler interface {
	// Marshal encodes v.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal decodes b into v.
	Unmarshal(b []byte, v interface{}) error
	// Name returns the stable codec identifier stored in Storm metadata.
	Name() string
}
