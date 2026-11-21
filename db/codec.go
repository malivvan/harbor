package db

import "encoding/json"

// jsonCodecName is the stable identifier stored in bucket metadata. It must not
// change: databases written by earlier versions record it.
const jsonCodecName = "json"

// Codec represents a codec used to marshal and unmarshal entities.
type Codec interface {
	// Marshal encodes v.
	Marshal(v interface{}) ([]byte, error)

	// Unmarshal decodes b into v.
	Unmarshal(b []byte, v interface{}) error

	// Name returns the stable codec identifier stored in Storm metadata.
	Name() string
}

// JSONCodec encodes and decodes entities with encoding/json. It is the codec
// Storm uses when Open or OpenWithConfig receives a nil codec.
var JSONCodec Codec = jsonCodec{}

// jsonCodec implements Codec on top of encoding/json.
type jsonCodec struct{}

// Marshal encodes v.
func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes b into v.
func (jsonCodec) Unmarshal(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}

// Name returns the codec identifier stored in Storm metadata.
func (jsonCodec) Name() string {
	return jsonCodecName
}
