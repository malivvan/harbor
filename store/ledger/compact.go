package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// canonicalBytes serialises v to the deterministic JSON that audit events are
// signed and hash-chained over.
//
// The output is byte-stable across runs and processes: object keys are sorted
// lexicographically at every level of nesting, there is no insignificant
// whitespace, and numbers keep their exact source representation. That is what
// makes it possible to sign the serialised form of a struct — or to compare two
// independently produced encodings for equality — without a canonicalisation
// step on the receiving side.
//
// Conventions, all of which the tests pin:
//
//   - map keys ascending by byte value at every depth;
//   - no spaces, indentation or trailing newline;
//   - numbers pass through unchanged (no float reformatting), so 1, 1.0 and 1e0
//     stay distinct;
//   - nil encodes as null;
//   - strings are escaped by encoding/json's rules.
//
// Input may be any JSON-marshallable value. A value that is already encoded
// (a json.RawMessage or []byte holding JSON) is re-encoded through the canonical
// path, so pre-encoded input cannot smuggle in unsorted keys.
func canonicalBytes(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("audit: marshal: %w", err)
	}
	return fromRaw(json.RawMessage(raw))
}

// fromRaw re-encodes an already-marshalled JSON value canonically.
func fromRaw(raw json.RawMessage) ([]byte, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep the literal numeric text instead of float64
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("audit: decode: %w", err)
	}
	return encode(v)
}

// encode recursively writes one decoded JSON value in canonical form.
func encode(v any) ([]byte, error) {
	switch val := v.(type) {
	case map[string]any:
		return encodeObject(val)
	case []any:
		return encodeArray(val)
	case json.Number:
		return []byte(val.String()), nil
	case string:
		return json.Marshal(val)
	case bool:
		return json.Marshal(val)
	case nil:
		return []byte("null"), nil
	default:
		return json.Marshal(val)
	}
}

func encodeObject(m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return nil, fmt.Errorf("audit: marshal key %q: %w", k, err)
		}
		buf.Write(keyBytes)
		buf.WriteByte(':')
		valBytes, err := encode(m[k])
		if err != nil {
			return nil, fmt.Errorf("audit: marshal value for key %q: %w", k, err)
		}
		buf.Write(valBytes)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func encodeArray(a []any) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, elem := range a {
		if i > 0 {
			buf.WriteByte(',')
		}
		b, err := encode(elem)
		if err != nil {
			return nil, fmt.Errorf("audit: marshal array element %d: %w", i, err)
		}
		buf.Write(b)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}
