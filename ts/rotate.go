package ts

import "fmt"

// MultiKeyProvider supplies several keys for rotation: the first is used for
// new writes, and every key is tried when decrypting existing data.
type MultiKeyProvider struct {
	providers []KeyProvider
}

// NewMultiKeyProvider creates a multi-key provider. At least one provider is
// required; the first one is the primary key.
func NewMultiKeyProvider(providers ...KeyProvider) (*MultiKeyProvider, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: at least one key provider required", ErrInvalidConfig)
	}
	return &MultiKeyProvider{providers: providers}, nil
}

// Key returns the primary key.
func (m *MultiKeyProvider) Key() ([]byte, error) {
	return m.providers[0].Key()
}

// TryKeys returns every configured key, primary first.
func (m *MultiKeyProvider) TryKeys() ([][]byte, error) {
	keys := make([][]byte, 0, len(m.providers))
	for _, provider := range m.providers {
		key, err := provider.Key()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}
