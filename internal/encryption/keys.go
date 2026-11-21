package encryption

import (
	"fmt"
	"os"
)

// KeyProvider supplies the raw 32-byte master key used for encryption.
type KeyProvider interface {
	Key() ([]byte, error)
}

// RawKeyProvider supplies a fixed 32-byte master key.
type RawKeyProvider struct {
	key []byte
}

// NewRawKeyProvider returns a provider that uses key directly.
func NewRawKeyProvider(key []byte) (*RawKeyProvider, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
	}
	return &RawKeyProvider{key: append([]byte(nil), key...)}, nil
}

// Key returns a copy of the raw master key.
func (p *RawKeyProvider) Key() ([]byte, error) {
	return append([]byte(nil), p.key...), nil
}

// EnvKeyProvider reads a raw 32-byte master key from an environment variable.
type EnvKeyProvider struct {
	envVar string
}

// NewEnvKeyProvider creates a key provider backed by an environment variable.
func NewEnvKeyProvider(envVar string) *EnvKeyProvider {
	return &EnvKeyProvider{envVar: envVar}
}

// Key returns the key stored in the configured environment variable.
func (e *EnvKeyProvider) Key() ([]byte, error) {
	key := []byte(os.Getenv(e.envVar))
	if len(key) == 0 {
		return nil, fmt.Errorf("%w: environment variable %s not set", ErrInvalidKey, e.envVar)
	}
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: key from environment variable must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
	}
	return key, nil
}

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

// Providers returns the wrapped providers, primary first.
func (m *MultiKeyProvider) Providers() []KeyProvider {
	return append([]KeyProvider(nil), m.providers...)
}

// ResolveKeys turns a Key/KeyProvider pair into the master keys, primary first.
// Exactly one of key and provider must be set; a MultiKeyProvider expands into
// all of its keys, and every key must be KeySize bytes.
func ResolveKeys(key []byte, provider KeyProvider) ([][]byte, error) {
	if key != nil && provider != nil {
		return nil, fmt.Errorf("%w: set either Key or KeyProvider, not both", ErrInvalidConfig)
	}

	if provider == nil {
		if len(key) != KeySize {
			return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
		}
		return [][]byte{append([]byte(nil), key...)}, nil
	}

	providers := []KeyProvider{provider}
	if multi, ok := provider.(*MultiKeyProvider); ok {
		providers = multi.providers
	}

	keys := make([][]byte, 0, len(providers))
	for _, p := range providers {
		resolved, err := p.Key()
		if err != nil {
			return nil, err
		}
		if len(resolved) != KeySize {
			return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(resolved))
		}
		keys = append(keys, append([]byte(nil), resolved...))
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: no key provider", ErrInvalidConfig)
	}
	return keys, nil
}

// RequireSingleKey reports whether deterministic key encryption is usable with
// the resolved keys. Deterministic names are derived from one master key, so a
// fallback key could decrypt values but never reproduce the physical names and
// lookups would silently miss. Callers add their own rotation hint.
func RequireSingleKey(keys [][]byte) error {
	if len(keys) > 1 {
		return fmt.Errorf("%w: deterministic key encryption requires a single master key", ErrInvalidConfig)
	}
	return nil
}
