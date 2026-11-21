package encryption

import "fmt"

// ResolveKey validates the master key and returns a copy of it. Every store
// takes a single raw 32-byte key; rotation is handled by the store's keychain,
// which keeps previous keys as decryption fallbacks.
func ResolveKey(key []byte) ([]byte, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
	}
	return append([]byte(nil), key...), nil
}
