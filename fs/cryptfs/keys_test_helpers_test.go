package cryptfs

import "crypto/sha256"

// testProvider returns a deterministic 32-byte raw key provider for tests.
// The seed lets tests derive independent keys without hard-coding key material.
func testProvider(seed string) *RawKeyProvider {
	sum := sha256.Sum256([]byte(seed))
	provider, err := NewRawKeyProvider(sum[:])
	if err != nil {
		panic(err)
	}
	return provider
}
