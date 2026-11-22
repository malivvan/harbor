package crypto

import (
	"crypto/rand"
	"fmt"
	"sync"
)

// Keychain holds the AES-256-GCM engines for one or more master keys. The first
// key is the primary key used for new writes; every key is tried in order when
// decrypting, so a rotation can keep the previous key as a fallback.
//
// A nil Keychain is a plaintext keychain: Seal and Open are exact passthroughs,
// which is what every store relies on when encryption is disabled.
type Keychain struct {
	engine  *AESGCMEngine
	engines []*AESGCMEngine
	keys    [][]byte
}

// NewKeychain builds a keychain for the given master keys, primary first. Every
// key must be KeySize bytes.
func NewKeychain(keys [][]byte) (*Keychain, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: at least one key required", ErrInvalidConfig)
	}

	kc := &Keychain{
		engines: make([]*AESGCMEngine, 0, len(keys)),
		keys:    make([][]byte, 0, len(keys)),
	}
	for _, key := range keys {
		if len(key) != KeySize {
			return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
		}
		engine, err := NewAESGCMEngine(key)
		if err != nil {
			return nil, err
		}
		kc.engines = append(kc.engines, engine)
		kc.keys = append(kc.keys, append([]byte(nil), key...))
	}
	kc.engine = kc.engines[0]
	return kc, nil
}

// Keys returns a copy of the master keys, primary first.
func (k *Keychain) Keys() [][]byte {
	if k == nil {
		return nil
	}
	keys := make([][]byte, 0, len(k.keys))
	for _, key := range k.keys {
		keys = append(keys, append([]byte(nil), key...))
	}
	return keys
}

// Seal encrypts plain with the primary key, prefixing a fresh random nonce. A
// nil keychain is a passthrough. Empty plaintext is sealed as well: an empty
// payload at rest is therefore distinguishable from a valid sealed value, so
// truncating a value to zero bytes cannot bypass authentication.
func (k *Keychain) Seal(plain []byte) ([]byte, error) {
	if k == nil {
		return plain, nil
	}

	nonceSize := k.engine.NonceSize()
	// Lay out nonce||ciphertext in one pre-sized buffer instead of
	// allocating the nonce, the ciphertext and the combined result.
	out := make([]byte, nonceSize, nonceSize+len(plain)+k.engine.Overhead())
	if err := gcmNonces.read(out); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}

	sealed, err := k.engine.SealInto(out, out[:nonceSize], plain)
	if err != nil {
		return nil, err
	}
	return sealed, nil
}

// Open decrypts raw with the primary key and then any fallback key. A nil
// keychain is a passthrough. An empty payload at rest is rejected: it cannot be
// a sealed value, so it must not be treated as empty plaintext.
func (k *Keychain) Open(raw []byte) ([]byte, error) {
	if k == nil {
		return raw, nil
	}
	if len(raw) == 0 {
		return nil, ErrAuthFailed
	}

	for _, engine := range k.engines {
		nonceSize := engine.NonceSize()
		if len(raw) < nonceSize+engine.Overhead() {
			continue
		}
		plain, err := engine.Decrypt(raw[:nonceSize], raw[nonceSize:])
		if err == nil {
			return plain, nil
		}
	}
	return nil, ErrAuthFailed
}

// WithKey returns a keychain that writes with key, dropping every current key.
func (k *Keychain) WithKey(key []byte) (*Keychain, error) {
	if k == nil {
		return nil, fmt.Errorf("%w: plaintext keychain has no key to rotate", ErrInvalidConfig)
	}
	return NewKeychain([][]byte{key})
}

// WithKeyKeepingFallbacks returns a keychain that writes with key and can still
// decrypt with the current keys, primary first. It is what per-entity rotation
// uses: the rewritten entity moves to the new key while the rest of the store
// stays readable with the old one until RotateAll.
func (k *Keychain) WithKeyKeepingFallbacks(key []byte) (*Keychain, error) {
	if k == nil {
		return nil, fmt.Errorf("%w: plaintext keychain has no key to rotate", ErrInvalidConfig)
	}
	keys := make([][]byte, 0, len(k.keys)+1)
	keys = append(keys, key)
	keys = append(keys, k.keys...)
	return NewKeychain(keys)
}

// nonceBatchSize is the number of CSPRNG bytes buffered per system call. The
// bytes still come from crypto/rand, so nonce uniqueness is unchanged; only
// the per-seal syscall is amortised.
const nonceBatchSize = 4096

// nonceSource serves nonces from a buffered CSPRNG stream. It is safe for
// concurrent use.
type nonceSource struct {
	mu  sync.Mutex
	buf []byte
}

func (s *nonceSource) read(p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(p) > 0 {
		if len(s.buf) == 0 {
			if cap(s.buf) < nonceBatchSize {
				s.buf = make([]byte, nonceBatchSize)
			} else {
				s.buf = s.buf[:nonceBatchSize]
			}
			if _, err := rand.Read(s.buf); err != nil {
				return err
			}
		}
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		p = p[n:]
	}
	return nil
}

// gcmNonces is the process-wide buffered nonce source.
var gcmNonces = &nonceSource{}
