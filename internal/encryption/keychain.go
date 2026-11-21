package encryption

import "fmt"

// Keychain holds the cipher engines for one cipher suite and one or more master
// keys. The first key is the primary key used for new writes; every key is
// tried in order when decrypting, so a rotation can keep the previous key as a
// fallback.
//
// A nil Keychain is a plaintext keychain: Seal and Open are exact passthroughs,
// which is what every store relies on when encryption is disabled.
type Keychain struct {
	cipher  CipherSuite
	engine  CipherEngine
	engines []CipherEngine
	keys    [][]byte
}

// NewKeychain builds a keychain for the given cipher suite and master keys,
// primary first. Every key must be KeySize bytes.
func NewKeychain(suite CipherSuite, keys [][]byte) (*Keychain, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: at least one key required", ErrInvalidConfig)
	}

	resolved, err := Resolve(suite)
	if err != nil {
		return nil, ErrUnsupportedCipher
	}

	kc := &Keychain{
		cipher:  resolved,
		engines: make([]CipherEngine, 0, len(keys)),
		keys:    make([][]byte, 0, len(keys)),
	}
	for _, key := range keys {
		if len(key) != KeySize {
			return nil, fmt.Errorf("%w: key must be %d bytes, got %d", ErrInvalidKey, KeySize, len(key))
		}
		engine, err := NewCipherEngine(resolved, key)
		if err != nil {
			return nil, err
		}
		kc.engines = append(kc.engines, engine)
		kc.keys = append(kc.keys, append([]byte(nil), key...))
	}
	kc.engine = kc.engines[0]
	return kc, nil
}

// Cipher returns the resolved cipher suite. A nil (plaintext) keychain reports
// CipherAuto, which is never written to a marker.
func (k *Keychain) Cipher() CipherSuite {
	if k == nil {
		return CipherAuto
	}
	return k.cipher
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

// PrimaryEngine returns the engine used for new writes, or nil for a plaintext
// keychain.
func (k *Keychain) PrimaryEngine() CipherEngine {
	if k == nil {
		return nil
	}
	return k.engine
}

// Seal encrypts plain with the primary engine, prefixing a fresh random nonce.
// An empty payload and a nil keychain are pass-throughs: stores that skip empty
// values therefore keep skipping them, and never write a nonce-only payload.
func (k *Keychain) Seal(plain []byte) ([]byte, error) {
	if k == nil || len(plain) == 0 {
		return plain, nil
	}

	nonce, err := GenerateNonce(k.cipher)
	if err != nil {
		return nil, err
	}
	sealed, err := k.engine.Encrypt(nonce, plain)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	return append(out, sealed...), nil
}

// Open decrypts raw with the primary engine and then any fallback engine. An
// empty payload and a nil keychain are pass-throughs; anything that no engine
// authenticates yields ErrAuthFailed.
func (k *Keychain) Open(raw []byte) ([]byte, error) {
	if k == nil || len(raw) == 0 {
		return raw, nil
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

// WithKey returns a keychain that keeps the cipher suite and writes with key,
// dropping every current key.
func (k *Keychain) WithKey(key []byte) (*Keychain, error) {
	if k == nil {
		return nil, fmt.Errorf("%w: plaintext keychain has no key to rotate", ErrInvalidConfig)
	}
	return NewKeychain(k.cipher, [][]byte{key})
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
	return NewKeychain(k.cipher, keys)
}
