package crypto

import (
	"encoding/hex"
	"testing"
)

// RFC 5869, Appendix A.1 (SHA-256).
func TestHKDFSHA256RFC5869(t *testing.T) {
	ikm := make([]byte, 22)
	for i := range ikm {
		ikm[i] = 0x0b
	}
	salt := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	info := []byte{0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9}
	want, err := hex.DecodeString("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865")
	if err != nil {
		t.Fatal(err)
	}
	got := hkdfSHA256(ikm, salt, info, 42)
	if string(got) != string(want) {
		t.Fatalf("HKDF = %x, want %x", got, want)
	}
}
