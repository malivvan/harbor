package encryption

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testMarkerSpec(extra int) MarkerSpec {
	return MarkerSpec{Magic: "TST1", ExtraFieldSize: extra}
}

func TestMarkerRoundTrip(t *testing.T) {
	t.Run("without extra field", func(t *testing.T) {
		spec := testMarkerSpec(0)
		canary := []byte("sealed-canary")

		raw, err := EncodeMarker(spec, Marker{Cipher: CipherXChaCha20Poly1305, Flags: 0b101, Canary: canary})
		require.NoError(t, err)
		require.Len(t, raw, markerHeaderSize+len(canary))

		decoded, err := DecodeMarker(spec, raw)
		require.NoError(t, err)
		require.Equal(t, CipherXChaCha20Poly1305, decoded.Cipher)
		require.Equal(t, uint8(0b101), decoded.Flags)
		require.Zero(t, decoded.Extra)
		require.Equal(t, canary, decoded.Canary)
	})

	t.Run("with extra field", func(t *testing.T) {
		spec := testMarkerSpec(8)
		canary := []byte("sealed-canary")

		raw, err := EncodeMarker(spec, Marker{
			Cipher: CipherAES256GCM,
			Flags:  0b011,
			Extra:  1 << 20,
			Canary: canary,
		})
		require.NoError(t, err)
		require.Len(t, raw, markerHeaderSize+8+len(canary))

		decoded, err := DecodeMarker(spec, raw)
		require.NoError(t, err)
		require.Equal(t, uint64(1<<20), decoded.Extra)
		require.Equal(t, canary, decoded.Canary)
	})

	t.Run("canary is copied", func(t *testing.T) {
		spec := testMarkerSpec(0)
		canary := []byte("sealed-canary")

		raw, err := EncodeMarker(spec, Marker{Canary: canary})
		require.NoError(t, err)

		decoded, err := DecodeMarker(spec, raw)
		require.NoError(t, err)

		raw[len(raw)-1] ^= 0xff
		require.NotEqual(t, raw[len(raw)-1], decoded.Canary[len(decoded.Canary)-1],
			"the decoded canary must not alias the caller's buffer")
	})
}

func TestMarkerDecodeErrors(t *testing.T) {
	spec := testMarkerSpec(0)

	valid, err := EncodeMarker(spec, Marker{Cipher: CipherAES256GCM, Canary: []byte("c")})
	require.NoError(t, err)

	t.Run("short", func(t *testing.T) {
		_, err := DecodeMarker(spec, valid[:markerHeaderSize-1])
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("wrong magic", func(t *testing.T) {
		other := MarkerSpec{Magic: "OTHR"}
		_, err := DecodeMarker(other, valid)
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("unsupported version", func(t *testing.T) {
		bumped := append([]byte(nil), valid...)
		bumped[4] = markerVersion + 1
		_, err := DecodeMarker(spec, bumped)
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("missing extra field", func(t *testing.T) {
		// A marker written without the extra field cannot be read as one with it.
		_, err := DecodeMarker(testMarkerSpec(8), valid)
		require.ErrorIs(t, err, ErrWrongMode)
	})
}

func TestMarkerSpecValidation(t *testing.T) {
	_, err := EncodeMarker(MarkerSpec{Magic: "TOOLONG"}, Marker{})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = EncodeMarker(MarkerSpec{Magic: "TST1", ExtraFieldSize: 4}, Marker{})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = DecodeMarker(MarkerSpec{Magic: ""}, nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestVerifyCanary(t *testing.T) {
	kc, err := NewKeychain(CipherAuto, [][]byte{testKey(30)})
	require.NoError(t, err)

	sealed, err := kc.Seal([]byte("storm/test/aead/v1"))
	require.NoError(t, err)
	require.NoError(t, VerifyCanary(kc, sealed, "storm/test/aead/v1"))

	// A different expectation is a mismatch.
	require.ErrorIs(t, VerifyCanary(kc, sealed, "storm/test/aead/v2"), ErrAuthFailed)

	// A different key cannot open the canary.
	other, err := NewKeychain(CipherAuto, [][]byte{testKey(31)})
	require.NoError(t, err)
	require.ErrorIs(t, VerifyCanary(other, sealed, "storm/test/aead/v1"), ErrAuthFailed)

	// A plaintext keychain cannot verify anything.
	require.ErrorIs(t, VerifyCanary(nil, sealed, "storm/test/aead/v1"), ErrInvalidConfig)
}
