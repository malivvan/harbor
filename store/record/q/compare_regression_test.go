package q

import (
	"go/token"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCompareMixedSignedUnsigned guards against fixed-width casts that wrapped
// large unsigned values or negative signed values.
func TestCompareMixedSignedUnsigned(t *testing.T) {
	require.True(t, compare(uint(1), int(-1), token.GTR))
	require.False(t, compare(int(-1), uint(1), token.GTR))
	require.True(t, compare(uint64(math.MaxUint64), int64(1), token.GTR))
	require.True(t, compare(int64(1), uint64(math.MaxUint64), token.LSS))
	require.True(t, compare(uint64(math.MaxUint64), uint64(math.MaxUint64), token.EQL))
}

// TestCompareNilPointersAreSymmetric guards the typo that made
// compare(nilPtr, nilSlice) true but compare(nilSlice, nilPtr) false.
func TestCompareNilPointersAreSymmetric(t *testing.T) {
	var np *int
	var ns []int
	require.True(t, compare(np, ns, token.EQL))
	require.True(t, compare(ns, np, token.EQL))
	require.False(t, compare(np, ns, token.GTR))
	require.False(t, compare(ns, np, token.GTR))
}

// TestCompareNilTimes guards against nil *time.Time matching every operator.
func TestCompareNilTimes(t *testing.T) {
	now := time.Now()
	nilTime := reflect.Zero(reflect.TypeOf(&now)).Interface()

	for _, tok := range []token.Token{token.EQL, token.GTR, token.GEQ, token.LSS, token.LEQ} {
		require.False(t, compare(nilTime, now, tok), "nil vs time with %s", tok)
		require.False(t, compare(now, nilTime, tok), "time vs nil with %s", tok)
	}
	require.True(t, compare(nilTime, nilTime, token.EQL))
	require.False(t, compare(nilTime, nilTime, token.LSS))
	require.True(t, compare(nilTime, now, token.NEQ))
}

// TestCompareStringFastPath covers every comparison token for same-kind
// strings, which bypasses go/constant on the hot matcher path.
func TestCompareStringFastPath(t *testing.T) {
	cases := []struct {
		a, b string
		tok  token.Token
		want bool
	}{
		{"a", "a", token.EQL, true},
		{"a", "b", token.EQL, false},
		{"a", "b", token.NEQ, true},
		{"a", "b", token.LSS, true},
		{"b", "a", token.LSS, false},
		{"a", "a", token.LEQ, true},
		{"b", "a", token.GTR, true},
		{"a", "b", token.GTR, false},
		{"a", "a", token.GEQ, true},
	}
	for _, c := range cases {
		require.Equal(t, c.want, compare(c.a, c.b, c.tok), "%q %s %q", c.a, c.tok, c.b)
	}
}

// TestCompareNumericFastPath covers same-kind numeric comparisons, which now
// bypass go/constant boxing.
func TestCompareNumericFastPath(t *testing.T) {
	for _, tok := range []token.Token{token.LSS, token.LEQ, token.GTR, token.GEQ, token.EQL, token.NEQ} {
		a, b := int64(-3), int64(7)
		want := map[token.Token]bool{
			token.LSS: true, token.LEQ: true, token.GTR: false,
			token.GEQ: false, token.EQL: false, token.NEQ: true,
		}[tok]
		require.Equal(t, want, compare(a, b, tok), "int64 %s", tok)
	}

	require.True(t, compare(int8(5), int64(5), token.EQL))
	require.True(t, compare(uint8(5), uint64(9), token.LSS))
	require.True(t, compare(float32(1.5), float64(1.5), token.EQL))
}

// TestCompareFloatSpecialValues verifies the direct float path does not panic
// on values that constant.MakeFloat64 rejects.
func TestCompareFloatSpecialValues(t *testing.T) {
	nan := math.NaN()
	inf := math.Inf(1)

	require.True(t, compare(nan, nan, token.NEQ))
	require.False(t, compare(nan, nan, token.EQL))
	require.False(t, compare(nan, 1.0, token.LSS))
	require.False(t, compare(nan, 1.0, token.GTR))

	require.True(t, compare(inf, inf, token.EQL))
	require.True(t, compare(inf, 1.0, token.GTR))
	require.True(t, compare(math.Inf(-1), 1.0, token.LSS))
}
