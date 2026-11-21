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
