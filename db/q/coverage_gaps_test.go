package q

import (
	"errors"
	"go/token"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type valueMatcher struct {
	ok  bool
	err error
}

func (m valueMatcher) Match(any) (bool, error) { return m.ok, m.err }
func (m valueMatcher) MatchValue(*reflect.Value) (bool, error) {
	return m.ok, m.err
}

type plainMatcher struct {
	ok  bool
	err error
}

func (m plainMatcher) Match(any) (bool, error) { return m.ok, m.err }

func matchValue(t *testing.T, m Matcher, v any) (bool, error) {
	t.Helper()
	rv := reflect.Indirect(reflect.ValueOf(v))
	return m.(ValueMatcher).MatchValue(&rv)
}

func TestCombinatorsUseValueMatcher(t *testing.T) {
	rec := struct{ A int }{1}

	ok, err := matchValue(t, True(), rec)
	require.NoError(t, err)
	require.True(t, ok)

	// Or: value matcher hit, plain matcher hit, both miss.
	ok, err = matchValue(t, Or(valueMatcher{ok: true}, plainMatcher{ok: false}), rec)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = matchValue(t, Or(valueMatcher{ok: false}, plainMatcher{ok: true}), rec)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = matchValue(t, Or(valueMatcher{ok: false}, plainMatcher{ok: false}), rec)
	require.NoError(t, err)
	require.False(t, ok)

	// And: all must match.
	ok, err = matchValue(t, And(valueMatcher{ok: true}, plainMatcher{ok: true}), rec)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = matchValue(t, And(valueMatcher{ok: false}, plainMatcher{ok: true}), rec)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = matchValue(t, And(valueMatcher{ok: true}, plainMatcher{ok: false}), rec)
	require.NoError(t, err)
	require.False(t, ok)

	// Not: any hit negates.
	ok, err = matchValue(t, Not(valueMatcher{ok: true}), rec)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = matchValue(t, Not(valueMatcher{ok: false}, plainMatcher{ok: false}), rec)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = matchValue(t, Not(plainMatcher{ok: true}), rec)
	require.NoError(t, err)
	require.False(t, ok)

	// Errors propagate from either matcher kind.
	sentinel := errors.New("boom")
	_, err = matchValue(t, Or(valueMatcher{err: sentinel}), rec)
	require.ErrorIs(t, err, sentinel)
	_, err = matchValue(t, Or(plainMatcher{err: sentinel}), rec)
	require.ErrorIs(t, err, sentinel)
	_, err = matchValue(t, And(valueMatcher{err: sentinel}), rec)
	require.ErrorIs(t, err, sentinel)
	_, err = matchValue(t, And(plainMatcher{err: sentinel}), rec)
	require.ErrorIs(t, err, sentinel)
	_, err = matchValue(t, Not(valueMatcher{err: sentinel}), rec)
	require.ErrorIs(t, err, sentinel)
	_, err = matchValue(t, Not(plainMatcher{err: sentinel}), rec)
	require.ErrorIs(t, err, sentinel)
}

func TestInMatcher(t *testing.T) {
	ok, err := In("A", []int{1, 2, 3}).Match(struct{ A int }{2})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = In("A", []int{1, 2, 3}).Match(struct{ A int }{4})
	require.NoError(t, err)
	require.False(t, ok)

	// A non-slice list never matches.
	ok, err = In("A", 42).Match(struct{ A int }{42})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestNumericCompareFallbackTokens(t *testing.T) {
	// Unsupported tokens fall back to go/constant for the helper functions.
	require.False(t, compareInt64(1, 2, token.ADD))
	require.False(t, compareUint64(1, 2, token.ADD))
	require.False(t, compareFloat64(1, 2, token.ADD))

	for _, tok := range []token.Token{token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ} {
		a, b := uint64(1), uint64(2)
		want := map[token.Token]bool{
			token.EQL: false, token.NEQ: true, token.LSS: true,
			token.LEQ: true, token.GTR: false, token.GEQ: false,
		}[tok]
		require.Equal(t, want, compareUint64(a, b, tok), "uint %s", tok)

		fa, fb := 1.5, 2.5
		wantF := map[token.Token]bool{
			token.EQL: false, token.NEQ: true, token.LSS: true,
			token.LEQ: true, token.GTR: false, token.GEQ: false,
		}[tok]
		require.Equal(t, wantF, compareFloat64(fa, fb, tok), "float %s", tok)
	}
}

func TestStrictEqAndCmpMatchers(t *testing.T) {
	ok, err := StrictEq("A", []int{1, 2}).Match(struct{ A []int }{[]int{1, 2}})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = Eq("A", 1).Match(struct{ A int }{1})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = Gt("A", 2).Match(struct{ A int }{3})
	require.NoError(t, err)
	require.True(t, ok)
}

type fmWithHidden struct {
	hidden int
	Name   string
}

func TestMatcherInvalidValues(t *testing.T) {
	m := Eq("A", 1)
	var invalid reflect.Value
	_, err := m.(ValueMatcher).MatchValue(nil)
	require.ErrorIs(t, err, ErrUnknownField)
	_, err = m.(ValueMatcher).MatchValue(&invalid)
	require.ErrorIs(t, err, ErrUnknownField)

	nonStruct := reflect.ValueOf(42)
	_, err = m.(ValueMatcher).MatchValue(&nonStruct)
	require.ErrorIs(t, err, ErrUnknownField)

	rec := reflect.ValueOf(&fmWithHidden{hidden: 1, Name: "x"})
	_, err = Eq("hidden", 1).(ValueMatcher).MatchValue(&rec)
	require.ErrorIs(t, err, ErrUnknownField)

	// Two-field matchers report a missing promoted field.
	_, err = EqF("Name", "Missing").(ValueMatcher).MatchValue(&rec)
	require.ErrorIs(t, err, ErrUnknownField)

	ok, err := EqF("Name", "Name").(ValueMatcher).MatchValue(&rec)
	require.NoError(t, err)
	require.True(t, ok)
}
