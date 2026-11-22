package q

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type fmEmbedded struct {
	Name string
}

type fmOuter struct {
	fmEmbedded
	Age int
}

// TestFieldMatcherPromotedField verifies that cached field paths handle
// promoted fields and unknown names correctly.
func TestFieldMatcherPromotedField(t *testing.T) {
	m := Eq("Name", "x")

	ok, err := m.Match(&fmOuter{fmEmbedded: fmEmbedded{Name: "x"}, Age: 1})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = m.Match(&fmOuter{fmEmbedded: fmEmbedded{Name: "y"}, Age: 1})
	require.NoError(t, err)
	require.False(t, ok)

	// Repeated calls exercise the cached path.
	for i := 0; i < 10; i++ {
		ok, err = m.Match(&fmOuter{fmEmbedded: fmEmbedded{Name: "x"}})
		require.NoError(t, err)
		require.True(t, ok)
	}

	_, err = Eq("Missing", 1).Match(&fmOuter{})
	require.ErrorIs(t, err, ErrUnknownField)
}

// TestField2FieldMatcherUnknownField covers the cached two-field path.
func TestField2FieldMatcherUnknownField(t *testing.T) {
	m := EqF("Age", "Missing")
	_, err := m.Match(&fmOuter{Age: 1})
	require.ErrorIs(t, err, ErrUnknownField)
}
