package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunGeneratesSerializers(t *testing.T) {
	out := filepath.Join(t.TempDir(), "serializers.go")
	require.NoError(t, run(out))

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(data), "ValueBinco")
	require.Contains(t, string(data), "StoredPointBinco")
	require.Contains(t, string(data), "ValueKindBinco")
}
