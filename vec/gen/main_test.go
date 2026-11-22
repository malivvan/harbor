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

func TestRunWriteError(t *testing.T) {
	out := filepath.Join(t.TempDir(), "missing", "serializers.go")
	require.Error(t, run(out))
}

func TestMainWritesIntoWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	old, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(old) })

	main()

	_, err = os.Stat(filepath.Join(dir, "binco.gen.go"))
	require.NoError(t, err)
}
