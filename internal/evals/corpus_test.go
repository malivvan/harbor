package evals

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyntheticCorpusDeterministic(t *testing.T) {
	vectors, queries := SyntheticCorpus(50, 8, 42)
	again, againQueries := SyntheticCorpus(50, 8, 42)
	require.Equal(t, vectors, again)
	require.Equal(t, queries, againQueries)

	other, _ := SyntheticCorpus(50, 8, 43)
	require.NotEqual(t, vectors, other)
}

func TestSyntheticCorpusShape(t *testing.T) {
	vectors, queries := SyntheticCorpus(100, 16, 1)
	require.Len(t, vectors, 100)
	require.NotEmpty(t, queries)
	for _, v := range vectors {
		require.Len(t, v, 16)
	}
	for _, q := range queries {
		require.Len(t, q, 16)
	}

	v, q := SyntheticCorpus(0, 16, 1)
	require.Nil(t, v)
	require.Nil(t, q)
	v, q = SyntheticCorpus(10, 0, 1)
	require.Nil(t, v)
	require.Nil(t, q)
}
