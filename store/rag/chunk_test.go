package rag

import (
	"crypto/sha256"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/search"
)

func TestSplitReconstructsAndKeepsOffsets(t *testing.T) {
	text := "One sentence here. Another sentence follows! A third one? And a fourth that is a little longer."
	chunks := Split(text, Options{MaxRunes: 24, Overlap: 6, Split: SplitSentence})
	require.NotEmpty(t, chunks)

	for i, chunk := range chunks {
		require.Equal(t, i, chunk.Index)
		require.Equal(t, text[chunk.Start:chunk.End], chunk.Text)
		require.Equal(t, sha256.Sum256([]byte(chunk.Text)), chunk.Digest)
	}

	var rebuilt strings.Builder
	for i, chunk := range chunks {
		if i == 0 {
			rebuilt.WriteString(chunk.Text)
			continue
		}
		overlap := chunks[i-1].End - chunk.Start
		require.GreaterOrEqual(t, overlap, 0)
		rebuilt.WriteString(chunk.Text[overlap:])
	}
	require.Equal(t, text, rebuilt.String())
}

func TestSplitStable(t *testing.T) {
	text := "Alpha. Beta. Gamma. Delta. Epsilon. Zeta."
	a := Split(text, Options{MaxRunes: 12, Overlap: 3, Split: SplitSentence})
	b := Split(text, Options{MaxRunes: 12, Overlap: 3, Split: SplitSentence})
	require.Equal(t, a, b)
}

func TestSplitEmptyAndShort(t *testing.T) {
	require.Empty(t, Split("", Options{}))
	chunks := Split("short", Options{MaxRunes: 100})
	require.Len(t, chunks, 1)
	require.Equal(t, "short", chunks[0].Text)
	require.Equal(t, 0, chunks[0].Start)
	require.Equal(t, 5, chunks[0].End)
}

func TestSplitParagraphAndNone(t *testing.T) {
	text := "first paragraph\n\nsecond paragraph\n\nthird"
	paras := Split(text, Options{MaxRunes: 30, Split: SplitParagraph})
	require.NotEmpty(t, paras)
	require.Contains(t, paras[0].Text, "paragraph")
	for _, chunk := range paras {
		require.NotEmpty(t, chunk.Text)
	}

	none := Split(text, Options{MaxRunes: 10, Split: SplitNone})
	require.NotEmpty(t, none)
	for _, chunk := range none {
		require.LessOrEqual(t, utf8.RuneCountInString(chunk.Text), 10)
	}
}

func TestSplitCJKSafe(t *testing.T) {
	text := strings.Repeat("你好世界，这是一个测试。", 10)
	chunks := Split(text, Options{MaxRunes: 7, Overlap: 2, Split: SplitSentence})
	require.NotEmpty(t, chunks)
	var rebuilt strings.Builder
	for i, chunk := range chunks {
		require.True(t, utf8.ValidString(chunk.Text))
		require.Equal(t, text[chunk.Start:chunk.End], chunk.Text)
		if i == 0 {
			rebuilt.WriteString(chunk.Text)
			continue
		}
		rebuilt.WriteString(chunk.Text[chunks[i-1].End-chunk.Start:])
	}
	require.Equal(t, text, rebuilt.String())
}

func TestSplitMaxTokens(t *testing.T) {
	text := "alpha beta gamma delta epsilon zeta eta theta"
	chunks := Split(text, Options{MaxRunes: 100, MaxTokens: 3, Tokenizer: search.Words()})
	require.NotEmpty(t, chunks)
	for _, chunk := range chunks {
		require.LessOrEqual(t, len(search.Words().Tokenize(chunk.Text)), 3)
	}
}

func TestProvenancePayloadAndWindow(t *testing.T) {
	payload := WithProvenance(Provenance{DocumentID: "doc", Index: 2, Start: 10, End: 20})
	require.Equal(t, "doc", payload[KeyDocumentID])
	require.Equal(t, 2, payload[KeyChunkIndex])
	require.Equal(t, 10, payload[KeyStart])
	require.Equal(t, 20, payload[KeyEnd])

	filter := WindowFilter("doc", 2)
	require.Len(t, filter.Must, 2)
	require.Equal(t, KeyDocumentID, filter.Must[0].Key)
	require.Equal(t, 1, filter.Must[1].Range.GTE)
	require.Equal(t, 3, filter.Must[1].Range.LTE)
}
