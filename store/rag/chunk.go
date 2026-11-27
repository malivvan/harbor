// Package rag provides deterministic text chunking and the provenance keys
// used to link a chunk back to its parent document and its neighbours.
//
// The package is deliberately policy-free: it does not call a model, choose an
// embedding, or decide which chunks to retrieve. It splits text at stable
// boundaries, reports byte offsets into the original, and stamps the vector
// payload keys a windowing query needs. Any caller that indexes long text can
// use it; nothing here is specific to a companion.
package rag

import (
	"crypto/sha256"
	"strings"
	"unicode/utf8"

	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/vector"
)

// Provenance payload keys. They are reserved names in the vector payload, so a
// caller can find a chunk and its neighbours with one filter.
const (
	KeyDocumentID = "__document_id__"
	KeyChunkIndex = "__chunk_index__"
	KeyStart      = "__start__"
	KeyEnd        = "__end__"
)

// SplitFunc chooses a cut point at or before maxEnd. start and maxEnd are byte
// offsets into text and are always rune boundaries. A return <= start or >
// maxEnd is clamped by Split, so an implementation may simply return maxEnd
// when it finds no boundary.
type SplitFunc func(text string, start, maxEnd int) int

// SplitSentence cuts after sentence punctuation or a newline, falling back to
// the rune limit.
func SplitSentence(text string, start, maxEnd int) int {
	for i := maxEnd - 1; i >= start; i-- {
		switch text[i] {
		case '.', '!', '?', '\n':
			return i + 1
		}
	}
	return maxEnd
}

// SplitParagraph cuts after a blank line, falling back to the rune limit.
func SplitParagraph(text string, start, maxEnd int) int {
	if i := strings.LastIndex(text[start:maxEnd], "\n\n"); i >= 0 {
		return start + i + 2
	}
	return maxEnd
}

// SplitNone cuts exactly at the rune limit.
func SplitNone(_ string, _, maxEnd int) int {
	return maxEnd
}

// Options controls Split. Zero values select MaxRunes 1000, no overlap, and
// SplitSentence.
type Options struct {
	// MaxRunes bounds a chunk's length in Unicode code points.
	MaxRunes int
	// Overlap is the number of runes repeated from the previous chunk. It must
	// be smaller than MaxRunes.
	Overlap int
	// Split chooses the boundary. A nil Split selects SplitSentence.
	Split SplitFunc
	// MaxTokens, when positive and Tokenizer is non-nil, further shrinks a
	// chunk until Tokenizer.Tokenize reports at most MaxTokens terms.
	MaxTokens int
	// Tokenizer counts terms for MaxTokens. It is ignored when MaxTokens is
	// zero.
	Tokenizer search.Tokenizer
}

// Chunk is one piece of a parent document.
type Chunk struct {
	// Index is the zero-based position in the parent.
	Index int
	// Text is the exact slice text[Start:End].
	Text string
	// Start and End are byte offsets into the parent.
	Start int
	// End is exclusive.
	End int
	// Digest is SHA-256 of Text, so identical chunks deduplicate.
	Digest [32]byte
}

// Split divides text into chunks. Concatenating the chunks by their offsets
// reconstructs the original exactly; successive chunks may overlap by Overlap
// runes.
func Split(text string, opts Options) []Chunk {
	if text == "" {
		return nil
	}
	if opts.MaxRunes <= 0 {
		opts.MaxRunes = 1000
	}
	if opts.Overlap < 0 {
		opts.Overlap = 0
	}
	if opts.Overlap >= opts.MaxRunes {
		opts.Overlap = opts.MaxRunes / 4
	}
	if opts.Split == nil {
		opts.Split = SplitSentence
	}

	var out []Chunk
	start := 0
	for start < len(text) {
		maxEnd := runeEnd(text, start, opts.MaxRunes)
		if maxEnd <= start {
			maxEnd = len(text)
		}
		cut := opts.Split(text, start, maxEnd)
		if cut <= start || cut > maxEnd {
			cut = maxEnd
		}
		cut = runeBoundary(text, cut)
		if cut <= start {
			cut = len(text)
		}
		if opts.MaxTokens > 0 && opts.Tokenizer != nil {
			for cut > start {
				if len(opts.Tokenizer.Tokenize(text[start:cut])) <= opts.MaxTokens {
					break
				}
				cut = prevRuneBoundary(text, cut)
			}
		}
		if cut <= start {
			break
		}
		piece := text[start:cut]
		out = append(out, Chunk{
			Index:  len(out),
			Text:   piece,
			Start:  start,
			End:    cut,
			Digest: sha256.Sum256([]byte(piece)),
		})
		if cut >= len(text) {
			break
		}
		next := start
		if opts.Overlap > 0 {
			next = prevNthRune(text, cut, opts.Overlap)
		}
		if next <= start {
			next = cut
		}
		start = next
	}
	return out
}

// Provenance identifies a chunk inside its parent.
type Provenance struct {
	DocumentID string
	Index      int
	Start      int
	End        int
}

// WithProvenance returns a payload carrying the chunk provenance keys. Merge
// it with the caller's own payload before upserting the vector.
func WithProvenance(p Provenance) vector.Payload {
	return vector.Payload{
		KeyDocumentID: p.DocumentID,
		KeyChunkIndex: p.Index,
		KeyStart:      p.Start,
		KeyEnd:        p.End,
	}
}

// WindowFilter returns a filter selecting a chunk and its immediate neighbours
// in the same document.
func WindowFilter(documentID string, index int) *vector.Filter {
	from := index - 1
	if from < 0 {
		from = 0
	}
	return &vector.Filter{Must: []vector.Condition{
		{Key: KeyDocumentID, Type: vector.MatchTypeExact, Match: vector.MatchValue{Value: documentID}},
		{Key: KeyChunkIndex, Type: vector.MatchTypeRange, Range: &vector.RangeValue{GTE: from, LTE: index + 1}},
	}}
}

// runeEnd returns the byte offset of the rune at start+maxRunes, or len(text).
func runeEnd(text string, start, maxRunes int) int {
	i := start
	for n := 0; n < maxRunes && i < len(text); n++ {
		_, size := utf8.DecodeRuneInString(text[i:])
		if size == 0 {
			break
		}
		i += size
	}
	return i
}

// runeBoundary moves i back to the nearest rune start.
func runeBoundary(text string, i int) int {
	if i >= len(text) {
		return len(text)
	}
	for i > 0 && !utf8.RuneStart(text[i]) {
		i--
	}
	return i
}

// prevRuneBoundary returns the start of the rune before i.
func prevRuneBoundary(text string, i int) int {
	if i <= 0 {
		return 0
	}
	return runeBoundary(text, i-1)
}

// prevNthRune walks back n runes from i, not before 0.
func prevNthRune(text string, i, n int) int {
	for ; n > 0 && i > 0; n-- {
		i = prevRuneBoundary(text, i)
	}
	return i
}
