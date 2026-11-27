package search

import (
	"strings"
	"unicode"
)

// Tokenizer splits text into terms.
//
// A Tokenizer must be deterministic: the same text must produce the same terms
// on every call and process. Tokens are stored as bucket keys, so they should be
// non-empty and stable across versions of the same index.
type Tokenizer interface {
	Tokenize(string) []string
}

// wordTokenizer emits runs of Unicode letters and digits, lowercased.
type wordTokenizer struct{}

// Words returns the default tokenizer: maximal runs of Unicode letters and
// digits, lowercased. Punctuation, whitespace and symbols separate terms. CJK
// text becomes one term per run unless CJK trigrams are added with WordsWithCJK.
func Words() Tokenizer { return wordTokenizer{} }

// Tokenize implements Tokenizer.
func (wordTokenizer) Tokenize(text string) []string {
	var out []string
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			out = append(out, strings.ToLower(b.String()))
			b.Reset()
		}
	}
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return out
}

// Name identifies the tokenizer in persisted index metadata.
func (wordTokenizer) Name() string { return "words" }

// cjkTokenizer emits word tokens plus overlapping trigrams for runs of Han,
// Hiragana, Katakana and Hangul characters. A run shorter than three characters
// contributes the run itself.
type cjkTokenizer struct{}

// WordsWithCJK returns Words plus CJK trigrams, so Chinese, Japanese and Korean
// text can match substrings without a dictionary-based segmenter.
func WordsWithCJK() Tokenizer { return cjkTokenizer{} }

// Tokenize implements Tokenizer.
func (cjkTokenizer) Tokenize(text string) []string {
	out := (wordTokenizer{}).Tokenize(text)

	var run []rune
	flush := func() {
		if len(run) == 0 {
			return
		}
		s := string(run)
		if len(run) < 3 {
			out = append(out, s)
		} else {
			for i := 0; i+3 <= len(run); i++ {
				out = append(out, string(run[i:i+3]))
			}
		}
		run = run[:0]
	}
	for _, r := range text {
		if isCJK(r) {
			run = append(run, r)
			continue
		}
		flush()
	}
	flush()
	return out
}

// Name identifies the tokenizer in persisted index metadata.
func (cjkTokenizer) Name() string { return "words+cjk" }

// isCJK reports whether r belongs to a CJK script covered by the trigram
// tokenizer.
func isCJK(r rune) bool {
	switch {
	case r >= 0x4E00 && r <= 0x9FFF: // CJK Unified Ideographs
		return true
	case r >= 0x3400 && r <= 0x4DBF: // CJK Extension A
		return true
	case r >= 0xF900 && r <= 0xFAFF: // CJK Compatibility Ideographs
		return true
	case r >= 0x3040 && r <= 0x309F: // Hiragana
		return true
	case r >= 0x30A0 && r <= 0x30FF: // Katakana
		return true
	case r >= 0xAC00 && r <= 0xD7AF: // Hangul Syllables
		return true
	case r >= 0x1100 && r <= 0x11FF: // Hangul Jamo
		return true
	}
	return false
}

// TokenizerName returns the persisted identifier for t: "words", "words+cjk"
// or "custom". A caller that must carry a tokenizer through a serialized
// command (for example cluster replication) can persist the name instead of the
// value.
func TokenizerName(t Tokenizer) string { return tokenizerName(t) }

// tokenizerName returns the persisted identifier for t.
func tokenizerName(t Tokenizer) string {
	if t == nil {
		return "words"
	}
	if named, ok := t.(interface{ Name() string }); ok {
		return named.Name()
	}
	return "custom"
}

// tokenizerByName resolves one of the built-in tokenizers from its persisted
// identifier. It reports false for an unknown name or for "custom", which only
// the caller can supply.
func tokenizerByName(name string) (Tokenizer, bool) {
	switch name {
	case "words":
		return Words(), true
	case "words+cjk":
		return WordsWithCJK(), true
	}
	return nil, false
}

// resolveTokenizer returns t, or Words when t is nil.
func resolveTokenizer(t Tokenizer) Tokenizer {
	if t == nil {
		return Words()
	}
	return t
}
