package cluster

import (
	"context"
	"fmt"

	"github.com/malivvan/tempest/store/search"
)

// Search is the replicated write view of the full-text sub-store.
type Search struct{ n *Node }

type searchCreateArg struct {
	Name      string  `json:"name"`
	K1        float64 `json:"k1"`
	B         float64 `json:"b"`
	Tokenizer string  `json:"tokenizer"`
}

type searchUpsertArg struct {
	Name string `json:"name"`
	ID   string `json:"id"`
	Text string `json:"text"`
}

type searchDeleteArg struct {
	Name string   `json:"name"`
	IDs  []string `json:"ids"`
}

type searchDropArg struct {
	Name string `json:"name"`
}

// Search returns the replicated full-text sub-store.
func (n *Node) Search() *Search {
	if n == nil {
		return nil
	}
	return &Search{n: n}
}

// Create replicates search.Store.Open for a new index. The command carries the
// tokenizer's persisted name, because a tokenizer value cannot be serialized: a
// custom tokenizer is rejected instead of being silently replaced by the
// default.
func (s *Search) Create(name string, cfg search.Config) error {
	if s == nil || s.n == nil {
		return ErrClosed
	}
	tokenizer := search.TokenizerName(cfg.Tokenizer)
	if tokenizer == "custom" {
		return fmt.Errorf("%w: custom search tokenizers cannot be replicated", ErrInvalidConfig)
	}
	_, err := s.n.apply(opSearchCreate, searchCreateArg{
		Name:      name,
		K1:        cfg.K1,
		B:         cfg.B,
		Tokenizer: tokenizer,
	})
	return err
}

// Upsert replicates search.Index.Upsert.
func (s *Search) Upsert(name, id, text string) error {
	if s == nil || s.n == nil {
		return ErrClosed
	}
	_, err := s.n.apply(opSearchUpsert, searchUpsertArg{Name: name, ID: id, Text: text})
	return err
}

// Delete replicates search.Index.Delete for one or more document ids.
func (s *Search) Delete(name string, ids ...string) error {
	if s == nil || s.n == nil {
		return ErrClosed
	}
	_, err := s.n.apply(opSearchDelete, searchDeleteArg{Name: name, IDs: ids})
	return err
}

// Drop replicates search.Store.Drop, removing an index and all its data.
func (s *Search) Drop(name string) error {
	if s == nil || s.n == nil {
		return ErrClosed
	}
	_, err := s.n.apply(opSearchDrop, searchDropArg{Name: name})
	return err
}

func (f *fsm) applySearchCreate(ctx context.Context, a searchCreateArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Search().Open(a.Name, search.Config{
		K1:        a.K1,
		B:         a.B,
		Tokenizer: searchTokenizerByName(a.Tokenizer),
	})
	return err
}

func (f *fsm) applySearchUpsert(ctx context.Context, a searchUpsertArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	idx, err := st.Search().Open(a.Name, search.Config{})
	if err != nil {
		return err
	}
	return idx.Upsert(ctx, a.ID, a.Text)
}

func (f *fsm) applySearchDelete(ctx context.Context, a searchDeleteArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	idx, err := st.Search().Open(a.Name, search.Config{})
	if err != nil {
		return err
	}
	return idx.Delete(ctx, a.IDs...)
}

func (f *fsm) applySearchDrop(ctx context.Context, a searchDropArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Search().Drop(ctx, a.Name)
}

// searchTokenizerByName resolves a replicated tokenizer name. Only the built-in
// tokenizers can travel through the log.
func searchTokenizerByName(name string) search.Tokenizer {
	if name == "words+cjk" {
		return search.WordsWithCJK()
	}
	return search.Words()
}
