package search

import (
	"context"
	"math"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// TermContribution is one query term's contribution to a hit score.
type TermContribution struct {
	Term         string
	IDF          float64
	TF           float64
	Contribution float64
}

// ExplainedHit is a search hit with its per-term score breakdown.
type ExplainedHit struct {
	Hit
	Terms []TermContribution
}

// SearchExplain returns the same hits as Search, each with the BM25 terms that
// produced its score. The contributions are recomputed from the persisted
// document frequencies, so they sum to Hit.Score.
func (idx *Index) SearchExplain(ctx context.Context, query string, limit int) ([]ExplainedHit, error) {
	hits, err := idx.Search(ctx, query, limit)
	if err != nil || len(hits) == 0 {
		return nil, err
	}
	terms := uniqueTerms(idx.tokenizer.Tokenize(query))
	if len(terms) == 0 {
		return nil, nil
	}

	out := make([]ExplainedHit, 0, len(hits))
	err = idx.store.view(func(tx *bolt.Tx) error {
		b, docs, termsBucket, err := idx.bucket(tx)
		if err != nil {
			return err
		}
		meta, found, err := idx.store.loadMeta(b)
		if err != nil {
			return err
		}
		if !found || meta.DocCount == 0 {
			return nil
		}
		n := float64(meta.DocCount)
		avgdl := float64(meta.TotalLen) / n
		if avgdl <= 0 {
			avgdl = 1
		}

		for _, hit := range hits {
			doc, exists, err := idx.loadDoc(docs, hit.ID)
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			contributions := make([]TermContribution, 0, len(terms))
			for _, term := range terms {
				tf := doc.TF[term]
				if tf == 0 {
					continue
				}
				posting, found, err := idx.loadTerm(termsBucket, term)
				if err != nil {
					return err
				}
				if !found || len(posting.Docs) == 0 {
					continue
				}
				df := float64(len(posting.Docs))
				idf := math.Log(1 + (n-df+0.5)/(df+0.5))
				denom := float64(tf) + idx.k1*(1-idx.b+idx.b*float64(doc.Length)/avgdl)
				if denom <= 0 {
					continue
				}
				contribution := idf * (float64(tf) * (idx.k1 + 1)) / denom
				contributions = append(contributions, TermContribution{
					Term:         term,
					IDF:          idf,
					TF:           float64(tf),
					Contribution: contribution,
				})
			}
			sort.Slice(contributions, func(i, j int) bool {
				return contributions[i].Term < contributions[j].Term
			})
			out = append(out, ExplainedHit{Hit: hit, Terms: contributions})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
