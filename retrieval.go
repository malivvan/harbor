package tempest

import "time"

// RetrievalEvent describes one successful hybrid retrieval. It is a plain
// value, so a caller can forward it to a log, a metrics system, or an audit
// store without tempest depending on any of them.
type RetrievalEvent struct {
	// At is when the call finished.
	At time.Time
	// Query is the lexical query, empty for a vector-only call.
	Query string
	// Hits are the returned IDs in rank order.
	Hits []string
	// Latency is the wall-clock duration of the call.
	Latency time.Duration
	// Why names the stages that produced the page, for example "text",
	// "dense", "rrf" or "rerank".
	Why []string
}

// SetRetrievalSink installs a callback that receives every successful
// HybridSearch. A nil sink disables it and costs only a nil check. The sink
// runs synchronously after the search, so it must not call back into the same
// store in a way that needs the writer lock.
func (s *Store) SetRetrievalSink(sink func(RetrievalEvent)) {
	if s == nil {
		return
	}
	s.retrievalMu.Lock()
	s.retrievalSink = sink
	s.retrievalMu.Unlock()
}

func (s *Store) emitRetrieval(event RetrievalEvent) {
	if s == nil {
		return
	}
	s.retrievalMu.RLock()
	sink := s.retrievalSink
	s.retrievalMu.RUnlock()
	if sink != nil {
		sink(event)
	}
}
