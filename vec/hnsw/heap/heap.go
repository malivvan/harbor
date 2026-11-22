package heap

// Lessable is an interface that allows a type to be compared to another of the same type.
// It is used to define the order of elements in the heap.
type Lessable[T any] interface {
	Less(T) bool
}

// Heap is a binary min-heap implemented directly on a slice. It deliberately
// does not use container/heap: that would box every element into an interface
// on Push and Pop, which is costly on the search hot path.
type Heap[T Lessable[T]] struct {
	data   []T
	maxIdx int
}

// Init establishes the heap invariants and adopts d as the backing slice.
func (h *Heap[T]) Init(d []T) {
	h.data = d
	for i := len(d)/2 - 1; i >= 0; i-- {
		h.siftDown(i)
	}
	h.rebuildMax()
}

// Len returns the number of elements in the heap.
func (h *Heap[T]) Len() int { return len(h.data) }

// Push pushes x onto the heap in O(log n).
func (h *Heap[T]) Push(x T) {
	h.data = append(h.data, x)
	i := len(h.data) - 1
	if h.maxIdx < 0 {
		// A previous removal of the maximum left the index invalid.
		h.rebuildMax()
	} else if h.data[h.maxIdx].Less(x) {
		h.maxIdx = i
	}
	h.siftUp(i)
}

// Pop removes and returns the minimum element in O(log n).
func (h *Heap[T]) Pop() T {
	last := len(h.data) - 1
	x := h.data[0]
	h.removeAt(0, last)
	return x
}

// Min returns the minimum element in O(1).
func (h *Heap[T]) Min() T { return h.data[0] }

// Max returns the maximum element in O(1) amortised. The index of the maximum
// is tracked across swaps; it is recomputed only after the maximum itself was
// removed.
func (h *Heap[T]) Max() T { return h.data[h.maxIndex()] }

// PopLast removes and returns the maximum element in O(log n), plus an O(n)
// recomputation of the maximum the next time it is needed.
func (h *Heap[T]) PopLast() T {
	i := h.maxIndex()
	x := h.data[i]
	h.Remove(i)
	return x
}

// Remove removes and returns the element at index i in O(log n).
func (h *Heap[T]) Remove(i int) T {
	x := h.data[i]
	last := len(h.data) - 1
	h.removeAt(i, last)
	return x
}

// Slice returns the backing slice in heap order.
func (h *Heap[T]) Slice() []T { return h.data }

func (h *Heap[T]) removeAt(i, last int) {
	if i == h.maxIdx {
		// The maximum is going away; recompute lazily on the next Max/PopLast.
		h.maxIdx = -1
	} else if h.maxIdx == last {
		// The element moved from the end into i.
		h.maxIdx = i
	}

	if i != last {
		h.data[i] = h.data[last]
	}
	h.data = h.data[:last]

	if i < len(h.data) {
		parent := (i - 1) / 2
		if i > 0 && h.data[i].Less(h.data[parent]) {
			h.siftUp(i)
		} else {
			h.siftDown(i)
		}
	}
}

func (h *Heap[T]) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.data[i].Less(h.data[parent]) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *Heap[T]) siftDown(i int) {
	n := len(h.data)
	for {
		left := 2*i + 1
		if left >= n {
			return
		}
		smallest := left
		if right := left + 1; right < n && h.data[right].Less(h.data[left]) {
			smallest = right
		}
		if !h.data[smallest].Less(h.data[i]) {
			return
		}
		h.swap(i, smallest)
		i = smallest
	}
}

func (h *Heap[T]) swap(i, j int) {
	switch h.maxIdx {
	case i:
		h.maxIdx = j
	case j:
		h.maxIdx = i
	}
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *Heap[T]) maxIndex() int {
	if len(h.data) == 0 {
		panic("heap: Max called on an empty heap")
	}
	if h.maxIdx < 0 || h.maxIdx >= len(h.data) {
		h.rebuildMax()
	}
	return h.maxIdx
}

func (h *Heap[T]) rebuildMax() {
	h.maxIdx = -1
	for i := range h.data {
		if h.maxIdx < 0 || h.data[h.maxIdx].Less(h.data[i]) {
			h.maxIdx = i
		}
	}
}
