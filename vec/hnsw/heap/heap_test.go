package heap

import "testing"

// testItem is a minimal Lessable used to exercise the heap ordering.
type testItem struct {
	id int
	v  int
}

func (i testItem) Less(o testItem) bool {
	return i.v < o.v
}

func newHeap(t *testing.T, values ...int) *Heap[testItem] {
	t.Helper()

	h := &Heap[testItem]{}
	h.Init(nil)
	for id, v := range values {
		h.Push(testItem{id: id, v: v})
	}
	return h
}

// TestMaxReturnsTrueMaximum is a regression test for Max returning the last
// element of the backing array instead of the largest element.
func TestMaxReturnsTrueMaximum(t *testing.T) {
	h := newHeap(t, 5, 1, 9, 3, 7, 2, 8)

	if got := h.Max().v; got != 9 {
		t.Fatalf("Max() = %d, want 9", got)
	}
	if got := h.Min().v; got != 1 {
		t.Fatalf("Min() = %d, want 1", got)
	}
	if h.Len() != 7 {
		t.Fatalf("Max must not remove elements: Len() = %d, want 7", h.Len())
	}
}

// TestPopLastRemovesTrueMaximum is a regression test for PopLast removing an
// arbitrary leaf instead of the largest element.
func TestPopLastRemovesTrueMaximum(t *testing.T) {
	h := newHeap(t, 5, 1, 9, 3, 7, 2, 8)

	want := []int{9, 8, 7, 5, 3, 2, 1}
	for i, w := range want {
		if got := h.PopLast().v; got != w {
			t.Fatalf("PopLast() #%d = %d, want %d", i, got, w)
		}
		// The heap invariant must survive: the minimum of what is left stays
		// reachable through Min().
		if h.Len() > 0 {
			remainingMin := want[len(want)-1]
			if got := h.Min().v; got != remainingMin {
				t.Fatalf("Min() after PopLast = %d, want %d", got, remainingMin)
			}
		}
	}

	if h.Len() != 0 {
		t.Fatalf("Len() = %d after draining, want 0", h.Len())
	}
}

// TestPopLastInterleavedWithPush keeps pushing and popping to make sure the
// heap stays consistent when the maximum is removed from the middle.
func TestPopLastInterleavedWithPush(t *testing.T) {
	h := newHeap(t, 4, 8, 2, 6)

	if got := h.PopLast().v; got != 8 {
		t.Fatalf("PopLast() = %d, want 8", got)
	}
	h.Push(testItem{10, 10})
	if got := h.Max().v; got != 10 {
		t.Fatalf("Max() = %d, want 10", got)
	}
	if got := h.PopLast().v; got != 10 {
		t.Fatalf("PopLast() = %d, want 10", got)
	}
	if got := h.PopLast().v; got != 6 {
		t.Fatalf("PopLast() = %d, want 6", got)
	}
	if got := h.Min().v; got != 2 {
		t.Fatalf("Min() = %d, want 2", got)
	}
}

// TestPopLastOnEmptyPanics documents that draining an empty heap still panics.
func TestPopLastOnEmptyPanics(t *testing.T) {
	h := &Heap[testItem]{}
	h.Init(nil)

	defer func() {
		if recover() == nil {
			t.Fatal("expected PopLast on an empty heap to panic")
		}
	}()
	h.PopLast()
}
