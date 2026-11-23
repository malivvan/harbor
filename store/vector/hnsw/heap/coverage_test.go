package heap

import (
	"sort"
	"testing"
)

func TestPopDrainsInOrderAndSlice(t *testing.T) {
	h := newHeap(t, 5, 1, 9, 3, 7, 2, 8)

	backing := h.Slice()
	if len(backing) != 7 {
		t.Fatalf("Slice() length = %d, want 7", len(backing))
	}

	want := []int{1, 2, 3, 5, 7, 8, 9}
	for i, w := range want {
		got := h.Pop()
		if got.v != w {
			t.Fatalf("Pop() #%d = %d, want %d", i, got.v, w)
		}
	}
	if h.Len() != 0 {
		t.Fatalf("Len() = %d after draining", h.Len())
	}
}

func TestInitFromSlice(t *testing.T) {
	h := &Heap[testItem]{}
	h.Init([]testItem{{0, 4}, {1, 8}, {2, 1}, {3, 6}, {4, 2}})

	if got := h.Min().v; got != 1 {
		t.Fatalf("Min() = %d", got)
	}
	if got := h.Max().v; got != 8 {
		t.Fatalf("Max() = %d", got)
	}

	got := make([]int, 0, h.Len())
	for h.Len() > 0 {
		got = append(got, h.Pop().v)
	}
	if !sort.IntsAreSorted(got) {
		t.Fatalf("Pop order = %v", got)
	}
}

func TestRemoveMiddleAndSwaps(t *testing.T) {
	h := newHeap(t, 5, 1, 9, 3, 7, 2, 8)
	mid := h.Slice()[1].v
	removed := h.Remove(1)
	if removed.v != mid {
		t.Fatalf("Remove(1) = %d, want %d", removed.v, mid)
	}
	if h.Len() != 6 {
		t.Fatalf("Len() = %d, want 6", h.Len())
	}

	prev := -1
	for h.Len() > 0 {
		v := h.Pop().v
		if v < prev {
			t.Fatalf("heap order broken: %d after %d", v, prev)
		}
		prev = v
	}
}

func TestPushAfterDrainKeepsTrackedMax(t *testing.T) {
	h := newHeap(t, 1, 2, 3)
	for h.Len() > 0 {
		h.Pop()
	}
	h.Push(testItem{0, 42})
	if got := h.Max().v; got != 42 {
		t.Fatalf("Max() after drain+push = %d, want 42", got)
	}
	if got := h.Min().v; got != 42 {
		t.Fatalf("Min() after drain+push = %d, want 42", got)
	}
}
