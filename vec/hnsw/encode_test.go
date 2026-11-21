package hnsw

import (
	"bytes"
	"math/rand"
	"testing"
)

// encodeHeader writes the graph parameters shared by every crafted stream.
func encodeHeader(t *testing.T, w *bytes.Buffer, nLayers int) {
	t.Helper()

	if _, err := multiBinaryWrite(w, encodingVersion, 4, 0.25, 20, "euclidean"); err != nil {
		t.Fatalf("encoding header: %v", err)
	}
	if _, err := binaryWrite(w, nLayers); err != nil {
		t.Fatalf("encoding layer count: %v", err)
	}
}

// TestImportRejectsMissingNeighbor is a regression test for Import wiring
// neighbor pointers to nil when the encoded graph references a key that is not
// present in the layer. The nil pointer used to panic on the first search.
func TestImportRejectsMissingNeighbor(t *testing.T) {
	var buf bytes.Buffer
	encodeHeader(t, &buf, 1)

	// One layer with two nodes: node 1 points at a neighbor (2) that is not in
	// the layer.
	if _, err := binaryWrite(&buf, 2); err != nil {
		t.Fatalf("encoding node count: %v", err)
	}
	if _, err := multiBinaryWrite(&buf, 1, Vector{0, 0}, 1); err != nil {
		t.Fatalf("encoding node 1: %v", err)
	}
	if _, err := binaryWrite(&buf, 2); err != nil { // dangling neighbor reference
		t.Fatalf("encoding neighbors: %v", err)
	}
	if _, err := multiBinaryWrite(&buf, 3, Vector{1, 1}, 0); err != nil {
		t.Fatalf("encoding node 3: %v", err)
	}

	g := NewGraph[int]()
	err := g.Import(&buf)
	if err == nil {
		t.Fatal("expected Import to reject a dangling neighbor reference")
	}
}

// TestImportRejectsNegativeLengths is a regression test for the varint lengths
// read by binaryRead being used to size slices without validation: a negative
// length used to panic instead of returning an error.
func TestImportRejectsNegativeLengths(t *testing.T) {
	t.Run("negative vector length", func(t *testing.T) {
		var buf bytes.Buffer
		encodeHeader(t, &buf, 1)
		if _, err := binaryWrite(&buf, 1); err != nil { // one node
			t.Fatalf("encoding node count: %v", err)
		}
		if _, err := binaryWrite(&buf, 1); err != nil { // node key
			t.Fatalf("encoding key: %v", err)
		}
		if _, err := binaryWrite(&buf, -5); err != nil { // vector length
			t.Fatalf("encoding vector length: %v", err)
		}

		g := NewGraph[int]()
		if err := g.Import(&buf); err == nil {
			t.Fatal("expected Import to reject a negative vector length")
		}
	})

	t.Run("negative string length", func(t *testing.T) {
		var buf bytes.Buffer
		var s string
		// Reading a string with a negative length must fail, not panic.
		if _, err := binaryWrite(&buf, -1); err != nil {
			t.Fatalf("encoding length: %v", err)
		}
		if _, err := binaryRead(&buf, &s); err == nil {
			t.Fatal("expected binaryRead to reject a negative length")
		}
	})

	t.Run("negative node count", func(t *testing.T) {
		var buf bytes.Buffer
		encodeHeader(t, &buf, 1)
		if _, err := binaryWrite(&buf, -3); err != nil {
			t.Fatalf("encoding node count: %v", err)
		}

		g := NewGraph[int]()
		if err := g.Import(&buf); err == nil {
			t.Fatal("expected Import to reject a negative node count")
		}
	})

	t.Run("oversized vector length", func(t *testing.T) {
		var buf bytes.Buffer
		encodeHeader(t, &buf, 1)
		if _, err := binaryWrite(&buf, 1); err != nil {
			t.Fatalf("encoding node count: %v", err)
		}
		if _, err := binaryWrite(&buf, 1); err != nil {
			t.Fatalf("encoding key: %v", err)
		}
		if _, err := binaryWrite(&buf, maxEncodedLength+1); err != nil {
			t.Fatalf("encoding vector length: %v", err)
		}

		g := NewGraph[int]()
		if err := g.Import(&buf); err == nil {
			t.Fatal("expected Import to reject an oversized vector length")
		}
	})
}

// TestExportImportRoundTrip makes sure the validation above did not break the
// happy path.
func TestExportImportRoundTrip(t *testing.T) {
	g := NewGraph[int]()
	g.M = 4
	g.EfSearch = 20
	g.Distance = EuclideanDistance
	g.Rng = rand.New(rand.NewSource(3))
	for i := 0; i < 20; i++ {
		g.Add(MakeNode(i, Vector{float32(i), float32(i % 3)}))
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported := NewGraph[int]()
	if err := imported.Import(&buf); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if imported.Len() != g.Len() {
		t.Fatalf("imported graph has %d nodes, want %d", imported.Len(), g.Len())
	}

	got := imported.Search(Vector{3, 0}, 3)
	if len(got) != 3 {
		t.Fatalf("search on the imported graph returned %d nodes, want 3", len(got))
	}
	for _, node := range got {
		if _, ok := imported.Lookup(node.Key); !ok {
			t.Fatalf("imported graph returned unknown node %d", node.Key)
		}
	}
}
