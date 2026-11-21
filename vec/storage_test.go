package vec

import (
	"os"
	"reflect"
	"testing"
	"unicode/utf8"

	"github.com/malivvan/binco"
)

func TestStorage(t *testing.T) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	// Test NewStorage
	storage, err := NewStorage(tempPath)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Test EnsureCollection
	err = storage.EnsureCollection("test-collection")
	if err != nil {
		t.Fatalf("Failed to ensure collection: %v", err)
	}

	// Test ListCollections
	collections, err := storage.ListCollections()
	if err != nil {
		t.Fatalf("Failed to list collections: %v", err)
	}
	if len(collections) != 1 || collections[0] != "test-collection" {
		t.Errorf("Expected [test-collection], got %v", collections)
	}

	// Test UpsertPoints with various payload types
	points := []PointStruct{
		{
			ID:     "1",
			Vector: []float32{1.0, 2.0, 3.0},
			Payload: map[string]interface{}{
				"string": "test1",
				"int":    int(42),
				"int64":  int64(100),
				"float":  float32(3.14),
				"double": float64(2.718),
				"bool":   true,
				"bytes":  []byte{0x01, 0x02, 0x03},
			},
		},
		{ID: "2", Vector: []float32{4.0, 5.0, 6.0}, Payload: map[string]interface{}{"name": "test2"}},
	}
	err = storage.UpsertPoints("test-collection", points)
	if err != nil {
		t.Fatalf("Failed to upsert points: %v", err)
	}

	// Test LoadCollection
	loadedPoints, err := storage.LoadCollection("test-collection")
	if err != nil {
		t.Fatalf("Failed to load collection: %v", err)
	}

	if len(loadedPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(loadedPoints))
	}

	p1 := loadedPoints["1"]
	if p1 == nil {
		t.Fatal("Expected point 1 to exist")
	}

	// Verify payload types and values
	expectedPayload := map[string]interface{}{
		"string": "test1",
		"int":    int64(42),     // integer payloads decode as int64
		"int64":  int64(100),    // integer payloads decode as int64
		"float":  float64(3.14), // float payloads decode as float64
		"double": float64(2.718),
		"bool":   true,
		"bytes":  []byte{0x01, 0x02, 0x03},
	}

	for k, v := range expectedPayload {
		if val, ok := p1.Payload[k]; !ok {
			t.Errorf("Expected payload key %s to exist", k)
		} else {
			if k == "float" {
				// Special handling for float32 precision when stored as float64
				if float32(val.(float64)) != 3.14 {
					t.Errorf("Expected payload key %s to be 3.14, got %v", k, val)
				}
			} else if !reflect.DeepEqual(val, v) {
				t.Errorf("Expected payload key %s to be %v (%T), got %v (%T)", k, v, v, val, val)
			}
		}
	}

	if loadedPoints["2"] == nil {
		t.Error("Expected point 2 to exist")
	}

	// Test DeletePoints
	err = storage.DeletePoints("test-collection", []string{"1"})
	if err != nil {
		t.Fatalf("Failed to delete points: %v", err)
	}

	// Test LoadCollection after deletion
	loadedPoints, err = storage.LoadCollection("test-collection")
	if err != nil {
		t.Fatalf("Failed to load collection after deletion: %v", err)
	}

	if len(loadedPoints) != 1 {
		t.Errorf("Expected 1 point after deletion, got %d", len(loadedPoints))
	}

	if loadedPoints["1"] != nil {
		t.Error("Expected point 1 to be deleted")
	}

	if loadedPoints["2"] == nil {
		t.Error("Expected point 2 to still exist")
	}

	// Test ListCollections with multiple
	err = storage.EnsureCollection("another-one")
	if err != nil {
		t.Fatalf("Failed to ensure collection: %v", err)
	}
	collections, err = storage.ListCollections()
	if err != nil {
		t.Fatalf("Failed to list collections: %v", err)
	}
	if len(collections) != 2 {
		t.Errorf("Expected 2 collections, got %d", len(collections))
	}

	// Test Close and closed errors
	storage.Close()
	if _, err := storage.ListCollections(); err == nil {
		t.Error("Expected error calling ListCollections on closed storage")
	}
	if err := storage.EnsureCollection("test"); err == nil {
		t.Error("Expected error calling EnsureCollection on closed storage")
	}
	if err := storage.UpsertPoints("test", nil); err == nil {
		t.Error("Expected error calling UpsertPoints on closed storage")
	}
	if _, err := storage.LoadCollection("test"); err == nil {
		t.Error("Expected error calling LoadCollection on closed storage")
	}
	if err := storage.DeletePoints("test", nil); err == nil {
		t.Error("Expected error calling DeletePoints on closed storage")
	}
}

func TestStorageWithQuantization(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "storage-quant-test")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage, _ := NewStorageWithQuantization(tempPath, true, nil, false)
	defer storage.Close()

	colName := "quantized-col"
	storage.EnsureCollection(colName)

	points := []PointStruct{
		{ID: "1", Vector: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}},
	}

	err := storage.UpsertPoints(colName, points)
	if err != nil {
		t.Fatalf("Failed to upsert points: %v", err)
	}

	loadedPoints, err := storage.LoadCollection(colName)
	if err != nil {
		t.Fatalf("Failed to load collection: %v", err)
	}

	p1, ok := loadedPoints["1"]
	if !ok {
		t.Fatal("Point 1 not found")
	}

	if len(p1.Vector) != 8 {
		t.Errorf("Expected vector length 8, got %d", len(p1.Vector))
	}

	// Verify that the vector is restored (it won't be exactly the same due to SQ8 quantization)
	for i, v := range p1.Vector {
		if v < points[0].Vector[i]-0.5 || v > points[0].Vector[i]+0.5 {
			t.Errorf("Vector element %d significantly different: got %f, expected around %f", i, v, points[0].Vector[i])
		}
	}
}

func TestCollectionMetaStorage(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "storage-meta-test")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage, _ := NewStorage(tempPath)
	defer storage.Close()

	meta := CollectionMeta{
		Name:      "test-meta",
		VectorLen: 128,
		Metric:    Cosine,
		UseHNSW:   true,
		HNSWParams: HNSWParams{
			M:              16,
			EfConstruction: 200,
		},
	}

	// Test SaveCollectionMeta
	err := storage.SaveCollectionMeta(meta.Name, meta)
	if err != nil {
		t.Fatalf("Failed to save collection meta: %v", err)
	}

	// Test LoadCollectionMeta
	loadedMeta, err := storage.LoadCollectionMeta(meta.Name)
	if err != nil {
		t.Fatalf("Failed to load collection meta: %v", err)
	}

	if !reflect.DeepEqual(*loadedMeta, meta) {
		t.Errorf("Expected meta %v, got %v", meta, *loadedMeta)
	}

	// Test ListCollectionMetas
	metas, err := storage.ListCollectionMetas()
	if err != nil {
		t.Fatalf("Failed to list collection metas: %v", err)
	}

	if len(metas) != 1 {
		t.Errorf("Expected 1 meta, got %d", len(metas))
	}

	if !reflect.DeepEqual(metas[0], meta) {
		t.Errorf("Expected meta %v, got %v", meta, metas[0])
	}

	// Test LoadCollectionMeta for non-existent collection
	_, err = storage.LoadCollectionMeta("non-existent")
	if err == nil {
		t.Error("Expected error loading non-existent meta")
	}

	// Test ListCollectionMetas when empty (it was not empty, but we can test it with a fresh storage)
}

func TestStorageErrorCases(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "storage-error-test")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	storage, _ := NewStorage(tempPath)

	// Test LoadCollectionMeta when bucket doesn't exist
	_, err := storage.LoadCollectionMeta("any")
	if err == nil {
		t.Error("Expected error when metadata bucket doesn't exist")
	}

	// Test ListCollectionMetas when bucket doesn't exist
	metas, err := storage.ListCollectionMetas()
	if err != nil {
		t.Fatalf("ListCollectionMetas failed: %v", err)
	}
	if len(metas) != 0 {
		t.Errorf("Expected 0 metas, got %d", len(metas))
	}

	storage.Close()
}

// TestToProtoPointInvalidUTF8 verifies that invalid UTF-8 strings in payload
// are sanitized (replaced with U+FFFD) before binco encoding.
// This covers the P1 fix: UTF-8 validation in toStoredPoint.
func TestToProtoPointInvalidUTF8(t *testing.T) {
	t.Run("InvalidUTF8String_Sanitized", func(t *testing.T) {
		// Create a string with embedded invalid UTF-8 bytes (e.g., from binary file content)
		invalidStr := "hello" + string([]byte{0x80, 0xfe, 0xff}) + "world"

		p := &PointStruct{
			ID:     "utf8-test",
			Vector: []float32{1.0, 2.0},
			Payload: map[string]interface{}{
				"content": invalidStr,
				"title":   "正常标题",
				"binary":  string([]byte{0xc0, 0xaf}), // overlong encoding
			},
		}

		converted := toStoredPoint(p)
		if converted == nil {
			t.Fatal("toStoredPoint returned nil")
		}

		// Verify all string values are valid UTF-8
		for k, v := range converted.Payload {
			if sv := v.GetStringValue(); sv != "" {
				if !utf8.ValidString(sv) {
					t.Errorf("payload key %q still contains invalid UTF-8 after sanitization", k)
				}
			}
		}
	})

	t.Run("ValidUTF8_PassThrough", func(t *testing.T) {
		p := &PointStruct{
			ID:     "valid-test",
			Vector: []float32{1.0},
			Payload: map[string]interface{}{
				"text": "hello 世界 🌍",
				"num":  int(42),
			},
		}

		converted := toStoredPoint(p)
		if converted == nil {
			t.Fatal("toStoredPoint returned nil")
		}

		if got := converted.Payload["text"].GetStringValue(); got != "hello 世界 🌍" {
			t.Errorf("valid UTF-8 should pass through unchanged, got %q", got)
		}
	})

	t.Run("RoundTrip_InvalidUTF8_EncodeSucceeds", func(t *testing.T) {
		// The real bug: encoding would fail on invalid UTF-8
		invalidStr := "data\x80corrupted"
		p := &PointStruct{
			ID:     "marshal-test",
			Vector: []float32{1.0},
			Payload: map[string]interface{}{
				"raw": invalidStr,
			},
		}

		converted := toStoredPoint(p)
		data := binco.Encode(StoredPointBinco, *converted)
		if len(data) == 0 {
			t.Fatal("encoded data is empty")
		}

		// Round-trip: decode and verify content is valid UTF-8
		decoded, err := binco.DecodeExact(StoredPointBinco, data)
		if err != nil {
			t.Fatalf("binco decode failed: %v", err)
		}
		content := decoded.Payload["raw"].GetStringValue()
		if !utf8.ValidString(content) {
			t.Errorf("round-trip payload still has invalid UTF-8: %q (hex: %x)", content, []byte(content))
		}
	})
}
