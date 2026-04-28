package packfile

import (
	"path/filepath"
	"testing"
)

func TestIndexCache_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.sst")

	// Create a test index
	w, err := NewIndexWriter(indexPath, CompressionNone)
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	if err := w.AddEntry(IndexEntry{
		Hash:             ContentHash([32]byte{0x01}),
		Offset:           100,
		SizeUncompressed: 1000,
		SizeCompressed:   500,
	}); err != nil {
		t.Fatalf("AddEntry error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Open the reader
	reader, err := NewIndexReader(indexPath)
	if err != nil {
		t.Fatalf("NewIndexReader error = %v", err)
	}

	// Create cache with capacity 2
	cache, err := NewIndexCache(2)
	if err != nil {
		t.Fatalf("NewIndexCache error = %v", err)
	}

	// Get on empty cache should return nil
	if r := cache.Get("key1"); r != nil {
		t.Error("expected nil for empty cache")
	}

	// Put and get
	cache.Put("key1", reader)
	if r := cache.Get("key1"); r == nil {
		t.Error("expected reader after Put")
	}
}

func TestIndexCache_Eviction(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple test indices
	indexPaths := make([]string, 3)
	readers := make([]*IndexReader, 3)
	for i := 0; i < 3; i++ {
		indexPaths[i] = filepath.Join(tmpDir, "test"+string(rune('a'+i))+".sst")
		w, err := NewIndexWriter(indexPaths[i], CompressionNone)
		if err != nil {
			t.Fatalf("NewIndexWriter error = %v", err)
		}
		if err := w.AddEntry(IndexEntry{
			Hash:             ContentHash([32]byte{byte(i + 1)}),
			Offset:           int64(i * 100),
			SizeUncompressed: 1000,
			SizeCompressed:   500,
		}); err != nil {
			t.Fatalf("AddEntry error = %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
		readers[i], _ = NewIndexReader(indexPaths[i])
	}

	// Create cache with capacity 2
	cache, err := NewIndexCache(2)
	if err != nil {
		t.Fatalf("NewIndexCache error = %v", err)
	}

	// Add 2 entries
	cache.Put("key1", readers[0])
	cache.Put("key2", readers[1])

	// Add 3rd entry - should evict key1
	cache.Put("key3", readers[2])

	// key1 should be evicted
	if r := cache.Get("key1"); r != nil {
		t.Error("key1 should be evicted")
	}
}

func TestIndexCache_Close(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.sst")

	// Create a test index
	w, err := NewIndexWriter(indexPath, CompressionNone)
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	if err := w.AddEntry(IndexEntry{
		Hash:             ContentHash([32]byte{0x01}),
		Offset:           100,
		SizeUncompressed: 1000,
		SizeCompressed:   500,
	}); err != nil {
		t.Fatalf("AddEntry error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	reader, _ := NewIndexReader(indexPath)
	cache, _ := NewIndexCache(10)
	cache.Put("key1", reader)

	// Close should not panic
	if err := cache.Close(); err != nil {
		t.Errorf("Close error = %v", err)
	}
}
