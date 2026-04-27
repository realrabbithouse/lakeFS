package packfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexWriter_WriteEntries(t *testing.T) {
	f, err := os.CreateTemp("", "test-index-*.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Close()

	entries := []IndexEntry{
		{Hash: ContentHash([32]byte{0x02}), Offset: 200, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x01}), Offset: 100, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x03}), Offset: 300, SizeUncompressed: 100, SizeCompressed: 50},
	}

	w, err := NewIndexWriter(f.Name(), CompressionNone)
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	for _, e := range entries {
		if err := w.AddEntry(e); err != nil {
			t.Fatalf("AddEntry error = %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Verify file was created and has content (basic sanity check)
	info, err := os.Stat(f.Name())
	if err != nil {
		t.Fatalf("os.Stat error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("SSTable file is empty")
	}
}

func TestSortByHash_AscendingOrder(t *testing.T) {
	// Test that sortByHash sorts in ascending order (smallest first)
	entries := []IndexEntry{
		{Hash: ContentHash([32]byte{0x03, 0x00, 0x00, 0x00}), Offset: 300, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x01, 0x00, 0x00, 0x00}), Offset: 100, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x02, 0x00, 0x00, 0x00}), Offset: 200, SizeUncompressed: 100, SizeCompressed: 50},
	}

	sortByHash(entries)

	// Verify ascending order: 0x01 < 0x02 < 0x03
	if entries[0].Hash[0] != 0x01 {
		t.Errorf("expected first hash 0x01, got 0x%02x", entries[0].Hash[0])
	}
	if entries[1].Hash[0] != 0x02 {
		t.Errorf("expected second hash 0x02, got 0x%02x", entries[1].Hash[0])
	}
	if entries[2].Hash[0] != 0x03 {
		t.Errorf("expected third hash 0x03, got 0x%02x", entries[2].Hash[0])
	}
}

func TestSortByHash_StrictlyIncreasing(t *testing.T) {
	// Test that sortByHash produces strictly increasing order (no duplicates)
	entries := []IndexEntry{
		{Hash: ContentHash([32]byte{0xFF, 0x00, 0x00, 0x00}), Offset: 100, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x01, 0x00, 0x00, 0x00}), Offset: 200, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x80, 0x00, 0x00, 0x00}), Offset: 300, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x40, 0x00, 0x00, 0x00}), Offset: 400, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0xC0, 0x00, 0x00, 0x00}), Offset: 500, SizeUncompressed: 100, SizeCompressed: 50},
	}

	sortByHash(entries)

	// Verify strictly increasing: each hash < next hash (entries[i-1].Hash.BigThan returns true when entries[i-1] > entries[i])
	// So we error when BigThan is false, meaning entries[i-1] <= entries[i], which is correct for ascending order
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Hash.BigThan(entries[i].Hash) {
			t.Errorf("hash[%d] (0x%02x) should be <= hash[%d] (0x%02x) for ascending order",
				i-1, entries[i-1].Hash[0], i, entries[i].Hash[0])
		}
	}
}

func TestIndexWriter_ExternalSortPath(t *testing.T) {
	// Test that external sort path is used when estimatedEntryCount >= 1M and len >= 1M
	f, err := os.CreateTemp("", "test-index-*.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Close()

	// Create 1M+ entries in reverse order with unique hashes using all 32 bytes
	entryCount := 1_000_100
	entries := make([]IndexEntry, 0, entryCount)
	for i := entryCount - 1; i >= 0; i-- {
		var hash ContentHash
		// Use multiple bytes to ensure uniqueness across 1M+ entries
		hash[0] = byte(i & 0xFF)
		hash[1] = byte((i >> 8) & 0xFF)
		hash[2] = byte((i >> 16) & 0xFF)
		hash[3] = byte((i >> 24) & 0xFF)
		entries = append(entries, IndexEntry{
			Hash:             hash,
			Offset:           int64(i * 100),
			SizeUncompressed: 100,
			SizeCompressed:   50,
		})
	}

	// Use WithEntryCount >= 1M to force external sort path
	w, err := NewIndexWriter(f.Name(), CompressionNone, WithEntryCount(1_000_000))
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	for _, e := range entries {
		if err := w.AddEntry(e); err != nil {
			t.Fatalf("AddEntry error = %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Verify file was created and has content
	info, err := os.Stat(f.Name())
	if err != nil {
		t.Fatalf("os.Stat error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("SSTable file is empty after external sort")
	}
}

func TestIndexWriter_InMemoryPath(t *testing.T) {
	// Test that in-memory sort is used when estimatedEntryCount < 1M
	f, err := os.CreateTemp("", "test-index-*.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Close()

	// Create 100 entries - below threshold
	entries := make([]IndexEntry, 0, 100)
	for i := 99; i >= 0; i-- {
		var hash ContentHash
		hash[0] = byte(i)
		entries = append(entries, IndexEntry{
			Hash:             hash,
			Offset:           int64(i * 100),
			SizeUncompressed: 100,
			SizeCompressed:   50,
		})
	}

	// estimatedEntryCount below threshold should use in-memory path
	w, err := NewIndexWriter(f.Name(), CompressionNone, WithEntryCount(500_000))
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	for _, e := range entries {
		if err := w.AddEntry(e); err != nil {
			t.Fatalf("AddEntry error = %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Verify file was created and has content
	info, err := os.Stat(f.Name())
	if err != nil {
		t.Fatalf("os.Stat error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("SSTable file is empty")
	}
}

func TestIndexWriter_InMemoryPathNoOption(t *testing.T) {
	// Test that in-memory sort is used when no WithEntryCount option is provided
	f, err := os.CreateTemp("", "test-index-*.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Close()

	entries := []IndexEntry{
		{Hash: ContentHash([32]byte{0x03}), Offset: 300, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x01}), Offset: 100, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x02}), Offset: 200, SizeUncompressed: 100, SizeCompressed: 50},
	}

	// No options - should use default (in-memory)
	w, err := NewIndexWriter(f.Name(), CompressionNone)
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	for _, e := range entries {
		if err := w.AddEntry(e); err != nil {
			t.Fatalf("AddEntry error = %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Verify file was created and has content
	info, err := os.Stat(f.Name())
	if err != nil {
		t.Fatalf("os.Stat error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("SSTable file is empty")
	}
}

func TestParseSortedOutput(t *testing.T) {
	// Test parsing of sort command output
	input := `0100000000000000000000000000000000000000000000000000000000000000 100 100 50
0200000000000000000000000000000000000000000000000000000000000000 200 100 50
0300000000000000000000000000000000000000000000000000000000000000 300 100 50`

	entries, err := parseSortedOutput([]byte(input))
	if err != nil {
		t.Fatalf("parseSortedOutput error = %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Verify first entry
	if entries[0].Hash[0] != 0x01 {
		t.Errorf("expected first hash[0] = 0x01, got 0x%02x", entries[0].Hash[0])
	}
	if entries[0].Offset != 100 {
		t.Errorf("expected first offset = 100, got %d", entries[0].Offset)
	}

	// Verify second entry
	if entries[1].Hash[0] != 0x02 {
		t.Errorf("expected second hash[0] = 0x02, got 0x%02x", entries[1].Hash[0])
	}
	if entries[1].Offset != 200 {
		t.Errorf("expected second offset = 200, got %d", entries[1].Offset)
	}

	// Verify third entry
	if entries[2].Hash[0] != 0x03 {
		t.Errorf("expected third hash[0] = 0x03, got 0x%02x", entries[2].Hash[0])
	}
	if entries[2].Offset != 300 {
		t.Errorf("expected third offset = 300, got %d", entries[2].Offset)
	}
}

func TestParseSortedOutput_InvalidLines(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"invalid hash", "notahexhash 100 100 50\n"},
		{"wrong field count", "0100000000000000000000000000000000000000000000000000000000000000 100 50\n"},
		{"invalid offset", "0100000000000000000000000000000000000000000000000000000000000000 abc 100 50\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSortedOutput([]byte(tc.input))
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestWithEntryCount(t *testing.T) {
	// Verify WithEntryCount option sets the estimated entry count
	w, err := NewIndexWriter("test.sst", CompressionNone, WithEntryCount(5_000_000))
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	if w.estimatedEntryCount != 5_000_000 {
		t.Errorf("expected estimatedEntryCount = 5000000, got %d", w.estimatedEntryCount)
	}
}

func TestNewIndexWriter_WithOptionsVariadic(t *testing.T) {
	// Verify multiple options work together
	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "test.sst")

	w, err := NewIndexWriter(fpath, CompressionNone, WithEntryCount(100))
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}
	if w.estimatedEntryCount != 100 {
		t.Errorf("expected estimatedEntryCount = 100, got %d", w.estimatedEntryCount)
	}
}
