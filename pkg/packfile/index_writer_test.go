package packfile

import (
	"os"
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
