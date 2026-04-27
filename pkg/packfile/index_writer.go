package packfile

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/sstable"
)

// IndexEntry represents an entry in the packfile index.
// It maps a content hash to the location of that object within the packfile.
type IndexEntry struct {
	Hash             ContentHash
	Offset           int64  // Byte offset of object in packfile
	SizeUncompressed int64  // Uncompressed size of object
	SizeCompressed   int64  // Compressed size in packfile
}

// IndexWriter writes a sorted index of packfile entries to an SSTable.
type IndexWriter struct {
	filename   string
	entries    []IndexEntry
	compression Compression
}

// NewIndexWriter creates a new IndexWriter that writes to the given filename.
func NewIndexWriter(filename string, compression Compression) (*IndexWriter, error) {
	return &IndexWriter{
		filename:    filename,
		entries:     make([]IndexEntry, 0),
		compression: compression,
	}, nil
}

// AddEntry adds an entry to the index. Entries are buffered and sorted on Close.
func (w *IndexWriter) AddEntry(entry IndexEntry) error {
	w.entries = append(w.entries, entry)
	return nil
}

// Close writes the sorted index to an SSTable file.
func (w *IndexWriter) Close() error {
	if len(w.entries) == 0 {
		return nil
	}

	// Sort entries by hash
	sortByHash(w.entries)

	// Determine compression
	var sstableCompression sstable.Compression
	switch w.compression {
	case CompressionNone:
		sstableCompression = sstable.NoCompression
	case CompressionZstd:
		sstableCompression = sstable.ZstdCompression
	default:
		sstableCompression = sstable.DefaultCompression
	}

	// Open the output file directly
	f, err := os.Create(w.filename)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}

	writer := sstable.NewWriter(f, sstable.WriterOptions{
		Compression: sstableCompression,
	})

	for _, entry := range w.entries {
		// Key is the content hash (32 bytes)
		key := entry.Hash[:]
		// Value is 24 bytes: offset(8) + sizeUncompressed(8) + sizeCompressed(8)
		var value [24]byte
		binary.LittleEndian.PutUint64(value[0:8], uint64(entry.Offset))
		binary.LittleEndian.PutUint64(value[8:16], uint64(entry.SizeUncompressed))
		binary.LittleEndian.PutUint64(value[16:24], uint64(entry.SizeCompressed))

		if err := writer.Set(key, value[:]); err != nil {
			f.Close()
			os.Remove(w.filename)
			return fmt.Errorf("setting key-value: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		os.Remove(w.filename)
		return fmt.Errorf("closing sstable writer: %w", err)
	}

	// writer.Close() closes the underlying file
	return nil
}

// sortByHash sorts entries by their content hash.
func sortByHash(entries []IndexEntry) {
	n := len(entries)
	for i := 1; i < n; i++ {
		entry := entries[i]
		j := i - 1
		for j >= 0 && entries[j].Hash.BigThan(entry.Hash) {
			entries[j+1] = entries[j]
			j--
		}
		entries[j+1] = entry
	}
}
