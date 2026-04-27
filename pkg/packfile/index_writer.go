package packfile

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/sstable"
)

// IndexEntry represents an entry in the packfile index.
// It maps a content hash to the location of that object within the packfile.
type IndexEntry struct {
	Hash             ContentHash
	Offset           int64 // Byte offset of object in packfile
	SizeUncompressed int64 // Uncompressed size of object
	SizeCompressed   int64 // Compressed size in packfile
}

// IndexWriter writes a sorted index of packfile entries to an SSTable.
type IndexWriter struct {
	filename            string
	entries             []IndexEntry
	compression         Compression
	estimatedEntryCount int
}

// Option configures IndexWriter behavior.
type Option func(*IndexWriter)

// WithEntryCount sets the estimated entry count for memory management.
// If estimated entries >= 1,000,000, external sort is used.
func WithEntryCount(count int) Option {
	return func(w *IndexWriter) {
		w.estimatedEntryCount = count
	}
}

// NewIndexWriter creates a new IndexWriter that writes to the given filename.
func NewIndexWriter(filename string, compression Compression, opts ...Option) (*IndexWriter, error) {
	w := &IndexWriter{
		filename:    filename,
		entries:     make([]IndexEntry, 0),
		compression: compression,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
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

	// Use external sort for large entry sets
	if w.estimatedEntryCount >= 1_000_000 && len(w.entries) >= 1_000_000 {
		if err := w.externalSortWrite(); err != nil {
			return err
		}
		return nil
	}

	// In-memory sort for smaller entry sets
	sortByHash(w.entries)
	return w.writeSSTable()
}

// externalSortWrite writes entries to a temp file, sorts using Linux sort command,
// then writes to SSTable.
func (w *IndexWriter) externalSortWrite() error {
	// Write entries to temp file in hex format: hash offset size_uncompressed size_compressed
	tmpFile, err := os.CreateTemp("", "lakefs-sort-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmpFile.Name()

	writer := bufio.NewWriter(tmpFile)
	for _, entry := range w.entries {
		// Format: hex_hash offset size_uncompressed size_compressed
		fmt.Fprintf(writer, "%s %d %d %d\n",
			entry.Hash.String(),
			entry.Offset,
			entry.SizeUncompressed,
			entry.SizeCompressed)
	}
	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Run external sort: sort by first field (hex hash) in ascending order
	sortCmd := exec.Command("sort", "-t", " ", "-k1", tmpName)
	sortOut, err := sortCmd.Output()
	if err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("running sort command: %w", err)
	}

	// Clean up temp file
	os.Remove(tmpName)

	// Parse sorted output into entries
	sortedEntries, err := parseSortedOutput(sortOut)
	if err != nil {
		return fmt.Errorf("parsing sorted output: %w", err)
	}
	w.entries = sortedEntries

	// Write SSTable with sorted entries
	return w.writeSSTable()
}

// parseSortedOutput parses the output of the sort command.
// Each line format: hash_hex offset size_uncompressed size_compressed
func parseSortedOutput(data []byte) ([]IndexEntry, error) {
	var entries []IndexEntry
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid line format: %s", line)
		}
		hashBytes, err := hex.DecodeString(parts[0])
		if err != nil {
			return nil, fmt.Errorf("decoding hash: %w", err)
		}
		if len(hashBytes) != 32 {
			return nil, fmt.Errorf("invalid hash length: %d", len(hashBytes))
		}
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing offset: %w", err)
		}
		sizeUncompressed, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing size_uncompressed: %w", err)
		}
		sizeCompressed, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing size_compressed: %w", err)
		}
		var h ContentHash
		copy(h[:], hashBytes)
		entries = append(entries, IndexEntry{
			Hash:             h,
			Offset:           offset,
			SizeUncompressed: sizeUncompressed,
			SizeCompressed:   sizeCompressed,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning: %w", err)
	}
	return entries, nil
}

// writeSSTable writes the sorted entries to an SSTable file.
func (w *IndexWriter) writeSSTable() error {
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

// sortByHash sorts entries by their content hash in ascending order (smallest first).
// SSTable requires keys in strictly increasing order.
func sortByHash(entries []IndexEntry) {
	sort.Slice(entries, func(i, j int) bool {
		// Return true when entries[i] < entries[j] (ascending order)
		// BigThan returns true when h > other, so !BigThan means h <= other
		// For strict ordering, we use !entries[i].Hash.BigThan(entries[j].Hash)
		// which is true when entries[i].Hash <= entries[j].Hash
		return !entries[i].Hash.BigThan(entries[j].Hash)
	})
}
