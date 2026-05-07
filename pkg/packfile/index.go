package packfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	// ErrObjectNotFound is returned when a content hash is not found in the index.
	ErrObjectNotFound = errors.New("packfile: object not found in index")

	pkgLogger = logging.Dummy()
)

// IndexEntry represents a single entry in the SSTable index.
type IndexEntry struct {
	ContentHash      ContentHash
	Offset           int64
	SizeUncompressed uint32
	SizeCompressed   uint32
}

// IndexReader provides random access to packfile objects via content hash lookup.
type IndexReader struct {
	reader *sstable.Reader
}

// Get returns the offset and sizes for a given content hash.
// Returns ErrObjectNotFound if the hash is not in the index.
func (r *IndexReader) Get(ctx context.Context, hash ContentHash) (offset int64, sizeUncompressed uint32, sizeCompressed uint32, err error) {
	// Create iterator to seek to the key
	iter, err := r.reader.NewIter(nil, nil)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("packfile: creating index iterator: %w", err)
	}
	defer iter.Close()

	// Seek to the content hash
	key, lazyValue := iter.SeekGE(hash[:], sstable.SeekGEFlags(0))
	if key == nil {
		if iter.Error() != nil {
			return 0, 0, 0, fmt.Errorf("packfile: seeking index: %w", iter.Error())
		}
		return 0, 0, 0, ErrObjectNotFound
	}

	// Check if we found the exact key
	if !bytes.Equal(key.UserKey, hash[:]) {
		return 0, 0, 0, ErrObjectNotFound
	}

	// Get the actual value bytes from LazyValue
	// The buffer may be borrowed from pebble internals, so we must copy it
	valueBytes, owned, err := lazyValue.Value(nil)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("packfile: reading index value: %w", err)
	}
	if !owned {
		// Must copy - buffer is borrowed and may be invalidated
		copied := make([]byte, len(valueBytes))
		copy(copied, valueBytes)
		valueBytes = copied
	}

	// Decode value: three varints (offset, size_uncompressed, size_compressed)
	// Using bytes.Reader to properly track position
	reader := bytes.NewReader(valueBytes)
	offset64, _ := binary.ReadUvarint(reader)
	sizeUncomp64, _ := binary.ReadUvarint(reader)
	sizeComp64, _ := binary.ReadUvarint(reader)

	return int64(offset64), uint32(sizeUncomp64), uint32(sizeComp64), nil
}

// Close closes the index reader.
func (r *IndexReader) Close() error {
	return r.reader.Close()
}

// BuildIndex builds an SSTable index from a packfile.
// Returns the path to the index file and the number of entries indexed.
func BuildIndex(ctx context.Context, packfilePath string, tmpDir string, compression string) (indexPath string, count int, err error) {
	// Open the packfile
	packfile, err := os.Open(packfilePath)
	if err != nil {
		return "", 0, fmt.Errorf("packfile: opening packfile: %w", err)
	}
	defer packfile.Close()

	// Parse header
	header, err := ParsePackfileHeader(packfile)
	if err != nil {
		return "", 0, fmt.Errorf("packfile: parsing header: %w", err)
	}

	pkgLogger.Info("packfile: index build started",
		"packfile_id", filepath.Base(packfilePath),
		"entries", header.ObjectCount)

	// Collect entries
	entries := make([]IndexEntry, 0, header.ObjectCount)
	var offset int64 = 40 // Data region starts after 40-byte header

	for i := uint64(0); i < header.ObjectCount; i++ {
		obj, err := ReadObject(packfile)
		if err != nil {
			return "", 0, fmt.Errorf("packfile: reading object %d: %w", i, err)
		}

		entry := IndexEntry{
			ContentHash:      obj.ContentHash,
			Offset:           offset,
			SizeUncompressed: obj.SizeUncompressed,
			SizeCompressed:   obj.SizeCompressed,
		}
		entries = append(entries, entry)

		// Advance offset: 40 bytes (object header) + size_compressed
		offset += 40 + int64(obj.SizeCompressed)
	}

	// Sort entries by content hash (required for SSTable)
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].ContentHash[:], entries[j].ContentHash[:]) < 0
	})

	// Generate index path
	indexPath = packfilePath + ".sst"

	// Build SSTable from sorted entries
	err = BuildIndexFromEntries(ctx, entries, indexPath, compression)
	if err != nil {
		return "", 0, fmt.Errorf("packfile: building index: %w", err)
	}

	pkgLogger.Info("packfile: index build completed",
		"packfile_id", filepath.Base(packfilePath),
		"entries", len(entries))

	return indexPath, len(entries), nil
}

// BuildIndexFromEntries builds an SSTable index from pre-sorted entries.
// Use this when entries are already sorted by content hash.
// ctx is unused but kept for future async/cancellation support.
// Entries with duplicate content hashes will cause an error.
func BuildIndexFromEntries(ctx context.Context, entries []IndexEntry, indexPath string, compression string) error {
	// Sort entries by content hash to ensure strictly increasing key order
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].ContentHash[:], entries[j].ContentHash[:]) < 0
	})

	// Check for duplicate hashes
	for i := 1; i < len(entries); i++ {
		if entries[i].ContentHash == entries[i-1].ContentHash {
			return fmt.Errorf("packfile: duplicate content hash: %x", entries[i].ContentHash)
		}
	}

	// Create the index file
	f, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("packfile: creating index file: %w", err)
	}

	// Determine compression
	var compress sstable.Compression
	switch compression {
	case "zstd":
		compress = sstable.ZstdCompression
	case "snappy":
		compress = sstable.SnappyCompression
	default:
		compress = sstable.NoCompression
	}

	// Create SSTable writer
	writer := sstable.NewWriter(f, sstable.WriterOptions{
		Compression: compress,
		BlockSize:  32 * 1024, // 32KB block size
		// Bloom filter is enabled by default in pebble/sstable
	})

	// Write entries
	for _, entry := range entries {
		// Encode value: three varints (offset, size_uncompressed, size_compressed)
		valueBuf := make([]byte, binary.MaxVarintLen64*3)
		n := binary.PutUvarint(valueBuf, uint64(entry.Offset))
		n += binary.PutUvarint(valueBuf[n:], uint64(entry.SizeUncompressed))
		n += binary.PutUvarint(valueBuf[n:], uint64(entry.SizeCompressed))
		valueBuf = valueBuf[:n]

		if err := writer.Set(entry.ContentHash[:], valueBuf); err != nil {
			// writer.Close() also closes the underlying file - just remove
			os.Remove(indexPath)
			return fmt.Errorf("packfile: writing index entry: %w", err)
		}
	}

	// Close writer (this also closes the underlying file)
	if err := writer.Close(); err != nil {
		os.Remove(indexPath)
		return fmt.Errorf("packfile: closing index writer: %w", err)
	}

	return nil
}

// OpenIndex opens an SSTable index file for reading.
// Returns an IndexReader that can lookup content hashes.
func OpenIndex(ctx context.Context, indexPath string) (*IndexReader, error) {
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("packfile: opening index: %w", err)
	}

	reader, err := sstable.NewReader(f, sstable.ReaderOptions{})
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("packfile: creating index reader: %w", err)
	}

	return &IndexReader{reader: reader}, nil
}

// ReadPackfileEntries reads all entries from a packfile and returns them sorted by content hash.
// This is used for building indexes.
func ReadPackfileEntries(packfilePath string) ([]IndexEntry, error) {
	packfile, err := os.Open(packfilePath)
	if err != nil {
		return nil, fmt.Errorf("packfile: opening packfile: %w", err)
	}
	defer packfile.Close()

	header, err := ParsePackfileHeader(packfile)
	if err != nil {
		return nil, fmt.Errorf("packfile: parsing header: %w", err)
	}

	entries := make([]IndexEntry, 0, header.ObjectCount)
	var offset int64 = 40

	for i := uint64(0); i < header.ObjectCount; i++ {
		obj, err := ReadObject(packfile)
		if err != nil {
			return nil, fmt.Errorf("packfile: reading object %d: %w", i, err)
		}

		entries = append(entries, IndexEntry{
			ContentHash:      obj.ContentHash,
			Offset:           offset,
			SizeUncompressed: obj.SizeUncompressed,
			SizeCompressed:   obj.SizeCompressed,
		})

		offset += 40 + int64(obj.SizeCompressed)
	}

	// Sort by content hash
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].ContentHash[:], entries[j].ContentHash[:]) < 0
	})

	return entries, nil
}
