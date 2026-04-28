package packfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// IndexReader reads from an SSTable index file for O(1) content hash lookups.
type IndexReader struct {
	filename string
	file     *os.File
	reader   *sstable.Reader
}

// NewIndexReader opens an SSTable index file for reading.
func NewIndexReader(filename string) (*IndexReader, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("opening index file: %w", err)
	}

	reader, err := sstable.NewReader(f, sstable.ReaderOptions{})
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("creating sstable reader: %w", err)
	}

	return &IndexReader{
		filename: filename,
		file:     f,
		reader:   reader,
	}, nil
}

// Lookup returns the IndexEntry for the given content hash.
// Returns nil, nil if the hash is not found.
func (r *IndexReader) Lookup(hash ContentHash) (*IndexEntry, error) {
	iter, err := r.reader.NewIter(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer iter.Close()

	key, lazyValue := iter.SeekGE(hash[:], sstable.SeekGEFlags(0))
	if key == nil {
		// Check if there was an error during seek
		if iter.Error() != nil {
			return nil, fmt.Errorf("seeking in index: %w", iter.Error())
		}
		return nil, nil // Not found
	}

	// Verify we found the exact key (not just the next one)
	if !bytes.Equal(hash[:], key.UserKey) {
		return nil, nil // Not found
	}

	value, err := r.retrieveValue(lazyValue)
	if err != nil {
		return nil, fmt.Errorf("retrieving value: %w", err)
	}

	if len(value) != 24 {
		return nil, fmt.Errorf("invalid index value length: expected 24, got %d", len(value))
	}

	offset := int64(binary.LittleEndian.Uint64(value[0:8]))
	sizeUncompressed := int64(binary.LittleEndian.Uint64(value[8:16]))
	sizeCompressed := int64(binary.LittleEndian.Uint64(value[16:24]))

	return &IndexEntry{
		Hash:             hash,
		Offset:           offset,
		SizeUncompressed: sizeUncompressed,
		SizeCompressed:   sizeCompressed,
	}, nil
}

// retrieveValue extracts the byte slice from a pebble.LazyValue.
func (r *IndexReader) retrieveValue(lazyValue pebble.LazyValue) ([]byte, error) {
	val, owned, err := lazyValue.Value(nil)
	if err != nil {
		return nil, err
	}
	if owned || val == nil {
		return val, nil
	}
	var copiedVal = make([]byte, len(val))
	copy(copiedVal, val)
	return copiedVal, nil
}

// Close closes the index reader.
func (r *IndexReader) Close() error {
	if r.reader != nil {
		r.reader.Close()
	}
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
