package packfile

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
)

// Unpacker reads objects from a packfile. Supports both random access (by offset)
// and sequential scan. Decompression is streaming — no in-memory []byte for bulk data.
type Unpacker interface {
	// GetObject returns a decompressed streaming io.Reader at the given byte offset.
	// Also returns (ContentHash, uncompressedSize, compressedSize) for the object.
	// The caller must close the returned reader.
	GetObject(ctx context.Context, offset int64) (ContentHash, io.Reader, int64, int64, error)

	// Scan returns a sequential iterator over all objects in creation order.
	// If ignoreData is true, data is discarded via io.Discard and r in Object() returns nil.
	Scan(ctx context.Context, ignoreData bool) PackfileObjectIterator
}

// PackfileObjectIterator iterates over objects in a packfile.
type PackfileObjectIterator interface {
	Next() bool
	// Object returns (contentHash, offset, uncompressedSize, compressedSize, decompressedReader, err).
	// If ignoreData was true in Scan(), r will be nil and data is already discarded.
	// The caller must close the returned reader if non-nil.
	Object() (ContentHash, int64, int64, int64, io.Reader, error)
	Err() error
	Close() error
}

// unpacker implements Unpacker.
type unpacker struct {
	r             io.Reader // for sequential scan
	rat           io.ReaderAt // for random access
	compression   Compression
	footerHasher  hash.Hash // SHA-256 of header + all object data (for integrity check)
	header        [HeaderSize]byte // loaded packfile header
}

// NewUnpacker creates an Unpacker from an io.ReadSeeker.
// The ReadSeeker must also implement io.ReaderAt for random access.
func NewUnpacker(r io.ReadSeeker) Unpacker {
	u := &unpacker{
		r:            r,
		footerHasher: sha256.New(),
	}
	// Get io.ReaderAt if available
	if rat, ok := r.(io.ReaderAt); ok {
		u.rat = rat
	}
	// Load packfile header
	if _, err := u.r.Read(u.header[:]); err != nil && err != io.EOF {
		// Will be caught on first operation
		return u
	}
	u.compression = Compression(u.header[6])
	return u
}

func (u *unpacker) GetObject(ctx context.Context, offset int64) (ContentHash, io.Reader, int64, int64, error) {
	// Read object header at offset
	var objHdr [40]byte
	if _, err := u.rat.ReadAt(objHdr[:], offset); err != nil {
		return ContentHash{}, nil, 0, 0, fmt.Errorf("reading object header: %w", err)
	}

	uncompressedSize := int64(binary.LittleEndian.Uint32(objHdr[0:4]))
	compressedSize := int64(binary.LittleEndian.Uint32(objHdr[4:8]))
	var h ContentHash
	copy(h[:], objHdr[8:40])

	// Build section reader for compressed data
	sr := io.NewSectionReader(u.rat, offset+40, compressedSize)

	// Build decompression reader
	dr, err := NewDecompressionReader(sr, u.compression)
	if err != nil {
		return ContentHash{}, nil, 0, 0, fmt.Errorf("creating decompression reader: %w", err)
	}

	return h, dr, uncompressedSize, compressedSize, nil
}

func (u *unpacker) Scan(ctx context.Context, ignoreData bool) PackfileObjectIterator {
	return &scanIter{
		r:          u.r,
		compression: u.compression,
		ignoreData:  ignoreData,
	}
}

type scanIter struct {
	r           io.Reader
	compression Compression
	ignoreData  bool
	pos         int64  // current position in packfile
	objIndex    int    // number of objects seen
	currentObj  objMeta
	currentHash ContentHash
	err         error
}

type objMeta struct {
	offset           int64
	uncompressedSize int64
	compressedSize   int64
}

func (it *scanIter) Next() bool {
	// Read header if first call
	if it.objIndex == 0 && it.pos == 0 {
		// Skip to end of header (already read by NewUnpacker)
		it.pos = HeaderSize
	}

	// Read next object header
	var objHdr [40]byte
	n, err := io.ReadFull(it.r, objHdr[:])
	if err == io.EOF {
		return false // no more objects
	}
	if err != nil {
		it.err = fmt.Errorf("reading object header: %w", err)
		return false
	}
	if n != 40 {
		it.err = errors.New("short read of object header")
		return false
	}

	// If this looks like the file header magic, it's the trailing header from Close() — stop.
	if string(objHdr[:4]) == Magic {
		return false
	}

	it.currentHash = ContentHash{}
	copy(it.currentHash[:], objHdr[8:40])
	it.currentObj = objMeta{
		offset:           it.pos,
		uncompressedSize: int64(binary.LittleEndian.Uint32(objHdr[0:4])),
		compressedSize:   int64(binary.LittleEndian.Uint32(objHdr[4:8])),
	}

	it.pos += 40 + it.currentObj.compressedSize // 40=objHdr, rest=compressed data
	it.objIndex++
	return true
}

func (it *scanIter) Object() (ContentHash, int64, int64, int64, io.Reader, error) {
	if it.err != nil {
		return ContentHash{}, 0, 0, 0, nil, it.err
	}

	// If ignoreData is set, seek past compressed data and return nil reader
	if it.ignoreData {
		// Seek past the compressed data by advancing the reader
		if _, err := io.Copy(io.Discard, io.LimitReader(it.r, it.currentObj.compressedSize)); err != nil {
			// If we can't discard, at least skip
		}
		return it.currentHash, it.currentObj.offset, it.currentObj.uncompressedSize, it.currentObj.compressedSize, nil, nil
	}

	// Return a reader that decompresses on demand
	pr, pw := io.Pipe()
	go func() {
		_, err := io.Copy(pw, io.LimitReader(it.r, it.currentObj.compressedSize))
		if closeErr := pw.Close(); err == nil {
			err = closeErr
		}
		_ = err
	}()

	// Wrap in decompression reader
	dr, err := NewDecompressionReader(pr, it.compression)
	if err != nil {
		return ContentHash{}, 0, 0, 0, nil, fmt.Errorf("creating decompression reader: %w", err)
	}
	return it.currentHash, it.currentObj.offset, it.currentObj.uncompressedSize, it.currentObj.compressedSize, dr, nil
}

func (it *scanIter) Err() error {
	return it.err
}

func (it *scanIter) Close() error {
	return nil
}
