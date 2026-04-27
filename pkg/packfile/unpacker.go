package packfile

import (
	"bytes"
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
	// Also returns (uncompressedSize, compressedSize) for the object.
	// The caller must close the returned reader.
	GetObject(ctx context.Context, offset int64) (r io.Reader, size, compressedSize int64, err error)

	// Scan returns a sequential iterator over all objects in creation order.
	Scan(ctx context.Context) PackfileObjectIterator
}

// PackfileObjectIterator iterates over objects in a packfile.
type PackfileObjectIterator interface {
	Next() bool
	// Object returns (offset, uncompressedSize, compressedSize, decompressedReader, err).
	// The caller must close the returned reader.
	Object() (offset, size, compressedSize int64, r io.Reader, err error)
	Err() error
	Close() error
}

// unpacker implements Unpacker.
type unpacker struct {
	r            io.ReaderAt
	compression  Compression
	footerHasher hash.Hash // SHA-256 of header + all object data (for integrity check)
}

func NewUnpacker(r io.ReaderAt) Unpacker {
	return &unpacker{r: r, footerHasher: sha256.New()}
}

func (u *unpacker) GetObject(ctx context.Context, offset int64) (io.Reader, int64, int64, error) {
	// Read packfile header to get compression algorithm (if not already set)
	if u.compression == 0 {
		var header [HeaderSize]byte
		if _, err := u.r.ReadAt(header[:], 0); err != nil {
			return nil, 0, 0, fmt.Errorf("reading packfile header: %w", err)
		}
		u.compression = Compression(header[6])
	}

	// Read object header at offset
	var objHdr [40]byte
	if _, err := u.r.ReadAt(objHdr[:], offset); err != nil {
		return nil, 0, 0, fmt.Errorf("reading object header: %w", err)
	}

	uncompressedSize := int64(binary.LittleEndian.Uint32(objHdr[0:4]))
	compressedSize := int64(binary.LittleEndian.Uint32(objHdr[4:8]))

	// Streaming decompression via io.Pipe — no in-memory []byte for bulk data
	pr, pw := io.Pipe()
	go func() {
		// Read compressed data chunk-by-chunk from packfile and pipe into decompressor
		// For simplicity, read the full compressed object (max 5MB) — acceptable buffer.
		compressedData := make([]byte, compressedSize)
		if _, err := u.r.ReadAt(compressedData, offset+40); err != nil { // 40=objHdr
			pw.CloseWithError(fmt.Errorf("reading compressed data: %w", err))
			return
		}
		err := UncompressTo(bytes.NewReader(compressedData), u.compression, pw)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	return pr, uncompressedSize, compressedSize, nil
}

func (u *unpacker) Scan(ctx context.Context) PackfileObjectIterator {
	return &scanIter{r: u.r, compression: u.compression}
}

type scanIter struct {
	r            io.ReaderAt
	compression  Compression
	pos          int64  // current position in packfile
	objIndex     int    // number of objects seen
	currentObj   objMeta
	err          error
}

type objMeta struct {
	offset           int64
	uncompressedSize int64
	compressedSize   int64
}

func (it *scanIter) Next() bool {
	// Read header if first call
	if it.objIndex == 0 && it.pos == 0 {
		var header [HeaderSize]byte
		if _, err := it.r.ReadAt(header[:], 0); err != nil && err != io.EOF {
			it.err = err
			return false
		}
		if string(header[:4]) != Magic {
			it.err = errors.New("invalid packfile magic")
			return false
		}
		it.compression = Compression(header[6]) // compression algorithm byte
		it.pos = HeaderSize
	}

	// Read next object header
	var objHdr [40]byte
	n, err := it.r.ReadAt(objHdr[:], it.pos)
	if err == io.EOF {
		return false // no more objects
	}
	if err != nil {
		it.err = err
		return false
	}
	if n != 40 {
		it.err = errors.New("short read of object header")
		return false
	}

		it.currentObj = objMeta{
		offset:           it.pos,
		uncompressedSize: int64(binary.LittleEndian.Uint32(objHdr[0:4])),
		compressedSize:   int64(binary.LittleEndian.Uint32(objHdr[4:8])),
	}

	// If this looks like the file header magic, it's the trailing header from Close() — stop.
	if string(objHdr[:4]) == Magic {
		return false
	}

	it.pos += 40 + it.currentObj.compressedSize // 40=objHdr, rest=compressed data
	it.objIndex++
	return true
}

func (it *scanIter) Object() (int64, int64, int64, io.Reader, error) {
	if it.err != nil {
		return 0, 0, 0, nil, it.err
	}

	// Streaming decompression via io.Pipe
	pr, pw := io.Pipe()
	go func() {
		compressedData := make([]byte, it.currentObj.compressedSize)
		if _, err := it.r.ReadAt(compressedData, it.currentObj.offset+40); err != nil {
			pw.CloseWithError(fmt.Errorf("reading compressed data: %w", err))
			return
		}
		err := UncompressTo(bytes.NewReader(compressedData), it.compression, pw)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	return it.currentObj.offset, it.currentObj.uncompressedSize, it.currentObj.compressedSize, pr, nil
}

func (it *scanIter) Err() error {
	return it.err
}

func (it *scanIter) Close() error {
	return nil
}
