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

// Packer builds a packfile. Append-only, Close() writes footer checksum.
type Packer interface {
	// AppendObject appends one object to the packfile.
	// uncompressedSize must be provided by caller (e.g., from file stats).
	// Content hash is computed via StreamingHash as data streams through.
	// Compressed output is buffered per-object (max 5MB) — ONE non-streaming trade-off.
	AppendObject(ctx context.Context, r io.Reader, uncompressedSize int64) (offset, compressedSize int64, err error)
	Close() error
}

// packer implements Packer.
type packer struct {
	w              io.Writer
	compression    Compression
	classification Classification
	footerHasher   hash.Hash       // SHA-256 of header + all object data (for footer checksum)
	contentHasher  *StreamingHash  // per-object content hash via streaming

	objectCount            uint64
	totalSizeUncompressed uint64
	totalSizeCompressed   uint64
	closed                 bool
}

// NewPacker creates a Packer that writes to w.
func NewPacker(w io.Writer, compression Compression, classification Classification) Packer {
	p := &packer{
		w:              w,
		compression:    compression,
		classification: classification,
		footerHasher:   sha256.New(),
		contentHasher:  NewStreamingHash(),
	}
	// Write packfile header (fields will be finalized on Close)
	var header [HeaderSize]byte
	copy(header[0:4], Magic)
	binary.LittleEndian.PutUint16(header[4:6], Version)
	header[6] = byte(compression)
	header[7] = byte(classification)
	p.w.Write(header[:])
	p.footerHasher.Write(header[:])
	return p
}

func (p *packer) AppendObject(ctx context.Context, r io.Reader, uncompressedSize int64) (int64, int64, error) {
	if p.closed {
		return 0, 0, errors.New("packer closed")
	}

	// Streaming: feed input into StreamingHash via TeeReader for content addr,
	// and into compressor via io.Pipe. The compressed output must be fully
	// materialized to get compressedSize before writing the header.
	// ONE non-streaming trade-off: per-object compressed buffer (max 5MB).
	// This is acceptable since max object (5MB) << max packfile (5GB).

	reader := io.TeeReader(r, p.contentHasher)

	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(CompressTo(reader, p.compression, pw))
	}()

	compressedData, err := io.ReadAll(pr)
	if err != nil {
		return 0, 0, fmt.Errorf("compress object: %w", err)
	}

	contentHash := p.contentHasher.Sum()
	p.contentHasher = NewStreamingHash()

	compressedSize := int64(len(compressedData))

	// Record offset (current position in packfile = header + all previous objects)
	offset := HeaderSize + int64(p.totalSizeCompressed)

	// Write per-object header: uncompressed size, compressed size, content hash
	var objHdr [40]byte
	binary.LittleEndian.PutUint32(objHdr[0:4], uint32(uncompressedSize))
	binary.LittleEndian.PutUint32(objHdr[4:8], uint32(compressedSize))
	copy(objHdr[8:40], contentHash[:])

	// Write to underlying writer AND update footer hasher
	if _, err := p.w.Write(objHdr[:]); err != nil {
		return 0, 0, err
	}
	p.footerHasher.Write(objHdr[:])

	if _, err := p.w.Write(compressedData); err != nil {
		return 0, 0, err
	}
	p.footerHasher.Write(compressedData)

	p.objectCount++
	p.totalSizeUncompressed += uint64(uncompressedSize)
	p.totalSizeCompressed += uint64(compressedSize)

	return offset, compressedSize, nil
}

func (p *packer) Close() error {
	if p.closed {
		return errors.New("packer already closed")
	}
	p.closed = true

	// Build final header with correct counts/sizes
	var finalHeader [HeaderSize]byte
	copy(finalHeader[0:4], Magic)
	binary.LittleEndian.PutUint16(finalHeader[4:6], Version)
	finalHeader[6] = byte(p.compression)
	finalHeader[7] = byte(p.classification)
	binary.LittleEndian.PutUint64(finalHeader[8:16], p.objectCount)
	binary.LittleEndian.PutUint64(finalHeader[16:24], p.totalSizeUncompressed)
	binary.LittleEndian.PutUint64(finalHeader[24:32], p.totalSizeCompressed)

	// Compute footer = SHA-256(new header + all hashed data)
	h := sha256.New()
	h.Write(finalHeader[:])
	h.Write(p.footerHasher.Sum(nil))
	checksum := h.Sum(nil)

	// Write updated header and footer
	if _, err := p.w.Write(finalHeader[:]); err != nil {
		return err
	}
	if _, err := p.w.Write(checksum); err != nil {
		return err
	}

	return nil
}
