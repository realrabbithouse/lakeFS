package packfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

// Magic constant for packfile header
const packfileMagic = "LKP1"

// Packfile version
const packfileVersion uint16 = 1

// Compression algorithms
const (
	CompressionNone uint8 = iota
	CompressionZstd
	CompressionGzip
	CompressionLz4
)

// Checksum algorithms
const (
	ChecksumSHA256 uint8 = iota
	ChecksumBLAKE3
	ChecksumxxHash
)

// Classification values
const (
	ClassificationFirstClass  uint8 = 0x01
	ClassificationSecondClass uint8 = 0x02
)

// PackfileHeader represents the 40-byte header of a packfile.
type PackfileHeader struct {
	Magic            [4]byte // "LKP1"
	Version          uint16
	ChecksumAlgo     uint8  // 0=SHA-256, 1=BLAKE3, 2=xxHash
	CompressionAlgo  uint8  // 0=none, 1=zstd, 2=gzip, 3=lz4
	Classification   uint8  // 0x01=First-class, 0x02=Second-class
	ObjectCount      uint64
	TotalSize        uint64 // uncompressed
	TotalSizeCompressed uint64
	Reserved         [7]byte
}

// PackfileObject represents a single object in the packfile data region.
type PackfileObject struct {
	SizeUncompressed uint32
	SizeCompressed   uint32
	ContentHash      ContentHash
	Data             io.Reader // decompressed data reader
}

// DataReader is the interface for objects to be written to a packfile.
type DataReader interface {
	Read(p []byte) (n int, err error)
	ContentHash() (ContentHash, error)
	SizeUncompressed() uint32
}

// ParsePackfileHeader reads and validates the 40-byte header from an io.Reader.
// Returns the header fields or an error if the packfile is invalid.
func ParsePackfileHeader(r io.Reader) (*PackfileHeader, error) {
	// Read exactly 40 bytes for the header
	var buf [40]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("packfile: header too short: %w", io.ErrUnexpectedEOF)
		}
		return nil, fmt.Errorf("packfile: reading header: %w", err)
	}

	h := &PackfileHeader{}

	// Magic (bytes 0-3)
	copy(h.Magic[:], buf[0:4])
	if string(h.Magic[:]) != packfileMagic {
		return nil, fmt.Errorf("packfile: invalid magic: got %q, want %q", string(h.Magic[:]), packfileMagic)
	}

	// Version (bytes 4-5, big-endian)
	h.Version = binary.BigEndian.Uint16(buf[4:6])
	if h.Version != packfileVersion {
		return nil, fmt.Errorf("packfile: unsupported version: %d (only version %d is supported)", h.Version, packfileVersion)
	}

	// Checksum algorithm (byte 6)
	h.ChecksumAlgo = buf[6]
	if h.ChecksumAlgo > ChecksumxxHash {
		return nil, fmt.Errorf("packfile: unknown checksum algorithm: %d", h.ChecksumAlgo)
	}

	// Compression algorithm (byte 7)
	h.CompressionAlgo = buf[7]
	// Note: compression algorithm is stored but actual decompression
	// is handled based on this field in future stories
	if h.CompressionAlgo > CompressionLz4 {
		return nil, fmt.Errorf("packfile: unknown compression algorithm: %d", h.CompressionAlgo)
	}

	// Classification (byte 8)
	h.Classification = buf[8]
	if h.Classification != ClassificationFirstClass && h.Classification != ClassificationSecondClass {
		return nil, fmt.Errorf("packfile: unknown classification: %d", h.Classification)
	}

	// Object count (bytes 9-16, big-endian)
	h.ObjectCount = binary.BigEndian.Uint64(buf[9:17])
	if h.ObjectCount == 0 {
		return nil, fmt.Errorf("packfile: invalid packfile: object_count is zero")
	}

	// Total size uncompressed (bytes 17-24, big-endian)
	h.TotalSize = binary.BigEndian.Uint64(buf[17:25])

	// Total size compressed (bytes 25-32, big-endian)
	h.TotalSizeCompressed = binary.BigEndian.Uint64(buf[25:33])

	// Reserved (bytes 33-39)
	copy(h.Reserved[:], buf[33:40])

	return h, nil
}

// WritePackfileHeader writes a 40-byte header to an io.Writer.
// Returns an error if writing fails.
func WritePackfileHeader(w io.Writer, h *PackfileHeader) error {
	var buf [40]byte

	// Magic (bytes 0-3)
	copy(buf[0:4], packfileMagic)

	// Version (bytes 4-5, big-endian)
	binary.BigEndian.PutUint16(buf[4:6], h.Version)

	// Checksum algorithm (byte 6)
	buf[6] = h.ChecksumAlgo

	// Compression algorithm (byte 7)
	buf[7] = h.CompressionAlgo

	// Classification (byte 8)
	buf[8] = h.Classification

	// Object count (bytes 9-16, big-endian)
	binary.BigEndian.PutUint64(buf[9:17], h.ObjectCount)

	// Total size uncompressed (bytes 17-24, big-endian)
	binary.BigEndian.PutUint64(buf[17:25], h.TotalSize)

	// Total size compressed (bytes 25-32, big-endian)
	binary.BigEndian.PutUint64(buf[25:33], h.TotalSizeCompressed)

	// Reserved (bytes 33-39) — explicitly zero to ensure consistent hash
	for i := 33; i < 40; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf[:])
	if err != nil {
		return fmt.Errorf("packfile: writing header: %w", err)
	}
	if n != 40 {
		return fmt.Errorf("packfile: header write short: wrote %d bytes, expected 40", n)
	}
	return nil
}

// ReadObject reads a single object from the packfile data region.
// Returns a PackfileObject with a decompressing reader wrapping the compressed data.
// The caller is responsible for closing the embedded reader if applicable.
func ReadObject(r io.Reader) (*PackfileObject, error) {
	// Read size_uncompressed (4 bytes, big-endian)
	var sizeUncompressedBuf [4]byte
	_, err := io.ReadFull(r, sizeUncompressedBuf[:])
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("packfile: unexpected end of file reading size_uncompressed: %w", err)
		}
		return nil, fmt.Errorf("packfile: reading size_uncompressed: %w", err)
	}
	sizeUncompressed := binary.BigEndian.Uint32(sizeUncompressedBuf[:])

	// Read size_compressed (4 bytes, big-endian)
	var sizeCompressedBuf [4]byte
	_, err = io.ReadFull(r, sizeCompressedBuf[:])
	if err != nil {
		return nil, fmt.Errorf("packfile: reading size_compressed: %w", err)
	}
	sizeCompressed := binary.BigEndian.Uint32(sizeCompressedBuf[:])

	// Validate: compressed size must not exceed uncompressed
	if sizeCompressed > sizeUncompressed {
		return nil, fmt.Errorf("packfile: corrupt object: compressed size (%d) exceeds uncompressed size (%d)", sizeCompressed, sizeUncompressed)
	}

	// Read content hash (32 bytes)
	var contentHash ContentHash
	_, err = io.ReadFull(r, contentHash[:])
	if err != nil {
		return nil, fmt.Errorf("packfile: reading content hash: %w", err)
	}

	// Read compressed data (size_compressed bytes)
	compressedData := make([]byte, sizeCompressed)
	_, err = io.ReadFull(r, compressedData)
	if err != nil {
		return nil, fmt.Errorf("packfile: reading compressed data: %w", err)
	}

	// Create decompressing reader based on compression algorithm
	dataReader, err := decompress(compressedData)
	if err != nil {
		return nil, err
	}

	return &PackfileObject{
		SizeUncompressed: sizeUncompressed,
		SizeCompressed:   sizeCompressed,
		ContentHash:      contentHash,
		Data:             dataReader,
	}, nil
}

// decompress wraps compressed data with the appropriate decompressor based on the compression algorithm.
// Currently only CompressionNone is supported.
func decompress(compressedData []byte) (io.Reader, error) {
	return &plainReader{data: compressedData}, nil
}

// plainReader is a simple io.Reader that returns data as-is (no compression).
type plainReader struct {
	data    []byte
	offset  int
}

func (p *plainReader) Read(buf []byte) (int, error) {
	if p.offset >= len(p.data) {
		return 0, io.EOF
	}
	n := copy(buf, p.data[p.offset:])
	p.offset += n
	return n, nil
}

// WriteObject writes a single object to the packfile.
// It writes size_uncompressed, size_compressed, content_hash (32 bytes), and compressed data.
//
// Note: This implementation buffers the object data in memory to compute the content hash
// before writing. For large objects, a future implementation could use a temporary file
// or redesign DataReader to provide the hash upfront.
func WriteObject(w io.Writer, obj DataReader) error {
	// Buffer data to compute hash in one pass, then write in second pass
	// Limit buffer to 64MB to prevent unbounded memory growth
	const maxObjectSize = 64 * 1024 * 1024
	var dataBuf bytes.Buffer
	buf := make([]byte, 32*1024) // 32KB chunks

	// Compute content hash and buffer data while reading
	var totalRead int64
	h := sha256.New()
	for {
		n, err := obj.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			totalRead += int64(n)
			dataBuf.Write(buf[:n])
			if dataBuf.Len() > maxObjectSize {
				return fmt.Errorf("packfile: object exceeds maximum size of %d bytes", maxObjectSize)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("packfile: reading object data: %w", err)
		}
	}

	// Get the computed hash
	var contentHash ContentHash
	copy(contentHash[:], h.Sum(nil))

	// Verify content hash matches what the DataReader reports
	reportedHash, err := obj.ContentHash()
	if err != nil {
		return fmt.Errorf("packfile: getting content hash: %w", err)
	}
	if contentHash != reportedHash {
		return fmt.Errorf("packfile: content hash mismatch: computed %x, reported %x", contentHash[:], reportedHash[:])
	}

	sizeUncompressed := obj.SizeUncompressed()
	if uint32(totalRead) != sizeUncompressed {
		return fmt.Errorf("packfile: size mismatch: read %d bytes, expected %d", totalRead, sizeUncompressed)
	}
	sizeCompressed := uint32(totalRead) // For now, no compression

	// Write size_uncompressed (4 bytes, big-endian)
	var sizeUncompressedBuf [4]byte
	binary.BigEndian.PutUint32(sizeUncompressedBuf[:], sizeUncompressed)
	_, err = w.Write(sizeUncompressedBuf[:])
	if err != nil {
		return fmt.Errorf("packfile: writing size_uncompressed: %w", err)
	}

	// Write size_compressed (4 bytes, big-endian)
	var sizeCompressedBuf [4]byte
	binary.BigEndian.PutUint32(sizeCompressedBuf[:], sizeCompressed)
	_, err = w.Write(sizeCompressedBuf[:])
	if err != nil {
		return fmt.Errorf("packfile: writing size_compressed: %w", err)
	}

	// Write content hash (32 bytes)
	_, err = w.Write(contentHash[:])
	if err != nil {
		return fmt.Errorf("packfile: writing content hash: %w", err)
	}

	// Write the actual object data
	_, err = w.Write(dataBuf.Bytes())
	if err != nil {
		return fmt.Errorf("packfile: writing object data: %w", err)
	}

	return nil
}

// ComputePackfileID computes the packfile ID by computing SHA-256 of all content.
// The input must be an io.Reader (e.g., file handle, bytes.Reader).
func ComputePackfileID(r io.Reader) (PackfileID, error) {
	h := sha256.New()
	_, err := io.Copy(h, r)
	if err != nil {
		return "", fmt.Errorf("packfile: computing packfile ID: %w", err)
	}
	var hash ContentHash
	copy(hash[:], h.Sum(nil))
	return PackfileID(hash.String()), nil
}

// ValidatePackfile validates a complete packfile (header + objects + footer).
// Returns nil if valid, or an error describing the validation failure.
// Computes SHA-256 in a single forward pass without requiring seekability.
func ValidatePackfile(r io.Reader) error {
	// Parse and validate header
	hdr, err := ParsePackfileHeader(r)
	if err != nil {
		return err
	}

	// Compute SHA-256 of header by re-creating header bytes from parsed struct
	hasher := sha256.New()
	headerBytes := headerToBytes(hdr)
	hasher.Write(headerBytes)

	// Validate and process each object, computing hash on compressed data
	var totalUncompressedRead int64
	for i := uint64(0); i < hdr.ObjectCount; i++ {
		// Read object header (40 bytes: 4+4+32)
		var objHdrBuf [40]byte
		_, err := io.ReadFull(r, objHdrBuf[:])
		if err != nil {
			return fmt.Errorf("packfile: reading object %d header: %w", i, err)
		}
		hasher.Write(objHdrBuf[:])

		sizeUncompressed := binary.BigEndian.Uint32(objHdrBuf[0:4])
		sizeCompressed := binary.BigEndian.Uint32(objHdrBuf[4:8])

		// Read compressed data
		compressedData := make([]byte, sizeCompressed)
		_, err = io.ReadFull(r, compressedData)
		if err != nil {
			return fmt.Errorf("packfile: reading object %d compressed data: %w", i, err)
		}
		hasher.Write(compressedData)
		totalUncompressedRead += int64(sizeUncompressed)

		// Validate: compressed size must not exceed uncompressed
		if sizeCompressed > sizeUncompressed {
			return fmt.Errorf("packfile: corrupt object: compressed size (%d) exceeds uncompressed size (%d)", sizeCompressed, sizeUncompressed)
		}

		// Validate content hash by decompressing and computing actual hash
		decompressedData, err := decompress(compressedData)
		if err != nil {
			return fmt.Errorf("packfile: decompressing object %d: %w", i, err)
		}
		actualHash := sha256.New()
		if _, err := io.Copy(actualHash, decompressedData); err != nil {
			return fmt.Errorf("packfile: computing object %d hash: %w", i, err)
		}
		var computedContentHash ContentHash
		copy(computedContentHash[:], actualHash.Sum(nil))

		// Compare with stored content hash
		var storedContentHash ContentHash
		copy(storedContentHash[:], objHdrBuf[8:40])
		if computedContentHash != storedContentHash {
			return fmt.Errorf("packfile: object %d content hash mismatch: computed %x, stored %x", i, computedContentHash[:], storedContentHash[:])
		}
	}

	// Validate total uncompressed size matches
	if totalUncompressedRead != int64(hdr.TotalSize) {
		return fmt.Errorf("packfile: size mismatch: expected %d uncompressed bytes, read %d",
			hdr.TotalSize, totalUncompressedRead)
	}

	// Read and validate footer checksum
	var footerChecksum [32]byte
	_, err = io.ReadFull(r, footerChecksum[:])
	if err != nil {
		return fmt.Errorf("packfile: reading footer checksum: %w", err)
	}

	// Compare computed hash with footer
	var computedHash [32]byte
	copy(computedHash[:], hasher.Sum(nil))
	if computedHash != footerChecksum {
		return fmt.Errorf("packfile: footer checksum mismatch: computed %x, footer %x", computedHash[:], footerChecksum[:])
	}

	return nil
}

// headerToBytes converts a PackfileHeader to its 40-byte binary representation.
// This is the same logic as WritePackfileHeader but returns bytes instead of writing.
func headerToBytes(h *PackfileHeader) []byte {
	var buf [40]byte
	copy(buf[0:4], h.Magic[:])
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	buf[6] = h.ChecksumAlgo
	buf[7] = h.CompressionAlgo
	buf[8] = h.Classification
	binary.BigEndian.PutUint64(buf[9:17], h.ObjectCount)
	binary.BigEndian.PutUint64(buf[17:25], h.TotalSize)
	binary.BigEndian.PutUint64(buf[25:33], h.TotalSizeCompressed)
	// Reserved (bytes 33-39) — explicitly zero
	for i := 33; i < 40; i++ {
		buf[i] = 0
	}
	return buf[:]
}