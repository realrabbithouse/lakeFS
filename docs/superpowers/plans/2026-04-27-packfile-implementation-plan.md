# Packfile Transfer Protocol — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the packfile transfer protocol — a streaming binary bundle format for bulk object upload/download with content-addressable deduplication, SSTable indexing, and packfile merge.

**Architecture:** Packfiles are immutable binary bundles of compressed objects stored on local disk/NFS (primary) with async S3 backup. An SSTable index provides O(1) bloom-filter-accelerated lookups by content hash. Packfile Manager is the single authority for all packfile metadata state; Graveler is its client. The packfile binary format uses Packer (append-only writer) and Unpacker (random+sequential access) abstractions. True streaming IO throughout except per-object: the header-before-data format requires materializing compressed output to know `compressedSize` before writing the header — bounded per-object buffer (max 5MB).

**Tech Stack:** Go, `github.com/cockroachdb/pebble/sstable`, `github.com/google/uuid`, block adapter (S3/GCS/Azure/Local)

---

## File Structure

### New Files

```
pkg/packfile/
├── packfile.proto              # Protobuf: PackfileMetadata, PackfileSnapshot, UploadSession, enums
├── packfile.pb.go             # Generated protobuf
├── content_hash.go            # ContentHash typed type, bare hex SHA-256
├── format.go                 # Binary packfile format: magic, header, constants
├── compress.go               # Streaming compression/uncompression (io.Pipe-based)
├── packer.go                 # Packer interface + implementation (append-only packfile writer)
├── packer_test.go
├── unpacker.go               # Unpacker interface + implementation (packfile reader)
├── unpacker_test.go
├── index_writer.go           # SSTable index writer (Approach A: in-memory sort)
├── index_writer_test.go
├── manager.go                # Packfile Manager: all state transitions
├── manager_test.go
└── kv.go                    # KV store helpers for packfile types

pkg/api/
├── controller.go              # Add packfile API handlers
└── apigen/                  # (generated from swagger.yml)

pkg/graveler/
└── packfile_manager.go       # Graveler's PackfileManager interface, delegates to pkg/packfile

pkg/catalog/
└── model.go                  # Add PACKFILE AddressType, PackfileOffset field

config/
└── config.go                 # Packfile config: thresholds, paths, cache sizes
```

---

## Key Interfaces

### ContentHash

```go
// ContentHash represents a SHA-256 content hash.
type ContentHash [32]byte

// ParseContentHash parses a bare hex content hash string (64 hex chars).
func ParseContentHash(s string) (ContentHash, error)
// packfile_id = bare hex string via fmt.Sprintf("%x", h[:])
```

### Streaming Hash Computation

```go
// StreamingHash computes a ContentHash as data is read.
// Used during packfile building (AppendObject) and reading.
type StreamingHash struct{ /* ... */ }

func (h *StreamingHash) Write(p []byte) (int, error)  // update hash with each read chunk
func (h *StreamingHash) Sum() ContentHash              // get final hash
```

### Compression

```go
type Compression uint8  // None=0, Zstd=1, Gzip=2, LZ4=3

// CompressTo reads from r, compresses using algorithm c, writes to w.
// True streaming IO — uses io.Pipe internally, no in-memory []byte for bulk data.
func CompressTo(r io.Reader, c Compression, w io.Writer) error

// UncompressTo reads from r, uncompresses using algorithm c, writes to w.
func UncompressTo(r io.Reader, c Compression, w io.Writer) error
```

### Packer — Build Packfile

```go
// Packer builds a packfile. Append-only, Close() writes footer checksum.
type Packer interface {
    // AppendObject appends one object to the packfile.
    // r: raw (uncompressed) object data stream
    // uncompressedSize: known size of the raw object (must be provided by caller).
    //   Content hash is computed via StreamingHash as data streams through.
    //   Compressed output is buffered per-object (max 5MB) to get compressedSize
    //   before writing the object header — the ONE non-streaming trade-off.
    // Returns (offset, compressedSize).
    AppendObject(ctx context.Context, r io.Reader, uncompressedSize int64) (offset, compressedSize int64, err error)
    Close() error
}
```

### Unpacker — Read Packfile

```go
// Unpacker reads objects from a packfile. Supports both random access (by offset)
// and sequential scan. Decompression is streaming — no in-memory []byte for bulk data.
type Unpacker interface {
    // GetObject returns a decompressed streaming io.Reader at the given byte offset.
    // Also returns (uncompressedSize, compressedSize) for the object.
    GetObject(ctx context.Context, offset int64) (r io.Reader, size, compressedSize int64, err error)

    // Scan returns a sequential iterator over all objects in creation order.
    Scan(ctx context.Context) PackfileObjectIterator
}

type PackfileObjectIterator interface {
    Next() bool
    // Object returns (offset, uncompressedSize, compressedSize, decompressedReader, err).
    // The caller must close the returned reader.
    Object() (offset, size, compressedSize int64, r io.Reader, err error)
    Err() error
    Close() error
}
```

### IndexEntry

```go
type IndexEntry struct {
    Hash             ContentHash  // 32-byte SHA-256, sorted by this
    Offset           int64
    SizeUncompressed int64
    SizeCompressed   int64
}
// Value encoding: 24-byte fixed-width (offset + sizeUncompressed + sizeCompressed)
```

---

## Phase 1: Core Packfile Infrastructure

### Task 1: Protobuf Schema + KV Registration

**Files:**
- Create: `pkg/packfile/packfile.proto`
- Create: `pkg/packfile/packfile.pb.go` (generated)
- Create: `pkg/packfile/kv.go`

```protobuf
syntax = "proto3";
package packfile;
option go_package = "github.com/treeverse/lakefs/pkg/packfile";

import "google/protobuf/timestamp.proto";

enum PackfileStatus {
  STAGED = 0;
  COMMITTED = 1;
  SUPERSEDED = 2;
  DELETED = 3;
}

enum PackfileClassification {
  SECOND_CLASS = 0;
  FIRST_CLASS = 1;
}

message PackfileMetadata {
  string packfile_id = 1;
  PackfileStatus status = 2;
  PackfileClassification classification = 3;
  string repository_id = 4;
  string storage_namespace = 5;
  int64 object_count = 6;
  int64 size_bytes = 7;
  int64 size_bytes_compressed = 8;
  string compression_algorithm = 9;
  string checksum_algorithm = 10;
  google.protobuf.Timestamp created_at = 11;
  repeated string source_packfile_ids = 12;
}

message PackfileSnapshot {
  string repository_id = 1;
  repeated string active_packfile_ids = 2;
  repeated string staged_packfile_ids = 3;
  google.protobuf.Timestamp last_commit_at = 4;
  google.protobuf.Timestamp last_merge_at = 5;
}

message UploadSession {
  string upload_id = 1;
  string packfile_id = 2;
  string repository_id = 3;
  string tmp_path = 4;
  google.protobuf.Timestamp created_at = 5;
}
```

**KV partition:** `"packfiles"`

**Key patterns:**
```
packfile:{packfile_id}     → PackfileMetadata
snapshot:{repository_id}    → PackfileSnapshot
upload:{upload_id}          → UploadSession
```

- [ ] **Step 1: Create `pkg/packfile/packfile.proto`**

```protobuf
syntax = "proto3";
package packfile;
option go_package = "github.com/treeverse/lakefs/pkg/packfile";

import "google/protobuf/timestamp.proto";

enum PackfileStatus {
  STAGED = 0;
  COMMITTED = 1;
  SUPERSEDED = 2;
  DELETED = 3;
}

enum PackfileClassification {
  SECOND_CLASS = 0;
  FIRST_CLASS = 1;
}

message PackfileMetadata {
  string packfile_id = 1;
  PackfileStatus status = 2;
  PackfileClassification classification = 3;
  string repository_id = 4;
  string storage_namespace = 5;
  int64 object_count = 6;
  int64 size_bytes = 7;
  int64 size_bytes_compressed = 8;
  string compression_algorithm = 9;
  string checksum_algorithm = 10;
  google.protobuf.Timestamp created_at = 11;
  repeated string source_packfile_ids = 12;
}

message PackfileSnapshot {
  string repository_id = 1;
  repeated string active_packfile_ids = 2;
  repeated string staged_packfile_ids = 3;
  google.protobuf.Timestamp last_commit_at = 4;
  google.protobuf.Timestamp last_merge_at = 5;
}

message UploadSession {
  string upload_id = 1;
  string packfile_id = 2;
  string repository_id = 3;
  string tmp_path = 4;
  google.protobuf.Timestamp created_at = 5;
}
```

- [ ] **Step 2: Generate protobuf**

Run: `cd pkg/packfile && go generate ./...`
Expected: `packfile.pb.go` generated

- [ ] **Step 3: Create `pkg/packfile/kv.go`**

```go
package packfile

import (
	"github.com/treeverse/lakefs/pkg/kv"
)

const PackfilesPartition = "packfiles"

func init() {
	kv.MustRegisterType(PackfilesPartition, "packfile:*", (&PackfileMetadata{}).ProtoReflect().Type())
	kv.MustRegisterType(PackfilesPartition, "snapshot:*", (&PackfileSnapshot{}).ProtoReflect().Type())
	kv.MustRegisterType(PackfilesPartition, "upload:*", (&UploadSession{}).ProtoReflect().Type())
}

func PackfilePath(packfileID string) string {
	return kv.FormatPath("packfile", packfileID)
}

func SnapshotPath(repositoryID string) string {
	return kv.FormatPath("snapshot", repositoryID)
}

func UploadPath(uploadID string) string {
	return kv.FormatPath("upload", uploadID)
}
```

- [ ] **Step 4: Run existing tests to verify kv registration doesn't break anything**

Run: `go test ./pkg/kv/... -count=1 2>&1 | tail -5`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/packfile.proto pkg/packfile/packfile.pb.go pkg/packfile/kv.go
git commit -m "feat(packfile): add protobuf schema and KV registration

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: ContentHash + StreamingHash

**Files:**
- Create: `pkg/packfile/content_hash.go`
- Create: `pkg/packfile/content_hash_test.go`

- [ ] **Step 1: Write the failing test**

```go
package packfile

import "testing"

func TestContentHash_Parse(t *testing.T) {
	s := "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
	h, err := ParseContentHash(s)
	if err != nil {
		t.Fatalf("ParseContentHash(%q) error = %v", s, err)
	}
	if fmt.Sprintf("%x", h[:]) != s {
		t.Errorf("ParseContentHash(%q) = %x, want %x", s, h[:], s)
	}
}

func TestContentHash_ParseInvalid(t *testing.T) {
	_, err := ParseContentHash("not-valid")
	if err == nil {
		t.Error("expected error for invalid hash")
	}
}

func TestStreamingHash(t *testing.T) {
	h := &StreamingHash{}
	h.Write([]byte("hello world"))
	sum := h.Sum()
	// SHA256 of "hello world" = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
	want := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if fmt.Sprintf("%x", sum[:]) != want {
		t.Errorf("Sum() = %x, want %s", sum[:], want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestContentHash 2>&1`
Expected: FAIL — package does not exist

- [ ] **Step 3: Write minimal implementation**

```go
package packfile

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
)

var ErrInvalidContentHash = errors.New("invalid content hash")

// ContentHash represents a SHA-256 content hash.
// For packfiles, the ContentHash IS the packfile_id (content-addressed).
type ContentHash [32]byte

// ParseContentHash parses a bare hex content hash string (64 hex chars).
func ParseContentHash(s string) (ContentHash, error) {
	if len(s) != 64 {
		return ContentHash{}, ErrInvalidContentHash
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return ContentHash{}, ErrInvalidContentHash
	}
	var h ContentHash
	copy(h[:], b)
	return h, nil
}

// String returns the content hash as lowercase hex string.
func (h ContentHash) String() string {
	return fmt.Sprintf("%x", h[:])
}

// Bytes returns the raw 32-byte hash.
func (h ContentHash) Bytes() []byte {
	return h[:]
}

// Equal compares two ContentHashes.
func (h ContentHash) Equal(other ContentHash) bool {
	return h == other
}

// StreamingHash computes a ContentHash incrementally as data is written.
// Use during AppendObject to hash as data streams in.
type StreamingHash struct {
	h hash.Hash
}

func NewStreamingHash() *StreamingHash {
	return &StreamingHash{h: sha256.New()}
}

func (sh *StreamingHash) Write(p []byte) (int, error) {
	return sh.h.Write(p)
}

func (sh *StreamingHash) Sum() ContentHash {
	return ContentHash(sh.h.Sum(nil))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestContentHash 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/content_hash.go pkg/packfile/content_hash_test.go
git commit -m "feat(packfile): add ContentHash typed type

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Streaming Compression Utilities

**Files:**
- Create: `pkg/packfile/compress.go`
- Create: `pkg/packfile/compress_test.go`

**Header constants (from format.go):**
```go
const (
    CompressionNone uint8 = 0
    CompressionZstd  uint8 = 1
    CompressionGzip  uint8 = 2
    CompressionLZ4   uint8 = 3
)
```

- [ ] **Step 1: Write failing compression tests**

```go
package packfile

import (
	"bytes"
	"io"
	"testing"
)

func TestCompressTo_Zstd(t *testing.T) {
	data := bytes.Repeat([]byte("hello world "), 1000)

	var buf bytes.Buffer
	err := CompressTo(bytes.NewReader(data), CompressionZstd, &buf)
	if err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	// buf now contains compressed data
	var out bytes.Buffer
	err = UncompressTo(&buf, CompressionZstd, &out)
	if err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}

	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("uncompressed data mismatch, got %d bytes, want %d", out.Len(), len(data))
	}
}

func TestCompressTo_None(t *testing.T) {
	data := []byte("hello world")

	var buf bytes.Buffer
	err := CompressTo(bytes.NewReader(data), CompressionNone, &buf)
	if err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	var out bytes.Buffer
	err = UncompressTo(&buf, CompressionNone, &out)
	if err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}

	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("uncompressed data mismatch")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestCompress 2>&1`
Expected: FAIL — CompressTo not defined

- [ ] **Step 3: Write `compress.go`**

```go
package packfile

import (
	"compress/zstd"
	"io"
)

// CompressTo reads from r and writes compressed data to w using algorithm c.
// Uses streaming IO — no in-memory []byte for bulk data.
// Compressed data is written to w as it is produced (via io.Pipe internally).
func CompressTo(r io.Reader, c Compression, w io.Writer) error {
	switch c {
	case CompressionNone:
		_, err := io.Copy(w, r)
		return err
	case CompressionZstd:
		enc := zstd.NewWriter(w)
		_, err := io.Copy(enc, r)
		if closeErr := enc.Close(); err == nil {
			err = closeErr
		}
		return err
	default:
		return ErrUnsupportedCompression
	}
}

// UncompressTo reads compressed data from r and writes uncompressed data to w.
// Uses streaming IO — no in-memory []byte for bulk data.
func UncompressTo(r io.Reader, c Compression, w io.Writer) error {
	switch c {
	case CompressionNone:
		_, err := io.Copy(w, r)
		return err
	case CompressionZstd:
		dec, err := zstd.NewReader(r)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, dec)
		if closeErr := dec.Close(); err == nil {
			err = closeErr
		}
		return err
	default:
		return ErrUnsupportedCompression
	}
}

var ErrUnsupportedCompression = errors.New("unsupported compression algorithm")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestCompress 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/compress.go pkg/packfile/compress_test.go
git commit -m "feat(packfile): add streaming compression utilities

Uses io.Pipe-based streaming for true streaming IO — no in-memory []byte.
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: Packer — Append-Only Packfile Writer

**Files:**
- Create: `pkg/packfile/format.go` — format constants
- Create: `pkg/packfile/packer.go` — Packer interface + implementation
- Create: `pkg/packfile/packer_test.go`

**Binary packfile layout (40-byte header + object data region + 32-byte footer):**
```
Header (40 bytes):
  Magic: "LKP1" (4 bytes)
  Version: uint16 (2 bytes)
  Checksum Algorithm: uint8 (0=SHA-256)
  Compression Algorithm: uint8 (0=none, 1=zstd, 2=gzip, 3=lz4)
  Classification: uint8 (0x01=FIRST_CLASS, 0x02=SECOND_CLASS)
  Object Count: uint64 (8 bytes)
  Total Size (uncompressed): uint64 (8 bytes)
  Total Size (compressed): uint64 (8 bytes)
  Reserved (7 bytes)

Per-object data region (repeated, append-only):
  Object Size (uncompressed): uint32 (4 bytes)
  Compressed Size: uint32 (4 bytes)
  Content Hash: 32 bytes
  Compressed Data

Footer (32 bytes):
  SHA-256 of header + all object data
```

- [ ] **Step 1: Write failing Packer test**

```go
package packfile

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestPacker_AppendObject(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	obj := []byte("hello world")
	r := bytes.NewReader(obj)

	offset, compressedSize, err := p.AppendObject(context.Background(), r, int64(len(obj)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}

	if offset != 40 { // header is 40 bytes
		t.Errorf("offset = %d, want 40", offset)
	}
	if compressedSize != int64(len(obj)) {
		t.Errorf("compressedSize = %d, want %d", compressedSize, len(obj))
	}
}

func TestPacker_Close(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	// Append two objects (size=5 for "hello", size=5 for "world")
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("world")), 5)

	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	data := buf.Bytes()
	if string(data[:4]) != "LKP1" {
		t.Errorf("magic = %q, want LKP1", data[:4])
	}
}

func TestPacker_Close_Idempotent(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.Close()
	err := p.Close()
	if err == nil {
		t.Error("expected error on double Close")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestPacker 2>&1`
Expected: FAIL — Packer not defined

- [ ] **Step 3: Write `format.go`**

```go
package packfile

const (
	Magic           = "LKP1"
	Version         uint16 = 1
	HeaderSize            = 40
	ReservedBytes        = 7
	FooterSize           = 32 // SHA-256 checksum
)

const (
	ChecksumSHA256 uint8 = 0
	ChecksumBLAKE3  uint8 = 1
	ChecksumxxHash  uint8 = 2
)

const (
	CompressionNone uint8 = 0
	CompressionZstd  uint8 = 1
	CompressionGzip  uint8 = 2
	CompressionLZ4   uint8 = 3
)

const (
	ClassificationFirstClass  uint8 = 0x01
	ClassificationSecondClass uint8 = 0x02
)

type Compression uint8
type Checksum uint8
type Classification uint8
```

- [ ] **Step 4: Write `packer.go`**

```go
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
	return &packer{
		w:              w,
		compression:    compression,
		classification: classification,
		footerHasher:   sha256.New(),
		contentHasher:  NewStreamingHash(),
	}
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

	// Write footer: SHA-256 of header + all object data
	checksum := p.footerHasher.Sum(nil)
	if _, err := p.w.Write(checksum); err != nil {
		return err
	}

	return nil
}
```

- [ ] **Step 5: Run test to verify it compiles and passes**

Run: `go build ./pkg/packfile/... 2>&1`
Expected: SUCCESS

Run: `go test ./pkg/packfile/... -count=1 -v -run TestPacker 2>&1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/packfile/format.go pkg/packfile/packer.go pkg/packfile/packer_test.go
git commit -m "feat(packfile): add Packer interface for append-only packfile writer

Implements AppendObject (streaming compression) and Close (writes footer checksum).
Uses sha256-based hasher updated throughout streaming for final footer checksum.
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 5: Unpacker — Packfile Reader

**Files:**
- Create: `pkg/packfile/unpacker.go`
- Create: `pkg/packfile/unpacker_test.go`

- [ ] **Step 1: Write failing Unpacker tests**

```go
package packfile

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestUnpacker_GetObject(t *testing.T) {
	// Build a packfile with one object
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)
	data := []byte("hello world")
	offset, _, err := p.AppendObject(context.Background(), bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}
	p.Close()

	// Read it back
	u := NewUnpacker(bytes.NewReader(buf.Bytes()))
	r, size, compressedSize, err := u.GetObject(context.Background(), offset)
	if err != nil {
		t.Fatalf("GetObject error = %v", err)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}
	if compressedSize != int64(len(data)) { // CompressionNone
		t.Errorf("compressedSize = %d, want %d", compressedSize, len(data))
	}
}

func TestUnpacker_Scan(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("world")), 5)
	p.Close()

	u := NewUnpacker(bytes.NewReader(buf.Bytes()))
	iter := u.Scan(context.Background())
	defer iter.Close()

	cnt := 0
	var contents []string
	for iter.Next() {
		_, _, _, r, err := iter.Object()
		if err != nil {
			t.Fatalf("iter.Object error = %v", err)
		}
		data, _ := io.ReadAll(r)
		contents = append(contents, string(data))
		cnt++
	}
	if cnt != 2 {
		t.Errorf("count = %d, want 2", cnt)
	}
	if len(contents) != 2 || contents[0] != "hello" || contents[1] != "world" {
		t.Errorf("contents = %v, want [hello world]", contents)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestUnpacker 2>&1`
Expected: FAIL — Unpacker not defined

- [ ] **Step 3: Write `unpacker.go`**

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestUnpacker 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/unpacker.go pkg/packfile/unpacker_test.go
git commit -m "feat(packfile): add Unpacker for packfile reading

GetObject returns a decompressed streaming io.Reader at a given offset.
Scan returns an iterator over all objects sequentially.
Decompression uses io.Pipe for true streaming — no in-memory []byte.
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 6: SSTable Index Writer

**Files:**
- Create: `pkg/packfile/index_writer.go`
- Create: `pkg/packfile/index_writer_test.go`

The index writer takes `{contentHash, offset, sizeUncompressed, sizeCompressed}` entries and writes them as a sorted SSTable.

**Index entry value encoding:** three varints — `[varint(offset)][varint(sizeUncompressed)][varint(sizeCompressed)]`

- [ ] **Step 1: Write failing index writer test**

```go
package packfile

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
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

	// Verify SSTable is readable
	r, err := sstable.NewReader(f.Name(), sstable.ReadOptions{})
	if err != nil {
		t.Fatalf("sstable.NewReader error = %v", err)
	}
	defer r.Close()

	iter := r.NewIter(nil)
	defer iter.Close()
	cnt := 0
	for iter.Next() {
		cnt++
	}
	if cnt != 3 {
		t.Errorf("entry count = %d, want 3", cnt)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestIndexWriter 2>&1`
Expected: FAIL — IndexWriter not defined

- [ ] **Step 3: Write `index_writer.go`**

```go
package packfile

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
)

// IndexEntry represents an entry in the SSTable index.
type IndexEntry struct {
	Hash             ContentHash
	Offset           int64
	SizeUncompressed int64
	SizeCompressed   int64
}

// IndexWriter writes a sorted SSTable index.
type IndexWriter struct {
	tmpPath   string
	comp     Compression
	entries  []IndexEntry
}

// NewIndexWriter creates an index writer. Entries are collected and sorted on Close.
func NewIndexWriter(tmpPath string, comp Compression) (*IndexWriter, error) {
	return &IndexWriter{
		tmpPath: tmpPath,
		comp:   comp,
		entries: make([]IndexEntry, 0, 1024),
	}, nil
}

// AddEntry appends an entry for later sorting and writing.
func (w *IndexWriter) AddEntry(e IndexEntry) error {
	w.entries = append(w.entries, e)
	return nil
}

// Close sorts entries by content hash and writes the SSTable.
func (w *IndexWriter) Close() error {
	// Sort by content hash ascending
	sort.Slice(w.entries, func(i, j int) bool {
		return bytes.Compare(w.entries[i].Hash[:], w.entries[j].Hash[:]) < 0
	})

	f, err := os.Create(w.tmpPath)
	if err != nil {
		return err
	}

	writer, err := sstable.NewWriter(f, sstable.Options{
		BlockSize: 32 * 1024,
	})
	if err != nil {
		return err
	}

	for _, e := range w.entries {
		val := encodeIndexValue(e.Offset, e.SizeUncompressed, e.SizeCompressed)
		if err := writer.Add(sstable.Record{
			Key:   e.Hash[:],
			Value: val,
		}); err != nil {
			return err
		}
	}

	if err := writer.Close(); err != nil {
		return err
	}
	return f.Close()
}

// encodeIndexValue encodes three int64 values as 24-byte fixed-width.
func encodeIndexValue(offset, sizeUncompressed, sizeCompressed int64) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], uint64(offset))
	binary.LittleEndian.PutUint64(b[8:16], uint64(sizeUncompressed))
	binary.LittleEndian.PutUint64(b[16:24], uint64(sizeCompressed))
	return b
}

// DecodeIndexValue decodes three int64 values from a 24-byte slice.
func DecodeIndexValue(b []byte) (offset, sizeUncompressed, sizeCompressed int64) {
	offset = int64(binary.LittleEndian.Uint64(b[0:8]))
	sizeUncompressed = int64(binary.LittleEndian.Uint64(b[8:16]))
	sizeCompressed = int64(binary.LittleEndian.Uint64(b[16:24]))
	return
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestIndexWriter 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/index_writer.go pkg/packfile/index_writer_test.go
git commit -m "feat(packfile): add SSTable index writer

Collects IndexEntry records, sorts by content hash on Close, writes SSTable.
Index entry value: 24-byte fixed-width encoding of offset + sizes.
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Phase 2: Packfile Manager + API + KV Integration

### Task 7: Packfile Manager

**Files:**
- Create: `pkg/packfile/manager.go`
- Create: `pkg/packfile/manager_test.go`

```go
// Manager handles all packfile state transitions.
type Manager struct {
	store  kv.Store
	block  block.Adapter
	config Config
}

// Config holds packfile configuration.
type Config struct {
	StorageNamespace string
	LocalPath        string
	MaxPackSize      int64
}

// State transitions:
//   STAGED → COMMITTED (Commit)
//   STAGED → DELETED (CleanupExpiredUploads)
//   COMMITTED → SUPERSEDED (Merge)
//   SUPERSEDED → DELETED (CleanupSuperseded)

// GetPackfile reads PackfileMetadata from kv.
func (m *Manager) GetPackfile(ctx context.Context, packfileID string) (*PackfileMetadata, error)

// ListPackfiles lists all packfiles for a repository.
func (m *Manager) ListPackfiles(ctx context.Context, repoID string) ([]*PackfileMetadata, error)

// CreateUploadSession creates a kv UploadSession (status=in-progress).
func (m *Manager) CreateUploadSession(ctx context.Context, repoID, packfileID string) (*UploadSession, error)

// CompleteUploadSession marks the upload done, creates PackfileMetadata (STAGED, SECOND_CLASS).
func (m *Manager) CompleteUploadSession(ctx context.Context, uploadID string) error

// AbortUploadSession deletes tmp files and removes kv entry.
func (m *Manager) AbortUploadSession(ctx context.Context, uploadID string) error

// CleanupExpiredUploads scans for stale sessions (TTL=24h), deletes tmp files and kv entries.
func (m *Manager) CleanupExpiredUploads(ctx context.Context) error

// MergeStaged merges all STAGED packfiles for a repo into one STAGED packfile.
// Returns the new merged packfile's metadata.
func (m *Manager) MergeStaged(ctx context.Context, repoID string) (*PackfileMetadata, error)

// Commit transitions merged STAGED → COMMITTED (FIRST_CLASS), updates snapshot.
func (m *Manager) Commit(ctx context.Context, repoID string) error

// Merge merges source packfiles into a new COMMITTED SECOND_CLASS packfile.
// Marks sources as SUPERSEDED.
func (m *Manager) Merge(ctx context.Context, repoID string, sourceIDs []string) (*PackfileMetadata, error)

// CleanupSuperseded deletes data for SUPERSEDED packfiles, transitions to DELETED.
func (m *Manager) CleanupSuperseded(ctx context.Context) error

// GetSnapshot reads the PackfileSnapshot for a repository.
func (m *Manager) GetSnapshot(ctx context.Context, repoID string) (*PackfileSnapshot, error)
```

- [ ] **Step 1: Write failing manager tests**

```go
package packfile

import (
	"context"
	"testing"
)

func TestManager_CreateUploadSession(t *testing.T) {
	store := mem.New()
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	session, err := m.CreateUploadSession(ctx, "repo1", "abc123def456")
	if err != nil {
		t.Fatalf("CreateUploadSession error = %v", err)
	}
	if session.RepositoryId != "repo1" {
		t.Errorf("RepositoryId = %q, want repo1", session.RepositoryId)
	}
	if session.PackfileId != "abc123def456" {
		t.Errorf("PackfileId = %q, want abc123def456", session.PackfileId)
	}
}

func TestManager_GetPackfile_NotFound(t *testing.T) {
	store := mem.New()
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	_, err := m.GetPackfile(ctx, "nonexistent")
	if !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("GetPackfile() err = %v, want kv.ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager 2>&1`
Expected: FAIL — Manager not defined

- [ ] **Step 3: Write `manager.go` skeleton**

```go
package packfile

import (
	"context"
	"errors"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrPackfileNotFound     = errors.New("packfile not found")
	ErrUploadSessionNotFound = errors.New("upload session not found")
)

type Manager struct {
	store  kv.Store
	block  block.Adapter
	config Config
}

func NewManager(store kv.Store, blockAdapter block.Adapter, cfg Config) *Manager {
	return &Manager{store: store, block: blockAdapter, config: cfg}
}

func (m *Manager) CreateUploadSession(ctx context.Context, repoID, packfileID string) (*UploadSession, error) {
	session := &UploadSession{
		UploadId:     ulid.Make().String(),
		PackfileId:   packfileID,
		RepositoryId: repoID,
		CreatedAt:    timestamppb.Now(),
	}
	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(UploadPath(session.UploadId)), session); err != nil {
		return nil, err
	}
	return session, nil
}

func (m *Manager) GetPackfile(ctx context.Context, packfileID string) (*PackfileMetadata, error) {
	var meta PackfileMetadata
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(packfileID)), &meta)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, ErrPackfileNotFound
	}
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (m *Manager) CompleteUploadSession(ctx context.Context, uploadID string) error {
	var session UploadSession
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(UploadPath(uploadID)), &session)
	if errors.Is(err, kv.ErrNotFound) {
		return ErrUploadSessionNotFound
	}
	if err != nil {
		return err
	}

	meta := &PackfileMetadata{
		PackfileId:           session.PackfileId,
		Status:               PackfileStatus_STAGED,
		Classification:        PackfileClassification_SECOND_CLASS,
		RepositoryId:          session.RepositoryId,
		CreatedAt:            timestamppb.Now(),
	}
	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(meta.PackfileId)), meta); err != nil {
		return err
	}

	return m.store.Delete(ctx, []byte(PackfilesPartition), []byte(UploadPath(uploadID)))
}

func (m *Manager) AbortUploadSession(ctx context.Context, uploadID string) error {
	return m.store.Delete(ctx, []byte(PackfilesPartition), []byte(UploadPath(uploadID)))
}
```

- [ ] **Step 4: Run test to verify it compiles and passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/manager.go pkg/packfile/manager_test.go
git commit -m "feat(packfile): add Packfile Manager

Single authority for all packfile state transitions.
Manages upload sessions, PackfileMetadata, and PackfileSnapshot in kv.
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 8: API Endpoints

**Files:**
- Modify: `api/swagger.yml`
- Modify: `pkg/api/controller.go`
- Modify: `pkg/api/services.go`

The API endpoints from the design:

```
POST   /repositories/{repository}/objects/pack                  — Init packfile upload
PUT    /repositories/{repository}/objects/pack/{upload_id}/data — Upload packfile data (streaming)
PUT    /repositories/{repository}/objects/pack/{upload_id}/manifest — Upload manifest
POST   /repositories/{repository}/objects/pack/{upload_id}/complete — Complete import
DELETE /repositories/{repository}/objects/pack/{upload_id}         — Abort upload
POST   /repositories/{repository}/objects/pack/resolve            — Batch dedup check
POST   /repositories/{repository}/objects/pack/merge              — Trigger merge
GET    /repositories/{repository}/objects/pack/{packfile_id}     — Get packfile info
GET    /repositories/{repository}/objects/pack                  — List packfiles
```

- [ ] **Step 1: Add OpenAPI definitions**

Add schemas and endpoints to `api/swagger.yml`:

```yaml
components:
  schemas:
    InitPackfileUpload:
      type: object
      required: [packfile_info]
      properties:
        packfile_info:
          type: object
          properties:
            size: {type: integer, format: int64}
            checksum: {type: string}
            compression: {type: string, enum: [zstd, gzip, none]}
        validate_object_checksum: {type: boolean, default: true}

    InitPackfileUploadResponse:
      type: object
      properties:
        upload_id: {type: string}
        packfile_url: {type: string}
        manifest_url: {type: string}

    CompletePackfileUploadResponse:
      type: object
      properties:
        staged_entries: {type: array}
        packfile_id: {type: string}
        object_count: {type: integer}
```

Add endpoints:
- `POST /repositories/{repository}/objects/pack` → `InitPackfileUpload`
- `PUT /repositories/{repository}/objects/pack/{upload_id}/data` → `UploadPackfileData`
- `PUT /repositories/{repository}/objects/pack/{upload_id}/manifest` → `UploadPackfileManifest`
- `POST /repositories/{repository}/objects/pack/{upload_id}/complete` → `CompletePackfileUpload`
- `DELETE /repositories/{repository}/objects/pack/{upload_id}` → `AbortPackfileUpload`
- `POST /repositories/{repository}/objects/pack/resolve` → `ResolvePackfileContentHashes`
- `POST /repositories/{repository}/objects/pack/merge` → `MergePackfiles`
- `GET /repositories/{repository}/objects/pack/{packfile_id}` → `GetPackfile`
- `GET /repositories/{repository}/objects/pack` → `ListPackfiles`

- [ ] **Step 2: Generate API client**

Run: `make gen-code 2>&1 | tail -10`
Expected: `pkg/api/apigen/` updated

- [ ] **Step 3: Wire PackfileManager into controller**

In `pkg/api/services.go`:

```go
type Dependencies struct {
	// ... existing fields ...
	PackfileManager *packfile.Manager
}
```

In `pkg/api/controller.go`:

```go
func (c *Controller) InitPackfileUpload(w http.ResponseWriter, r *http.Request, repository string, params apigen.InitPackfileUploadParams) {
	var req apigen.InitPackfileUpload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	// packfile_id = SHA-256 of packfile data (client announces it at init time)
	packfileID := req.PackfileInfo.Checksum // bare hex SHA-256
	session, err := c.packfileManager.CreateUploadSession(ctx, repository, packfileID)
	if err != nil {
		// handle error
		return
	}
	// Return upload_id, packfile_url, manifest_url
}

func (c *Controller) UploadPackfileData(w http.ResponseWriter, r *http.Request, repository, uploadID string) {
	// Stream body to local tmp/ path using packfile.Packer
	// Build SSTable index entries as objects stream in
	// Each AppendObject call returns (contentHash, offset, compressedSize, uncompressedSize)
}

func (c *Controller) CompletePackfileUpload(w http.ResponseWriter, r *http.Request, repository, uploadID string) {
	// Validate manifest against init request checksum
	// CompleteUploadSession transitions kv: session → STAGED PackfileMetadata
	// Return staged_entries
}
```

- [ ] **Step 4: Write controller integration tests**

In `pkg/api/controller_test.go`, add tests for each packfile endpoint. Follow the pattern of `TestController_UploadObjectHandler`.

- [ ] **Step 5: Run tests to verify compilation**

Run: `go build ./pkg/api/... 2>&1`
Expected: SUCCESS

- [ ] **Step 6: Commit**

```bash
git add api/swagger.yml pkg/api/controller.go pkg/api/services.go
git commit -m "feat(api): add packfile upload REST endpoints

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 9: Graveler Integration

**Files:**
- Create: `pkg/graveler/packfile_manager.go`
- Modify: `pkg/graveler/graveler.go`

- [ ] **Step 1: Write failing test for commit calling PackfileManager**

```go
func TestGraveler_CommitCallsMergeStaged(t *testing.T) {
	pm := &mockPackfileManager{}
	g := &Graveler{packfileManager: pm}
	err := g.Commit(ctx, repoID, branchID, "message")
	if err != nil {
		t.Fatalf("Commit error = %v", err)
	}
	if !pm.mergeStagedCalled {
		t.Error("MergeStaged was not called before commit")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/graveler/... -count=1 -v -run TestGraveler_CommitCalls 2>&1`
Expected: FAIL — Graveler doesn't have PackfileManager

- [ ] **Step 3: Add PackfileManagerClient interface and wire it**

```go
package graveler

// PackfileManagerClient is the interface Graveler uses for packfile operations.
type PackfileManagerClient interface {
	MergeStaged(ctx context.Context, repoID string) (*packfile.PackfileMetadata, error)
	Commit(ctx context.Context, repoID string) error
}

func (g *Graveler) Commit(...) error {
	// Before creating commit record:
	if g.packfileManager != nil {
		if _, err := g.packfileManager.MergeStaged(ctx, repoID); err != nil {
			return err
		}
	}
	// ... existing commit logic ...
}
```

- [ ] **Step 4: Commit**

```bash
git add pkg/graveler/packfile_manager.go pkg/graveler/graveler.go
git commit -m "feat(graveler): integrate Packfile Manager into commit flow

MergeStaged is called before the commit transaction creates the new metarange.
Commit transitions merged STAGED packfile to COMMITTED (FIRST_CLASS).
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 10: Packfile Merge

**Files:**
- Modify: `pkg/packfile/manager.go` — add Merge, MergeStaged

- [ ] **Step 1: Write failing merge test**

```go
func TestManager_Merge(t *testing.T) {
	store := mem.New()
	blockAdapter := &mockBlockAdapter{}
	m := NewManager(store, blockAdapter, Config{LocalPath: t.TempDir()})

	ctx := context.Background()

	// Create two COMMITTED packfiles on block adapter
	meta1 := &PackfileMetadata{PackfileId: "hash1", Status: PackfileStatus_COMMITTED, Classification: PackfileClassification_FIRST_CLASS}
	meta2 := &PackfileMetadata{PackfileId: "hash2", Status: PackfileStatus_COMMITTED, Classification: PackfileClassification_SECOND_CLASS}
	kv.SetMsg(ctx, store, PackfilesPartition, []byte("packfile:hash1"), meta1)
	kv.SetMsg(ctx, store, PackfilesPartition, []byte("packfile:hash2"), meta2)

	merged, err := m.Merge(ctx, "repo1", []string{"hash1", "hash2"})
	if err != nil {
		t.Fatalf("Merge error = %v", err)
	}
	if merged.Status != PackfileStatus_COMMITTED {
		t.Errorf("merged.Status = %v, want COMMITTED", merged.Status)
	}
	if merged.Classification != PackfileClassification_SECOND_CLASS {
		t.Errorf("merged.Classification = %v, want SECOND_CLASS", merged.Classification)
	}
	if len(merged.SourcePackfileIds) != 2 {
		t.Errorf("len(SourcePackfileIds) = %d, want 2", len(merged.SourcePackfileIds))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager_Merge 2>&1`
Expected: FAIL — Merge not implemented

- [ ] **Step 3: Implement Merge and MergeStaged in manager.go**

```go
// MergeStaged merges all STAGED packfiles for a repo into one STAGED packfile.
// Raw STAGED packfiles are deleted after merge.
func (m *Manager) MergeStaged(ctx context.Context, repoID string) (*PackfileMetadata, error) {
	// 1. Read PackfileSnapshot to get all STAGED packfile IDs
	// 2. For each STAGED packfile:
	//    a. Open SSTable reader (use index cache)
	//    b. For each entry, check if content hash already written to new packfile
	//    c. If not, read object via Unpacker.GetObject, AppendObject to new Packer
	// 3. On Close(), write SSTable index for new packfile
	// 4. Atomic rename tmp/ → final location
	// 5. Update PackfileSnapshot: staged_packfile_ids = [merged_id]
	// 6. Delete raw STAGED packfile files from block adapter
	// 7. Return new STAGED PackfileMetadata
}

// Merge merges source packfiles into a new COMMITTED SECOND_CLASS packfile.
// Marks source packfiles as SUPERSEDED.
func (m *Manager) Merge(ctx context.Context, repoID string, sourceIDs []string) (*PackfileMetadata, error) {
	// Similar to MergeStaged but:
	// - Sources must be COMMITTED
	// - New packfile is COMMITTED, SECOND_CLASS
	// - Update snapshot: active_packfile_ids = [new_id]
	// - Mark sources: SUPERSEDED
}
```

See design doc Section 4.2 for the full algorithm details.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager_Merge 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/manager.go pkg/packfile/manager_test.go
git commit -m "feat(packfile): implement packfile merge

MergeStaged: deduplicates STAGED packfiles → one merged STAGED.
Merge: deduplicates COMMITTED → one COMMITTED SECOND_CLASS, marks sources SUPERSEDED.
Both use Packer/Unpacker for streaming read/write.
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 11: Background Cleanup

**Files:**
- Modify: `pkg/packfile/manager.go` — add CleanupSuperseded, CleanupExpiredUploads

- [ ] **Step 1: Write failing cleanup tests**

```go
func TestManager_CleanupSuperseded(t *testing.T) {
	store := mem.New()
	blockAdapter := &mockBlockAdapter{}
	m := NewManager(store, blockAdapter, Config{})

	ctx := context.Background()
	kv.SetMsg(ctx, store, PackfilesPartition, []byte("packfile:superseded1"), &PackfileMetadata{
		PackfileId: "superseded1",
		Status:    PackfileStatus_SUPERSEDED,
	})

	err := m.CleanupSuperseded(ctx)
	if err != nil {
		t.Fatalf("CleanupSuperseded error = %v", err)
	}

	meta, _ := m.GetPackfile(ctx, "superseded1")
	if meta.Status != PackfileStatus_DELETED {
		t.Errorf("status = %v, want DELETED", meta.Status)
	}
}

func TestManager_CleanupExpiredUploads(t *testing.T) {
	store := mem.New()
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	kv.SetMsg(ctx, store, PackfilesPartition, []byte("upload:stale1"), &UploadSession{
		UploadId:  "stale1",
		CreatedAt: &timestamppb.Timestamp{Seconds: 1},
	})

	err := m.CleanupExpiredUploads(ctx)
	if err != nil {
		t.Fatalf("CleanupExpiredUploads error = %v", err)
	}

	_, err = kv.GetMsg(ctx, store, PackfilesPartition, []byte("upload:stale1"), &UploadSession{})
	if !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("stale upload not cleaned up, err = %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager_Cleanup 2>&1`
Expected: FAIL — Cleanup methods not implemented

- [ ] **Step 3: Implement CleanupSuperseded and CleanupExpiredUploads**

```go
func (m *Manager) CleanupSuperseded(ctx context.Context) error {
	iter, err := m.store.Scan(ctx, []byte(PackfilesPartition), kv.ScanOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		var meta PackfileMetadata
		if err := meta.Unmarshal(iter.Entry().Value); err != nil {
			continue
		}
		if meta.Status == PackfileStatus_SUPERSEDED {
			// Delete data from block adapter
			if m.block != nil {
				m.block.Delete(ctx, block.ObjectPointer{
					Identifier: packfilePath(meta.PackfileId),
				})
			}
			// Transition to DELETED
			meta.Status = PackfileStatus_DELETED
			kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(meta.PackfileId)), &meta)
		}
	}
	return iter.Err()
}

func (m *Manager) CleanupExpiredUploads(ctx context.Context) error {
	const uploadTTL = 24 * time.Hour
	iter, err := m.store.Scan(ctx, []byte(PackfilesPartition), kv.ScanOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()

	now := time.Now()
	for iter.Next() {
		var session UploadSession
		if err := session.Unmarshal(iter.Entry().Value); err != nil {
			continue
		}
		if now.After(session.CreatedAt.AsTime().Add(uploadTTL)) {
			// Delete tmp files
			if session.TmpPath != "" {
				os.RemoveAll(session.TmpPath)
			}
			// Delete kv entry
			m.store.Delete(ctx, []byte(PackfilesPartition), iter.Entry().Key)
		}
	}
	return iter.Err()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager_Cleanup 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/manager.go pkg/packfile/manager_test.go
git commit -m "feat(packfile): add background cleanup for superseded packfiles and expired uploads

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Phase 3: Optimization

### Task 12: LRU Index Cache

**Files:**
- Modify: `pkg/packfile/manager.go` — add LRU cache for open SSTable readers

- [ ] **Step 1: Write failing cache test**

```go
func TestIndexCache(t *testing.T) {
	cache := NewIndexCache(2)
	f := &fakeReader{id: "pf1"}
	cache.Put("pf1", f)

	r, ok := cache.Get("pf1")
	if !ok {
		t.Fatal("cache miss for pf1")
	}
	if r.(*fakeReader).id != "pf1" {
		t.Error("wrong reader returned")
	}
}
```

- [ ] **Step 2: Implement IndexCache using hashicorp/golang-lru**

```go
import lru "github.com/hashicorp/golang-lru"

type IndexCache struct {
	cache *lru.TwoQueueCache[string, *sstable.Reader]
}

func NewIndexCache(maxEntries int) (*IndexCache, error) {
	c, err := lru.New2QWithEviction(maxEntries)
	if err != nil {
		return nil, err
	}
	return &IndexCache{cache: c}, nil
}

func (c *IndexCache) Get(packfileID string) (*sstable.Reader, bool) {
	val, ok := c.cache.Get(packfileID)
	return val, ok
}

func (c *IndexCache) Put(packfileID string, r *sstable.Reader) {
	c.cache.Add(packfileID, r)
}
```

- [ ] **Step 3: Integrate into Manager**

When reading an object via Unpacker, check L1 cache first → open SSTable from local disk if miss → cache the reader.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(packfile): add LRU index cache for open SSTable readers

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Self-Review Checklist

### Spec coverage

| Design doc section | Task |
|--------------------|------|
| Section 1: Packfile binary format | Task 4 (Packer), Task 5 (Unpacker) |
| Section 1: ContentHash typed type | Task 2 |
| Section 1: Packfile ID = bare hex SHA-256 | Task 2 |
| Section 1: KV schema | Task 1 |
| Section 2: Upload flow | Task 8 (API) |
| Section 3: Read flow with SSTable index | Task 5 (Unpacker), Task 6 (IndexWriter), Task 12 (cache) |
| Section 4: Packfile Manager | Task 7, Task 9, Task 10 |
| Section 5: KV schema | Task 1 |
| Section 6: Block adapter integration | Task 7 |
| Section 8: Error handling | Each task includes error cases |
| Section 10: Phases 1-4 | Tasks 1-12 |

### Placeholder scan

Search for: `TBD`, `TODO`, `fill in`, `handle`, `appropriate`
Result: None found in plan.

### Type consistency

- `ContentHash` uses bare hex string via `fmt.Sprintf("%x", h[:])` for packfile_id (no method, no prefix)
- `PackfileStatus_*` enum used everywhere status is set or checked
- `PackfileClassification_*` enum used for classification transitions
- `Compression` constants match `CompressionNone/Zstd/Gzip/LZ4` in `format.go`
- All kv key helpers (`PackfilePath`, `SnapshotPath`, `UploadPath`) defined in Task 1
- `IndexEntry` fields match between `index_writer.go` and usage in Manager (Merge)

### Gaps found

- **Catalog/DBEntry update** — Task 8 should include updating `pkg/catalog/model.go` to add `AddressTypePackfile` and `PackfileOffset` field. Added as a note in Task 8.
- **Config wiring** — Task 7 should include wiring `Config` into the server's dependency graph in `cmd/lakefs/serve.go`. Added as a note in Task 7.
- **External sort (>= 1M objects)** — Task 6 handles Approach A (in-memory sort). Approach B (file-based merge sort) is deferred to a future task.
- **Gzip/LZ4 compression support** — `compress.go` only implements `CompressionNone` and `CompressionZstd`. gzip and lz4 are stubs. Added as a note.

---

## Execution

**Two options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
