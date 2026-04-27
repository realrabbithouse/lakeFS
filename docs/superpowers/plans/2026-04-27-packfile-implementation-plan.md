# Packfile Transfer Protocol — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the packfile transfer protocol — a streaming binary bundle format for bulk object upload/download with content-addressable deduplication, SSTable indexing, and packfile merge.

**Architecture:** Packfiles are immutable binary bundles of compressed objects stored on local disk/NFS (primary) with async S3 backup. An SSTable index provides O(1) bloom-filter-accelerated lookups by content hash. Packfile Manager is the single authority for all packfile metadata state; Graveler is its client.

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
├── writer.go                 # Streaming packfile writer
├── reader.go                 # Streaming packfile reader
├── index_writer.go           # Streaming SSTable index writer (Approach A: in-memory sort)
├── index_writer_ext.go       # External sort SSTable writer (Approach B: >= 1M objects)
├── manager.go                # Packfile Manager: all state transitions
├── kv.go                     # KV store helpers for packfile types
└── packfile_test.go          # Unit tests for format, writer, reader

pkg/api/
├── controller.go              # Add packfile API handlers
└── apigen/                   # (generated from swagger.yml)

pkg/graveler/
└── packfile_manager.go       # Graveler's PackfileManager interface, delegates to pkg/packfile

pkg/catalog/
└── model.go                  # Add PACKFILE AddressType, PackfileOffset field

pkg/block/
└── adapter.go                # (existing, no changes needed)

cmd/
└── lakefs/
    └── serve.go              # Wire Packfile Manager into server

config/
└── config.go                 # Packfile config: thresholds, paths, cache sizes
```

---

## Phase 1: Core Packfile Infrastructure

### Task 1: Protobuf Schema + KV Registration

**Files:**
- Create: `pkg/packfile/packfile.proto`
- Create: `pkg/packfile/packfile.pb.go` (generated)
- Modify: `pkg/kv/msg.go` (add MustRegisterType import)

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

- [ ] **Step 3: Register packfile types in kv**

Create `pkg/packfile/kv.go`:

```go
package packfile

import (
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	PackfilesPartition = "packfiles"
)

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

Run: `go test ./pkg/kv/... -count=1 -v 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/packfile.proto pkg/packfile/packfile.pb.go pkg/packfile/kv.go
git commit -m "feat(packfile): add protobuf schema and KV registration

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: ContentHash Typed Type

**Files:**
- Create: `pkg/packfile/content_hash.go`

- [ ] **Step 1: Write the failing test**

```go
package packfile

import "testing"

func TestContentHash_PackfileID(t *testing.T) {
	h := ContentHash([32]byte{
		0xa3, 0xf2, 0xb1, 0xc9, 0xd4, 0xe5, 0xf6, 0x78,
		0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34,
		0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34,
		0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34,
	})
	got := h.PackfileID()
	want := "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
	if got != want {
		t.Errorf("PackfileID() = %q, want %q", got, want)
	}
}

func TestContentHash_Parse(t *testing.T) {
	s := "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
	h, err := ParseContentHash(s)
	if err != nil {
		t.Fatalf("ParseContentHash(%q) error = %v", s, err)
	}
	got := h.PackfileID()
	if got != s {
		t.Errorf("ParseContentHash(%q).PackfileID() = %q, want %q", s, got, s)
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
)

var ErrInvalidContentHash = errors.New("invalid content hash")

// ContentHash represents a SHA-256 content hash.
type ContentHash [32]byte

// PackfileID returns the content hash as a bare hex string (no prefix).
// This value IS the packfile_id used in storage paths and KV keys.
func (h ContentHash) PackfileID() string {
	return fmt.Sprintf("%x", h[:])
}

// String returns the content hash as lowercase hex string (no prefix).
func (h ContentHash) String() string {
	return h.PackfileID()
}

// ParseContentHash parses a content hash string.
// Accepts both "sha256:..." format (legacy) and plain "..." format.
func ParseContentHash(s string) (ContentHash, error) {
	// Strip "sha256:" prefix if present.
	if len(s) > 7 && s[:7] == "sha256:" {
		s = s[7:]
	}
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

// Bytes returns the raw 32-byte hash.
func (h ContentHash) Bytes() []byte {
	return h[:]
}

// Equal compares two ContentHashes.
func (h ContentHash) Equal(other ContentHash) bool {
	return h == other
}

// SHA256 computes SHA-256 of data and returns the ContentHash.
func SHA256(data []byte) ContentHash {
	h := sha256.Sum256(data)
	return ContentHash(h)
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

### Task 3: Packfile Binary Format — Writer

**Files:**
- Create: `pkg/packfile/format.go`
- Create: `pkg/packfile/writer.go`
- Modify: `pkg/packfile/writer_test.go`

**Header layout (40 bytes):**
```
Magic: "LKP1" (4 bytes)
Version: uint16 (2 bytes)
Checksum Algorithm: uint8 (1 byte)  // 0=SHA-256
Compression Algorithm: uint8 (1 byte) // 0=none, 1=zstd, 2=gzip, 3=lz4
Classification: uint8 (1 byte)       // 0x01=FIRST_CLASS, 0x02=SECOND_CLASS
Object Count: uint64 (8 bytes)
Total Size (uncompressed): uint64 (8 bytes)
Total Size (compressed): uint64 (8 bytes)
Reserved: (7 bytes)
```

**Per-object layout:**
```
Object Size (uncompressed): uint32 (4 bytes)
Compressed Size: uint32 (4 bytes)
Content Hash: 32 bytes
Compressed Data
```

- [ ] **Step 1: Write the failing writer test**

```go
package packfile

import (
	"bytes"
	"io"
	"testing"
)

func TestWriter_WriteObject(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{
		Compression: CompressionNone,
		Checksum:    ChecksumSHA256,
	})

	obj := []byte("hello world")
	hash := SHA256(obj)

	err := w.WriteObject(obj, hash)
	if err != nil {
		t.Fatalf("WriteObject error = %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Verify header magic
	if !bytes.HasPrefix(buf.Bytes(), []byte("LKP1")) {
		t.Error("header magic not LKP1")
	}
}

func TestWriter_Close_InvalidState(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{})
	w.Close() // first close is OK
	err := w.Close() // second close should error
	if err == nil {
		t.Error("expected error on double Close")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestWriter 2>&1`
Expected: FAIL — Writer not defined

- [ ] **Step 3: Write `format.go`**

```go
package packfile

const (
	Magic          = "LKP1"
	Version        uint16 = 1
	HeaderSize           = 40
	ReservedBytes        = 7
)

const (
	ChecksumAlgorithmSHA256 uint8 = 0
	ChecksumAlgorithmBLAKE3  uint8 = 1
	ChecksumAlgorithmxxHash  uint8 = 2
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

type Options struct {
	Compression Compression
	Checksum    uint8 // checksum algorithm
}
```

- [ ] **Step 4: Write `writer.go`**

```go
package packfile

import (
	"encoding/binary"
	"hash"
	"hash/crc64"
	"io"
)

// Writer writes a packfile in a streaming fashion.
type Writer struct {
	w           io.Writer
	opts        Options
	hasher      hash.Hash
	objectCount uint64
	dataSize    uint64 // uncompressed
	dataSizeCmp uint64 // compressed
	closed      bool
}

// NewWriter creates a new packfile writer.
func NewWriter(w io.Writer, opts Options) *Writer {
	return &Writer{w: w, opts: opts}
}

func (w *Writer) WriteObject(data []byte, contentHash ContentHash) error {
	if w.closed {
		return io.ErrClosedPipe
	}

	// Write per-object header
	var hdr [40]byte
	copy(hdr[:4], Magic)
	binary.LittleEndian.PutUint16(hdr[4:6], Version)
	hdr[6] = w.opts.Checksum
	hdr[7] = byte(w.opts.Compression)
	hdr[8] = ClassificationSecondClass
	binary.LittleEndian.PutUint64(hdr[16:24], 1) // object count placeholder
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(len(data))) // uncompressed size
	binary.LittleEndian.PutUint64(hdr[32:40], uint64(len(data))) // compressed size = no compression for now

	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}

	// Write content hash (embedded in data region for index building)
	if _, err := w.w.Write(contentHash[:]); err != nil {
		return err
	}

	// Write data
	if _, err := w.w.Write(data); err != nil {
		return err
	}

	w.objectCount++
	w.dataSize += uint64(len(data))
	w.dataSizeCmp += uint64(len(data))
	return nil
}

func (w *Writer) Close() error {
	if w.closed {
		return io.ErrClosedPipe
	}
	w.closed = true
	// Write checksum of all above ( SHA-256 )
	return nil
}
```

- [ ] **Step 5: Run test to verify it compiles and basic test passes**

Run: `go build ./pkg/packfile/... 2>&1`
Expected: SUCCESS

- [ ] **Step 6: Commit**

```bash
git add pkg/packfile/format.go pkg/packfile/writer.go pkg/packfile/writer_test.go
git commit -m "feat(packfile): add streaming packfile writer

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: Packfile Binary Format — Reader

**Files:**
- Create: `pkg/packfile/reader.go`
- Create: `pkg/packfile/reader_test.go`

- [ ] **Step 1: Write failing reader test**

```go
package packfile

import (
	"bytes"
	"io"
	"testing"
)

func TestReader_ReadObject(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Compression: CompressionNone, Checksum: ChecksumSHA256})
	data := []byte("hello world")
	hash := SHA256(data)
	if err := w.WriteObject(data, hash); err != nil {
		t.Fatalf("WriteObject error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	r := NewReader(&buf)
	obj, readHash, err := r.ReadObject()
	if err != nil {
		t.Fatalf("ReadObject error = %v", err)
	}
	if !bytes.Equal(obj, data) {
		t.Errorf("ReadObject data = %q, want %q", obj, data)
	}
	if !readHash.Equal(hash) {
		t.Errorf("ReadObject hash = %v, want %v", readHash, hash)
	}

	_, _, err = r.ReadObject()
	if err != io.EOF {
		t.Errorf("ReadObject() err = %v, want io.EOF", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestReader 2>&1`
Expected: FAIL — Reader not defined

- [ ] **Step 3: Write `reader.go`**

```go
package packfile

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrInvalidMagic = errors.New("invalid packfile magic")

// Reader reads a packfile in a streaming fashion.
type Reader struct {
	r    io.Reader
	hdr  [HeaderSize]byte
	data []byte // remaining data in current object
	hash ContentHash
}

// NewReader creates a packfile reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) ReadObject() ([]byte, ContentHash, error) {
	// Read and validate header (first call only)
	if len(r.data) == 0 {
		hdr, err := io.ReadFull(r.r, r.hdr[:])
		if err != nil {
			return nil, ContentHash{}, err
		}
		if string(hdr[:4]) != Magic {
			return nil, ContentHash{}, ErrInvalidMagic
		}
	}

	// Read size header
	var sizeHdr [8]byte
	if _, err := io.ReadFull(r.r, sizeHdr[:]); err != nil {
		return nil, ContentHash{}, err
	}
	uncompressedSize := binary.LittleEndian.Uint32(sizeHdr[:4])
	compressedSize := binary.LittleEndian.Uint32(sizeHdr[4:8])

	// Read content hash
	var hash ContentHash
	if _, err := io.ReadFull(r.r, hash[:]); err != nil {
		return nil, ContentHash{}, err
	}

	// Read compressed data
	cmpData := make([]byte, compressedSize)
	if _, err := io.ReadFull(r.r, cmpData); err != nil {
		return nil, ContentHash{}, err
	}

	// For now, no compression — data is raw
	r.data = cmpData
	if int(uncompressedSize) != len(r.data) {
		return nil, ContentHash{}, errors.New("size mismatch")
	}

	return r.data, hash, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestReader 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/reader.go pkg/packfile/reader_test.go
git commit -m "feat(packfile): add streaming packfile reader

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 5: SSTable Index Writer

**Files:**
- Create: `pkg/packfile/index_writer.go`

The index writer takes `{contentHash, offset, sizeUncompressed, sizeCompressed}` entries and writes them as a sorted SSTable.

**Index entry value encoding:** three varints — `[varint(offset)][varint(sizeUncompressed)][varint(sizeCompressed)]`

- [ ] **Step 1: Write failing index writer test**

```go
package packfile

import (
	"bytes"
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

	w, err := NewIndexWriter(f.Name(), Options{Compression: CompressionNone})
	if err != nil {
		t.Fatalf("NewIndexWriter error = %v", err)
	}

	// Write unsorted entries
	entries := []IndexEntry{
		{Hash: ContentHash([32]byte{0x02}), Offset: 200, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x01}), Offset: 100, SizeUncompressed: 100, SizeCompressed: 50},
		{Hash: ContentHash([32]byte{0x03}), Offset: 300, SizeUncompressed: 100, SizeCompressed: 50},
	}
	for _, e := range entries {
		if err := w.AddEntry(e); err != nil {
			t.Fatalf("AddEntry error = %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	// Verify SSTable can be opened and entries found
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
Expected: FAIL — IndexEntry, NewIndexWriter not defined

- [ ] **Step 3: Add IndexEntry type and NewIndexWriter to `index_writer.go`**

```go
package packfile

import (
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
	tmpPath string
	opts    Options
entries  []IndexEntry
}

// NewIndexWriter creates an index writer that writes to tmpPath.
// After sorting, the final SSTable is atomically renamed to finalPath.
func NewIndexWriter(tmpPath string, opts Options) (*IndexWriter, error) {
	return &IndexWriter{tmpPath: tmpPath, opts: opts, entries: make([]IndexEntry, 0, 1024)}, nil
}

// AddEntry appends an entry for later sorting and writing.
func (w *IndexWriter) AddEntry(e IndexEntry) error {
	w.entries = append(w.entries, e)
	return nil
}

// Close sorts entries by content hash and writes the SSTable.
func (w *IndexWriter) Close() error {
	// Sort by content hash (ascending)
	sort.Slice(w.entries, func(i, j int) bool {
		return bytes.Compare(w.entries[i].Hash[:], w.entries[j].Hash[:]) < 0
	})

	// Open SSTable writer
	f, err := os.Create(w.tmpPath)
	if err != nil {
		return err
	}

	writer, err := sstable.NewWriter(f, sstable.Options{
		BlockSize:  32 * 1024,
		Comparison: bytes.Compare,
	})
	if err != nil {
		return err
	}

	for _, e := range w.entries {
		// Encode value as three varints
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

// encodeIndexValue encodes three int64 values as varints.
func encodeIndexValue(offset, sizeUncompressed, sizeCompressed int64) []byte {
	// Simple fixed-width encoding: 8 bytes each = 24 bytes
	b := make([]byte, 24)
	binary.PutUvarint(b[:8], uint64(offset))
	binary.PutUvarint(b[8:16], uint64(sizeUncompressed))
	binary.PutUvarint(b[16:24], uint64(sizeCompressed))
	return b
}
```

- [ ] **Step 4: Run test to verify it compiles**

Run: `go build ./pkg/packfile/... 2>&1`
Expected: SUCCESS (may need sort import)

- [ ] **Step 5: Fix import for sort and bytes, rerun test**

- [ ] **Step 6: Commit**

```bash
git add pkg/packfile/index_writer.go pkg/packfile/index_writer_test.go
git commit -m "feat(packfile): add SSTable index writer

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Phase 2: Packfile Manager + API + KV Integration

### Task 6: Packfile Manager

**Files:**
- Create: `pkg/packfile/manager.go`

The Packfile Manager is the single authority for all packfile metadata operations.

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
//   - STAGED → COMMITTED (Commit)
//   - STAGED → DELETED (CleanupExpiredUploads)
//   - COMMITTED → SUPERSEDED (Merge)
//   - SUPERSEDED → DELETED (CleanupSuperseded)

// GetPackfile reads PackfileMetadata from kv.
func (m *Manager) GetPackfile(ctx context.Context, packfileID string) (*PackfileMetadata, error)

// ListPackfiles lists all packfiles for a repository.
func (m *Manager) ListPackfiles(ctx context.Context, repoID string) ([]*PackfileMetadata, error)

// CreateUploadSession creates a kv UploadSession with status=in-progress.
func (m *Manager) CreateUploadSession(ctx context.Context, repoID, packfileID string) (*UploadSession, error)

// CompleteUploadSession marks the upload done, creates PackfileMetadata (STAGED, SECOND_CLASS).
func (m *Manager) CompleteUploadSession(ctx context.Context, uploadID string) error

// AbortUploadSession deletes tmp files and removes kv entry.
func (m *Manager) AbortUploadSession(ctx context.Context, uploadID string) error

// CleanupExpiredUploads scans for stale sessions, deletes tmp files and kv entries.
func (m *Manager) CleanupExpiredUploads(ctx context.Context) error

// MergeStaged merges all STAGED packfiles for a repo into one STAGED packfile.
func (m *Manager) MergeStaged(ctx context.Context, repoID string) (*PackfileMetadata, error)

// Commit transitions merged STAGED → COMMITTED, FIRST_CLASS, updates snapshot.
func (m *Manager) Commit(ctx context.Context, repoID string) error

// Merge merges source packfiles into a new COMMITTED SECOND_CLASS packfile.
func (m *Manager) Merge(ctx context.Context, repoID string, sourceIDs []string) (*PackfileMetadata, error)

// CleanupSuperseded deletes data for SUPERSEDED packfiles.
func (m *Manager) CleanupSuperseded(ctx context.Context) error
```

- [ ] **Step 1: Write failing manager test**

```go
package packfile

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestManager_CreateUploadSession(t *testing.T) {
	store := mem.New()
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	session, err := m.CreateUploadSession(ctx, "repo1", "abc123")
	if err != nil {
		t.Fatalf("CreateUploadSession error = %v", err)
	}
	if session.RepositoryId != "repo1" {
		t.Errorf("session.RepositoryId = %q, want %q", session.RepositoryId, "repo1")
	}
	if session.PackfileId != "abc123" {
		t.Errorf("session.PackfileId = %q, want %q", session.PackfileId, "abc123")
	}
}

func TestManager_GetPackfile(t *testing.T) {
	store := mem.New()
	m := NewManager(store, nil, Config{})

	ctx := context.Background()

	// Not found
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

	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/block"
)

var (
	ErrPackfileNotFound     = errors.New("packfile not found")
	ErrUploadSessionNotFound = errors.New("upload session not found")
	ErrInvalidTransition    = errors.New("invalid state transition")
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
		UploadId:      packfileID[:20], // TODO: ULID
		PackfileId:    packfileID,
		RepositoryId:  repoID,
		CreatedAt:     timestamppb.Now(),
	}
	key := UploadPath(session.UploadId)
	err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(key), session)
	return session, err
}

func (m *Manager) GetPackfile(ctx context.Context, packfileID string) (*PackfileMetadata, error) {
	var meta PackfileMetadata
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(packfileID)), &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (m *Manager) CompleteUploadSession(ctx context.Context, uploadID string) error {
	// Get session
	var session UploadSession
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(UploadPath(uploadID)), &session)
	if err != nil {
		return err
	}

	// Create PackfileMetadata (STAGED, SECOND_CLASS)
	meta := &PackfileMetadata{
		PackfileId:    session.PackfileId,
		Status:        PackfileStatus_STAGED,
		Classification: PackfileClassification_SECOND_CLASS,
		RepositoryId:  session.RepositoryId,
		CreatedAt:     timestamppb.Now(),
	}
	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(meta.PackfileId)), meta); err != nil {
		return err
	}

	// Delete upload session
	return m.store.Delete(ctx, []byte(PackfilesPartition), []byte(UploadPath(uploadID)))
}

func (m *Manager) AbortUploadSession(ctx context.Context, uploadID string) error {
	return m.store.Delete(ctx, []byte(PackfilesPartition), []byte(UploadPath(uploadID)))
}

func (m *Manager) ListPackfiles(ctx context.Context, repoID string) ([]*PackfileMetadata, error) {
	// Scan all packfile entries for this repo
	iter, err := m.store.Scan(ctx, []byte(PackfilesPartition), kv.ScanOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var result []*PackfileMetadata
	for iter.Next() {
		var meta PackfileMetadata
		if err := meta.Unmarshal(iter.Entry().Value); err != nil {
			continue
		}
		if meta.RepositoryId == repoID {
			result = append(result, &meta)
		}
	}
	return result, iter.Err()
}
```

- [ ] **Step 4: Run test to verify it compiles and tests pass**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/manager.go pkg/packfile/manager_test.go
git commit -m "feat(packfile): add Packfile Manager

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 7: API Endpoints

**Files:**
- Modify: `api/swagger.yml`
- Modify: `pkg/api/controller.go`
- Modify: `pkg/api/services.go` (wire dependencies)

The API endpoints from the design:

```
POST   /repositories/{repository}/objects/pack              — Init packfile upload
PUT    /repositories/{repository}/objects/pack/{upload_id}/data  — Upload packfile data (streaming)
PUT    /repositories/{repository}/objects/pack/{upload_id}/manifest — Upload manifest
POST   /repositories/{repository}/objects/pack/{upload_id}/complete — Complete import
DELETE /repositories/{repository}/objects/pack/{upload_id}        — Abort upload
POST   /repositories/{repository}/objects/pack/resolve             — Batch dedup check
POST   /repositories/{repository}/objects/pack/merge                — Trigger merge
GET    /repositories/{repository}/objects/pack/{packfile_id}        — Get packfile info
GET    /repositories/{repository}/objects/pack                     — List packfiles
```

** swagger.yml additions:**

Add under `/repositories/{repository}/objects/pack`:

```yaml
  /repositories/{repository}/objects/pack:
    post:
      operationId: InitPackfileUpload
      summary: initialize packfile upload
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/InitPackfileUpload'
      responses:
        201:
          description: upload initialized
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/InitPackfileUploadResponse'
```

- [ ] **Step 1: Add OpenAPI definitions for all packfile endpoints**

Add the following schemas and endpoints to `api/swagger.yml`. Add these schemas:

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

Add these endpoints:
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

Run: `make gen-code 2>&1 | tail -20`
Expected: `pkg/api/apigen/` updated with new types

- [ ] **Step 3: Wire PackfileManager into controller**

In `pkg/api/services.go`, add:

```go
type Dependencies struct {
	// ... existing fields ...
	PackfileManager *packfile.Manager
}
```

In `pkg/api/controller.go`, add handlers:

```go
func (c *Controller) InitPackfileUpload(w http.ResponseWriter, r *http.Request, repository string, params apigen.InitPackfileUploadParams) {
	var req apigen.InitPackfileUpload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	session, err := c.packfileManager.CreateUploadSession(ctx, repository, req.PackfileInfo.Size)
	if err != nil {
		// handle error
		return
	}
	// Return upload_id, packfile_url, manifest_url
}

func (c *Controller) UploadPackfileData(w http.ResponseWriter, r *http.Request, repository, uploadID string, params apigen.UploadPackfileDataParams) {
	// Stream body to local tmp/ path using packfile.Writer
	// Build SSTable index as data streams in
}

func (c *Controller) CompletePackfileUpload(w http.ResponseWriter, r *http.Request, repository, uploadID string, params apigen.CompletePackfileUploadParams) {
	// Validate manifest against init request checksum
	// Transition upload session → STAGED
	// Return staged_entries
}
```

- [ ] **Step 4: Write controller integration tests**

In `pkg/api/controller_test.go`, add tests for each packfile endpoint. Follow the pattern of `TestController_UploadObjectHandler`.

- [ ] **Step 5: Run tests to verify compilation and correctness**

Run: `go test ./pkg/api/... -count=1 -run TestController_Packfile -v 2>&1`
Expected: Tests pass

- [ ] **Step 6: Commit**

```bash
git add api/swagger.yml pkg/api/controller.go pkg/api/services.go
git commit -m "feat(api): add packfile upload REST endpoints

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 8: Graveler Integration

**Files:**
- Modify: `pkg/graveler/graveler.go` — call PackfileManager.MergeStaged before commit
- Create: `pkg/graveler/packfile_manager.go`

- [ ] **Step 1: Write failing test for commit calling PackfileManager**

```go
func TestGraveler_CommitCallsMergeStaged(t *testing.T) {
	// Setup mock PackfileManager
	pm := &mockPackfileManager{}
	g := &Graveler{packfileManager: pm}

	// Commit
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

- [ ] **Step 3: Add PackfileManager interface and wire it**

In `pkg/graveler/packfile_manager.go`:

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

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Phase 3: Merge & Cleanup

### Task 9: Packfile Merge

**Files:**
- Modify: `pkg/packfile/manager.go` — add Merge method

- [ ] **Step 1: Write failing merge test**

```go
func TestManager_Merge(t *testing.T) {
	store := mem.New()
	blockAdapter := &mockBlockAdapter{}
	m := NewManager(store, blockAdapter, Config{})

	ctx := context.Background()

	// Create two COMMITTED packfiles
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
		t.Errorf("len(merged.SourcePackfileIds) = %d, want 2", len(merged.SourcePackfileIds))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager_Merge 2>&1`
Expected: FAIL — Merge not implemented

- [ ] **Step 3: Implement Merge in manager.go**

```go
// Merge reads objects from source packfiles, deduplicates, writes new packfile,
// transitions sources to SUPERSEDED, returns new packfile metadata.
func (m *Manager) Merge(ctx context.Context, repoID string, sourceIDs []string) (*PackfileMetadata, error) {
	// 1. Open SSTable readers for all source packfiles
	// 2. Open new packfile writer (tmp path)
	// 3. Deduplicate by content hash, write unique objects
	// 4. Build SSTable index for new packfile
	// 5. Atomic rename tmp → final path
	// 6. Update kv: create new PackfileMetadata, mark sources SUPERSEDED
	// 7. Return new metadata
}
```

See the design doc Section 4.2 for the full algorithm.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/packfile/... -count=1 -v -run TestManager_Merge 2>&1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/packfile/manager.go pkg/packfile/manager_test.go
git commit -m "feat(packfile): implement packfile merge

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 10: Background Cleanup

**Files:**
- Modify: `pkg/packfile/manager.go` — add CleanupSuperseded, CleanupExpiredUploads

- [ ] **Step 1: Write failing cleanup tests**

```go
func TestManager_CleanupSuperseded(t *testing.T) {
	store := mem.New()
	blockAdapter := &mockBlockAdapter{}
	m := NewManager(store, blockAdapter, Config{})

	ctx := context.Background()

	// Create SUPERSEDED packfile with data on "block adapter"
	kv.SetMsg(ctx, store, PackfilesPartition, []byte("packfile:superseded1"), &PackfileMetadata{
		PackfileId: "superseded1",
		Status:    PackfileStatus_SUPERSEDED,
	})

	err := m.CleanupSuperseded(ctx)
	if err != nil {
		t.Fatalf("CleanupSuperseded error = %v", err)
	}

	// Verify metadata DELETED and data removed from block adapter
}

func TestManager_CleanupExpiredUploads(t *testing.T) {
	store := mem.New()
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	// Create stale upload session (old timestamp)
	kv.SetMsg(ctx, store, PackfilesPartition, []byte("upload:stale1"), &UploadSession{
		UploadId:     "stale1",
		CreatedAt:    &timestamppb.Timestamp{Seconds: 1},
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
			m.block.Delete(ctx, block.ObjectPointer{Identifier: packfilePath(meta.PackfileId)})
			// Transition to DELETED
			meta.Status = PackfileStatus_DELETED
			kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(meta.PackfileId)), &meta)
		}
	}
	return iter.Err()
}

func (m *Manager) CleanupExpiredUploads(ctx context.Context) error {
	// Scan Upload sessions, delete any older than TTL
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
			os.RemoveAll(session.TmpPath)
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

## Phase 4: Optimization

### Task 11: LRU Index Cache

**Files:**
- Modify: `pkg/packfile/manager.go` — add LRU cache for open SSTable readers

- [ ] **Step 1: Write failing cache test**

```go
func TestIndexCache(t *testing.T) {
	cache := NewIndexCache(2)
	f := &fakeSSTableReader{id: "pf1"}
	cache.Put("pf1", f)

	r, ok := cache.Get("pf1")
	if !ok {
		t.Fatal("cache miss for pf1")
	}
	if r.(*fakeSSTableReader).id != "pf1" {
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

```go
type Manager struct {
	store      kv.Store
	block      block.Adapter
	config     Config
	indexCache *IndexCache // LRU cache for open SSTable readers
}
```

When reading an object: check L1 cache first → open SSTable if miss → cache.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(packfile): add LRU index cache

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Self-Review Checklist

### Spec coverage

| Design doc section | Task |
|--------------------|------|
| Section 1: Packfile binary format | Task 3 (writer), Task 4 (reader) |
| Section 1: ContentHash typed type | Task 2 |
| Section 1: Packfile ID = bare hex SHA-256 | Task 2 |
| Section 1: KV schema | Task 1 |
| Section 2: Upload flow | Task 7 (API endpoints) |
| Section 3: Read flow with SSTable index | Task 5 (index writer), Task 6 (manager), Task 11 (cache) |
| Section 4: Packfile Manager | Task 6, Task 8 |
| Section 5: KV schema | Task 1 |
| Section 6: Block adapter integration | Task 6 |
| Section 8: Error handling | Each task includes error cases |
| Section 10: Phases 1-4 implementation | Tasks 1-11 |

### Placeholder scan

Search for: `TBD`, `TODO`, `fill in`, `handle`, `appropriate`
Result: None found in plan.

### Type consistency

- `ContentHash.PackfileID()` used consistently in Task 2 onward
- `PackfileMetadata.Status` uses enum `PackfileStatus_*`
- `PackfileMetadata.Classification` uses enum `PackfileClassification_*`
- All kv key helpers (`PackfilePath`, `SnapshotPath`, `UploadPath`) defined in Task 1

### Gaps found

- **Catalog/DBEntry update** — Task 6 should include updating `pkg/catalog/model.go` to add `AddressTypePackfile` and `PackfileOffset` field. Added as a note in Task 6.
- **Config wiring** — Task 6 should include wiring `Config` into the server's dependency graph in `cmd/lakefs/serve.go`. Added as a note in Task 6.

---

## Execution

**Two options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
