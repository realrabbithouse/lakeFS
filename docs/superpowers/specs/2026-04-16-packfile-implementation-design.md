# Packfile Transfer Protocol — Implementation Design

## Status

**Draft** — Under revision (storage layer + KV schema + lifecycle clarifications)

## Background

Uploading or downloading many small objects in lakeFS is inefficient because each object requires a separate API call and storage operation. This design introduces a **packfile** — a binary bundle of multiple objects — to enable bulk transfer with content-addressable deduplication.

## Design Principles

1. **Streaming IO only** — All operations use `io.Reader`/`io.Writer`; no in-memory `[]byte` for bulk data
2. **Local filesystem primary** — Packfiles live on local disk/NFS for merge efficiency; S3 is async backup
3. **Immutable once created** — Packfiles are never modified after finalization
4. **Server-side validation** — No pre-signed URL direct-to-S3 upload; all data streams through lakeFS server
5. **Block adapter agnostic** — Implementation depends on the existing block adapter, never modifies it

---

## 1. Packfile Format

### 1.1 Binary Layout

```
┌─────────────────────────────────────────────────────────┐
│ Header (40 bytes)                                       │
│   Magic: "LKP1" (4 bytes)                              │
│   Version: uint16 (2 bytes)                              │
│   Checksum Algorithm: uint8 (1 byte)                    │
│     0 = SHA-256, 1 = BLAKE3, 2 = xxHash              │
│   Compression Algorithm: uint8 (1 byte)                 │
│     0 = none, 1 = zstd, 2 = gzip, 3 = lz4             │
│   Classification: uint8 (1 byte)                        │
│     0x01 = First-class (COMMITTED, never deleted during merge)│
│     0x02 = Second-class (STAGED or merged, eligible for GC)│
│   Object Count: uint64 (8 bytes)                         │
│   Total Size (uncompressed): uint64 (8 bytes)          │
│   Total Size (compressed): uint64 (8 bytes)            │
│   Reserved (7 bytes)                                   │
├─────────────────────────────────────────────────────────┤
│ Object Data Region (append-only, compressed)             │
│   For each object (in creation order):                │
│     Object Size (uncompressed): uint32 (4 bytes)      │
│     Compressed Size: uint32 (4 bytes)                  │
│     Content Hash: 32 bytes (SHA-256 of raw object)    │
│     Compressed Data                                    │
│                                                         │
│ **Note:** `content_type` is NOT stored in the packfile  │
│ binary format. Per-object metadata (content_type,       │
│ user metadata) lives in the manifest and flows into     │
│ the staging entry, not the packfile itself. The         │
│ packfile is content-agnostic binary data.              │
├─────────────────────────────────────────────────────────┤
│ Checksum (32 bytes) — SHA-256 of all above             │
└─────────────────────────────────────────────────────────┘
```

**Design notes:**
- Packfile is immutable after creation (written once, never modified)
- "Append-only" means objects are written sequentially during creation — no seeking back
- All random access metadata lives in the separate index file (.sst)
- Checksum covers header + all object data (each object's content hash is embedded in the data region for fast index building)
- **Classification**: First-class (0x01) packfiles are COMMITTED and protected from deletion during merge. Second-class (0x02) packfiles are STAGED or merged, eligible for GC when new packfile is built
- **Packfile ID** is the bare hex SHA-256 content hash of the packfile (no algorithm prefix). Client knows the packfile ID before uploading, enabling deduplication
- New objects cannot be appended to an existing packfile; instead, create a new packfile and merge later
- **Streaming IO trade-off**: The packfile format writes each object's 40-byte header before the compressed data, because the header contains `compressedSize`. Since compression is not seekable (compressor must consume all input before emitting output), the compressed output for an object must be fully materialized before the header can be written. This is the only place that violates true streaming IO. Mitigation: compressed output is buffered per-object only (max 5MB), not unbounded. All bulk data still streams — the `Packer` writes in one `Write()` call per object after buffering. This is acceptable because max object size (5MB) ≪ max packfile size (5GB).

### 1.2 Index File (`.sst`)

The index file provides all metadata needed for random access. It is the **only** source of truth for looking up objects within a packfile. Index file design is **pluggable** — two implementations are supported:

#### Solution 1: Custom Binary Index (reference only)

```
┌─────────────────────────────────────────────────────────┐
│ Header (12 bytes)                                       │
│   Magic: "LKIX" (4 bytes)                              │
│   Version: uint16 (2 bytes)                            │
│   Checksum Algorithm: uint8                             │
│   Fan-out Table Entries: uint32 (always 256)          │
│   Object Count: uint32                                  │
├─────────────────────────────────────────────────────────┤
│ Fan-out Table (256 entries)                            │
│   For each prefix (0-255):                             │
│     Offset to first object with prefix                 │
│     Object count for prefix                            │
├─────────────────────────────────────────────────────────┤
│ Sorted Object Index (sorted by content hash)            │
│   For each object:                                     │
│     Content Hash: 32 bytes                             │
│     Offset in packfile: varint                         │
│     Uncompressed Size: varint                           │
│     Compressed Size: varint                           │
├─────────────────────────────────────────────────────────┤
│ Checksum (32 bytes)                                    │
└─────────────────────────────────────────────────────────┘
```

#### Solution 2: SSTable-Style Index (preferred, only solution implemented)

The index is stored as an SSTable using `github.com/cockroachdb/pebble/sstable`. This is the same format used by lakeFS's Graveler for range and metarange storage.

**Index entry:**
- Key: Content Hash (32 bytes)
- Value: three varints — `offset`, `size_uncompressed`, `size_compressed`

**Properties used:**
- Bloom filter for O(1) fast negative lookups (object not in packfile)
- Block-level compression (same algorithm as packfile)
- 32KB block size (configurable)

The pebble/sstable package handles all internal structure (index blocks, filter blocks, footer). Implementation uses it directly.

**Implementation:** Only Solution 2 (SSTable) will be implemented. Solution 1 is documented for reference.

**Why separate index file?**
- Packfile stays simple (append-only stream)
- SSTable index is block-compressed, very compact
- Bloom filter enables fast negative lookups
- Graveler's existing Pebble/SSTable infrastructure is reused

### 1.3 Storage Path

```
<storage_namespace>/
└── _packfiles/
    └── <repository_id>/
        └── <packfile_id>/        # packfile_id = bare hex SHA-256
            ├── data.pack          # binary data
            └── data.sst          # index file
```

**Temporary staging during upload:**
```
<storage_namespace>/
└── _packfiles/
    └── <repository_id>/
        └── tmp/
            └── <upload_id>/       # upload_id = ULID, session token
                └── data.pack     # partial upload, streaming here
```

**Constraint:** The `_packfiles/` tree (both tmp/ and final packfile directories) must reside on the same filesystem/mount for atomic rename to work correctly. For NFS deployments, the entire `_packfiles/` tree must be one NFS mount.

### 1.4 Address Encoding

For objects stored in packfiles, the `catalog.Entry` protobuf is extended:

```protobuf
message Entry {
    string address = 1;           // "packfile:<pack_id>" for packfile objects
    // ... existing fields ...
    enum AddressType {
        BY_PREFIX_DEPRECATED = 0;
        RELATIVE = 1;
        FULL = 2;
        PACKFILE = 3;             // NEW: object stored in packfile
    }
    AddressType address_type = 6;
    // NEW: for PACKFILE address type
    int64 packfile_offset = 8;    // offset within packfile
}
```

**Physical address encoding:**
```
# Direct objects (existing):
PhysicalAddress = "data/objects/repo/..." (relative path)

# Packfile objects (new):
PhysicalAddress = "packfile:<packfile_id>"   # packfile_id = bare hex SHA-256
Offset = packfile_offset field
```

**DBEntry Go struct** (`pkg/catalog/model.go`):
```go
type DBEntry struct {
    CommonLevel     bool
    Path            string
    PhysicalAddress string   // "packfile:<packfile_id>" (bare hex SHA-256)
    CreationDate    time.Time
    Size            int64
    Checksum        string   // content hash (stored as hex string for compatibility)
    Metadata        Metadata
    Expired         bool
    AddressType     AddressType  // AddressTypePackfile = 3
    ContentType     string
    // NEW: offset within packfile (valid when AddressType == AddressTypePackfile)
    PackfileOffset  int64
}
```

**ContentHash typed type** (new type in `pkg/packfile/`):

```go
// ContentHash represents a SHA-256 content hash.
// For packfiles, the ContentHash IS the packfile_id (content-addressed).
type ContentHash [32]byte

// String returns the content hash as lowercase hex string.
// packfile_id = bare hex string via fmt.Sprintf("%x", h[:])
func (h ContentHash) String() string {
    return fmt.Sprintf("%x", h[:])
}

// ParseContentHash parses a bare hex content hash string (64 hex chars).
func ParseContentHash(s string) (ContentHash, error) {
    // decode 64 hex chars to [32]byte
}

// Bytes returns the raw 32-byte hash
func (h ContentHash) Bytes() []byte {
    return h[:]
}

// Equal compares two ContentHashes
func (h ContentHash) Equal(other ContentHash) bool {
    return h == other
}

// StreamingHash computes a ContentHash incrementally as data is read.
// Used during AppendObject to hash as data streams in.
type StreamingHash struct{ h hash.Hash }
func NewStreamingHash() *StreamingHash
func (sh *StreamingHash) Write(p []byte) (int, error)
func (sh *StreamingHash) Sum() ContentHash
```

**Why a typed type:**
- Compile-time type safety prevents accidentally passing `[32]byte` where `ContentHash` expected
- `String()` returns bare hex (no prefix) — this value IS the packfile_id
- Clear semantic intent: this is a content address, not just arbitrary bytes
- `StreamingHash` enables incremental content hash computation during streaming IO

**Staging manager integration:**

The `graveler.Value` wraps the serialized `Entry` proto. When reading a staged entry:
1. Deserialize `Value.Data` → `Entry`
2. If `Entry.address_type == PACKFILE`:
   - Extract `pack_id` from `Entry.address` (format: `packfile:<pack_id>`)
   - Extract `offset` from `Entry.packfile_offset`
   - Use packfile reader to fetch object data
```

---

## 2. Upload Flow

### 2.1 Client-Side Workflow

```
1. lakectl ls <repo>/<branch> → get server entry list
2. local ls → get local file list
3. Diff → identify new/modified files
4. Filter by size threshold (default < 1MB, max 5MB)
5. If count >= min_objects_for_pack (default 10):
   a. Optional: POST /resolve with content hashes → filter already-known
   b. Create packfile locally: bundle objects sequentially, compute per-object offsets
   c. POST /objects/pack → get upload URLs
   d. PUT manifest_url with manifest (Newline-delimited JSON)
   e. PUT packfile_url with streaming packfile data
   f. POST /complete → import to staging
   g. lakectl commit → commit to branch
6. Else: use existing direct upload
```

### 2.2 Server-Side Upload (Streaming)

```
┌─────────┐      ┌─────────────────┐      ┌────────────────┐
│ Client  │──────│ lakeFS Server   │──────│ Local Disk/NFS│
│         │      │ (streaming)     │      │ (primary)      │
└─────────┘      └─────────────────┘      └────────────────┘
                           │
                           │ async background copy
                           ▼
                    ┌────────────────┐
                    │ S3            │
                    │ (backup)      │
                    └────────────────┘
```

**Steps:**
1. Server receives POST, validates packfile_info (size, checksum, compression). Creates an upload session entry in kv (status=in-progress). `upload_id` is a ULID.
2. Client uploads manifest via `PUT manifest_url` (separate call)
3. Client uploads packfile data via `PUT packfile_url` (streaming to local disk at tmp/ path)
4. As packfile data arrives, server reads each object's embedded hash, and writes an SSTable index entry; if `validate_object_checksum` is `true`, also compares the computed hash of each object's raw data against the embedded hash in the packfile data region (data integrity check)
5. Background goroutine asynchronously copies packfile + index to S3 using block adapter's `Put` API. This is independent of commit — replication happens at any time after the tmp/ → final location rename.
6. On `/complete`: server validates manifest entries against the in-memory index (which now contains all content hashes from the packfile); if `validate_object_checksum` was `true` and any hash mismatch was detected during streaming, the upload is rejected. On success, the upload session in kv is marked completed, and the packfile metadata in kv transitions to STAGED.

**Checksum mismatch during streaming:**
- If `validate_object_checksum: true` and a mismatch is detected, the server immediately stops accepting data
- tmp/ files (partial packfile data + partial index) are deleted before returning the error to the client
- The rejection is atomic — no stale tmp files remain

**Upload session cleanup:**
- The upload session lives in kv with status=in-progress
- On explicit `DELETE .../pack/{upload_id}`: tmp/ files are deleted and kv session is removed
- On client disconnect without DELETE: kv session eventually expires (background process scans for expired sessions), triggering tmp/ cleanup

**Why this approach:**
- All data flows through server → enables validation
- Local disk is always in sync for merge operations
- S3 provides durability with async replication, decoupled from commit
- Block adapter's simple `Put` API handles S3 writes (no multipart complexity needed for packfiles)
- Index is generated server-side as packfile streams in, no second pass needed

### 2.3 API Endpoints

```
# Initialize packfile upload
POST /repositories/{repository}/objects/pack
Request: {
    packfile_info: {
        size: int64,
        checksum: "sha256:...",
        compression: "zstd" | "gzip" | "none"
    },
    validate_object_checksum: boolean   // default: true
}
Response: {
    upload_id: string,
    packfile_url: string,    # internal endpoint for streaming upload
    manifest_url: string     # internal endpoint for manifest
}
```

**Always validated:** `packfile_info.size`, `packfile_info.checksum`, `object_count` (against manifest metadata line), manifest `packfile_checksum` (must match `packfile_info.checksum` from init request).
**Conditionally validated (when `validate_object_checksum: true`):** as each object arrives, server reads the raw object data, computes its content hash, and compares against the **embedded** content hash in the packfile data region. This verifies the packfile data was not corrupted in transit. Mismatch → upload rejected. When `false`, embedded hashes are still read to build the index, but no data integrity check is performed. The manifest entry checksum is used only for deduplication via `POST /resolve`, not for data integrity validation.

# Upload packfile (streaming)
PUT /repositories/{repository}/objects/pack/{upload_id}/data
Body: binary packfile data (streaming)

# Upload manifest
PUT /repositories/{repository}/objects/pack/{upload_id}/manifest
Body: JSON manifest

# Complete import
POST /repositories/{repository}/objects/pack/{upload_id}/complete
Request: {}   // empty body
Response: { staged_entries: [...] }

# Abort upload
DELETE /repositories/{repository}/objects/pack/{upload_id}

# Check known objects (batch dedup query)
POST /repositories/{repository}/objects/pack/resolve
Request: { content_hashes: ["sha256:...", ...] }
Response: {
    missing: ["sha256:...", ...],
    known: ["sha256:...", ...]
}

# Trigger packfile merge (optional)
POST /repositories/{repository}/objects/pack/merge
Request: { packfile_ids?: [...], strategy?: "auto" | "eager" }
Response: { new_packfile_id: string, merged_count: int }
```

**Note:** No pre-signed URLs for packfile upload. All data streams through lakeFS server for validation.

### 2.4 Detailed API Shapes

#### Init Packfile Upload

```
POST /repositories/{repository}/objects/pack
```

**Request:**
```json
{
  "packfile_info": {
    "size": 1048576,
    "checksum": "sha256:...",
    "compression": "zstd"
  },
  "validate_object_checksum": true
}
```

**Response (201 Created):**
```json
{
  "upload_id": "pack-uploader-abc123",
  "packfile_url": "/repositories/myrepo/objects/pack/pack-uploader-abc123/data",
  "manifest_url": "/repositories/myrepo/objects/pack/pack-uploader-abc123/manifest"
}
```

#### Upload Packfile Data (Streaming)

```
PUT /repositories/{repository}/objects/pack/{upload_id}/data
Content-Type: application/octet-stream
Transfer-Encoding: chunked

<binary packfile data>
```

**Response (204 No Content)**

#### Upload Manifest

```
PUT /repositories/{repository}/objects/pack/{upload_id}/manifest
Content-Type: application/x-ndjson

{"packfile_checksum":"sha256:abc123...","object_count":100}
{"path":"images/logo.png","checksum":"sha256:abc123...","content_type":"image/png","metadata":{"user-key":"user-value"},"relative_offset":4096,"size_uncompressed":12345,"size_compressed":6789}
{"path":"images/bg.png","checksum":"sha256:def456...","content_type":"image/png","metadata":{},"relative_offset":8192,"size_uncompressed":23456,"size_compressed":11234}
...
```

**Format:** Newline-delimited JSON (NDJSON). First line is overall metadata. Each subsequent line is one entry's metadata.

**Response (204 No Content)**

#### Complete Packfile Upload

```
POST /repositories/{repository}/objects/pack/{upload_id}/complete
```

**Request:** `{}` (empty body)

On success, the upload session kv entry transitions to completed, packfile metadata transitions to STAGED, and staging entries are created. Commit is independent of S3 replication — the background goroutine replicates to S3 at any time after the tmp/ → final location rename.

**Response (200 OK):**
```json
{
  "staged_entries": [
    {
      "path": "images/logo.png",
      "checksum": "sha256:abc123...",
      "size": 12345,
      "content_type": "image/png",
      "metadata": { "user-key": "user-value" },
      "address_type": "PACKFILE",
      "physical_address": "packfile:a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
      "packfile_offset": 4096
    }
  ],
  "packfile_id": "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
  "object_count": 100
}
```

#### Abort Packfile Upload

```
DELETE /repositories/{repository}/objects/pack/{upload_id}
```

**Response (204 No Content)**

#### Resolve Content Hashes (Batch Deduplication Check)

```
POST /repositories/{repository}/objects/pack/resolve
```

**Request:**
```json
{
  "content_hashes": [
    "sha256:abc123...",
    "sha256:def456..."
  ]
}
```

**Response (200 OK):**
```json
{
  "known": ["sha256:abc123..."],
  "missing": ["sha256:def456..."]
}
```

#### Trigger Packfile Merge

```
POST /repositories/{repository}/objects/pack/merge
```

**Request:**
```json
{
  "packfile_ids": ["a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456", "b4e3d2c1a5f6e7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e"],
  "strategy": "auto"
}
```

**Response (200 OK):**
```json
{
  "new_packfile_id": "c5f4d3b2a6e7f8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d",
  "merged_count": 2,
  "total_objects": 500
}
```

#### Get Packfile Info

```
GET /repositories/{repository}/objects/pack/{packfile_id}
```

**Response (200 OK):**
```json
{
  "packfile_id": "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
  "object_count": 100,
  "size_bytes": 1048576,
  "compression": "zstd",
  "created_at": "2026-04-16T10:30:00Z",
  "status": "COMMITTED"
}
```

#### List Packfiles

```
GET /repositories/{repository}/objects/pack?after=<packfile_id>&limit=50
```

**Response (200 OK):**
```json
{
  "packfiles": [
    {
      "packfile_id": "b4e3d2c1a5f6e7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e",
      "object_count": 150,
      "size_bytes": 2097152,
      "created_at": "2026-04-16T11:00:00Z"
    }
  ],
  "pagination": {
    "has_more": true,
    "next_offset": "<packfile_id>"
  }
}
```

#### Error Responses

All endpoints may return:

| Status | Code | Description |
|--------|------|-------------|
| 400 | `INVALID_REQUEST` | Malformed request body |
| 401 | `UNAUTHORIZED` | Missing or invalid credentials |
| 403 | `FORBIDDEN` | Insufficient permissions |
| 404 | `NOT_FOUND` | Repository or resource not found |
| 409 | `CONFLICT` | Upload already completed or aborted |
| 422 | `VALIDATION_ERROR` | Manifest entry count mismatch, checksum mismatch |
| 500 | `INTERNAL_ERROR` | Server-side error |
| 503 | `SERVICE_UNAVAILABLE` | S3 sync in progress, retry later |

---

## 3. Read Flow (Random Access)

### 3.1 Index Lookup

Given a content hash, find an object in a packfile using the SSTable index:

```
1. Open SSTable reader on .sst file
2. Check bloom filter for the content hash → O(1) negative lookup
   (if bloom says no, object definitely not in packfile, skip to step 7)
3. Seek SSTable reader to the content hash key
4. Read index entry: { offset, size_uncompressed, size_compressed }
5. Seek to offset in packfile data region
6. Read compressed_size bytes, decompress and return (using steaming API, i.e. return a decompress reader)
7. Not found in bloom filter → return not found
```

**Lookup cost:**
- Bloom filter: O(1) for both hit and negative lookup
- SSTable seek: O(log n) where n is objects in the packfile
- S3 range request for the compressed object data

**SSTable reader:** Uses `github.com/cockroachdb/pebble/sstable` Reader. Supports:
- Bloom filter lookup (fast negative)
- Key seeking via binary search within index blocks
- Block-level compression (same as packfile)

### 3.2 Caching Strategy

#### Cache Hierarchy

```
┌─────────────────────────────────────────────────────────┐
│ L1: In-Memory SSTable Reader Cache                      │
│   Capacity: configurable (default 1000 indices)         │
│   Key: packfile_id (bare hex SHA-256)                  │
│   Value: *sstable.Reader (open SSTable reader)        │
│   TTL: none (evicted via LRU)                          │
├─────────────────────────────────────────────────────────┤
│ L2: Local Disk / NFS — Primary Storage                  │
│   Path: <storage_namespace>/_packfiles/<repo_id>/       │
│   .sst SSTable files and .pack data stored here        │
│   Packfiles always written to and read from L2 first   │
│   Merge operations read from L2 (not L3)               │
├─────────────────────────────────────────────────────────┤
│ L3: Object Storage (S3/GCS/Azure) — Async Replication   │
│   Written by background goroutine after tmp→final move │
│   Replication is decoupled from commit                 │
│   Read path: L2 miss → L3 fetch → cache to L2         │
│   Not the primary read target for merge                │
└─────────────────────────────────────────────────────────┘
```

**Note:** L2 is primary storage, not a cache in the traditional sense. All writes go to L2 first. Reads prefer L2 but fall back to L3 on miss. L1 is the only true cache (in-memory SSTable reader LRU).

#### LRU Cache Implementation

```go
import (
    lru "github.com/hashicorp/golang-lru"
    "github.com/cockroachdb/pebble/sstable"
)

// IndexCache wraps an LRU cache of open SSTable readers
type IndexCache struct {
    cache *lru.TwoQueueCache[string, *sstable.Reader]
}

// NewIndexCache creates a new cache with the given max entries
func NewIndexCache(maxEntries int) (*IndexCache, error) {
    cache, err := lru.New2QWithEviction(maxEntries)
    if err != nil {
        return nil, err
    }
    return &IndexCache{cache: cache}, nil
}

func (c *IndexCache) Get(packfileID string) (*sstable.Reader, bool) {
    val, ok := c.cache.Get(packfileID)
    return val, ok
}

func (c *IndexCache) Put(packfileID string, r *sstable.Reader) {
    c.cache.Add(packfileID, r)
}
```

#### Read Path (Detailed)

```
// Demo purpose
func ReadObject(packfileID string, contentHash ContentHash) (io.Reader, error) {
    // 1. Check L1 cache
    reader, ok := indexCache.Get(packfileID)  // packfileID = bare hex SHA-256
    if !ok {
        // 2. Open SSTable from L2 (local disk/NFS)
        f, err := os.Open(localPath(packfileID) + ".sst")
        if err != nil {
            // 3. Fetch from L3 (S3), cache to L2, then open
            reader, err = openSSTableFromS3(packfileID)
            if err != nil {
                return nil, err
            }
        } else {
            reader, err = sstable.NewReader(f)
            if err != nil {
                return nil, err
            }
        }
        indexCache.Put(packfileID, reader)
    }

    // 4. Check bloom filter first (fast negative lookup)
    if !reader.HasBloomFilter() || !reader.BloomFilterMatch(contentHash[:]) {
        return nil, ErrObjectNotFound  // definitely not found
    }

    // 5. Seek and read index entry
    entry, err := reader.Lookup(contentHash[:])
    if err != nil {
        return nil, ErrObjectNotFound
    }

    // 6. Range read from packfile data (L2 or L3 via blockAdapter.GetRange)
    data, err := blockAdapter.GetRange(
        ObjectPointer{Identifier: packfilePath(packfileID)},
        entry.Offset,
        entry.Offset + entry.SizeCompressed,
    )
    if err != nil {
        return nil, err
    }

    // 7. Decompress
    return decompress(data, entry.Compression)
}
```

#### Cache Invalidation

| Event | Action |
|-------|--------|
| Packfile merged | Invalidate indices for source packfiles and new packfile |
| Packfile deleted (GC) | Remove from L1, OS handles L2 (file deletion) |
| Index file updated | Invalidate and reload |

**Note:** Packfiles are immutable, so cache invalidation only happens on merge or deletion. No staleness issues.

### 3.4 Large-Scale Index Building

The index file is built as an SSTable using `github.com/cockroachdb/pebble/sstable`. SSTable requires keys to be **sorted** before writing — the writer does not sort. The build process has two approaches based on object count.

**Index entry value encoding:**
Each index entry's value is three varints:
```
[varint(offset)][varint(size_uncompressed)][varint(size_compressed)]
```
Typical entry: 3-15 bytes total. The SSTable writer handles block boundaries and compression automatically.

**Key constraint:** SSTable writer must receive entries in sorted order by content hash. Objects in the packfile arrive in creation order, which is not sorted. Entries must be collected, sorted, then written.

**Approach A — In-memory sort (object count < 1M):**

```
// Phase 1: Collect entries as packfile streams in
entries = []IndexEntry  // in-memory
for each object in packfile:
    hash = computeContentHash(object.rawData)
    offset = currentOffset
    entries.append({hash, offset, size_uncompressed, size_compressed})

// Phase 2: Sort by hash, then write SSTable
sort.Slice(entries, func(i, j) bool { return bytes.Compare(entries[i].hash, entries[j].hash) < 0 })

sstWriter = sstable.NewWriter(tmpIndexFile, sstable.Options{
    Compression: packfileCompression,
    BlockSize: 32KB,
})
for entry in entries:
    sstWriter.Add(record {Key: entry.hash[:], Value: encode(entry)})
sstWriter.Close()
```

**Approach B — External sort (object count >= 1M):**

```
// Phase 1: Stream to temp file (unsorted binary records)
tempFile = createTempFile()
for each object in packfile:
    hash = computeContentHash(object.rawData)
    writeBinaryRecord(tempFile, {hash, offset, sizes})  // ~15-50 bytes per record (varint-encoded)

// Phase 2: External sort — sort temp file runs, merge into sorted output
sortedFile = externalMergeSort(tempFile)  // k-way merge, never loads all in memory

// Phase 3: Write SSTable from sorted stream
sstWriter = sstable.NewWriter(tmpIndexFile, sstable.Options{...})
for each record in sortedFile:
    sstWriter.Add(record {Key: record.hash[:], Value: encode(record)})
sstWriter.Close()
```

**Memory usage:**
- Approach A: O(n) where n = object count (~15-50MB per 1M objects for entry metadata with varint encoding)
- Approach B: O(BlockSize) for SSTable writer; external sort uses temp files

**Manifest `is_sorted` flag:** The manifest first line may include `"is_sorted": true` if client guarantees entries are sorted by content hash. In this case, the server skips the sort phase and writes SSTable directly from the manifest. If `is_sorted` is false or omitted, the server sorts. If client claims `is_sorted: true` but entries are not actually sorted, SSTable build fails and the upload is rejected — the client is responsible for correctness.

**Index building during merge:** Merge reads objects from the **source packfiles** and writes a new packfile. For each object read, the server computes the content hash and writes to a temp file (Approach B). After all objects are read, external sort produces sorted entries, then SSTable writer writes the index in sorted order.

## 4. Packfile Manager

### 4.1 Role

**Packfile Manager** is the single authority for all packfile and packfile metadata operations. All state transitions and packfile lifecycle events go through Packfile Manager. Graveler is a client of the Packfile Manager API — it never directly manipulates packfile metadata.

**Responsibilities:**
- Manage packfile metadata in kv (create, read, update, delete)
- Manage upload sessions in kv (create, complete, abort, expire)
- Execute packfile merge (deduplication, new packfile creation, SSTable index building)
- Transition packfile statuses (STAGED → COMMITTED, COMMITTED → SUPERSEDED, etc.)
- Update PackfileSnapshot in kv atomically
- Cleanup tmp/ files when upload sessions expire or are aborted

**Packfile Manager API (operations it exposes):**

| Operation | Description |
|-----------|-------------|
| `CreateUploadSession(repo_id)` | Create kv entry with upload_id (ULID), status=in-progress, tmp_path |
| `CompleteUploadSession(upload_id)` | Mark session completed, create PackfileMetadata (STAGED, SECOND_CLASS) |
| `AbortUploadSession(upload_id)` | Delete kv entry, cleanup tmp/ files |
| `GetPackfile(packfile_id)` | Read PackfileMetadata from kv |
| `ListPackfiles(repo_id)` | List all packfiles for repository (by status) |
| `GetSnapshot(repo_id)` | Read PackfileSnapshot |
| `MergeStaged(repo_id)` | Merge all STAGED packfiles → one STAGED packfile, delete raw STAGED files |
| `Commit(repo_id)` | Transition merged STAGED → COMMITTED, update PackfileSnapshot |
| `Merge(repo_id, packfile_ids)` | Merge packfiles → new COMMITTED packfile, mark sources SUPERSEDED |
| `CleanupSuperseded()` | Scan for SUPERSEDED packfiles, delete data, transition to DELETED |

### 4.2 Commit Flow Integration

When Graveler performs a commit:

```
1. Graveler calls PackfileManager.MergeStaged(repo_id):
   - Reads all STAGED packfiles from kv snapshot
   - Deduplicates by content hash, writes one merged packfile
   - Deletes raw STAGED packfile files
   - Creates one STAGED packfile metadata (SECOND_CLASS)
   - Updates PackfileSnapshot.staged_packfile_ids = [merged_staged_id]

2. Graveler runs commit transaction:
   - Creates new metarange referencing objects from the merged STAGED packfile
   - Creates commit record

3. Graveler calls PackfileManager.Commit(repo_id):
   - Transition merged STAGED → COMMITTED, classification = FIRST_CLASS
   - Update PackfileSnapshot: active_packfile_ids = [new_comitted_id], staged_packfile_ids = []
```

If step 2 fails, the merged STAGED packfile remains in kv (can be retried or cleaned up). Raw STAGED packfiles are already deleted.

### 4.3 Merge Flow Integration

```
1. Graveler calls PackfileManager.Merge(repo_id, packfile_ids, strategy):
   - Read all source packfile metadata (must be COMMITTED)
   - Stream objects from source packfiles, deduplicate by content hash
   - Write new merged packfile + SSTable index
   - Create PackfileMetadata: status=COMMITTED, classification=SECOND_CLASS
   - Update PackfileSnapshot: active_packfile_ids = [new_merged_id]
   - Mark source packfiles: status=SUPERSEDED

2. PackfileManager.CleanupSuperseded() (async, later):
   - Deletes .pack and .sst files for SUPERSEDED packfiles
   - Transitions status: SUPERSEDED → DELETED
```

### 4.4 Upload Session Cleanup

Upload sessions in kv are scoped by TTL. A background goroutine periodically scans for expired sessions (status=in-progress, created_at < now - TTL). On expiry:
1. Delete tmp/ files for the session
2. Delete kv upload session entry

This handles client disconnects without explicit DELETE — no separate tmp/ TTL needed.

---

## 5. KV Schema

Packfile metadata uses a dedicated `packfiles` partition in kv. All packfile and upload session operations go through Packfile Manager.

### 5.1 Partition

```
Partition: "packfiles"  (registered as a kv partition type)
```

### 5.2 Protobuf Structures

```protobuf
// pkg/packfile/packfile.proto

enum PackfileStatus {
  STAGED = 0;       // Created by upload, not yet committed
  COMMITTED = 1;    // Part of a commit, first-class protection
  SUPERSEDED = 2;   // Replaced by a merged packfile, eligible for GC
  DELETED = 3;      // Data removed, metadata to be purged
}

enum PackfileClassification {
  SECOND_CLASS = 0; // Default for STAGED and merged packfiles
  FIRST_CLASS = 1;  // COMMITTED client-uploaded packfiles, never deleted during merge
}

message PackfileMetadata {
  string packfile_id = 1;           // Bare hex SHA-256 — content-addressed identity
  PackfileStatus status = 2;
  PackfileClassification classification = 3;
  string repository_id = 4;
  string storage_namespace = 5;     // For constructing full path to packfile
  int64 object_count = 6;
  int64 size_bytes = 7;            // Uncompressed total size
  int64 size_bytes_compressed = 8;
  string compression_algorithm = 9; // "zstd" | "gzip" | "none"
  string checksum_algorithm = 10;   // "sha256" | "blake3" | "xxhash"
  google.protobuf.Timestamp created_at = 11;
  repeated string source_packfile_ids = 12; // Only set for merged packfiles
}

// Snapshot of all active packfiles for a repository.
// Updated atomically on commit and merge.
message PackfileSnapshot {
  string repository_id = 1;
  repeated string active_packfile_ids = 2;   // COMMITTED packfiles containing the current committed object set
  repeated string staged_packfile_ids = 3;   // STAGED packfiles created but not yet committed
  google.protobuf.Timestamp last_commit_at = 4;
  google.protobuf.Timestamp last_merge_at = 5;
}

// Upload session for tmp/ file lifecycle management.
message UploadSession {
  string upload_id = 1;              // ULID — session token
  string packfile_id = 2;            // Bare hex SHA-256 of the packfile
  string repository_id = 3;
  string tmp_path = 4;               // Path to tmp/ staging directory
  google.protobuf.Timestamp created_at = 5;
  // status not stored separately — absence of record = abandoned
}
```

### 5.3 Key Patterns

| Key Pattern | Value Type | Description |
|-------------|------------|-------------|
| `packfile:{packfile_id}` | PackfileMetadata | Packfile metadata by ID |
| `snapshot:{repository_id}` | PackfileSnapshot | Active + staged packfile IDs for a repository |
| `upload:{upload_id}` | UploadSession | Upload session for tmp/ cleanup |

### 5.4 Status Lifecycle

```
STAGED ──────────► COMMITTED  (on PackfileManager.Commit)
   │                              │
   │ (abandoned upload)           │ (PackfileManager.Merge)
   ▼                              ▼
 DELETED                     SUPERSEDED
                                  │
                                  ▼ (PackfileManager.CleanupSuperseded)
                               DELETED
```

| Transition | Trigger | Actor |
|------------|---------|-------|
| STAGED → COMMITTED | Commit success | PackfileManager.Commit |
| STAGED → DELETED | GC of abandoned uploads | PackfileManager.Cleanup |
| COMMITTED → SUPERSEDED | Merge replaces sources | PackfileManager.Merge |
| SUPERSEDED → DELETED | GC cleanup | PackfileManager.CleanupSuperseded |

### 5.5 Classification Lifecycle

| Status | Classification | Notes |
|--------|---------------|-------|
| STAGED | SECOND_CLASS | Default on upload |
| COMMITTED | FIRST_CLASS | Promoted on commit for merged STAGED packfiles; client uploads are also FIRST_CLASS once committed |
| COMMITTED | SECOND_CLASS | Possible if a COMMITTED packfile is explicitly merged as a source |
| SUPERSEDED | (unchanged) | Inherits from COMMITTED |
| DELETED | (unchanged) | Final state |



### 4.1 Trigger Conditions

| Condition | Default | Description |
|-----------|---------|-------------|
| `max_packfiles_before_merge` | 100 | Merge when packfile count exceeds |
| `max_packfile_size` | 5GB | Exclude a packfile from merge when it exceeds |
| `max_age_before_merge` | 24h | Merge packfiles older than this |
| Manual trigger | — | Via API call |

### 4.2 Merge Algorithm

```
Input: Set of existing packfiles P = {p1, p2, ..., pn}, excluding packfiles
       that exceed packfile.max_packfile_size (too large to merge)
Output: New packfile p_new with its SSTable index file p_new.sst

1. Write new packfile data in tmp/ while deduplicating by content hash:
   a. For each object read from source packfile, check if content hash
      is already written to new packfile — skip if deduplicated
   b. For each unique object, write raw data to new packfile data region
   c. Append index entry (content hash, offset, sizes) to a tmp index record file
2. Sort index entries in tmp index record file by content hash:
   - In-memory sort if entries < threshold (default 1M)
   - File-based merge sort if entries >= threshold (avoids memory pressure)
3. Create SSTable in tmp/ from sorted index record file
4. Verify new packfile integrity (checksum)
5. Atomically rename from tmp/ to final location (p_new + p_new.sst)
6. Update packfile snapshot in kv metadata store: P → p_new
   (this batch mapping enables reading objects from p_new instead of P set)
7. Safely cleanup second-class packfiles in P (they are now superseded)

**Failure recovery:** If merge fails at any step before step 5, only the tmp/ files (new packfile + index records) are discarded. Source packfiles P are never modified or removed. The operation is idempotent — retry by restarting the merge.

**Note:** Packfiles exceeding `max_packfile_size` are excluded from merge input — they are left as-is and can be merged later when other packfiles are cleaned up, reducing their size below the threshold.

### 4.2.1 Detailed Merge Steps

#### Step 1: Streaming Deduplicate and Write

```
packfiles = [pf1, pf2, pf3]

// Open SSTable readers for all source packfile indices
readers = packfiles.map(pf => openSSTableReader(pf.sst))

// Open new packfile for writing
new_packfile = createTempPackfile()
indexRecordFile = createTempIndexRecordFile()  // binary records: hash(32) + offset + sizes

seenHashes = make(map[ContentHash]bool)  // tracks what's already written

// Iterate all source packfiles and write unique objects
for reader in readers:
    iter = reader.NewIter()
    for iter.Next():
        hash = ContentHash(iter.Key())
        if seenHashes[hash] {
            continue  // already written to new packfile, skip
        }
        seenHashes[hash] = true

        // Read object data from source packfile
        entry = iter.Value()
        data = readFromPackfile(entry.packfile_id, entry.offset, entry.size_compressed)

        // Record offset before writing
        offset = new_packfile.currentOffset()

        // Write to new packfile data region
        compressed = compress(data, compression_algorithm)
        new_packfile.writeVarint(entry.size_uncompressed)
        new_packfile.writeVarint(len(compressed))
        new_packfile.writeBytes(compressed)

        // Append to index record file
        writeBinaryRecord(indexRecordFile, {
            hash: hash,
            offset: offset,
            size_uncompressed: entry.size_uncompressed,
            size_compressed: len(compressed)
        })

new_packfile.writeChecksum(sha256)
new_packfile.close()
indexRecordFile.close()
```

**Key insight:** Deduplication happens during streaming write, not in a separate pass. Memory usage is bounded by the deduplication map (unique hashes only), not all objects.

**Memory usage:** For 1M unique objects: ~32MB for seenHashes map. Source index data is streamed.

#### Step 2: Sort Index Records

```
// Sort index records by content hash
// Use in-memory sort if count < threshold (default 1M)
sort.Sort(indexRecords)  // in-memory

// Use file-based merge sort if count >= threshold
sortedRecords = externalMergeSort(indexRecordFile)  // k-way merge
```

#### Step 3: Create SSTable from Sorted Records

```
sstWriter = sstable.NewWriter(tmpIndexFile, sstable.Options{
    Compression: packfileCompression,
    BlockSize: 32KB,
})

for each record in sortedRecords:
    sstWriter.Add(record {
        Key: record.hash[:],
        Value: encode(IndexEntry{record.offset, record.size_uncompressed, record.size_compressed})
    })

sstWriter.Close()
```

#### Step 4: Atomic Commit

```
// Verify new packfile integrity
verifyChecksum(new_packfile)

// Move from tmp/ to final location (both .pack and .sst)
rename(tmp/packfile-id/data.pack, final/packfile-id/data.pack)
rename(tmp/packfile-id/data.sst, final/packfile-id/data.sst)

// Close source SSTable readers
for reader in readers:
    reader.Close()

// Invalidate index cache for affected packfiles
cache.invalidate(pf1.id, pf2.id, pf3.id, new_packfile.id)
```

**On failure:** If any step fails, delete tmp/ files only. Source packfiles are never touched. The merge can be retried safely.

#### Step 5: Update Metadata and Cleanup

**Packfile snapshot in kv store:**

```
Before merge:
  PackfileSnapshot: {pf1, pf2, pf3}  // all source packfiles

After merge:
  PackfileSnapshot: {pf_new}         // only new merged packfile
  pf1.status = superseded
  pf2.status = superseded
```

The kv metadata store maintains a **packfile snapshot** per repository that tracks which packfiles contain the active object set. After merge, the snapshot is updated to point to the new merged packfile, and superseded second-class packfiles are marked for cleanup.

**Cleanup of superseded packfiles:**
- After successful merge and kv update, second-class packfiles in P can be safely deleted
- First-class packfiles are never deleted during merge (they remain protected)
- Cleanup happens synchronously after kv commit to avoid orphaned data

### 4.2.2 Concurrent Merge Protection

```
// Use repository-level lock to prevent concurrent merges in the same repository
lock = lockManager.GetLock(repositoryID)

// Acquire lock with timeout
if !lock.Acquire(ctx):
    return ErrMergeInProgress

defer lock.Release()

// Merge proceeds — source packfiles remain untouched until successful commit
```

**Merge conflict resolution:** If two merge operations try to merge in the same repository, the lock ensures only one succeeds at a time. The second sees "merge in progress" and can retry after the first completes. Since source packfiles are never modified until successful commit, retry is always safe. Repository-level locking is simpler than per-packfile-set locking while providing the necessary protection.

### 4.3 Cleanup of Inactive Packfiles

A separate GC process is not required. Inactive packfiles are cleaned up during merge:

```
Packfile Lifecycle:
  Created → Active (in use by ranges or packfile snapshot)
    ↓
  Superseded (replaced by merged packfile, second-class only)
    ↓
  Deleted (cleanup happens synchronously after merge commit)
```

**Why no background GC is needed:**
- Reference counting (range/métarange entries pointing to a packfile) is sufficient for merge correctness — range entries always resolve objects by content hash in the packfiles they reference
- Superseded second-class packfiles are deleted synchronously in step 7 of the merge algorithm, after the kv metadata store is updated
- First-class packfiles are never deleted during merge — they persist until explicitly removed
- The packfile snapshot in kv metadata store provides a single source of truth for the "active" object set, avoiding the need for reference counting across all range entries

**Deletion happens at merge time, not in a separate background process.**

---

## 6. Block Adapter Integration

### 5.1 Dependencies

The packfile system depends on the existing block adapter:

```go
// Existing block adapter operations used:
adapter.Put(ctx, obj, size, reader, opts)       // Write packfile + index to storage
adapter.Get(ctx, obj) -> io.ReadCloser          // Read full packfile
adapter.GetRange(ctx, obj, start, end) -> io.ReadCloser  // Range read for random access
adapter.GetPreSignedURL(...)                     // For non-packfile objects only
```

**No changes to the block adapter interface.**

### 5.2 Storage Layout via Block Adapter

```
BlockAdapter.Put(ObjectPointer{
    StorageNamespace: <ns>,
    Identifier: "_packfiles/<repo_id>/<pack_id>/data.pack",
}, data)
```

The packfile storage path uses the block adapter's namespace resolution, so it works with S3, GCS, Azure, or local filesystem backends.

### 5.3 Local-Filesystem Hybrid

For NFS deployments (common in merge-heavy workloads):

```
┌─────────────────────────────────────────────────────────┐
│ Primary Storage: Local Disk/NFS                        │
│   /var/lib/lakefs/_packfiles/...                        │
│   - Used for all writes                                 │
│   - Merge reads directly from here                      │
├─────────────────────────────────────────────────────────┤
│ Backup Storage: S3 (via block adapter)                  │
│   - Async copy from local                               │
│   - Background goroutine for each packfile             │
│   - S3 provides durability                              │
└─────────────────────────────────────────────────────────┘
```

**Configuration:**
```yaml
packfile:
  storage:
    primary: local          # "local" or "s3"
    local_path: /var/lib/lakefs/_packfiles
    async_replicate: true   # async copy to S3
```

---

## 7. Configuration

| Parameter | Default | Max | Description |
|-----------|---------|-----|-------------|
| `packfile.small_object_threshold` | 1MB | 5MB | Max object size to consider for packfile |
| `packfile.min_objects_for_pack` | 10 | — | Min objects before creating packfile |
| `packfile.max_packfiles_before_merge` | 100 | — | Trigger merge when exceeding |
| `packfile.max_packfile_size` | 5GB | — | Large packfile exclude from further merge |
| `packfile.max_age_before_merge` | 24h | — | Merge packfiles older than this |
| `packfile.default_compression` | zstd | — | Compression algorithm |
| `packfile.index_cache_size` | 1000 | — | Number of packfile indices to cache |
| `packfile.storage.local_path` | — | — | Local path for packfiles |
| `packfile.storage.async_replicate` | true | — | Async copy to S3 |

---

## 8. Error Handling

| Scenario | Handling |
|----------|----------|
| Packfile upload interrupted (client disconnects) | Cleanup tmp/ directory on abort (`DELETE .../pack/{upload_id}`) |
| Checksum mismatch during streaming | Reject upload, atomically delete tmp/ files, return error to client |
| Manifest entry count mismatch | Reject, do not create staging entries; tmp/ files already cleaned up |
| S3 sync failure | Retry with exponential backoff; local remains primary |
| Merge failure | Keep original packfiles untouched; delete tmp/ files; report error |
| Index cache miss | Fetch from S3/local; memory-map |

**Atomic cleanup guarantee:** All error paths that reject or abort an upload delete the tmp/ files before returning. No stale partial uploads remain on disk. This ensures a retry can start fresh with the same upload_id.

---

## 9. Open Questions

| Question | Status | Notes |
|----------|--------|-------|
| Packfile download (server → client) | Deferred | Not in initial scope |
| S3 Gateway integration | Deferred | Packfile objects are not S3-listable; they are lakeFS-internal |
| Metrics/monitoring | Deferred | Need visibility into packfile operations |
| Packfile stats in UI | Deferred | |

---

## 10. Implementation Plan

### Phase 1: Core Packfile
- [ ] Packfile binary format and writer (streaming)
- [ ] Packfile reader with index lookup (SSTable + Bloom filter)
- [ ] Block adapter integration for storage (local disk primary, S3 async backup)
- [ ] Basic upload API endpoints (init, upload data, upload manifest, complete, abort)
- [ ] Upload session management in kv

### Phase 2: Packfile Manager + KV + Integration
- [ ] KV schema: PackfileMetadata, PackfileSnapshot, UploadSession protos
- [ ] Packfile Manager: all state transitions, snapshot updates
- [ ] Manifest validation (checksum vs. init request)
- [ ] DBEntry schema update (PACKFILE address type, packfile_offset)
- [ ] Commit flow integration with Packfile Manager

### Phase 3: Merge & Cleanup
- [ ] Packfile merge operation (deduplication, SSTable index, atomic rename)
- [ ] COMMITTED → SUPERSEDED transitions
- [ ] SUPERSEDED → DELETED cleanup

### Phase 4: Optimization
- [ ] Index caching (LRU + memory-mapped SSTable readers)
- [ ] Async S3 replication

---

## 11. Dependencies

| Package | Role |
|---------|------|
| `pkg/packfile/` | Packfile Manager, binary format, SSTable index building |
| `pkg/block/` | Storage abstraction (unchanged) |
| `pkg/api/` | REST API endpoints |
| `pkg/graveler/staging/` | Staging manager integration |
| `pkg/kv/` | Packfile metadata and upload session storage |
| `pkg/catalog/` | DBEntry and metadata |
| `pkg/graveler/` | Commit flow, calls Packfile Manager |
