# Pack Transfer Protocol Design

## Overview

Implement a pack transfer protocol for lakeFS to drastically improve transfer efficiency for small objects. Instead of transferring objects one by one, clients bundle multiple small objects into a packfile for efficient bulk transfer.

**Design Goals:**
- Improve upload efficiency for many small objects (client-server sync)
- Improve download efficiency (read from packfiles)
- Enable content-addressable deduplication within repository
- Backward compatible with existing range/meta-range architecture

---

## 1. Packfile Format & Storage Layer

### 1.1 Packfile Binary Format

```
┌─────────────────────────────────────────┐
│ Header (24 bytes)                       │
│   - Magic: "LKP1" (4 bytes)             │
│   - Version: uint16 (2 bytes)           │
│   - Checksum Algorithm: uint8 (1 byte)  │
│     0 = SHA-256, 1 = BLAKE3, 2 = xxHash │
│   - Compression Algorithm: uint8 (1 byte)│
│     0 = none, 1 = zstd, 2 = gzip, 3 = lz4│
│   - Reserved (2 bytes)                  │
│   - Object Count: varint                │
│   - Total Size (uncompressed): varint   │
│   - Total Size (compressed): varint     │
│   - Reserved (8 bytes)                  │
├─────────────────────────────────────────┤
│ Object Data (append-only, compressed)   │
│   For each object (in hash order):      │
│   - Object Size (uncompressed): varint  │
│   - Compressed Size: varint             │
│   - Compressed Data                     │
├─────────────────────────────────────────┤
│ Object Table (for random access)        │
│   For each object:                      │
│   - Content Hash: fixed (32 bytes)      │
│   - Offset: varint                      │
│   - Uncompressed Size: varint           │
│   - Compressed Size: varint             │
├─────────────────────────────────────────┤
│ Checksum (32 bytes, algorithm from header)│
└─────────────────────────────────────────┘
```

### 1.2 Index File (SSTable-style fan-out)

```
┌─────────────────────────────────────────┐
│ Header (12 bytes)                       │
│   - Magic: "LKIX" (4 bytes)             │
│   - Version: uint16 (2 bytes)           │
│   - Checksum Algorithm: uint8           │
│   - Fan-out Table Entries: uint32       │
│   - Object Count: uint32                │
├─────────────────────────────────────────┤
│ Fan-out Table (256 entries)             │
│   For each prefix (0-255):              │
│   - Offset to first object with prefix  │
│   - Object count for prefix             │
├─────────────────────────────────────────┤
│ Sorted Object Index                     │
│   - Content Hash (32 bytes)             │
│   - Offset in packfile (varint)         │
│   - Uncompressed Size (varint)          │
│   - Compressed Size (varint)            │
├─────────────────────────────────────────┤
│ Checksum (32 bytes)                     │
└─────────────────────────────────────────┘
```

### 1.3 Storage Path (block adapter)

```
s3://<bucket>/_packfiles/
  └── <repository_id>/
      └── <pack_id>/
          ├── data.pack
          └── data.idx
```

### 1.4 Append-Only Behavior

- Packfile is written sequentially, no seeking back
- Object Table appended at the end after all data written
- Allows streaming upload: client sends chunks, server appends to packfile
- Resumable uploads: store upload offset in metadata, resume from there

---

## 2. Version Control Integration

### 2.1 Design Principle

Packfile objects are content-addressable (by SHA-256 hash) and integrate with the existing Graveler architecture through the staging manager.

### 2.2 Object Lifecycle

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Client       │    │ Packfile     │    │ Staging      │
│ bundles      │───▶│ Upload       │───▶│ Manager      │
│ objects      │    │ (to block)   │    │ (per-branch) │
└──────────────┘    └──────────────┘    └──────────────┘
                                                │
                                                ▼ (on commit)
                    ┌──────────────┐    ┌──────────────┐
                    │ Range Files  │◀───│ MetaRange    │
                    │ (committed)  │    │ (committed)  │
                    └──────────────┘    └──────────────┘
```

### 2.3 DBEntry Schema (Backward Compatible)

```go
type DBEntry struct {
    CommonLevel     bool
    Path            string
    PhysicalAddress string   // "data/objects/repo/..." or "packfile:<id>"
    CreationDate    time.Time
    Size            int64
    Checksum        string   // content hash
    Metadata        Metadata
    Expired         bool
    AddressType     AddressType
    ContentType     string
    // NEW: offset in packfile (for AddressTypePackfile)
    Offset          int64
}
```

### 2.4 New AddressType

```go
const (
    AddressTypeRelative AddressType = 1
    AddressTypeFull     AddressType = 2
    AddressTypePackfile AddressType = 3  // NEW
)
```

### 2.5 PhysicalAddress Encoding

```
# Direct objects:
PhysicalAddress = "data/objects/repo/..." (randomly generated)

# Packfile objects:
PhysicalAddress = "packfile:<pack_id>"
# Offset stored in DBEntry.Offset field
```

### 2.6 Packfile Merge (Transparent to Ranges)

- Objects in packfiles are content-addressable by checksum (SHA-256)
- When packfiles are merged, the new packfile contains all objects
- Range entries still reference the new packfile ID
- On read: look up object by content hash in the new packfile
- Range files are immutable — no updates needed during merge

---

## 3. API & Protocol

### 3.1 Design Goals

- Streaming packfile upload with pre-signed URLs
- Objects land in staging area after upload completes
- Manifest file provides entry metadata (path, content-type, user metadata)
- Server deduplication only on packfile merge (not on initial upload)

### 3.2 Manifest Format (JSON)

```json
{
  "version": 1,
  "packfile_checksum": "sha256:...",
  "object_count": 100,
  "entries": [
    {
      "path": "images/logo.png",
      "checksum": "sha256:abc123...",        // content hash for validation
      "content_type": "image/png",
      "metadata": { "user-key": "user-value" },
      "relative_offset": 4096,
      "size_uncompressed": 12345,
      "size_compressed": 6789
    }
  ]
}
```

### 3.3 API Endpoints

```
# Initialize packfile upload (sends manifest)
POST /repositories/{repository}/objects/pack
Request: {
    manifest: {
        entries: [{ path, checksum, content_type, metadata, relative_offset, ... }]
    },
    packfile_info: {
        size: int64,
        checksum: "sha256:...",
        compression: "zstd" | "gzip" | "none"
    }
}
Response: {
    upload_id: string,
    packfile_url: string,    # pre-signed URL for packfile
    manifest_url: string     # pre-signed URL for manifest
}

# Upload packfile
PUT {packfile_url}
Body: binary packfile data

# Upload manifest
PUT {manifest_url}
Body: JSON manifest

# Complete import
POST /repositories/{repository}/objects/pack/{upload_id}/complete
Request: {}
Response: { staged_entries: [...] }

# Abort upload
DELETE /repositories/{repository}/objects/pack/{upload_id}

# Check known objects (batch dedup query)
POST /repositories/{repository}/objects/pack/resolve
Request: { content_hashes: ["sha256:...", ...] }
Response: {
    missing: ["sha256:...", ...],  // server doesn't have these
    known: ["sha256:...", ...]     // server already has these
}

# Trigger packfile merge (optional)
POST /repositories/{repository}/objects/pack/merge
Request: { packfile_ids?: [...], strategy?: "auto" | "eager" }
Response: { new_packfile_id: string, merged_count: int }
```

### 3.4 Protocol Flow

```
Client                                      Server
  │                                           │
  │──── POST /pack (send manifest) ──────────▶│
  │<─── { upload_id, packfile_url, manifest_url } │
  │                                           │
  │──── PUT {packfile_url} ──────────────────▶│
  │    (upload packfile data)                 │
  │                                           │
  │──── PUT {manifest_url} ──────────────────▶│
  │    (upload manifest)                      │
  │                                           │
  │──── POST /pack/{upload_id}/complete ─────▶│
  │<─── { staged_entries[] } ─────────────────│
```

### 3.5 Client-Side Workflow

```
1. lakectl ls <repo>/<branch> → get server entry list
2. local ls → get local file list
3. Diff → identify new/modified files
4. Filter by size threshold (default < 1MB, max 5MB, configurable)
5. If count >= threshold (e.g., 10 objects):
   a. Optional: POST /resolve with content hashes → filter already-known
   b. Create packfile locally (bundle objects)
   c. POST /objects/pack → get upload URLs
   d. PUT packfile_url with packfile data
   e. PUT manifest_url with manifest
   f. POST /complete → import to staging
   g. lakectl commit → commit to branch
6. Else: use existing direct upload
```

### 3.6 Server-Side Processing (on /complete)

1. Validate packfile checksum
2. Validate manifest entry count matches packfile object count
3. For each manifest entry:
   - Read object from packfile at relative_offset
   - Compute content hash, validate against entry.checksum
   - Create staging entry: path → { content_hash, offset, packfile_id }
4. Update staging manager
5. (Later) Packfile merge triggered by:
   - Too many packfiles (e.g., > 100)
   - Periodic GC
   - Manual API call

---

## 4. Configuration

| Parameter | Default | Max | Description |
|-----------|---------|-----|-------------|
| packfile.small_object_threshold | 1MB | 5MB | Max object size to consider for packfile |
| packfile.min_objects_for_pack | 10 | - | Min objects before creating packfile |
| packfile.max_packfiles_before_merge | 100 | - | Trigger merge when exceeding |
| packfile.default_compression | zstd | - | Compression algorithm |

---

## 5. Open Questions (Deferred)

- How to handle packfile download (server → client) efficiently?
- How to expose packfile stats in UI?
- How to integrate with S3 Gateway (should packfile objects be S3-listable)?
- What metrics/monitoring for packfile operations?

---

## 6. Dependencies

- `pkg/block/` — block adapter for packfile storage
- `pkg/api/` — REST API endpoints
- `pkg/graveler/staging/` — staging manager integration
- `pkg/catalog/model.go` — DBEntry schema update