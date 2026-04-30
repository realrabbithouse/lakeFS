# 1. Packfile Format

## 1.1 Binary Layout

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
│ the staging entry, not the packfile itself. The        │
│ packfile is content-agnostic binary data.              │
├─────────────────────────────────────────────────────────┤
│ Footer Checksum (32 bytes) — SHA-256 of all above             │
└─────────────────────────────────────────────────────────┘
```

**Design notes:**
- Packfile is immutable after creation (written once, never modified)
- "Append-only" means objects are written sequentially during creation — no seeking back
- All random-access metadata lives in the separate index file (.sst)
- Footer checksum covers header + all object data (each object's content hash is embedded in the data region for fast index building)
- **Packfile ID** is the footer checksum (SHA-256) of the packfile, used as the content-addressed identity
- **Classification**: First-class (0x01) packfiles are COMMITTED and protected from deletion during merge. Second-class (0x02) packfiles are STAGED or merged, eligible for GC when new packfile is built
- New objects cannot be appended to an existing packfile; instead, create a new packfile and merge later

**Binary validation:**
- Reject packfile if object_count == 0
- Reject if compressed_size for any object exceeds its size_uncompressed (corrupt or attacker)
- Reject if declared total size exceeds actual data received during streaming
- Enum fields (checksum_algo, compression_algo, classification) must be within defined range; reject on unrecognized value

## 1.2 Index File (`.sst`)

The index file provides all metadata needed for random access. It is the **only** source of truth for looking up objects within a packfile. Index file design is **pluggable** — two implementations are supported:

### Solution 1: Custom Binary Index (reference only)

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

### Solution 2: SSTable-Style Index (preferred, only solution implemented)

The index is stored as an SSTable using `github.com/cockroachdb/pebble/sstable`. This is the same format lakeFS's Graveler uses for range and metarange storage.

**Index entry:**
- Key: Content Hash (32 bytes)
- Value: three varints — `offset`, `size_uncompressed`, `size_compressed`

**Properties used:**
- Bloom filter for O(1) fast negative lookups (object not in packfile)
- Block-level compression (same algorithm as packfile)
- 32KB block size (configurable)

The pebble/sstable package handles all internal structure (index blocks, filter blocks, footer) directly.

**Implementation:** Only Solution 2 (SSTable) will be implemented. Solution 1 is documented for reference.

**Why separate index file?**
- Packfile stays simple (append-only stream)
- SSTable index is block-compressed, very compact
- Bloom filter enables fast negative lookups
- Graveler's existing Pebble/SSTable infrastructure is reused

## 1.3 Storage Path

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

**Constraint:** The `_packfiles/` tree (both tmp/ and final packfile directories) must reside on the same filesystem/mount for atomic rename to work correctly. For NFS deployments, the entire `_packfiles/` tree must be on a single NFS mount.

## 1.4 Address Encoding

For objects stored in packfiles, the `catalog.Entry` protobuf is extended as follows:

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
    // NEW: uncompressed size for reading from block store's GetRange
    int64 size_uncompressed = 9;
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
    Size            int64    // compressed size for packfile objects
    Checksum        string   // content hash (stored as hex string, no prefix)
    Metadata        Metadata
    Expired         bool
    AddressType     AddressType  // AddressTypePackfile = 3
    ContentType     string
    // NEW: offset within packfile (valid when AddressType == AddressTypePackfile)
    PackfileOffset  int64
    // NEW: uncompressed size for block store's GetRange
    SizeUncompressed int64
}
```

**ContentHash typed type** (new type in `pkg/packfile/`):

```go
// ContentHash represents a SHA-256 content hash.
// For packfiles, the ContentHash IS the packfile_id (content-addressed).
// Binary format: raw 32-byte SHA-256. API format: bare hex string (64 chars).
// Packfile ID = footer checksum = bare hex string via fmt.Sprintf("%x", h[:])
type ContentHash [32]byte

// String returns the content hash as lowercase hex string.
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
```

**Why a typed type**
- Compile-time type safety prevents accidentally passing `[32]byte` where a `ContentHash` is expected
- `String()` returns bare hex (no prefix)
- Clear semantic intent: this is a content address, not arbitrary bytes

**Staging manager integration:**

The `graveler.Value` wraps the serialized `Entry` proto. When reading a staged entry:
1. Deserialize `Value.Data` → `Entry`
2. If `Entry.address_type == PACKFILE`:
   - Extract `pack_id` from `Entry.address` (format: `packfile:<pack_id>`)
   - Extract `offset` from `Entry.packfile_offset`
   - Use packfile reader to fetch object data

---
