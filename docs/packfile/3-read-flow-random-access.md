# 3. Read Flow (Random Access)

## 3.1 Index Lookup

Given a content hash, find an object in a packfile using the SSTable index:

```
1. Open SSTable reader on .sst file
2. Check bloom filter for the content hash → O(1) negative lookup
   (if bloom says no, object definitely not in packfile, skip to step 7)
3. Seek SSTable reader to the content hash key
4. Read index entry: { offset, size_uncompressed, size_compressed }
5. Seek to offset in packfile data region
6. Read compressed_size bytes, decompress and return (using a streaming API — return a decompressed reader)
7. Not found (bloom negative or SSTable miss) → return not found
```

**Lookup cost:**
- Bloom filter: O(1) for both hit and negative lookup
- SSTable seek: O(log n) where n is objects in the packfile
- S3 range request for the compressed object data

**SSTable (Sorted String Table) reader:** Uses `github.com/cockroachdb/pebble/sstable` Reader. Supports:
- Bloom filter lookup (fast negative)
- Key seeking via binary search within index blocks
- Block-level compression (same as packfile)

**Packfile Reader interface:**
```go
type PackfileReader interface {
    // Open a packfile by packfile_id. Looks up packfile location from kv
    // and opens the appropriate SSTable reader.
    Open(packfile_id ContentHash) (*PackfileHandle, error)
}

type PackfileHandle struct {
    SSTableReader *sstable.Reader  // owned by handle, caller does not close
    DataFile      *os.File          // owned by handle, caller does not close
}

// GetObject reads and decompresses a single object by content hash.
// Returns ErrObjectNotFound if hash not in index.
GetObject(contentHash ContentHash) (io.ReadCloser, error)

// Close releases all resources (both readers and file handles).
Close() error
```

**Open contract:** `PackfileReader.Open` first checks L1 cache; on miss, opens from L2. If not on L2 and not on L3, returns `ErrPackfileUnavailable`. L3 fetch copies to L2 after opening.

## 3.2 Caching Strategy

### Cache Hierarchy

```
┌─────────────────────────────────────────────────────────┐
│ L1: In-Memory SSTable Reader Cache                      │
│   Capacity: configurable (default 1000 indices)         │
│   Key: packfile_id (bare hex SHA-256)                  │
│   Value: *sstable.Reader (opened SSTable reader)        │
│   TTL: none (evicted via LRU)                          │
├─────────────────────────────────────────────────────────┤
│ L2: Local Disk / NFS — Primary Storage                  │
│   Path: <storage_namespace>/_packfiles/<repo_id>/       │
│   .sst SSTable files and .pack data stored here        │
│   Packfiles always written to and read from L2 first   │
├─────────────────────────────────────────────────────────┤
│ L3: Object Storage (S3/GCS/Azure) — Async Replication   │
│   Written by background goroutine after tmp→final move │
│   Replication is decoupled from commit                 │
│   Read path: L2 miss → L3 fetch → cache to L2         │
│   Not the primary read target for merge                │
└─────────────────────────────────────────────────────────┘
```

**Note:** L2 is primary storage, not a cache in the traditional sense. All writes go to L2 first. Reads prefer L2 but fall back to L3 on miss. L1 is the only true cache (in-memory SSTable reader LRU).

### L1 Eviction Protocol

When LRU evicts an entry:
1. Call `Close()` on the `*sstable.Reader`
2. Remove entry from `sync.Map`
3. Delete underlying `.sst` file from disk

If `Close()` fails, log error and proceed with deletion — file handle leak is worse than potential data loss during cleanup.

### Cache Invalidation

| Event                 | Action                                                   |
| --------------------- | -------------------------------------------------------- |
| Packfile merged       | Invalidate indices for source packfiles and new packfile |
| Packfile deleted (GC) | Remove from L1, OS handles L2 (file deletion)            |
| Index file updated    | Invalidate and reload                                    |

**Note:** Packfiles are immutable, so cache invalidation only happens on merge or deletion. No staleness issues.

## 3.3 Large-Scale Index Building

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
// Requires temp disk space of at least 2× the packfile size. Check available space
// before starting; if insufficient, abort and return error.
sortedFile = externalMergeSort(tempFile)  // k-way merge, never loads all in memory

// Phase 3: Write SSTable from sorted stream
sstWriter = sstable.NewWriter(tmpIndexFile, sstable.Options{...})
for each record in sortedFile:
    sstWriter.Add(record {Key: record.hash[:], Value: encode(record)})
sstWriter.Close()
```

**Disk space requirement:** External sort requires temp disk space of at least 2× the packfile size. If insufficient space is available before Phase 2 begins, abort and return error.

**Memory usage:**
- Approach A: O(n) where n = object count (~15-50MB per 1M objects for entry metadata with varint encoding)
- Approach B: O(BlockSize) for SSTable writer; external sort uses temp files

**Manifest `is_sorted` flag:** The manifest first line may include `"is_sorted": true` if client guarantees entries are sorted by content hash. In this case, the server skips the sort phase and writes SSTable directly from the manifest. If `is_sorted` is false or omitted, the server sorts. If client claims `is_sorted: true` but entries are not actually sorted, SSTable build fails and the upload is rejected — the client is responsible for correctness.

**Index building during merge:** Merge reads objects from the **source packfiles** and writes a new packfile. For each object read, the server computes the content hash and writes to a temp file (Approach B). After all objects are read, external sort produces sorted entries, then SSTable writer writes the index in sorted order.
