# 4. Packfile Manager

## 4.1 Role

**Packfile Manager** is the single authority for all packfile and packfile metadata operations. All state transitions and packfile lifecycle events go through Packfile Manager. Graveler is a client of the Packfile Manager API — it never directly manipulates packfile metadata.

**Responsibilities:**
- Manage packfile metadata in kv (create, read, update, delete)
- Manage upload sessions in kv (create, complete, abort, expire)
- Execute packfile merge (deduplication, new packfile creation, SSTable index building)
- Transition packfile statuses (STAGED → COMMITTED, COMMITTED → SUPERSEDED, etc.)
- Update PackfileSnapshot in kv atomically
- Cleanup tmp/ files when upload sessions expire or are aborted

**Packfile Manager API** — operations it exposes:

| Operation                          | Description                                                               |
| ---------------------------------- | ------------------------------------------------------------------------- |
| `CreateUploadSession(repo_id)`     | Create kv entry with upload_id (ULID), status=in-progress, tmp_path       |
| `CompleteUploadSession(upload_id)` | Mark session completed, create PackfileMetadata (STAGED, SECOND_CLASS)    |
| `AbortUploadSession(upload_id)`    | Delete kv entry, cleanup tmp/ files                                       |
| `GetPackfile(packfile_id)`         | Read PackfileMetadata from kv                                             |
| `ListPackfiles(repo_id)`           | List all packfiles for repository (by status)                             |
| `GetSnapshot(repo_id)`             | Read PackfileSnapshot                                                     |
| `MergeStaged(repo_id)`             | Merge all STAGED packfiles → one STAGED packfile, delete raw STAGED files |
| `Commit(repo_id)`                  | Transition merged STAGED → COMMITTED, update PackfileSnapshot             |
| `Merge(repo_id, packfile_ids)`     | Merge packfiles → new COMMITTED packfile, mark sources SUPERSEDED         |
| `CleanupSuperseded()`              | Scan for SUPERSEDED packfiles, delete data, transition to DELETED         |

## 4.2 Commit Flow Integration

When Graveler performs a commit:

```
1. Graveler calls PackfileManager.MergeStaged(repo_id):
   - Reads all STAGED packfiles from kv snapshot
   - Deduplicates by content hash, writes one merged packfile
   - Creates one STAGED packfile metadata (SECOND_CLASS)
   - **Note:** Source STAGED raw files are retained here and deleted only after the commit succeeds, to prevent data loss if the commit transaction fails.
   - Updates PackfileSnapshot.staged_packfile_ids = [merged_staged_id]
   - **Atomicity:** Snapshot and metadata updates happen in a single kv transaction. If the snapshot update fails, the kv store rolls back the metadata creation — no partial state is visible externally.

2. Graveler runs commit transaction:
   - Creates new metarange referencing objects from the merged STAGED packfile
   - Creates commit record

3. Graveler calls PackfileManager.Commit(repo_id):
   - Transition merged STAGED → COMMITTED, classification = FIRST_CLASS
   - Update PackfileSnapshot: active_packfile_ids = [new_comitted_id], staged_packfile_ids = []
   - **Atomicity:** Snapshot and metadata updates happen in a single kv transaction. If the snapshot update fails, the kv store rolls back the metadata transition — no partial state is visible externally.
   - Delete source STAGED raw packfile files (the files that were merged into the new STAGED packfile)

If step 2 fails, the merged STAGED packfile remains in kv. The source STAGED raw files are retained and can be retried or cleaned up by the TTL scanner.

## 4.3 Merge Flow Integration

```
1. Graveler calls PackfileManager.Merge(repo_id, packfile_ids, strategy):
   - Acquire repository-level lock (lockManager.GetLock(repositoryID))
   - If lock.Acquire(ctx) times out, return ErrMergeInProgress with retry-after hint
   - **Lock semantics:** If the lock manager has no distributed backend (e.g., single-node deployment), locking is a no-op. In this mode, concurrent merges on the same repository are unsafe and must be prevented by external coordination. Multi-node deployments require a distributed lock backend (Redis, etcd, or equivalent).
   - Read all source packfile metadata (must be COMMITTED)
   - Stream objects from source packfiles, deduplicate by content hash
   - Write new merged packfile + SSTable index
   - Create PackfileMetadata: status=COMMITTED, classification=SECOND_CLASS
   - Update PackfileSnapshot: active_packfile_ids = [new_merged_id]
   - Mark source packfiles: status=SUPERSEDED (FIRST_CLASS packfiles are excluded — they remain COMMITTED)
   - **Atomicity:** All kv updates (new packfile metadata, snapshot, source status) happen in a single transaction. If snapshot is updated but metadata fails, snapshot update rolls back — no partial state.
   - Release lock

2. PackfileManager.CleanupSuperseded() (async, later):
   - Deletes .pack and .sst files for SUPERSEDED packfiles
   - Transitions status: SUPERSEDED → DELETED
```

## 4.4 Upload Session Cleanup

Upload sessions in kv are scoped by TTL. A background goroutine (started on process startup, supervised for panic recovery) periodically scans for expired sessions (status=in-progress, created_at < now - TTL). On expiry:
1. Delete tmp/ files for the session
2. Delete kv upload session entry

This handles client disconnects without explicit DELETE — no separate tmp/ TTL needed.

# 5. KV Schema

## 5.1 Partition

PackfileMetadata and PackfileSnapshot are stored under **Graveler's RepoPartition** (`repo:{repository_id}`) to support fast listing of all packfiles in a repository. UploadSession remains under its own partition for session lifecycle management.

```
Partition: "packfiles"  (for UploadSession only)
Partition: Graveler's RepoPartition (for PackfileMetadata and PackfileSnapshot)
```

## 5.2 Protobuf Structures

```protobuf
// pkg/graveler/packfile/packfile.proto

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

## 5.3 Key Patterns

PackfileMetadata and PackfileSnapshot keys are prefixed within Graveler's RepoPartition namespace. UploadSession uses its own partition.

| Key Pattern | Value Type | Description |
|-------------|------------|-------------|
| `packfile:{repository_id}:{packfile_id}` | PackfileMetadata | Packfile metadata by ID (under RepoPartition) |
| `snapshot:{repository_id}` | PackfileSnapshot | Active + staged packfile IDs for a repository (under RepoPartition) |
| `upload:{upload_id}` | UploadSession | Upload session for tmp/ cleanup (packfiles partition) |

## 5.4 Status Lifecycle

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
| STAGED → DELETED | GC of abandoned uploads | PackfileManager.Cleanup (tmp/ files deleted synchronously with kv transition) |
| COMMITTED → SUPERSEDED | Merge replaces sources (FIRST_CLASS packfiles are excluded) | PackfileManager.Merge |
| SUPERSEDED → DELETED | GC cleanup | PackfileManager.CleanupSuperseded |

## 5.5 Classification Lifecycle

| Status | Classification | Notes |
|--------|---------------|-------|
| STAGED | SECOND_CLASS | Default on upload |
| COMMITTED | FIRST_CLASS | Client-uploaded packfiles; also merged STAGED packfiles promoted on commit |
| COMMITTED | SECOND_CLASS | Possible if a COMMITTED packfile is explicitly merged as a source |
| SUPERSEDED | (unchanged) | Inherits from COMMITTED |
| DELETED | (unchanged) | Final state |

**First-class deletion rule:** FIRST_CLASS packfiles are never deleted during merge. They may be merged as source packfiles (producing a SECOND_CLASS merged result), but the original FIRST_CLASS packfile is never deleted or transitioned to SUPERSEDED.

---

# 6. Block Adapter Integration

## 6.1 Dependencies

The packfile system depends on the existing block adapter:

```go
// Existing block adapter operations used:
adapter.Put(ctx, obj, size, reader, opts)       // Write packfile + index to storage
adapter.Get(ctx, obj) -> io.ReadCloser          // Read full packfile
adapter.GetRange(ctx, obj, start, end) -> io.ReadCloser  // Range read for random access
adapter.GetPreSignedURL(...)                     // For non-packfile objects only
```

**No changes to the block adapter interface.**

## 6.2 Storage Layout via Block Adapter

```
BlockAdapter.Put(ObjectPointer{
    StorageNamespace: <ns>,
    Identifier: "_packfiles/<repo_id>/<pack_id>/data.pack",
}, data)
```

The packfile storage path uses the block adapter's namespace resolution, so it works with S3, GCS, Azure, or local filesystem backends.

## 6.3 Local-Filesystem Hybrid

For NFS deployments — common in merge-heavy workloads:

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

# 7. Configuration

| Parameter | Default | Max | Description |
|-----------|---------|-----|-------------|
| `packfile.small_object_threshold` | 1MB | 5MB | Max object size to consider for packfile |
| `packfile.min_objects_for_pack` | 10 | — | Min objects before creating packfile |
| `packfile.max_packfiles_before_merge` | 100 | — | Trigger merge when exceeding |
| `packfile.max_packfile_size` | 5GB | — | Large packfile exclude from further merge |
| `packfile.max_age_before_merge` | 24h | — | Merge packfiles older than this |
| `packfile.default_compression` | zstd | — | Compression algorithm |
| `packfile.index_cache_size` | 1000 | — | Number of packfile indices to cache |
| `packfile.sstable_block_size` | 32KB | — | SSTable index block size |
| `packfile.upload_session_ttl` | 24h | 7d | Upload session timeout before cleanup |
| `packfile.storage.local_path` | — | — | Local path for packfiles |
| `packfile.storage.async_replicate` | true | — | Async copy to S3 |

---

# 8. Error Handling

| Scenario | Handling |
|----------|----------|
| Packfile upload interrupted (client disconnects) | Cleanup tmp/ directory on abort (`DELETE .../pack/{upload_id}`) |
| Checksum mismatch during streaming | Reject upload, atomically delete tmp/ files, return error to client |
| Manifest entry count mismatch | Reject, do not create staging entries; tmp/ files already cleaned up |
| S3 sync failure | Retry with exponential backoff (1s, 2s, 4s, 8s), max 10 retries; after 10 failures, mark replication FAILED. A background reconciliation loop periodically re-checks FAILED replications and retries them with fresh exponential backoff. Local disk remains primary. |
| Merge failure | Keep original packfiles untouched; delete tmp/ files; report error |
| Index cache miss | Fetch from S3/local; memory-map |

**Atomic cleanup guarantee:** All error paths that reject or abort an upload delete the tmp/ files before returning. No stale partial uploads remain on disk. This ensures a retry can start fresh with the same upload_id.

---

# 9. Open Questions

| Question                            | Status   | Notes                                                          |
| ----------------------------------- | -------- | -------------------------------------------------------------- |
| Packfile download (server → client) | Deferred | Not in initial scope                                           |
| S3 Gateway integration              | Deferred | Packfile objects are not S3-listable; they are lakeFS-internal |
| Metrics/monitoring                  | Deferred | Need visibility into packfile operations                       |
| Pre-signed URL for large packfiles  | Open     | Would reduce server load for large uploads; evaluate tradeoff   |
| Multi-node NFS rename failure        | Open     | The `_packfiles/` tree must reside on a single NFS mount for atomic rename to work. On multi-node deployments where tmp/ and final reside on different NFS mounts, the rename from tmp/ to final location may silently fail on some nodes, leaving packfile data on one node and metadata pointing to a path that is inaccessible from other nodes. Mitigation options: (a) enforce that the entire `_packfiles/` tree is on a shared NFS mount, (b) use a copy + delete protocol instead of atomic rename (requires additional error handling), (c) use a distributed filesystem (e.g., GlusterFS) that guarantees rename atomicity across nodes. |
| Repository deletion                  | Open     | Packfiles left orphaned until GC runs; not deleted synchronously |
| Footer checksum includes itself       | Resolved | Footer checksum excludes the 32 checksum bytes themselves. Computation covers header + all object data. Implementation computes the checksum before writing the footer. |
| Per-object size limited to ~4GB      | Resolved | Pack objects are constrained by `bigFileThreshold` (configurable, default 1MB–5MB per the Configuration section). The uint32 field limit of ~4GB is not a practical constraint given this threshold; objects exceeding the threshold are not placed in packfiles. |
| PackfileSnapshot has no version field | Resolved | Add a `version` (uint64) field to PackfileSnapshot. Use optimistic concurrency control: on commit/merge, read the current version, increment and write with the new version. If the kv store returns a conflict on write, retry the operation. |
| KV partition split — cross-partition atomicity | Resolved | No atomic multi-key update across partitions. CompleteUploadSession performs two separate operations: (1) delete UploadSession from "packfiles" partition, (2) create PackfileMetadata in RepoPartition. These are not atomic. On failure after step 1 succeeds, the UploadSession is gone but PackfileMetadata is not created; the tmp/ files remain orphaned until TTL cleanup. Accept this as a known limitation; a future improvement would move UploadSession into RepoPartition. |
| L1 cache eviction while readers hold open handles | Resolved | L1 eviction must wait for all open PackfileHandle references to close before calling Close() on the sstable.Reader and deleting the .sst file. Use reference counting: increment on PackfileHandle.Open(), decrement on PackfileHandle.Close(). Eviction only proceeds when the refcount reaches zero. |
| Disk exhaustion during external sort — partial data cleanup | Resolved | If Phase 2 (external sort) fails due to insufficient disk space, the temp sort file from Phase 1 must be deleted before returning the error. The packfile's tmp/ data is cleaned up separately by the upload session TTL mechanism; the sort temp file is cleaned up by the sort failure handler. |
| Concurrent CompleteUploadSession race | Open | Two concurrent POST /complete calls for the same upload_id: one succeeds, one gets 409 CONFLICT. The semantics during the race window are unspecified — if the first call transitions the session but the second arrives before the kv entry is fully updated, the behavior is undefined. Idempotency guarantees should be specified. |
| S3 replication goroutine crash — no checkpoint | Open | The async S3 replication goroutine has no checkpoint or dirty-bit mechanism. If the process restarts mid-copy, the packfile is not retried unless CleanupSuperseded happens to encounter it. A packfile that was being replicated at shutdown may never replicate to S3. |

---

# 10. Dependencies

| Package | Role |
|---------|------|
| `pkg/graveler/packfile/` | PackfileManager, KV wrappers, protobuf definitions |
| `pkg/packfile/` | Binary format, SSTable index building, physical storage |
| `pkg/block/` | Storage abstraction (unchanged) |
| `pkg/api/` | REST API endpoints |
| `pkg/graveler/staging/` | Staging manager integration |
| `pkg/kv/` | Packfile metadata and upload session storage |
| `pkg/catalog/` | DBEntry and metadata |
| `pkg/graveler/` | Commit flow, calls Packfile Manager |
