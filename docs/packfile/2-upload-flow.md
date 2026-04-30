# 2. Upload Flow

## 2.1 Client-Side Workflow

```
1. Diff → identify new/modified files
2. Filter by size threshold (default < 1MB, max 5MB)
3. If count >= min_objects_for_pack (default 100):
   a. Optional: POST /resolve with content hashes → filter already-known
   b. Create packfile locally: bundle objects sequentially, compute per-object offsets
   c. POST /objects/pack → get upload URLs
   d. PUT manifest_url with manifest (Newline-delimited JSON)
   e. PUT packfile_url with streaming packfile data
   f. POST /complete → import to staging
   g. lakectl commit → commit to branch
4. Else: use existing direct upload
```

## 2.2 Server-Side Upload (Streaming)

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

**Upload sequence (ordered):**
1. Server receives POST, validates packfile_info (size, checksum, compression). Creates an upload session entry in kv (status=in-progress). `upload_id` is a ULID.
2. Client uploads manifest via `PUT manifest_url` (server saves manifest JSON file directly to tmp/ directory, no buffering in memory)
3. Client uploads packfile data via `PUT packfile_url` (streaming to local disk at tmp/ path)
4. As packfile data arrives, the server reads each object's embedded hash from the data stream and writes an SSTable index entry; if `validate_object_checksum` is `true`, it also compares the computed hash of each object's raw data against the embedded hash (data integrity check)
5. Background goroutine asynchronously copies packfile + index to S3 using block adapter's `Put` API. Retry with exponential backoff (1s, 2s, 4s, 8s, max 10 retries). After 10 consecutive failures, log error and stop — do not block indefinitely. Mark packfile replication as FAILED; alert if configured. Local disk remains primary.
6. On `/complete`: server validates manifest entries against the SSTable index (built during streaming); if `validate_object_checksum` was `true` and a hash mismatch was detected during streaming, the upload is rejected. On success, the upload session in kv is marked completed and the packfile metadata in kv transitions to STAGED.

**Checksum mismatch during streaming**
- If `validate_object_checksum: true` and a mismatch is detected, the server immediately stops accepting data
- tmp/ files (partial packfile data + partial index) are deleted before returning the error to the client
- The rejection is atomic — no stale tmp files remain

**Upload session cleanup**
- The upload session lives in kv with status=in-progress
- On explicit `DELETE .../pack/{upload_id}`: tmp/ files are deleted and kv session is removed
- On client disconnect without DELETE, the kv session eventually expires (a background process scans for expired sessions), triggering tmp/ cleanup

**Why this approach**
- All data flows through the server, enabling validation (see Design Principles §2-4)
- Local disk is always in sync for merge operations (see Design Principle §2)
- S3 provides durability with async replication, decoupled from commit (see Design Principle §2)
- The block adapter's simple `Put` API handles S3 writes (no multipart complexity needed for packfiles)
- The index is generated server-side as the packfile streams in, so no second pass is needed

## 2.3 API Endpoints

```
# Initialize packfile upload
POST /repositories/{repository}/objects/pack
Request: {
    packfile_info: {
        size: int64,
        checksum: "...",      // bare hex, no algorithm prefix
        compression: "zstd" | "gzip" | "none"
    },
    validate_object_checksum: boolean
}
Response: {
    upload_id: string,
    packfile_url: string,    # internal endpoint for streaming upload
    manifest_url: string     # internal endpoint for manifest
}
```

**Always validated:** `packfile_info.size`, `packfile_info.checksum`, `object_count` (against manifest metadata line), manifest `packfile_checksum` (must match `packfile_info.checksum` from init request).
**Conditionally validated (when `validate_object_checksum: true`):** as each object arrives, server reads the raw object data, computes its content hash, and compares against the **embedded** content hash in the packfile data region. This verifies the packfile data was not corrupted in transit. Mismatch → upload rejected. When `false`, embedded hashes are still read to build the index, but no data integrity check is performed. The manifest entry checksum is used only for deduplication via `POST /resolve`, not for data integrity validation.

### Upload packfile (streaming)
PUT /repositories/{repository}/objects/pack/{upload_id}/data
Body: binary packfile data (streaming)

### Upload manifest
PUT /repositories/{repository}/objects/pack/{upload_id}/manifest
Body: JSON manifest

### Complete import
POST /repositories/{repository}/objects/pack/{upload_id}/complete
Request: {}   // empty body
Response: { staged_entries: [...] }

### Abort upload
DELETE /repositories/{repository}/objects/pack/{upload_id}

### Check known objects (batch dedup query)
POST /repositories/{repository}/objects/pack/resolve
Request: { content_hashes: ["<hex>", ...] }  // max 10,000 hashes per request, bare hex
Response: {
    missing: ["<hex>", ...],
    known: ["<hex>", ...]
}

### Trigger Packfile Merge
POST /repositories/{repository}/objects/pack/merge
Request: { packfile_ids?: [...], strategy?: "auto" | "eager" }  // max 100 packfile_ids
Response: { new_packfile_id: string, merged_count: int }

**Note:** No pre-signed URLs for packfile uploads. All data streams through the lakeFS server for validation.

## 2.4 Detailed API Shapes

### Init Packfile Upload

```
POST /repositories/{repository}/objects/pack
```
**Request:**
```json
{
  "packfile_info": {
    "size": 1048576,
    "checksum": "abc123...",  // bare hex, no algorithm prefix
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

### Upload Packfile Data (Streaming)

```
PUT /repositories/{repository}/objects/pack/{upload_id}/data
Content-Type: application/octet-stream
Transfer-Encoding: chunked

<binary packfile data>
```

**Response (204 No Content)**

### Upload Manifest

```
PUT /repositories/{repository}/objects/pack/{upload_id}/manifest
Content-Type: application/x-ndjson

{"packfile_checksum":"abc123...","object_count":100}
{"path":"images/logo.png","checksum":"abc123...","content_type":"image/png","metadata":{"user-key":"user-value"},"relative_offset":4096,"size_uncompressed":12345,"size_compressed":6789}
{"path":"images/bg.png","checksum":"def456...","content_type":"image/png","metadata":{},"relative_offset":8192,"size_uncompressed":23456,"size_compressed":11234}
...
```

**Format:** Newline-delimited JSON (NDJSON). First line is overall metadata. Each subsequent line is one entry's metadata. All checksums are bare hex strings (no algorithm prefix).

**Entry fields:**
- `path`: object path in the repository
- `checksum`: content hash of the raw (uncompressed) object data, bare hex
- `content_type`: MIME type of the object
- `metadata`: user-defined key-value pairs
- `relative_offset`: byte offset of this object's compressed data relative to the start of the packfile data region
- `size_uncompressed`: uncompressed size of the object in bytes
- `size_compressed`: compressed size of the object in the packfile

**Manifest parser rules:**
- Reject if the metadata line is missing or is not valid JSON
- Reject if `object_count` in metadata line is 0 when objects expected
- Reject if subsequent lines are not valid JSON (no trailing commas, no blank lines, escaped characters handled)
- Reject if entry line missing required fields (`path`, `checksum`)
- Reject if `object_count` in the metadata line does not equal the number of entry lines that follow
- Log and reject on parse error; do not attempt recovery

**Response (204 No Content)**

### Complete Packfile Upload

```
POST /repositories/{repository}/objects/pack/{upload_id}/complete
```

**Request:** `{}` (empty body)

On success, the upload session kv entry transitions to completed, packfile metadata transitions to STAGED, and staging entries are created. Commit is independent of S3 replication — the background goroutine replicates to S3 at any time after the tmp/ → final location rename.

**Response (200 OK):**
```json
{
  "packfile_id": "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
  "object_count": 100,
  "size_bytes": 1048576,
  "compression": "zstd",
  "status": "STAGED"
}
```

### Abort Packfile Upload

```
DELETE /repositories/{repository}/objects/pack/{upload_id}
```

**Response (204 No Content)**

### Resolve Content Hashes (Batch Deduplication Check)

```
POST /repositories/{repository}/objects/pack/resolve
```

**Request:**
```json
{
  "content_hashes": [
    "abc123...",
    "def456..."
  ]
}
```

**Response (200 OK):**
```json
{
  "known": ["abc123..."],
  "missing": ["def456..."]
}
```

### Trigger Packfile Merge

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

**`merged_count` semantics:** number of source packfiles merged into the new packfile (not number of objects deduplicated).

### Get Packfile Info

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

### List Packfiles

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

### Error Responses

All endpoints may return:

| Status | Code                  | Description                                      |
| ------ | --------------------- | ------------------------------------------------ |
| 400    | `INVALID_REQUEST`     | Malformed request body                           |
| 401    | `UNAUTHORIZED`        | Missing or invalid credentials                   |
| 403    | `FORBIDDEN`           | Insufficient permissions                         |
| 404    | `NOT_FOUND`           | Repository or resource not found                 |
| 409    | `CONFLICT`            | Upload already completed or aborted              |
| 422    | `VALIDATION_ERROR`    | Manifest entry count mismatch, checksum mismatch |
| 500    | `INTERNAL_ERROR`      | Server-side error                                |
| 503    | `SERVICE_UNAVAILABLE` | S3 sync in progress, retry later                 |
| 503    | `PACKFILE_UNAVAILABLE` | Packfile not yet replicated to L3; retry after delay |

---
