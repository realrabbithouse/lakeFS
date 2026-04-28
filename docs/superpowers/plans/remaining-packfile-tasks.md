# Packfile Implementation - Remaining Tasks Plan

> **For agentic workers:** Use executing-plans skill to implement task-by-task.

**Goal:** Complete the packfile implementation by adding remaining API endpoints, DBEntry schema update, manifest validation, and async S3 replication.

**Architecture:** Packfile system enables bulk object transfer with content-addressable deduplication. Streaming upload flows through lakeFS server for validation, with local disk primary and S3 async backup.

**Tech Stack:** Go, lakeFS kv store, block adapter, Pebble SSTable

---

## Task 1: API Endpoints for Packfile Upload

### Files
- Create: `pkg/api/apis/packfile.go` (new packfile API handlers)
- Modify: `pkg/api/handlers/packfile.go` (register handlers)
- Modify: `pkg/api/routes.go` (add routes)

### Steps

- [ ] **Step 1: Create packfile API request/response types**

```go
// pkg/api/models/packfile.go
type InitPackfileUploadRequest struct {
    PackfileInfo PackfileInfo `json:"packfile_info"`
    ValidateObjectChecksum bool `json:"validate_object_checksum"`
}

type PackfileInfo struct {
    Size int64 `json:"size"`
    Checksum string `json:"checksum"`
    Compression string `json:"compression"`
}

type InitPackfileUploadResponse struct {
    UploadID string `json:"upload_id"`
    PackfileURL string `json:"packfile_url"`
    ManifestURL string `json:"manifest_url"`
}
```

- [ ] **Step 2: Implement InitPackfileUpload handler**

```go
// POST /repositories/{repository}/objects/pack
func (h *PackfileHandler) InitPackfileUpload(w http.ResponseWriter, r *http.Request) {
    // 1. Parse request
    // 2. Create upload session via PackfileManager
    // 3. Return upload_id, packfile_url, manifest_url
}
```

- [ ] **Step 3: Implement UploadPackfileData handler**

```go
// PUT /repositories/{repository}/objects/pack/{upload_id}/data
func (h *PackfileHandler) UploadPackfileData(w http.ResponseWriter, r *http.Request) {
    // Stream body to tmp path
}
```

- [ ] **Step 4: Implement UploadManifest handler**

```go
// PUT /repositories/{repository}/objects/pack/{upload_id}/manifest
func (h *PackfileHandler) UploadManifest(w http.ResponseWriter, r *http.Request) {
    // Parse NDJSON manifest, validate against init request
}
```

- [ ] **Step 5: Implement CompletePackfileUpload handler**

```go
// POST /repositories/{repository}/objects/pack/{upload_id}/complete
func (h *PackfileHandler) CompletePackfileUpload(w http.ResponseWriter, r *http.Request) {
    // Complete upload session, return staged entries
}
```

- [ ] **Step 6: Implement AbortPackfileUpload handler**

```go
// DELETE /repositories/{repository}/objects/pack/{upload_id}
func (h *PackfileHandler) AbortPackfileUpload(w http.ResponseWriter, r *http.Request) {
    // Abort session, cleanup tmp files
}
```

- [ ] **Step 7: Register routes in routes.go**

Add routes for all packfile endpoints.

- [ ] **Step 8: Verify build passes**

Run: `go build ./pkg/api/...`

---

## Task 2: DBEntry Schema Update for PACKFILE Address Type

### Files
- Modify: `pkg/catalog/model.go` (DBEntry struct)
- Modify: `pkg/graveler/model.go` (Value/Entry conversion)
- Create: `pkg/packfile/address.go` (packfile address parsing)

### Steps

- [ ] **Step 1: Add AddressType constants to model.go**

```go
type AddressType int

const (
    AddressTypeByPrefix AddressType = iota
    AddressTypeRelative
    AddressTypeFull
    AddressTypePackfile  // NEW: object stored in packfile
)
```

- [ ] **Step 2: Add PackfileOffset field to DBEntry**

```go
type DBEntry struct {
    // ... existing fields ...
    AddressType AddressType
    PackfileOffset int64  // Valid when AddressType == AddressTypePackfile
}
```

- [ ] **Step 3: Create packfile address parsing utility**

```go
// pkg/packfile/address.go
// ParsePackfileAddress extracts packfile_id from "packfile:<packfile_id>"
func ParsePackfileAddress(addr string) (packfileID string, ok bool)
```

- [ ] **Step 4: Update graveler Value→Entry conversion**

Handle AddressTypePackfile in entry conversion.

- [ ] **Step 5: Verify build passes**

Run: `go build ./pkg/catalog/... ./pkg/graveler/...`

---

## Task 3: Manifest Validation

### Files
- Modify: `pkg/packfile/manager.go` (add ValidateManifest method)
- Modify: `pkg/api/apis/packfile.go` (call validation)

### Steps

- [ ] **Step 1: Add manifest validation to manager.go**

```go
// ManifestEntry represents one line in the NDJSON manifest
type ManifestEntry struct {
    Path string
    Checksum string
    ContentType string
    Metadata map[string]string
    RelativeOffset int64
    SizeUncompressed int64
    SizeCompressed int64
}

// ValidateManifest validates manifest entries against init request
func (m *Manager) ValidateManifest(ctx context.Context, uploadID string, manifest []ManifestEntry) error
```

- [ ] **Step 2: Validate object count matches**

Compare manifest entry count against PackfileInfo.object_count.

- [ ] **Step 3: Validate packfile_checksum matches**

Compare manifest's packfile_checksum against init request.

- [ ] **Step 4: Verify build passes**

Run: `go build ./pkg/packfile/...`

---

## Task 4: Async S3 Replication

### Files
- Modify: `pkg/packfile/manager.go` (add async replication goroutine)
- Modify: `pkg/packfile/config.go` (add async_replicate config)

### Steps

- [ ] **Step 1: Add packfile config**

```go
type Config struct {
    // ... existing fields ...
    AsyncReplicate bool
}
```

- [ ] **Step 2: Implement replication goroutine**

```go
// replicatePackfile copies packfile to S3 via block adapter
func (m *Manager) replicatePackfile(ctx context.Context, packfileID string) error

// StartReplication starts background replication for a packfile
func (m *Manager) StartReplication(ctx context.Context, packfileID string)
```

- [ ] **Step 3: Call StartReplication after packfile is finalized**

In CompleteUploadSession, call StartReplication.

- [ ] **Step 4: Verify build passes**

Run: `go build ./pkg/packfile/...`

---

## Verification

After all tasks:
```bash
go build ./pkg/packfile/... ./pkg/api/... ./pkg/catalog/... ./pkg/graveler/...
go test -count=1 ./pkg/packfile/... ./pkg/api/...
```
