# pkg/block - Block Storage Abstraction Layer

This package provides a unified abstraction layer for object storage in lakeFS, supporting multiple backends through a common interface.

## Overview

The block package implements an adapter pattern that abstracts away the differences between various object storage providers (S3, GCS, Azure Blob, Local, Memory, Transient). This allows lakeFS to work with any supported storage backend through a consistent API.

## Architecture

### Core Interface: `Adapter`

The `Adapter` interface (`adapter.go:187-226`) is the central abstraction defining all storage operations:

```go
type Adapter interface {
    Put(ctx, obj, sizeBytes, reader, opts) (*PutResponse, error)
    Get(ctx, obj) (io.ReadCloser, error)
    GetWalker(storageID, opts) (Walker, error)
    GetPreSignedURL(ctx, obj, mode, filename) (string, time.Time, error)
    Exists(ctx, obj) (bool, error)
    GetRange(ctx, obj, start, end) (io.ReadCloser, error)
    GetProperties(ctx, obj) (Properties, error)
    Copy(ctx, source, dest) error
    CreateMultiPartUpload(ctx, obj, r, opts) (*CreateMultiPartUploadResponse, error)
    UploadPart(ctx, obj, size, reader, uploadID, partNumber) (*UploadPartResponse, error)
    UploadCopyPart(ctx, source, dest, uploadID, partNumber) (*UploadPartResponse, error)
    UploadCopyPartRange(ctx, source, dest, uploadID, partNumber, start, end) (*UploadPartResponse, error)
    ListParts(ctx, obj, uploadID, opts) (*ListPartsResponse, error)
    ListMultipartUploads(ctx, obj, opts) (*ListMultipartUploadsResponse, error)
    AbortMultiPartUpload(ctx, obj, uploadID) error
    CompleteMultiPartUpload(ctx, obj, uploadID, multipartList) (*CompleteMultiPartUploadResponse, error)
    BlockstoreType() string
    BlockstoreMetadata(ctx) (*BlockstoreMetadata, error)
    GetStorageNamespaceInfo(storageID) *StorageNamespaceInfo
    ResolveNamespace(storageID, storageNamespace, key, identifierType) (QualifiedKey, error)
    GetRegion(ctx, storageID, storageNamespace) (string, error)
    RuntimeStats() map[string]string
}
```

### Key Data Structures

#### ObjectPointer (`adapter.go:65-75`)
Unique identifier for objects in the object store:
```go
type ObjectPointer struct {
    StorageID        string
    StorageNamespace string
    Identifier       string
    IdentifierType   IdentifierType  // Relative, Full, or UnknownDeprecated
}
```

#### Walker Interface (`walker.go:10-14`)
For iterating over objects in storage:
```go
type Walker interface {
    Walk(ctx, storageURI, opts, walkFn) error
    Marker() Mark
    GetSkippedEntries() []ObjectStoreEntry
}
```

#### ObjectStoreEntry (`walker.go:16-30`)
Represents a single object during iteration:
```go
type ObjectStoreEntry struct {
    FullKey      string    // Fully qualified path in the object store
    RelativeKey  string    // Path relative to prefix/directory
    Address      string    // Full URI (e.g., s3://bucket/path/to/key)
    ETag         string    // Content hash (typically MD5)
    Mtime        time.Time // Last-modified datetime
    Size         int64     // Size in bytes
}
```

## Storage Types

The package supports six storage backends:

| Type | Constant | Description |
|------|----------|-------------|
| S3 | `BlockstoreTypeS3` | Amazon S3 (primary production backend) |
| GS | `BlockstoreTypeGS` | Google Cloud Storage |
| Azure | `BlockstoreTypeAzure` | Azure Blob Storage |
| Local | `BlockstoreTypeLocal` | Local filesystem |
| Mem | `BlockstoreTypeMem` | In-memory (testing/development) |
| Transient | `BlockstoreTypeTransient` | Discards all data (testing) |

## Adapter Implementations

### S3 Adapter (`s3/adapter.go`)
- Uses AWS SDK v2 (`github.com/aws/aws-sdk-go-v2`)
- Supports server-side encryption (SSE-S3, SSE-KMS)
- Client caching for connection reuse (`s3/client_cache.go`)
- Pre-signed URL generation with credential expiry handling
- Multipart uploads with retry logic for non-seekable readers

### GS Adapter (`gs/adapter.go`)
- Uses Google Cloud Storage client (`cloud.google.com/go/storage`)
- Multipart uploads implemented using object composition
- Server-side encryption support (CSE-K, KMS)
- Pre-signed URLs via V4 signing

### Azure Adapter (`azure/adapter.go`)
- Uses Azure SDK for Go (`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`)
- Supports multiple domains (default, China, US Gov, Azurite)
- Large file uploads via block staging
- Async copy for large objects
- User delegation SAS tokens

### Local Adapter (`local/adapter.go`)
- Maps storage namespaces to filesystem directories
- Path traversal security checks
- Multipart uploads via temporary files
- Import support for external paths

### Mem Adapter (`mem/adapter.go`)
- In-memory storage using maps
- Fully featured but not production-safe
- Useful for testing

### Transient Adapter (`transient/adapter.go`)
- Discards all written data
- Returns random data for reads
- Used for benchmarking

## Factory Pattern

The `factory/build.go` module creates the appropriate adapter based on configuration:

```go
func BuildBlockAdapter(ctx, statsCollector, config, opts...) (block.Adapter, error)
```

Flow:
1. Extract blockstore type from config
2. Get type-specific parameters
3. Create adapter with options
4. Wrap with metrics collection

## Namespace Resolution

The `namespace.go` module handles converting storage identifiers to qualified keys:

- `IdentifierTypeRelative`: Key is relative to storage namespace (e.g., `/foo/bar`)
- `IdentifierTypeFull`: Full address (e.g., `s3://bucket/foo/bar`)
- `IdentifierTypeUnknownDeprecated`: Auto-detect (tries full first, then relative)

## Metrics

The `metrics.go` module provides a `MetricsAdapter` wrapper that:
- Tracks concurrent operations per blockstore type
- Adds client tracing
- Wraps any adapter without modifying its behavior

## Testing Utilities

### blocktest (`blocktest/adapter.go`)
Provides test suites for adapter implementations:
- `AdapterTest`: Basic + multipart tests
- `AdapterPresignedEndpointOverrideTest`: Presigned URL with endpoint override

## HashingReader (`hashing_reader.go`)

Utility for computing checksums while reading data:
- Supports MD5 and SHA256
- Used for ETag computation

## Key Patterns

### 1. Client Caching
S3 and Azure adapters cache clients per bucket/storage account to avoid repeated connection setup.

### 2. Option Builders
Each adapter uses functional options for configuration:
```go
func WithPreSignedExpiry(v time.Duration) func(a *Adapter)
func WithDisablePreSigned(b bool) func(a *Adapter)
```

### 3. Error Translation
Each adapter translates provider-specific errors to common errors:
- `ErrDataNotFound`: Object doesn't exist
- `ErrOperationNotSupported`: Feature not available
- `ErrInvalidAddress`: Invalid storage namespace/key

### 4. Metrics Collection
All operations report metrics via `reportMetrics()` for observability.

## Common Tasks

- Run adapter tests: `go test -v ./pkg/block/s3 -run TestAdapter`
- Test specific backend: `go test -v ./pkg/block/mem -run TestAdapter`
- Add new storage type: Implement `Adapter` interface, add to factory