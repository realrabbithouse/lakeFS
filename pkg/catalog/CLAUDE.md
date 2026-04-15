# pkg/catalog - Filesystem Namespace and Entry Management

This package provides the high-level catalog layer that translates between lakeFS's Git-like model (branches, commits) and the filesystem-like model (paths, entries) that users interact with.

## Overview

The catalog package sits on top of graveler and provides:

1. **Entry Management**: Converts between low-level graveler values and high-level Entry objects
2. **Path Resolution**: Handles filesystem-style paths (e.g., `data/file.csv`)
3. **Listing Operations**: Provides directory-like listing with CommonPrefixes
4. **Import**: Bulk data import from external sources
5. **Async Operations**: Handles long-running operations like commits, merges, imports

## Architecture

### Entry Model (`catalog.proto`)

```protobuf
message Entry {
    string address = 1;           // Storage location of the data
    google.protobuf.Timestamp last_modified = 2;
    int64 size = 3;               // Size in bytes
    string e_tag = 4;             // Content hash
    map<string, string> metadata = 5;
    AddressType address_type = 6; // RELATIVE or FULL
    string content_type = 7;      // MIME type
}
```

**Address Types:**
- `RELATIVE`: Address is relative to storage namespace
- `FULL`: Full URI (e.g., `s3://bucket/path`)

### Entry Conversion (`entry.go`)

The catalog converts between graveler's low-level Value and high-level Entry:

```go
func EntryToValue(entry *Entry) (*graveler.Value, error) {
    // 1. Marshal entry to protobuf
    data, _ := proto.Marshal(entry)
    // 2. Calculate identity (content address)
    checksum := ident.NewAddressWriter().
        MarshalInt64(entry.Size).
        MarshalString(entry.ETag).
        MarshalStringMap(entry.Metadata).
        MarshalStringOpt(entry.ContentType).
        Identity()
    return &graveler.Value{
        Identity: checksum,  // SHA-256 based content address
        Data:     data,      // Serialized Entry
    }, nil
}

func ValueToEntry(value *graveler.Value) (*Entry, error) {
    // Unmarshal protobuf data to Entry
}
```

The **identity** is a content address that uniquely identifies the entry based on its attributes, enabling deduplication.

### Key Interfaces

#### EntryIterator (`catalog.go:115-121`)
```go
type EntryIterator interface {
    Next() bool
    SeekGE(id Path)
    Value() *EntryRecord  // Path + Entry
    Err() error
    Close()
}
```

#### EntryListingIterator (`catalog.go:123-129`)
For directory-style listing with CommonPrefixes:
```go
type EntryListingIterator interface {
    Next() bool
    SeekGE(id Path)
    Value() *EntryListing  // Has CommonPrefix bool
    Err() error
    Close()
}
```

#### EntryDiffIterator (`catalog.go:131-137`)
```go
type EntryDiffIterator interface {
    Next() bool
    SeekGE(id Path)
    Value() *EntryDiff  // Type (Added/Removed/Changed), Path, Entry
    Err() error
    Close()
}
```

## Core Components

### Catalog (`catalog.go:240-257`)

The main struct that orchestrates everything:

```go
type Catalog struct {
    BlockAdapter    block.Adapter     // Object storage
    Store           Store             // Graveler store interface
    workPool        pond.Pool         // Background task pool
    PathProvider    *upload.PathPartitionProvider
    addressProvider *ident.HexAddressProvider
    deleteSensor    *graveler.DeleteSensor
    // ...
}
```

**Store Interface** (`catalog.go:143-150`):
```go
type Store interface {
    graveler.KeyValueStore      // Get/Set entries
    graveler.VersionController  // Commit/Revert/Branch
    graveler.Dumper             // Export repository
    graveler.Loader             // Import repository
    graveler.Plumbing           // Low-level operations
    graveler.Collaborator       // User management
}
```

### Import (`import.go`)

Bulk import from external sources:

```go
type Import struct {
    db            *pebble.DB  // Local staging DB
    kvStore       kv.Store
    status        graveler.ImportStatus
}
```

**Import Flow:**
1. Create temporary Pebble DB
2. Walk external source (S3, GCS, etc.)
3. Ingest entries into local DB
4. Create staging token from local DB
5. Commit to branch

### EntryDiffIterator (`entry_diff_iterator.go`)

Converts graveler diffs to catalog entry diffs:

```go
type entryDiffIterator struct {
    it    graveler.DiffIterator
    value *EntryDiff
}

func (e *entryDiffIterator) Next() bool {
    // Convert graveler.Diff → catalog.EntryDiff
    // Deserialize Entry from value.Data
}
```

## Entry Listing

### CommonPrefixes

When listing with a delimiter (typically `/`), entries that are "directory" prefixes are returned as CommonPrefixes:

```
data/
data/file1.csv
data/file2.csv
```

Listing with `Delimiter="/"` returns:
- `CommonPrefix: true, Path: "data/"`
- `CommonPrefix: false, Path: "data/file1.csv"`
- `CommonPrefix: false, Path: "data/file2.csv"`

### EntryListingIterator

Wraps underlying iterators to provide the CommonPrefix behavior.

## Path Handling

### Path Type

```go
type Path string  // e.g., "data/2024/january/file.csv"
```

### Path Operations

- **Delimiter**: Default is `/`
- **Prefix filtering**: `ListEntries` with `prefix` parameter
- **Pagination**: `after` parameter for keyset pagination

## Async Operations

The catalog manages long-running operations through tasks:

### Task Types

```go
const (
    OpDumpRefs         = "dump_refs"
    OpRestoreRefs      = "restore_refs"
    OpGCPrepareCommits = "gc_prepare_commits"
)
```

### Task Status

Tasks are stored in KV store and monitored for progress:
- `progress`: 0-100 percentage
- `error_msg`: Error message if failed
- `updated_at`: Last update timestamp

## Entry Diff Types

```go
type EntryDiff struct {
    Type  graveler.DiffType  // Added, Removed, Changed
    Path  Path
    Entry *Entry
}
```

## Configuration

```go
type Config struct {
    Config                config.Config
    KVStore               kv.Store
    SettingsManagerOption settings.ManagerOption
    PathProvider         *upload.PathPartitionProvider
}
```

## Initialization Flow

1. **Build Block Adapter**: Create object storage adapter
2. **Create Tiered FS**: Setup Pyramid tiered storage for ranges/metaranges
3. **Build Committed Manager**: Handle committed data operations
4. **Create Ref Manager**: Manage branches, tags, commits
5. **Create Staging Manager**: Handle uncommitted changes
6. **Setup Delete Sensor**: Track objects for deletion

## Key Operations

### GetEntry
```go
func (c *Catalog) GetEntry(ctx, repo, ref, path, params) (*Entry, error)
```

1. Resolve ref to commit
2. Get staging token for branch
3. Check staging first (unless StageOnly)
4. Check committed data
5. Return Entry or NotFound

### ListEntries
```go
func (c *Catalog) ListEntries(ctx, repo, ref, prefix, after, delimiter) (EntryListingIterator, error)
```

1. Resolve ref to commit
2. Create iterator over committed data
3. Wrap with entry listing iterator (handles CommonPrefixes)
4. Support pagination via After

### CreateEntry/Upload
```go
func (c *Catalog) CreateEntry(ctx, repo, branch, entry, source) error
```

1. Get staging token for branch
2. Upload data to object storage (getting address)
3. Create Entry with address
4. Write to staging

### Commit
```go
func (c *Catalog) Commit(ctx, repo, branch, message, metadata) (*CommitRecord, error)
```

1. Read staging token
2. Read parent commit's metarange
3. Merge staging + parent → new metarange
4. Create commit record
5. Update branch pointer

## Garbage Collection Support

The catalog tracks:
- **Uncommitted entries**: Staging tokens with entries not yet committed
- **Expired addresses**: Physical addresses no longer referenced
- **Expired results**: Iterator over addresses to delete

```go
type ExpireResult struct {
    Repository        string
    Branch            string
    PhysicalAddress   string
    InternalReference string
}
```

## Path Provider

The `PathProvider` (`upload.PathPartitionProvider`) handles:
- Partitioning large imports across multiple directories
- Preventing hot spots in object storage
- Configurable partition strategies

## Common Tasks

- **List directory**: Use ListEntries with Delimiter="/"
- **Get file metadata**: Use GetEntry
- **Diff two branches**: Use Diff with both branch refs
- **Import from S3**: Create Import, add paths, commit
- **Revert commit**: Use Revert with parent number for merge commits