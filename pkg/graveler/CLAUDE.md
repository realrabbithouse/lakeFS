# pkg/graveler - Version Control for Data Lakes

This package implements lakeFS's Git-like version control for data lakes, providing branching, commits, merges, and tags for object storage.

## Overview

Graveler is the core version control engine that transforms object storage (S3, GCS, Azure Blob) into a Git-like repository. It provides:
- **Branches**: Isolated development lines
- **Commits**: Immutable snapshots of data
- **Tags**: Named references to commits
- **Merges**: Combining changes from different branches
- **Staging**: Temporary area for uncommitted changes

## Architecture

### Core Data Model

```
Repository
├── Default Branch (main)
├── Multiple Branches
│   ├── Each Branch → CommitID + StagingToken
│   └── StagingToken → Temporary uncommitted changes
├── Commits (DAG)
│   ├── Each Commit → MetaRangeID (pointer to data)
│   └── Parents: list of parent CommitIDs
└── Tags
    └── Each Tag → CommitID
```

### Key Types (from graveler.proto)

**Repository** (`graveler.proto:13-22`):
- `StorageID`: The underlying storage bucket/container
- `StorageNamespace`: Full path to the data
- `DefaultBranchID`: Usually "main"
- `State`: ACTIVE or IN_DELETION
- `InstanceUID`: Unique identifier for this lakeFS instance

**Commit** (`graveler.proto:37-47`):
- `ID`: Unique commit identifier (SHA-256 based)
- `Message`: User-provided commit message
- `MetaRangeID`: Pointer to the actual data ( immutable snapshot)
- `Parents`: List of parent commit IDs (for merge commits, multiple parents)
- `Generation`: Number of commits from the root
- `Metadata`: Key-value pairs for custom commit data

**Branch** (`graveler.proto:24-30`):
- `CommitID`: The tip of the branch
- `StagingToken`: Temporary storage for uncommitted changes
- `SealedTokens`: Previous staging tokens (for cleanup)

## Storage Hierarchy

### Tier 1: Object Storage (S3/GCS/Azure)
Physical data stored in the underlying object store.

### Tier 2: MetaRange
A metarange is a collection of sorted ranges. It acts as an index that points to multiple range files, enabling efficient lookups across large datasets.

### Tier 3: Range
A sorted file containing key-value entries. Each range has:
- `MinKey` / `MaxKey`: Bounding keys
- `Count`: Number of entries
- `EstimatedSize`: Size estimate

### Tier 4: Value
The actual data stored for each key:
- `Identity`: Content address (typically SHA-256 of data)
- `Data`: The actual byte content

## Key Components

### 1. Committed Manager (`committed/manager.go`)

Manages committed data (data that has been committed). Key operations:

```go
type CommittedManager interface {
    Exists(ctx, storageID, ns, metaRangeID) (bool, error)
    Get(ctx, storageID, ns, metaRangeID, key) (*Value, error)
    List(ctx, storageID, ns, metaRangeID) (ValueIterator, error)
    WriteRange(ctx, storageID, ns, iterator) (*RangeInfo, error)
    WriteMetaRange(ctx, storageID, ns, ranges) (*MetaRangeInfo, error)
    Diff(ctx, storageID, ns, left, right) (DiffIterator, error)
    Merge(ctx, storageID, ns, base, source, dest, strategy) (*MetaRangeID, error)
}
```

**Writing Data Flow:**
1. Receive a ValueIterator
2. Write ranges (sorted chunks) to object storage
3. Create a metarange that references all ranges
4. Return the metarange ID

### 2. Staging Manager (`staging/manager.go`)

Manages uncommitted changes (staging area). Similar to Git's index/staging area.

```go
type Manager interface {
    Get(ctx, stagingToken, key) (*Value, error)
    Set(ctx, stagingToken, key, value, requireExists) error
    Update(ctx, stagingToken, key, updateFunc) error
    List(ctx, stagingToken, batchSize) ValueIterator
    Drop(ctx, stagingToken) error
    DropAsync(ctx, stagingToken) error  // Deferred cleanup
}
```

**Tombstone Handling:**
- Setting a value to `nil` creates a tombstone (deletion marker)
- Subsequent reads filter out tombstones

**Async Cleanup:**
- Staging tokens are cleaned up asynchronously
- Uses a background goroutine with a wakeup channel

### 3. Ref Manager (`ref/manager.go`)

Manages references (branches, tags, commits).

```go
type Manager interface {
    GetRepository(ctx, repoID) (*RepositoryRecord, error)
    CreateRepository(ctx, repoID, repo) (*RepositoryRecord, error)
    GetBranch(ctx, repoID, branchID) (*BranchRecord, error)
    CreateBranch(ctx, repoID, branchID, sourceBranchID) (*BranchRecord, error)
    GetTag(ctx, repoID, tagID) (*TagRecord, error)
    CreateTag(ctx, repoID, tagID, commitID) error
    ResolveRef(ctx, repoID, ref) (*ResolvedRef, error)
    Log(ctx, repoID, commitID) (CommitIterator, error)
}
```

**Reference Resolution:**
- `main` → branch "main"
- `HEAD` → current branch tip
- `abc123` (40 chars) → commit ID
- `v1.0.0` → tag "v1.0.0"

**Commit Cache:** Caches commit metadata for performance

### 4. Branch Protection (`branch/protection_manager.go`)

Prevents forced pushes and deletions to protected branches.

```go
type ProtectionManager interface {
    GetRules(ctx, repo) (*BranchProtectionRules, checksum, error)
    SetRules(ctx, repo, rules, checksum) error
    IsBlocked(ctx, repo, branchID, action) (bool, error)
}
```

Uses glob patterns for branch matching (e.g., `main`, `release-*`).

## Data Structures

### Value Serialization (`committed/value.go`)

Values are serialized in a simple format:
```
| len(Identity) | Identity | len(Data) | Data |
```
- Each length is a varint
- Max component size: 640KB

### Diff Types

```go
type DiffType int
const (
    DiffTypeAdded DiffType = iota
    DiffTypeRemoved
    DiffTypeChanged
)
```

### Merge Strategy

```go
type MergeStrategy int
const (
    MergeStrategyNone MergeStrategy = iota  // Fail on conflict
    MergeStrategyDest                        // Prefer destination
    MergeStrategySrc                         // Prefer source
)
```

## Iterator Patterns

Graveler uses iterators extensively for efficient data traversal:

**ValueIterator**: Iterate over key-value pairs
```go
type ValueIterator interface {
    Next() bool
    Value() *ValueRecord
    SeekGE(key Key)
    Err() error
    Close()
}
```

**DiffIterator**: Compare two commits
```go
type DiffIterator interface {
    Next() bool
    NextRange() bool  // Skip to next range
    Value() (*Diff, *RangeDiff)
    SeekGE(key Key)
    Err() error
    Close()
}
```

**CommitIterator**: Walk commit history
```go
type CommitIterator interface {
    Next() bool
    Value() *CommitRecord
    Err() error
    Close()
}
```

## Commit Flow

1. **Stage Changes**: Write to staging token (uncommitted)
2. **Create Commit**:
   - Read staging changes
   - Read parent commit's metarange
   - Merge staging + parent (new metarange)
   - Create commit record with new metarange ID
   - Update branch to point to new commit

## Merge Flow

1. Find merge base (common ancestor)
2. Compare source and dest with base:
   - Detect additions, deletions, modifications
   - Handle conflicts based on strategy
3. Write new metarange with merged data
4. Create merge commit (2+ parents)
5. Update destination branch

## Storage Layout

In the underlying object store:

```
storageNamespace/
├── _lakefs/
│   ├── ranges/
│   │   └── {rangeID}          # Range files (sorted key-values)
│   └── metaranges/
│       └── {metaRangeID}      # Metarange files (index to ranges)
└── data/                      # Actual user data (optional)
```

## Key Files

| File | Purpose |
|------|---------|
| `graveler.go` | Main entry point, orchestration |
| `model.go` | Proto conversion utilities |
| `graveler.proto` | Data model definitions |
| `committed/manager.go` | Committed data operations |
| `committed/value.go` | Value serialization |
| `committed/diff.go` | Diff between commits |
| `committed/merge.go` | Merge logic |
| `staging/manager.go` | Staging area management |
| `ref/manager.go` | Reference management |
| `branch/protection_manager.go` | Branch protection |

## Common Tasks

- **View commit log**: `ref.manager.Log()` returns iterator over commits
- **Diff two commits**: `committed.manager.Diff()` returns diff iterator
- **Merge branches**: `committed.manager.Merge()` with strategy
- **Protect branch**: `branch.protection_manager.SetRules()`
- **Stage changes**: `staging.manager.Set()` with staging token