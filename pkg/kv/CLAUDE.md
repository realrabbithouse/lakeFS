# pkg/kv - Key-Value Store Abstraction

This package provides a pluggable key-value store abstraction layer for lakeFS, supporting multiple database backends through a common interface.

## Overview

The kv package provides:
- **Unified Store Interface**: Common API for all KV backends
- **Multiple Drivers**: PostgreSQL, DynamoDB, CosmosDB, Local/Pebble, Memory
- **Protobuf Support**: Easy serialization/deserialization of protobuf messages
- **Partition Support**: Data organization into partitions
- **Migration Support**: Schema version management
- **Dump/Load**: Export and import functionality

## Architecture

### Core Interface: Store (`store.go`)

```go
type Store interface {
    // Get returns value and predicate for key, or ErrNotFound
    Get(ctx, partitionKey, key []byte) (*ValueWithPredicate, error)

    // Set stores value, overwriting existing
    Set(ctx, partitionKey, key, value []byte) error

    // SetIf conditional update using predicate
    SetIf(ctx, partitionKey, key, value []byte, predicate Predicate) error

    // Delete removes key (no error if not exists)
    Delete(ctx, partitionKey, key []byte) error

    // Scan returns iterator over partition
    Scan(ctx, partitionKey []byte, options ScanOptions) (EntriesIterator, error)

    Close()
}
```

### EntriesIterator (`store.go:113-132`)

```go
type EntriesIterator interface {
    Next() bool
    SeekGE(key []byte)
    Entry() *Entry
    Err() error
    Close()
}

type Entry struct {
    PartitionKey []byte
    Key          []byte
    Value        []byte
}
```

### Driver Interface (`store.go:57-63`)

```go
type Driver interface {
    Open(ctx context.Context, params kvparams.Config) (Store, error)
}
```

### Partition Keys

Data is organized into partitions:
- `graveler` - Repositories
- `repo-*` - Per-repository data (branches, commits, tags)
- `auth` - Authentication/authorization
- `kv-internal-metadata` - Internal metadata (schema version)

## Protobuf Support (`msg.go`)

High-level helpers for protobuf messages:

```go
// Get and unmarshal protobuf message
func GetMsg(ctx, s Store, partitionKey string, key []byte, msg protoreflect.ProtoMessage) (Predicate, error)

// Marshal and store protobuf message
func SetMsg(ctx, s Store, partitionKey string, key []byte, msg protoreflect.ProtoMessage) error

// Conditional update with predicate
func SetMsgIf(ctx, s Store, partitionKey string, key []byte, msg protoreflect.ProtoMessage, predicate Predicate) error
```

### Predicate for Conditional Updates

The predicate pattern enables compare-and-swap operations:
1. Get current value → returns Predicate
2. Modify value locally
3. SetIf with predicate → only succeeds if value hasn't changed

## Record Type Matching (`record_match.go`)

Auto-discover protobuf types based on partition and key patterns:

```go
func RegisterType(partitionPattern, pathPattern string, mt protoreflect.MessageType) error
```

Used in init() to register types:
```go
// From graveler/model.go
func init() {
    kv.MustRegisterType("graveler", "repos", (&RepositoryData{}).ProtoReflect().Type())
    kv.MustRegisterType("*", "branches", (&BranchData{}).ProtoReflect().Type())
    kv.MustRegisterType("*", "commits", (&CommitData{}).ProtoReflect().Type())
    kv.MustRegisterType("*", "tags", (&TagData{}).ProtoReflect().Type())
    kv.MustRegisterType("*", "*", (&StagedEntryData{}).ProtoReflect().Type())
}
```

Pattern matching:
- `*` matches any value
- Prefix matching (e.g., `repo` matches `repo/something`)

## Drivers

### PostgreSQL (`postgres/store.go`)

- **Partitioning**: 100 hash partitions by default
- **Connection**: Uses pgx connection pool
- **Features**: 
  - Advisory locking for setup
  - Prometheus metrics for pool
  - Partitioned table with hash partition

```go
const (
    DefaultTableName = "kv"
    DefaultPartitions = 100
    DefaultScanPageSize = 1000
)
```

### DynamoDB (`dynamodb/store.go`)

- **Table**: Single table with partition key + sort key
- **Partition Key**: SHA-256 hash of partition_key
- **Features**:
  - Rate limiting support
  - On-demand or provisioned capacity

### CosmosDB (`cosmosdb/store.go`)

- **Partition**: Container per store
- **API**: SQL API

### Local/Pebble (`local/store.go`)

- **Backend**: Pebble key-value store (embedded)
- **Use**: Local development/testing

### Memory (`mem/store.go`)

- **Backend**: Go map
- **Use**: Testing only

## Scan Options

```go
type ScanOptions struct {
    KeyStart  []byte  // Start key for scan
    BatchSize int     // Results per iteration
}
```

## Migration Support (`migration.go`)

Schema version management:

```go
const (
    InitialMigrateVersion = iota + 1
    ACLMigrateVersion
    ACLNoReposMigrateVersion
    ACLImportMigrateVersion
    NextSchemaVersion
)

func GetDBSchemaVersion(ctx context.Context, store Store) (int, error)
func SetDBSchemaVersion(ctx context.Context, store Store, version int) error
```

## Dump/Load (`dump.go`)

Export and import KV data:

```go
type DumpFormat struct {
    Version   string     `json:"version"`
    Timestamp string     `json:"timestamp"`
    Entries   []RawEntry `json:"entries,omitempty"`
}

type RawEntry struct {
    Partition string `json:"partition"`
    Key       string `json:"key"`
    Value     string `json:"value"` // base64 encoded
}

type Dumper struct{ store Store }
type Loader struct{ store Store }
```

### Load Strategy

```go
type LoadStrategy string
const (
    LoadStrategyOverride LoadStrategy = "override"  // Replace existing
    LoadStrategyUpdate   LoadStrategy = "update"    // Merge with existing
)
```

## Store Limiter (`store_limiter.go`)

Rate-limited wrapper for store:

```go
type StoreLimiter struct {
    Store  Store
    Limiter rate.Limiter
}
```

## Metrics

Prometheus metrics for KV operations:

```go
// Per-operation metrics
kv_get_duration_seconds
kv_set_duration_seconds
kv_delete_duration_seconds
kv_scan_duration_seconds

// Per-backend metrics
kv_postgres_pool_connections
kv_dynamodb_throttle_errors
```

## Error Types

```go
var (
    ErrClosedEntries       = errors.New("closed entries")
    ErrConnectFailed       = errors.New("connect failed")
    ErrDriverConfiguration = errors.New("driver configuration")
    ErrMissingPartitionKey = errors.New("missing partition key")
    ErrNotFound            = errors.New("not found")
    ErrPredicateFailed     = errors.New("predicate failed")  // CAS failure
    ErrUnknownDriver       = errors.New("unknown driver")
    ErrUnsupported         = errors.New("unsupported")
)
```

## Common Tasks

### Opening a store:
```go
store, err := kv.Open(ctx, kvparams.Config{
    Type: "postgres",
    Postgres: &kvparams.Postgres{
        ConnectionString: "postgres://...",
    },
})
```

### Conditional update:
```go
// Get current value
pred, err := kv.GetMsg(ctx, store, "partition", key, &MyProto{})
if errors.Is(err, kv.ErrNotFound) {
    pred = nil  // Key doesn't exist
}

// Modify
myProto.Field = "new value"

// Conditional set - fails if someone else updated
err = kv.SetMsgIf(ctx, store, "partition", key, myProto, pred)
if errors.Is(err, kv.ErrPredicateFailed) {
    // Retry with new value
}
```

### Scanning a partition:
```go
iter, err := store.Scan(ctx, []byte("partition"), kv.ScanOptions{BatchSize: 100})
defer iter.Close()

for iter.Next() {
    entry := iter.Entry()
    // Process entry.Key, entry.Value
}
```

### Register protobuf type:
```go
func init() {
    kv.MustRegisterType("my-partition", "key-prefix-*", (&MyMessage{}).ProtoReflect().Type())
}
```

## Configuration

KV parameters are configured in config:

```yaml
database:
  type: postgres
  postgres:
    connection_string: "postgres://user:pass@host:5432/db"
    max_open_connections: 25
```