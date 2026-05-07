package packfile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// Partition names for KV store
const (
	PackfilesPartition = "packfiles"
)

// kv key prefixes
const (
	packfileKeyPrefix = "packfile"
	snapshotKeyPrefix = "snapshot"
	uploadKeyPrefix   = "upload"
)

// Default cache and batch config
const (
	DefaultCacheSize     = 1024
	DefaultCacheExpiry   = 5 * time.Minute
	DefaultMaxBatchDelay = 10 * time.Millisecond
)

// Errors specific to packfile store operations
var (
	ErrPackfileNotFound      = errors.New("packfile not found")
	ErrSnapshotNotFound      = errors.New("snapshot not found")
	ErrUploadSessionNotFound = errors.New("upload session not found")
)

// Store provides CRUD operations for packfile-related KV entries using protobuf serialization.
// It uses caching and batch execution patterns consistent with graveler's ref manager.
type Store struct {
	kvStore       kv.Store
	batchExecutor batch.Batcher
	cache         cache.Cache
	maxBatchDelay time.Duration
}

// StoreInterface defines the operations needed by PackfileManager.
// Implemented by Store and test mocks.
type StoreInterface interface {
	GetPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) (*PackfileMetadata, error)
	PutPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string, meta *PackfileMetadata) error
	DeletePackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) error
	GetSnapshot(ctx context.Context, repoID graveler.RepositoryID) (*PackfileSnapshot, error)
	PutSnapshot(ctx context.Context, repoID graveler.RepositoryID, snapshot *PackfileSnapshot) error
	GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error)
	PutUploadSession(ctx context.Context, uploadID string, session *UploadSession) error
	DeleteUploadSession(ctx context.Context, uploadID string) error
	UpdateSnapshotWithRetry(ctx context.Context, repoID graveler.RepositoryID, updateFn func(*PackfileSnapshot) error) error
}

// StoreConfig holds configuration for Store.
type StoreConfig struct {
	KVStore       kv.Store
	BatchExecutor batch.Batcher
	Cache         cache.Cache
	MaxBatchDelay time.Duration
	Store         StoreInterface // Can be *Store or mock for testing
}

// NewStore creates a new packfile Store wrapping the provided kv.Store.
// Uses default cache and batch settings if not provided.
func NewStore(cfg StoreConfig) *Store {
	if cfg.MaxBatchDelay == 0 {
		cfg.MaxBatchDelay = DefaultMaxBatchDelay
	}
	if cfg.Cache == nil {
		cfg.Cache = cache.NewCache(DefaultCacheSize, DefaultCacheExpiry, cache.NewJitterFn(time.Second))
	}
	if cfg.BatchExecutor == nil {
		cfg.BatchExecutor = batch.NewExecutor(logging.Dummy())
	}
	return &Store{
		kvStore:       cfg.KVStore,
		batchExecutor: cfg.BatchExecutor,
		cache:         cfg.Cache,
		maxBatchDelay: cfg.MaxBatchDelay,
	}
}

// KV key builders

func packfileKey(repoID graveler.RepositoryID, packfileID string) []byte {
	return []byte(kv.FormatPath(packfileKeyPrefix, string(repoID), packfileID))
}

func snapshotKey(repoID graveler.RepositoryID) []byte {
	return []byte(kv.FormatPath(snapshotKeyPrefix, string(repoID)))
}

func uploadKey(uploadID string) []byte {
	return []byte(kv.FormatPath(uploadKeyPrefix, uploadID))
}

// GetPackfile retrieves a packfile metadata by repository ID and packfile ID.
// Uses batch-for for coalescing concurrent requests.
func (s *Store) GetPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) (*PackfileMetadata, error) {
	key := packfileKey(repoID, packfileID)
	batchKey := fmt.Sprintf("batch:packfile:%s:%s", repoID, packfileID)

	// Use batch-for to coalesce concurrent requests for the same packfile
	result, err := s.batchExecutor.BatchFor(ctx, batchKey, s.maxBatchDelay, batch.ExecuterFunc(func() (any, error) {
		var meta PackfileMetadata
		_, err := kv.GetMsg(ctx, s.kvStore, PackfilesPartition, key, &meta)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				return nil, ErrPackfileNotFound
			}
			return nil, fmt.Errorf("packfile: getting packfile metadata: %w", err)
		}
		return &meta, nil
	}))
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrPackfileNotFound
	}
	return result.(*PackfileMetadata), nil
}

// PutPackfile stores packfile metadata.
// Note: Writes bypass batch-for intentionally — concurrent writes to the same
// packfile are rare and should fail rather than be coalesced. Reads use
// batch-for because concurrent reads for the same packfile are common (coalescing
// improves performance under hot-spot read workloads).
func (s *Store) PutPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string, meta *PackfileMetadata) error {
	key := packfileKey(repoID, packfileID)
	if err := kv.SetMsg(ctx, s.kvStore, PackfilesPartition, key, meta); err != nil {
		return fmt.Errorf("packfile: storing packfile metadata: %w", err)
	}
	return nil
}

// DeletePackfile removes a packfile metadata entry.
func (s *Store) DeletePackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) error {
	key := packfileKey(repoID, packfileID)
	if err := s.kvStore.Delete(ctx, []byte(PackfilesPartition), key); err != nil {
		return fmt.Errorf("packfile: deleting packfile metadata: %w", err)
	}
	return nil
}

// GetSnapshot retrieves the packfile snapshot for a repository.
// Uses cache for read optimization. Transient errors are not cached -
// only successful reads or ErrSnapshotNotFound are stored in cache.
func (s *Store) GetSnapshot(ctx context.Context, repoID graveler.RepositoryID) (*PackfileSnapshot, error) {
	key := snapshotKey(repoID)
	cacheKey := fmt.Sprintf("snapshot:%s", repoID)

	// Use cache for snapshot reads
	result, err := s.cache.GetOrSet(cacheKey, func() (any, error) {
		var snap PackfileSnapshot
		_, err := kv.GetMsg(ctx, s.kvStore, PackfilesPartition, key, &snap)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				return nil, ErrSnapshotNotFound
			}
			// Transient error - return nil to bypass cache and allow retry
			return nil, fmt.Errorf("packfile: getting snapshot: %w", err)
		}
		return &snap, nil
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrSnapshotNotFound
	}
	return result.(*PackfileSnapshot), nil
}

// PutSnapshot stores a packfile snapshot for a repository.
// Uses optimistic concurrency - if version doesn't match, returns kv.ErrPredicateFailed.
// Cache note: GetSnapshot caches results with key "snapshot:{repoID}". A snapshot is
// identified by (repoID, version) — after PutSnapshot increments version, the next
// GetSnapshot will miss the cache and re-fetch the updated data. Concurrent writes
// use predicate-based optimistic locking so they cannot silently overwrite each other.
func (s *Store) PutSnapshot(ctx context.Context, repoID graveler.RepositoryID, snapshot *PackfileSnapshot) error {
	key := snapshotKey(repoID)

	// Get current snapshot to obtain predicate for optimistic concurrency
	var existing PackfileSnapshot
	pred, err := kv.GetMsg(ctx, s.kvStore, PackfilesPartition, key, &existing)
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return fmt.Errorf("packfile: getting existing snapshot: %w", err)
	}

	if errors.Is(err, kv.ErrNotFound) {
		// No existing snapshot - just store it
		return kv.SetMsg(ctx, s.kvStore, PackfilesPartition, key, snapshot)
	}

	// Use predicate for conditional update (optimistic concurrency)
	return kv.SetMsgIf(ctx, s.kvStore, PackfilesPartition, key, snapshot, pred)
}

// GetUploadSession retrieves an upload session by upload ID.
func (s *Store) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	key := uploadKey(uploadID)

	var session UploadSession
	_, err := kv.GetMsg(ctx, s.kvStore, PackfilesPartition, key, &session)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil, ErrUploadSessionNotFound
		}
		return nil, fmt.Errorf("packfile: getting upload session: %w", err)
	}
	return &session, nil
}

// PutUploadSession stores an upload session.
func (s *Store) PutUploadSession(ctx context.Context, uploadID string, session *UploadSession) error {
	key := uploadKey(uploadID)
	if err := kv.SetMsg(ctx, s.kvStore, PackfilesPartition, key, session); err != nil {
		return fmt.Errorf("packfile: storing upload session: %w", err)
	}
	return nil
}

// DeleteUploadSession removes an upload session.
func (s *Store) DeleteUploadSession(ctx context.Context, uploadID string) error {
	key := uploadKey(uploadID)
	if err := s.kvStore.Delete(ctx, []byte(PackfilesPartition), key); err != nil {
		return fmt.Errorf("packfile: deleting upload session: %w", err)
	}
	return nil
}

// UpdateSnapshotWithRetry performs an atomic read-modify-write on the snapshot.
// It retries on conflict by re-reading the current version.
func (s *Store) UpdateSnapshotWithRetry(ctx context.Context, repoID graveler.RepositoryID, updateFn func(*PackfileSnapshot) error) error {
	for attempt := 0; attempt < 3; attempt++ {
		snapshot, err := s.GetSnapshot(ctx, repoID)

		// ErrSnapshotNotFound means we need to create a new snapshot - treat as success case
		if errors.Is(err, ErrSnapshotNotFound) {
			snapshot = &PackfileSnapshot{
				RepositoryId: string(repoID),
			}
		} else if err != nil {
			// Transient error - retry if we have attempts left
			if attempt < 2 {
				continue
			}
			return err
		}

		if err := updateFn(snapshot); err != nil {
			return err
		}

		// Increment version on each update
		snapshot.Version++

		err = s.PutSnapshot(ctx, repoID, snapshot)
		if err == nil {
			return nil
		}

		// Check if it was a predicate failure (conflict)
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // Retry with fresh read
		}
		return err
	}
	return fmt.Errorf("packfile: snapshot update failed after 3 retries (concurrent modification)")
}