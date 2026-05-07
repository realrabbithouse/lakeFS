package packfile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockKVStore implements kv.Store for testing
type mockKVStore struct {
	data       map[string]map[string][]byte
	predicates map[string]map[string]kv.Predicate
}

func newMockStore() *mockKVStore {
	return &mockKVStore{
		data:       make(map[string]map[string][]byte),
		predicates: make(map[string]map[string]kv.Predicate),
	}
}

func (m *mockKVStore) Get(_ context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	partitionStr := string(partitionKey)
	keyStr := string(key)

	if m.data[partitionStr] == nil {
		return nil, kv.ErrNotFound
	}

	val, ok := m.data[partitionStr][keyStr]
	if !ok {
		return nil, kv.ErrNotFound
	}

	return &kv.ValueWithPredicate{
		Value:     val,
		Predicate: m.predicates[partitionStr][keyStr],
	}, nil
}

func (m *mockKVStore) Set(_ context.Context, partitionKey, key, value []byte) error {
	partitionStr := string(partitionKey)
	keyStr := string(key)

	if m.data[partitionStr] == nil {
		m.data[partitionStr] = make(map[string][]byte)
		m.predicates[partitionStr] = make(map[string]kv.Predicate)
	}

	m.data[partitionStr][keyStr] = value
	m.predicates[partitionStr][keyStr] = value // Predicate is the old value
	return nil
}

func (m *mockKVStore) SetIf(_ context.Context, partitionKey, key, value []byte, predicate kv.Predicate) error {
	partitionStr := string(partitionKey)
	keyStr := string(key)

	if m.data[partitionStr] == nil {
		return kv.ErrNotFound
	}

	oldVal, ok := m.data[partitionStr][keyStr]
	if !ok {
		// Key doesn't exist, but predicate is nil for new keys
		if predicate == nil {
			m.data[partitionStr][keyStr] = value
			return nil
		}
		return kv.ErrPredicateFailed
	}

	// Check predicate - predicate should match the stored value
	if predicate != nil {
		oldBytes, canConvert := predicate.([]byte)
		if canConvert && string(oldVal) != string(oldBytes) {
			return kv.ErrPredicateFailed
		}
	}

	m.data[partitionStr][keyStr] = value
	m.predicates[partitionStr][keyStr] = oldVal
	return nil
}

func (m *mockKVStore) Delete(_ context.Context, partitionKey, key []byte) error {
	partitionStr := string(partitionKey)
	keyStr := string(key)

	if m.data[partitionStr] != nil {
		delete(m.data[partitionStr], keyStr)
	}
	return nil
}

func (m *mockKVStore) Scan(_ context.Context, partitionKey []byte, _ kv.ScanOptions) (kv.EntriesIterator, error) {
	return nil, errors.New("not implemented")
}

func (m *mockKVStore) Close() {}

var _ kv.Store = (*mockKVStore)(nil)

func newTestStore() *Store {
	mock := newMockStore()
	return NewStore(StoreConfig{
		KVStore:       mock,
		BatchExecutor: batch.NopExecutor(),
		Cache:         cache.NewCache(1024, 5*time.Minute, cache.NewJitterFn(time.Second)),
		MaxBatchDelay: DefaultMaxBatchDelay,
	})
}

func TestPackfileMetadataRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	meta := &PackfileMetadata{
		PackfileId:           "abcd1234",
		Status:               PackfileStatus_STAGED,
		Classification:       PackfileClassification_SECOND_CLASS,
		RepositoryId:         "my-repo",
		StorageNamespace:     "s3://my-bucket/my-ns/",
		ObjectCount:          100,
		SizeBytes:            1000000,
		SizeBytesCompressed:  500000,
		CompressionAlgorithm: "zstd",
		ChecksumAlgorithm:    "sha256",
		CreatedAt:            timestamppb.Now(),
		SourcePackfileIds:    nil,
	}

	repoID := graveler.RepositoryID("my-repo")
	packfileID := "abcd1234"

	// Test PutPackfile and GetPackfile
	err := store.PutPackfile(ctx, repoID, packfileID, meta)
	require.NoError(t, err)

	retrieved, err := store.GetPackfile(ctx, repoID, packfileID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, meta.PackfileId, retrieved.PackfileId)
	assert.Equal(t, meta.Status, retrieved.Status)
	assert.Equal(t, meta.Classification, retrieved.Classification)
	assert.Equal(t, meta.RepositoryId, retrieved.RepositoryId)
	assert.Equal(t, meta.StorageNamespace, retrieved.StorageNamespace)
	assert.Equal(t, meta.ObjectCount, retrieved.ObjectCount)
	assert.Equal(t, meta.SizeBytes, retrieved.SizeBytes)
	assert.Equal(t, meta.SizeBytesCompressed, retrieved.SizeBytesCompressed)
	assert.Equal(t, meta.CompressionAlgorithm, retrieved.CompressionAlgorithm)
	assert.Equal(t, meta.ChecksumAlgorithm, retrieved.ChecksumAlgorithm)
}

func TestPackfileMetadataWithSourcePackfileIDs(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	meta := &PackfileMetadata{
		PackfileId:           "merged123",
		Status:               PackfileStatus_COMMITTED,
		Classification:       PackfileClassification_SECOND_CLASS,
		RepositoryId:         "my-repo",
		StorageNamespace:     "s3://my-bucket/my-ns/",
		ObjectCount:          200,
		SizeBytes:            2000000,
		SizeBytesCompressed:  1000000,
		CompressionAlgorithm: "zstd",
		ChecksumAlgorithm:    "sha256",
		CreatedAt:            timestamppb.Now(),
		SourcePackfileIds:    []string{"source1", "source2", "source3"},
	}

	repoID := graveler.RepositoryID("my-repo")
	packfileID := "merged123"

	// Put and retrieve
	err := store.PutPackfile(ctx, repoID, packfileID, meta)
	require.NoError(t, err)

	retrieved, err := store.GetPackfile(ctx, repoID, packfileID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, []string{"source1", "source2", "source3"}, retrieved.SourcePackfileIds)
}

func TestGetPackfileNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	_, err := store.GetPackfile(ctx, graveler.RepositoryID("nonexistent-repo"), "nonexistent-packfile")
	assert.ErrorIs(t, err, ErrPackfileNotFound)
}

func TestDeletePackfile(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	meta := &PackfileMetadata{
		PackfileId:   "to-delete",
		Status:       PackfileStatus_STAGED,
		RepositoryId: "my-repo",
	}

	repoID := graveler.RepositoryID("my-repo")
	packfileID := "to-delete"

	// Put then delete
	err := store.PutPackfile(ctx, repoID, packfileID, meta)
	require.NoError(t, err)

	// Verify it exists
	_, err = store.GetPackfile(ctx, repoID, packfileID)
	require.NoError(t, err)

	// Delete
	err = store.DeletePackfile(ctx, repoID, packfileID)
	require.NoError(t, err)

	// Verify it's gone
	_, err = store.GetPackfile(ctx, repoID, packfileID)
	assert.ErrorIs(t, err, ErrPackfileNotFound)
}

func TestPackfileSnapshotRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	snapshot := &PackfileSnapshot{
		RepositoryId:      "my-repo",
		ActivePackfileIds: []string{"active1", "active2"},
		StagedPackfileIds: []string{"staged1"},
		LastCommitAt:      timestamppb.Now(),
		LastMergeAt:       timestamppb.Now(),
		Version:           5,
	}

	repoID := graveler.RepositoryID("my-repo")

	// Test PutSnapshot and GetSnapshot
	err := store.PutSnapshot(ctx, repoID, snapshot)
	require.NoError(t, err)

	retrieved, err := store.GetSnapshot(ctx, repoID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, snapshot.RepositoryId, retrieved.RepositoryId)
	assert.Equal(t, snapshot.ActivePackfileIds, retrieved.ActivePackfileIds)
	assert.Equal(t, snapshot.StagedPackfileIds, retrieved.StagedPackfileIds)
	assert.Equal(t, snapshot.Version, retrieved.Version)
}

func TestPackfileSnapshotEmpty(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	_, err := store.GetSnapshot(ctx, graveler.RepositoryID("nonexistent-repo"))
	assert.ErrorIs(t, err, ErrSnapshotNotFound)
}

func TestUploadSessionRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	session := &UploadSession{
		UploadId:     "upload123",
		PackfileId:   "packfile456",
		RepositoryId: "my-repo",
		TmpPath:      "/tmp/upload-session",
		CreatedAt:    timestamppb.Now(),
	}

	uploadID := "upload123"

	// Test PutUploadSession and GetUploadSession
	err := store.PutUploadSession(ctx, uploadID, session)
	require.NoError(t, err)

	retrieved, err := store.GetUploadSession(ctx, uploadID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, session.UploadId, retrieved.UploadId)
	assert.Equal(t, session.PackfileId, retrieved.PackfileId)
	assert.Equal(t, session.RepositoryId, retrieved.RepositoryId)
	assert.Equal(t, session.TmpPath, retrieved.TmpPath)
}

func TestGetUploadSessionNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	_, err := store.GetUploadSession(ctx, "nonexistent-upload")
	assert.ErrorIs(t, err, ErrUploadSessionNotFound)
}

func TestDeleteUploadSession(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	session := &UploadSession{
		UploadId: "to-delete",
		TmpPath:  "/tmp/delete-me",
	}

	uploadID := "to-delete"

	// Put then delete
	err := store.PutUploadSession(ctx, uploadID, session)
	require.NoError(t, err)

	// Verify it exists
	_, err = store.GetUploadSession(ctx, uploadID)
	require.NoError(t, err)

	// Delete
	err = store.DeleteUploadSession(ctx, uploadID)
	require.NoError(t, err)

	// Verify it's gone
	_, err = store.GetUploadSession(ctx, uploadID)
	assert.ErrorIs(t, err, ErrUploadSessionNotFound)
}

func TestUpdateSnapshotWithRetry(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	repoID := graveler.RepositoryID("my-repo")

	// Create initial snapshot
	err := store.PutSnapshot(ctx, repoID, &PackfileSnapshot{
		RepositoryId: string(repoID),
		Version:      0,
	})
	require.NoError(t, err)

	// Update using the retry function
	err = store.UpdateSnapshotWithRetry(ctx, repoID, func(snapshot *PackfileSnapshot) error {
		snapshot.ActivePackfileIds = append(snapshot.ActivePackfileIds, "new-packfile-1")
		snapshot.LastCommitAt = timestamppb.Now()
		return nil
	})
	require.NoError(t, err)

	// Verify the update
	snapshot, err := store.GetSnapshot(ctx, repoID)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), snapshot.Version)
	assert.Equal(t, []string{"new-packfile-1"}, snapshot.ActivePackfileIds)
}

func TestUpdateSnapshotWithRetryCreatesNew(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()

	repoID := graveler.RepositoryID("new-repo")

	// Update non-existent snapshot
	err := store.UpdateSnapshotWithRetry(ctx, repoID, func(snapshot *PackfileSnapshot) error {
		snapshot.ActivePackfileIds = []string{"initial-packfile"}
		return nil
	})
	require.NoError(t, err)

	// Verify it was created
	snapshot, err := store.GetSnapshot(ctx, repoID)
	require.NoError(t, err)
	assert.Equal(t, string(repoID), snapshot.RepositoryId)
	assert.Equal(t, uint64(1), snapshot.Version)
	assert.Equal(t, []string{"initial-packfile"}, snapshot.ActivePackfileIds)
}

func TestKeyPatterns(t *testing.T) {
	// Test key pattern construction
	repoID := graveler.RepositoryID("my-repo")
	packfileID := "abcd1234"

	key := packfileKey(repoID, packfileID)
	assert.Equal(t, "packfile/my-repo/abcd1234", string(key))

	snapKey := snapshotKey(repoID)
	assert.Equal(t, "snapshot/my-repo", string(snapKey))

	uploadKeyStr := uploadKey("upload123")
	assert.Equal(t, "upload/upload123", string(uploadKeyStr))
}