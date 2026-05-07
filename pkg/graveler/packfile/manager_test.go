package packfile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// mockStoreManager implements Store operations for manager testing
type mockStoreManager struct {
	packfiles  map[string]*PackfileMetadata
	snapshots  map[string]*PackfileSnapshot
	uploadSess map[string]*UploadSession
}

func newMockStoreManager() *mockStoreManager {
	return &mockStoreManager{
		packfiles:  make(map[string]*PackfileMetadata),
		snapshots:  make(map[string]*PackfileSnapshot),
		uploadSess: make(map[string]*UploadSession),
	}
}

func (m *mockStoreManager) GetPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) (*PackfileMetadata, error) {
	key := string(repoID) + "/" + packfileID
	meta, ok := m.packfiles[key]
	if !ok {
		return nil, ErrPackfileNotFound
	}
	return meta, nil
}

func (m *mockStoreManager) PutPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string, meta *PackfileMetadata) error {
	key := string(repoID) + "/" + packfileID
	m.packfiles[key] = meta
	return nil
}

func (m *mockStoreManager) GetSnapshot(ctx context.Context, repoID graveler.RepositoryID) (*PackfileSnapshot, error) {
	key := string(repoID)
	snap, ok := m.snapshots[key]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	return snap, nil
}

func (m *mockStoreManager) PutSnapshot(ctx context.Context, repoID graveler.RepositoryID, snap *PackfileSnapshot) error {
	key := string(repoID)
	m.snapshots[key] = snap
	return nil
}

func (m *mockStoreManager) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	sess, ok := m.uploadSess[uploadID]
	if !ok {
		return nil, ErrUploadSessionNotFound
	}
	return sess, nil
}

func (m *mockStoreManager) PutUploadSession(ctx context.Context, uploadID string, session *UploadSession) error {
	m.uploadSess[uploadID] = session
	return nil
}

func (m *mockStoreManager) DeleteUploadSession(ctx context.Context, uploadID string) error {
	if _, ok := m.uploadSess[uploadID]; !ok {
		return ErrUploadSessionNotFound
	}
	delete(m.uploadSess, uploadID)
	return nil
}

func (m *mockStoreManager) DeletePackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) error {
	key := string(repoID) + "/" + packfileID
	delete(m.packfiles, key)
	return nil
}

func (m *mockStoreManager) UpdateSnapshotWithRetry(ctx context.Context, repoID graveler.RepositoryID, updateFn func(*PackfileSnapshot) error) error {
	return nil
}

// mockKVStoreManager is a minimal kv.Store implementation for testing
type mockKVStoreManager struct{}

func (m *mockKVStoreManager) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	return nil, kv.ErrNotFound
}
func (m *mockKVStoreManager) Set(ctx context.Context, partitionKey, key, value []byte) error   { return nil }
func (m *mockKVStoreManager) SetIf(ctx context.Context, partitionKey, key, value []byte, predicate kv.Predicate) error {
	return nil
}
func (m *mockKVStoreManager) Delete(ctx context.Context, partitionKey, key []byte) error { return nil }
func (m *mockKVStoreManager) Scan(ctx context.Context, partitionKey []byte, opts kv.ScanOptions) (kv.EntriesIterator, error) {
	return nil, nil
}
func (m *mockKVStoreManager) Close() {}

var _ kv.Store = (*mockKVStoreManager)(nil)

// managerStoreAdapter wraps mockStoreManager to satisfy StoreInterface
type managerStoreAdapter struct {
	mock *mockStoreManager
}

func (w *managerStoreAdapter) GetPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) (*PackfileMetadata, error) {
	return w.mock.GetPackfile(ctx, repoID, packfileID)
}
func (w *managerStoreAdapter) PutPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string, meta *PackfileMetadata) error {
	return w.mock.PutPackfile(ctx, repoID, packfileID, meta)
}
func (w *managerStoreAdapter) GetSnapshot(ctx context.Context, repoID graveler.RepositoryID) (*PackfileSnapshot, error) {
	return w.mock.GetSnapshot(ctx, repoID)
}
func (w *managerStoreAdapter) PutSnapshot(ctx context.Context, repoID graveler.RepositoryID, snap *PackfileSnapshot) error {
	return w.mock.PutSnapshot(ctx, repoID, snap)
}
func (w *managerStoreAdapter) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	return w.mock.GetUploadSession(ctx, uploadID)
}
func (w *managerStoreAdapter) PutUploadSession(ctx context.Context, uploadID string, session *UploadSession) error {
	return w.mock.PutUploadSession(ctx, uploadID, session)
}
func (w *managerStoreAdapter) DeleteUploadSession(ctx context.Context, uploadID string) error {
	return w.mock.DeleteUploadSession(ctx, uploadID)
}
func (w *managerStoreAdapter) DeletePackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) error {
	return w.mock.DeletePackfile(ctx, repoID, packfileID)
}
func (w *managerStoreAdapter) UpdateSnapshotWithRetry(ctx context.Context, repoID graveler.RepositoryID, updateFn func(*PackfileSnapshot) error) error {
	return w.mock.UpdateSnapshotWithRetry(ctx, repoID, updateFn)
}

func TestNewPackfileManager(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})
	require.NotNil(t, manager)
}

func TestManagerGetPackfile(t *testing.T) {
	mock := newMockStoreManager()
	mock.packfiles["repo1/pf123"] = &PackfileMetadata{
		PackfileId:   "pf123",
		RepositoryId: "repo1",
		Status:       PackfileStatus_STAGED,
	}

	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	meta, err := manager.GetPackfile(ctx, graveler.RepositoryID("repo1"), "pf123")
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, "pf123", meta.PackfileId)
}

func TestManagerGetPackfileNotFound(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	_, err := manager.GetPackfile(ctx, graveler.RepositoryID("repo1"), "nonexistent")
	assert.ErrorIs(t, err, ErrPackfileNotFound)
}

func TestManagerListPackfiles(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	packfiles, hasMore, err := manager.ListPackfiles(ctx, graveler.RepositoryID("repo1"), "", 10)
	require.NoError(t, err)
	assert.Empty(t, packfiles)
	assert.False(t, hasMore)
}

func TestManagerGetSnapshot(t *testing.T) {
	mock := newMockStoreManager()
	mock.snapshots["repo1"] = &PackfileSnapshot{
		RepositoryId:     "repo1",
		ActivePackfileIds: []string{"pf1", "pf2"},
		StagedPackfileIds: []string{"pf3"},
		Version:           5,
	}

	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	snap, err := manager.GetSnapshot(ctx, graveler.RepositoryID("repo1"))
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, "repo1", snap.RepositoryId)
	assert.Len(t, snap.ActivePackfileIds, 2)
	assert.Equal(t, uint64(5), snap.Version)
}

func TestManagerGetSnapshotNotFound(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	_, err := manager.GetSnapshot(ctx, graveler.RepositoryID("repo1"))
	assert.ErrorIs(t, err, ErrSnapshotNotFound)
}

func TestManagerCreateUploadSession(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	session, err := manager.CreateUploadSession(ctx, graveler.RepositoryID("repo1"), "upload123", "/tmp/packfiles/repo1/upload123")
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, "upload123", session.UploadId)
	assert.Equal(t, "repo1", session.RepositoryId)
	assert.Equal(t, "/tmp/packfiles/repo1/upload123", session.TmpPath)
}

func TestManagerCompleteUploadSession(t *testing.T) {
	mock := newMockStoreManager()
	mock.uploadSess["upload123"] = &UploadSession{
		UploadId:     "upload123",
		RepositoryId: "repo1",
		TmpPath:      "/tmp/packfiles/repo1/upload123",
	}

	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	err := manager.CompleteUploadSession(ctx, "upload123")
	require.NoError(t, err)
}

func TestManagerAbortUploadSession(t *testing.T) {
	mock := newMockStoreManager()
	mock.uploadSess["upload123"] = &UploadSession{
		UploadId:     "upload123",
		RepositoryId: "repo1",
		TmpPath:      "/tmp/packfiles/repo1/upload123",
	}

	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	err := manager.AbortUploadSession(ctx, "upload123")
	require.NoError(t, err)

	// Verify session is deleted
	_, err = mock.GetUploadSession(ctx, "upload123")
	assert.ErrorIs(t, err, ErrUploadSessionNotFound)
}

func TestManagerAbortUploadSessionNotFound(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	err := manager.AbortUploadSession(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrUploadSessionNotFound)
}

func TestManagerContextPropagation(t *testing.T) {
	// Verify all methods accept context.Context as first parameter
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()

	// These should compile - proving context propagation
	_, _ = manager.GetPackfile(ctx, graveler.RepositoryID("r"), "p")
	_, _, _ = manager.ListPackfiles(ctx, graveler.RepositoryID("r"), "", 10)
	_, _ = manager.GetSnapshot(ctx, graveler.RepositoryID("r"))
	_, _ = manager.CreateUploadSession(ctx, graveler.RepositoryID("r"), "u", "/tmp")
	_ = manager.CompleteUploadSession(ctx, "u")
	_ = manager.AbortUploadSession(ctx, "u")
	_ = manager.MergeStaged(ctx, graveler.RepositoryID("r"))
	_ = manager.Commit(ctx, graveler.RepositoryID("r"))
	_ = manager.CleanupSuperseded(ctx, graveler.RepositoryID("r"))
}