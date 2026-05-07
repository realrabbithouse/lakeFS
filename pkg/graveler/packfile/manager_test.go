package packfile

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

func (m *mockStoreManager) ScanPackfiles(ctx context.Context, repoID graveler.RepositoryID, after string, limit int) (PackfilesIterator, error) {
	return &mockPackfilesIterator{packfiles: m.packfiles, repoID: string(repoID), after: after, limit: limit}, nil
}

func (m *mockStoreManager) ScanPackfilesByStatus(ctx context.Context, repoID graveler.RepositoryID, status PackfileStatus) (PackfilesIterator, error) {
	return &mockPackfilesByStatusIterator{packfiles: m.packfiles, repoID: string(repoID), status: status}, nil
}

// mockPackfilesByStatusIterator filters packfiles by status.
type mockPackfilesByStatusIterator struct {
	packfiles map[string]*PackfileMetadata
	repoID    string
	status    PackfileStatus
	index     int
	keys      []string
	current   *PackfileMetadata
}

func (m *mockPackfilesByStatusIterator) Next() bool {
	if m.keys == nil {
		for k := range m.packfiles {
			if strings.HasPrefix(k, m.repoID+"/") {
				m.keys = append(m.keys, k)
			}
		}
		sort.Strings(m.keys)
	}
	for m.index < len(m.keys) {
		key := m.keys[m.index]
		m.index++
		meta := m.packfiles[key]
		if meta != nil && meta.Status == m.status {
			m.current = meta
			return true
		}
	}
	m.current = nil
	return false
}

func (m *mockPackfilesByStatusIterator) Value() *PackfileMetadata {
	return m.current
}

func (m *mockPackfilesByStatusIterator) Err() error {
	return nil
}

func (m *mockPackfilesByStatusIterator) Close() {
}

// mockPackfilesIterator is a simple iterator for testing.
type mockPackfilesIterator struct {
	packfiles map[string]*PackfileMetadata
	repoID    string
	after     string
	limit     int
	index     int
	keys      []string
	current   *PackfileMetadata
}

func (m *mockPackfilesIterator) Next() bool {
	if m.keys == nil {
		// Build sorted keys on first call
		for k := range m.packfiles {
			if strings.HasPrefix(k, m.repoID+"/") {
				m.keys = append(m.keys, k)
			}
		}
		sort.Strings(m.keys)
		// Skip past 'after'
		if m.after != "" {
			for i, k := range m.keys {
				if strings.Contains(k, m.after) {
					m.keys = m.keys[i+1:]
					break
				}
			}
		}
	}
	if m.index >= len(m.keys) || m.index >= m.limit {
		return false
	}
	key := m.keys[m.index]
	m.current = m.packfiles[key]
	m.index++
	return true
}

func (m *mockPackfilesIterator) Value() *PackfileMetadata {
	return m.current
}

func (m *mockPackfilesIterator) Err() error {
	return nil
}

func (m *mockPackfilesIterator) Close() {
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
func (w *managerStoreAdapter) ScanPackfiles(ctx context.Context, repoID graveler.RepositoryID, after string, limit int) (PackfilesIterator, error) {
	return w.mock.ScanPackfiles(ctx, repoID, after, limit)
}
func (w *managerStoreAdapter) ScanPackfilesByStatus(ctx context.Context, repoID graveler.RepositoryID, status PackfileStatus) (PackfilesIterator, error) {
	return w.mock.ScanPackfilesByStatus(ctx, repoID, status)
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

func TestManagerListPackfilesSuccess(t *testing.T) {
	mock := newMockStoreManager()
	mock.packfiles["repo1/pf1"] = &PackfileMetadata{
		PackfileId:   "pf1",
		RepositoryId: "repo1",
		Status:       PackfileStatus_STAGED,
	}
	mock.packfiles["repo1/pf2"] = &PackfileMetadata{
		PackfileId:   "pf2",
		RepositoryId: "repo1",
		Status:       PackfileStatus_COMMITTED,
	}

	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	result, err := manager.ListPackfiles(ctx, graveler.RepositoryID("repo1"), "", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Packfiles, 2)
	assert.False(t, result.HasMore)
}

func TestManagerListPackfilesPagination(t *testing.T) {
	mock := newMockStoreManager()
	// Add 3 packfiles
	for i := 1; i <= 3; i++ {
		pfid := fmt.Sprintf("pf%d", i)
		mock.packfiles["repo1/"+pfid] = &PackfileMetadata{
			PackfileId:   pfid,
			RepositoryId: "repo1",
			Status:       PackfileStatus_STAGED,
		}
	}

	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()

	// Request limit=2 should return has_more=true
	result, err := manager.ListPackfiles(ctx, graveler.RepositoryID("repo1"), "", 2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Packfiles, 2)
	assert.True(t, result.HasMore)
	assert.NotEmpty(t, result.NextOffset)
}

func TestManagerListPackfilesEmpty(t *testing.T) {
	mock := newMockStoreManager()
	manager := NewPackfileManager(PackfileManagerConfig{
		Store:   &managerStoreAdapter{mock: mock},
	})

	ctx := context.Background()
	result, err := manager.ListPackfiles(ctx, graveler.RepositoryID("repo1"), "", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Packfiles)
	assert.False(t, result.HasMore)
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
	_, _ = manager.ListPackfiles(ctx, graveler.RepositoryID("r"), "", 10)
	_, _ = manager.GetSnapshot(ctx, graveler.RepositoryID("r"))
	_, _ = manager.CreateUploadSession(ctx, graveler.RepositoryID("r"), "u", "/tmp")
	_ = manager.CompleteUploadSession(ctx, "u")
	_ = manager.AbortUploadSession(ctx, "u")
	_, _ = manager.MergeStaged(ctx, graveler.RepositoryID("r"))
	_ = manager.Commit(ctx, graveler.RepositoryID("r"))
	_ = manager.CleanupSuperseded(ctx, graveler.RepositoryID("r"))
}