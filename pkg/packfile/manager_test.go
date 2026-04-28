package packfile

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem" // Import mem driver
)

func makeStore(t *testing.T) kv.Store {
	t.Helper()
	ctx := context.Background()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func TestManager_CreateUploadSession(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	session, err := m.CreateUploadSession(ctx, "repo1", "abc123def456", 1024, "sha256:abc123", "zstd", 10)
	require.NoError(t, err)
	require.Equal(t, "repo1", session.RepositoryId)
	require.Equal(t, "abc123def456", session.PackfileId)
	require.NotEmpty(t, session.UploadId)
}

func TestManager_GetPackfile_NotFound(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	_, err := m.GetPackfile(ctx, "nonexistent")
	require.True(t, errors.Is(err, ErrPackfileNotFound))
}

func TestManager_CompleteUploadSession(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()

	// Create an upload session
	session, err := m.CreateUploadSession(ctx, "repo1", "abc123def456", 1024, "sha256:abc123", "zstd", 10)
	require.NoError(t, err)

	// Complete it
	err = m.CompleteUploadSession(ctx, session.UploadId)
	require.NoError(t, err)

	// Verify packfile exists
	meta, err := m.GetPackfile(ctx, "abc123def456")
	require.NoError(t, err)
	require.Equal(t, "abc123def456", meta.PackfileId)
	require.Equal(t, PackfileStatus_STAGED, meta.Status)
	require.Equal(t, PackfileClassification_SECOND_CLASS, meta.Classification)
}

func TestManager_AbortUploadSession(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()

	// Create an upload session
	session, err := m.CreateUploadSession(ctx, "repo1", "abc123def456", 1024, "sha256:abc123", "zstd", 10)
	require.NoError(t, err)

	// Abort it
	err = m.AbortUploadSession(ctx, session.UploadId)
	require.NoError(t, err)

	// Verify upload session is gone
	_, err = m.GetUploadSession(ctx, session.UploadId)
	require.True(t, errors.Is(err, ErrUploadSessionNotFound))
}

func TestManager_GetSnapshot_NotFound(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()
	_, err := m.GetSnapshot(ctx, "nonexistent")
	require.True(t, errors.Is(err, ErrSnapshotNotFound))
}

func TestManager_UpdateSnapshot(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()

	// Update non-existent snapshot - should create it
	err := m.UpdateSnapshot(ctx, "repo1", func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = []string{"pf1", "pf2"}
		return nil
	})
	require.NoError(t, err)

	// Verify snapshot was created
	snap, err := m.GetSnapshot(ctx, "repo1")
	require.NoError(t, err)
	require.Equal(t, []string{"pf1", "pf2"}, snap.StagedPackfileIds)

	// Update existing snapshot
	err = m.UpdateSnapshot(ctx, "repo1", func(snap *PackfileSnapshot) error {
		snap.ActivePackfileIds = []string{"pf3"}
		return nil
	})
	require.NoError(t, err)

	// Verify snapshot was updated (staged preserved, active added)
	snap, err = m.GetSnapshot(ctx, "repo1")
	require.NoError(t, err)
	require.Equal(t, []string{"pf1", "pf2"}, snap.StagedPackfileIds)
	require.Equal(t, []string{"pf3"}, snap.ActivePackfileIds)
}

func TestManager_ListPackfiles_UsesSnapshot(t *testing.T) {
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	ctx := context.Background()

	// Create packfiles via upload sessions
	session1, err := m.CreateUploadSession(ctx, "repo1", "packfile1", 1024, "sha256:abc123", "zstd", 10)
	require.NoError(t, err)
	session2, err := m.CreateUploadSession(ctx, "repo1", "packfile2", 1024, "sha256:def456", "zstd", 10)
	require.NoError(t, err)

	// Complete both
	err = m.CompleteUploadSession(ctx, session1.UploadId)
	require.NoError(t, err)
	err = m.CompleteUploadSession(ctx, session2.UploadId)
	require.NoError(t, err)

	// Set snapshot with these packfiles as staged
	err = m.UpdateSnapshot(ctx, "repo1", func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = []string{"packfile1", "packfile2"}
		return nil
	})
	require.NoError(t, err)

	// ListPackfiles should use snapshot IDs
	pfList, err := m.ListPackfiles(ctx, "repo1")
	require.NoError(t, err)
	require.Len(t, pfList, 2)
}
