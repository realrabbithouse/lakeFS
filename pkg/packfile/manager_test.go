package packfile

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem" // Import mem driver
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestManager_FullFlow(t *testing.T) {
	ctx := context.Background()
	store := makeStore(t)

	// Create a temp dir for packfiles
	tmpDir := t.TempDir()

	// Create block adapter (using mem adapter)
	blockAdapter := mem.New(ctx)

	// Create manager with real block adapter and config
	m := NewManager(store, blockAdapter, Config{
		StorageNamespace: "mem://storage",
		LocalPath:        tmpDir,
		MaxPackSize:      5 * 1024 * 1024 * 1024,
		AsyncReplicate:   false, // Sync for testing
	})

	// Helper to create a packfile and store it in block adapter
	createPackfile := func(packfileID string, objects []string) error {
		f, err := os.CreateTemp(tmpDir, "packfile-*.pack")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		defer f.Close()

		packer := NewPacker(f, CompressionZstd, ClassificationSecondClass)
		for _, obj := range objects {
			_, _, err := packer.AppendObject(ctx, strings.NewReader(obj), int64(len(obj)))
			if err != nil {
				return err
			}
		}
		if err := packer.Close(); err != nil {
			return err
		}

		// Seek and upload to block storage
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			return err
		}
		obj := block.ObjectPointer{
			StorageNamespace: "mem://storage",
			Identifier:       packfileID,
			IdentifierType:   block.IdentifierTypeRelative,
		}
		_, err = blockAdapter.Put(ctx, obj, info.Size(), f, block.PutOpts{})
		return err
	}

	// Create packfile pf1 with objects "obj1" and "obj2"
	err := createPackfile("pf1", []string{"object1data", "object2data"})
	require.NoError(t, err)

	// Create packfile pf2 with objects "obj3" and "obj4"
	err = createPackfile("pf2", []string{"object3data", "object4data"})
	require.NoError(t, err)

	// Manually create PackfileMetadata entries (simulating what CompleteUploadSession does)
	// since we stored data directly in block adapter
	meta1 := &PackfileMetadata{
		PackfileId:            "pf1",
		Status:                PackfileStatus_STAGED,
		Classification:        PackfileClassification_SECOND_CLASS,
		RepositoryId:          "repo1",
		StorageNamespace:      "mem://storage",
		ObjectCount:           2,
		SizeBytes:             22, // len("object1data") + len("object2data")
		SizeBytesCompressed:   0,
		CompressionAlgorithm:   "zstd",
		ChecksumAlgorithm:      "sha256",
		CreatedAt:             timestamppb.Now(),
		SourcePackfileIds:     nil,
	}
	err = kv.SetMsg(ctx, store, PackfilesPartition, []byte(PackfilePath("pf1")), meta1)
	require.NoError(t, err)

	meta2 := &PackfileMetadata{
		PackfileId:            "pf2",
		Status:                PackfileStatus_STAGED,
		Classification:        PackfileClassification_SECOND_CLASS,
		RepositoryId:          "repo1",
		StorageNamespace:      "mem://storage",
		ObjectCount:           2,
		SizeBytes:             22, // len("object3data") + len("object4data")
		SizeBytesCompressed:   0,
		CompressionAlgorithm:   "zstd",
		ChecksumAlgorithm:      "sha256",
		CreatedAt:             timestamppb.Now(),
		SourcePackfileIds:     nil,
	}
	err = kv.SetMsg(ctx, store, PackfilesPartition, []byte(PackfilePath("pf2")), meta2)
	require.NoError(t, err)

	// Step: Update snapshot to mark them as staged for repo1
	err = m.UpdateSnapshot(ctx, "repo1", func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = []string{"pf1", "pf2"}
		return nil
	})
	require.NoError(t, err)

	// Step: MergeStaged - should merge pf1 and pf2 into one
	merged, err := m.MergeStaged(ctx, "repo1")
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, PackfileStatus_STAGED, merged.Status)
	require.Equal(t, int64(4), merged.ObjectCount) // pf1 + pf2 = 4 objects

	// Verify source packfiles are now SUPERSEDED
	meta1, err = m.GetPackfile(ctx, "pf1")
	require.NoError(t, err)
	require.Equal(t, PackfileStatus_SUPERSEDED, meta1.Status)

	meta2, err = m.GetPackfile(ctx, "pf2")
	require.NoError(t, err)
	require.Equal(t, PackfileStatus_SUPERSEDED, meta2.Status)

	// Step: Commit - transitions from STAGED to COMMITTED
	err = m.Commit(ctx, "repo1")
	require.NoError(t, err)

	// Verify merged packfile is now COMMITTED
	merged, err = m.GetPackfile(ctx, merged.PackfileId)
	require.NoError(t, err)
	require.Equal(t, PackfileStatus_COMMITTED, merged.Status)
	require.Equal(t, PackfileClassification_FIRST_CLASS, merged.Classification)

	// Verify snapshot: staged should be empty, active should have merged
	snap, err := m.GetSnapshot(ctx, "repo1")
	require.NoError(t, err)
	require.Empty(t, snap.StagedPackfileIds)
	require.Len(t, snap.ActivePackfileIds, 1)
	require.Equal(t, merged.PackfileId, snap.ActivePackfileIds[0])
}

func TestManager_MergeStaged_SinglePackfile(t *testing.T) {
	ctx := context.Background()
	store := makeStore(t)

	// Create packfile manager with nil block adapter (not needed for this test)
	m := NewManager(store, nil, Config{})

	// Create and complete a single upload session
	session, err := m.CreateUploadSession(ctx, "repo1", "pf1", 1024, "sha256:abc", "zstd", 5)
	require.NoError(t, err)
	err = m.CompleteUploadSession(ctx, session.UploadId)
	require.NoError(t, err)

	// Set snapshot with single staged packfile
	err = m.UpdateSnapshot(ctx, "repo1", func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = []string{"pf1"}
		return nil
	})
	require.NoError(t, err)

	// MergeStaged with single packfile - should return it unchanged
	merged, err := m.MergeStaged(ctx, "repo1")
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, "pf1", merged.PackfileId)
	require.Equal(t, PackfileStatus_STAGED, merged.Status)

	// pf1 should still be STAGED (not SUPERSEDED since no merge happened)
	meta1, err := m.GetPackfile(ctx, "pf1")
	require.NoError(t, err)
	require.Equal(t, PackfileStatus_STAGED, meta1.Status)
}

func TestManager_MergeStaged_NoStaged(t *testing.T) {
	ctx := context.Background()
	store := makeStore(t)
	m := NewManager(store, nil, Config{})

	// Update snapshot with empty staged
	err := m.UpdateSnapshot(ctx, "repo1", func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = nil
		return nil
	})
	require.NoError(t, err)

	// MergeStaged with no staged packfiles - should return nil
	merged, err := m.MergeStaged(ctx, "repo1")
	require.NoError(t, err)
	require.Nil(t, merged)
}
