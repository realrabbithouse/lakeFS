package packfile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/packfile"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ListPackfilesResult contains the paginated list result.
type ListPackfilesResult struct {
	Packfiles  []*PackfileMetadata
	HasMore    bool
	NextOffset string
}

// PackfileManagerConfig holds dependencies for PackfileManager.
type PackfileManagerConfig struct {
	BlockAdapter block.Adapter
	Store        StoreInterface
	Logger       logging.Logger
}

// PackfileManager is the single facade for all packfile operations.
// Graveler and REST API handlers call this type, not individual sub-package types.
// PackfileManager owns all packfile state — Graveler is a client, not an owner.
type PackfileManager struct {
	blockAdapter block.Adapter
	store        StoreInterface
	logger       logging.Logger
}

// NewPackfileManager creates a new PackfileManager with all dependencies wired.
func NewPackfileManager(cfg PackfileManagerConfig) *PackfileManager {
	if cfg.Logger == nil {
		cfg.Logger = logging.Dummy()
	}
	return &PackfileManager{
		blockAdapter: cfg.BlockAdapter,
		store:        cfg.Store,
		logger:       cfg.Logger,
	}
}

// GetPackfile retrieves packfile metadata by repository ID and packfile ID.
func (m *PackfileManager) GetPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) (*PackfileMetadata, error) {
	meta, err := m.store.GetPackfile(ctx, repoID, packfileID)
	if err != nil {
		return nil, fmt.Errorf("packfile: getting packfile: %w", err)
	}
	m.logger.Info("packfile: get packfile",
		"repo_id", repoID,
		"packfile_id", packfileID)
	return meta, nil
}

// ListPackfiles returns a paginated list of packfile metadata for a repository.
func (m *PackfileManager) ListPackfiles(ctx context.Context, repoID graveler.RepositoryID, after string, limit int) (*ListPackfilesResult, error) {
	if limit <= 0 {
		limit = 100
	}

	// Use KV scan to list packfiles for this repository
	var packfiles []*PackfileMetadata
	var lastKey string

	iter, err := m.store.ScanPackfiles(ctx, repoID, after, limit+1)
	if err != nil {
		return nil, fmt.Errorf("packfile: listing packfiles: %w", err)
	}
	defer iter.Close()

	for iter.Next() {
		meta := iter.Value()
		if meta == nil {
			continue
		}
		packfiles = append(packfiles, meta)
		lastKey = meta.PackfileId
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("packfile: scanning packfiles: %w", err)
	}

	hasMore := len(packfiles) > limit
	if hasMore {
		packfiles = packfiles[:limit]
		lastKey = packfiles[len(packfiles)-1].PackfileId
	}

	var nextOffset string
	if hasMore {
		nextOffset = lastKey
	}

	m.logger.Info("packfile: list packfiles",
		"repo_id", repoID,
		"count", len(packfiles),
		"has_more", hasMore)

	return &ListPackfilesResult{
		Packfiles:  packfiles,
		HasMore:    hasMore,
		NextOffset: nextOffset,
	}, nil
}

// GetSnapshot retrieves the packfile snapshot for a repository.
func (m *PackfileManager) GetSnapshot(ctx context.Context, repoID graveler.RepositoryID) (*PackfileSnapshot, error) {
	return m.store.GetSnapshot(ctx, repoID)
}

// CreateUploadSession creates a new upload session and returns the session info.
// The tmp/ path is set up for receiving packfile data.
func (m *PackfileManager) CreateUploadSession(ctx context.Context, repoID graveler.RepositoryID, uploadID string, tmpPath string) (*UploadSession, error) {
	session := &UploadSession{
		UploadId:     uploadID,
		RepositoryId: string(repoID),
		TmpPath:      tmpPath,
	}
	if err := m.store.PutUploadSession(ctx, uploadID, session); err != nil {
		return nil, fmt.Errorf("packfile: creating upload session: %w", err)
	}
	return session, nil
}

// CompleteUploadSession marks an upload session as completed.
// The packfile transitions from staging to STAGED status.
// TODO: Implement completion logic in Epic 3 (rename tmp/, create metadata).
func (m *PackfileManager) CompleteUploadSession(ctx context.Context, uploadID string) error {
	session, err := m.store.GetUploadSession(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("packfile: getting upload session: %w", err)
	}

	// TODO: Use session.TmpPath and session.RepositoryId to finalize the packfile
	_ = session
	return nil
}

// AbortUploadSession removes an upload session and cleans up tmp/ files.
func (m *PackfileManager) AbortUploadSession(ctx context.Context, uploadID string) error {
	if err := m.store.DeleteUploadSession(ctx, uploadID); err != nil {
		return fmt.Errorf("packfile: aborting upload session: %w", err)
	}
	return nil
}

// MergeStaged merges all STAGED packfiles into one STAGED packfile.
// Called by Graveler before the commit transaction.
// It deduplicates objects across all STAGED packfiles and creates one merged STAGED packfile.
func (m *PackfileManager) MergeStaged(ctx context.Context, repoID graveler.RepositoryID) (*PackfileMetadata, error) {
	// Get the snapshot to find all STAGED packfiles
	snapshot, err := m.store.GetSnapshot(ctx, repoID)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			// No snapshot means no STAGED packfiles
			return nil, nil
		}
		return nil, fmt.Errorf("packfile: getting snapshot: %w", err)
	}

	if len(snapshot.StagedPackfileIds) == 0 {
		// No STAGED packfiles to merge
		return nil, nil
	}

	m.logger.Info("packfile: merge staged started",
		"repo_id", repoID,
		"staged_count", len(snapshot.StagedPackfileIds))

	// Get metadata for all STAGED packfiles to find storage paths
	var stagedPackfiles []*PackfileMetadata
	var stagedIDs []packfile.PackfileID
	for _, pfID := range snapshot.StagedPackfileIds {
		meta, err := m.store.GetPackfile(ctx, repoID, pfID)
		if err != nil {
			return nil, fmt.Errorf("packfile: getting staged packfile %s: %w", pfID, err)
		}
		stagedPackfiles = append(stagedPackfiles, meta)
		stagedIDs = append(stagedIDs, packfile.PackfileID(pfID))
	}

	// Perform the merge using pkg/packfile/merge.go
	// Use storage namespace from the first staged packfile's metadata
	storagePath := stagedPackfiles[0].StorageNamespace
	mergeInput := packfile.MergeInput{
		RepoID:            string(repoID),
		SourcePackfileIDs: stagedIDs,
		CompressionAlgo:   packfile.CompressionZstd, // Default compression
		Classification:    packfile.ClassificationSecondClass,
		StoragePath:       storagePath,
	}

	result, err := packfile.Merge(ctx, mergeInput)
	if err != nil {
		return nil, fmt.Errorf("packfile: merge operation: %w", err)
	}

	// Create new STAGED packfile metadata
	now := time.Now()
	// Convert packfile IDs to strings for protobuf field
	sourceIDs := make([]string, len(stagedIDs))
	for i, id := range stagedIDs {
		sourceIDs[i] = string(id)
	}
	newMeta := &PackfileMetadata{
		PackfileId:            string(result.NewPackfileID),
		Status:                PackfileStatus_STAGED,
		Classification:        PackfileClassification_SECOND_CLASS,
		RepositoryId:          string(repoID),
		ObjectCount:          int64(result.MergedCount),
		CompressionAlgorithm: "zstd",
		CreatedAt:            timestamppb.New(now),
		SourcePackfileIds:    sourceIDs,
	}

	// Store the new packfile metadata
	if err := m.store.PutPackfile(ctx, repoID, string(result.NewPackfileID), newMeta); err != nil {
		return nil, fmt.Errorf("packfile: storing merged packfile metadata: %w", err)
	}

	// Update snapshot with retry (optimistic concurrency)
	err = m.store.UpdateSnapshotWithRetry(ctx, repoID, func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = []string{string(result.NewPackfileID)}
		snap.LastMergeAt = timestamppb.New(now)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("packfile: updating snapshot: %w", err)
	}

	m.logger.Info("packfile: merge staged completed",
		"repo_id", repoID,
		"new_packfile_id", result.NewPackfileID,
		"objects", result.MergedCount,
		"deduplicated", result.DeduplicatedCount)

	return newMeta, nil
}

// Commit transitions merged STAGED packfiles to COMMITTED status.
// Graveler calls this after the commit transaction succeeds.
// It promotes the merged STAGED packfile to FIRST_CLASS COMMITTED status.
func (m *PackfileManager) Commit(ctx context.Context, repoID graveler.RepositoryID) error {
	snapshot, err := m.store.GetSnapshot(ctx, repoID)
	if err != nil {
		return fmt.Errorf("packfile: getting snapshot: %w", err)
	}

	if len(snapshot.StagedPackfileIds) == 0 {
		return fmt.Errorf("packfile: no staged packfiles to commit")
	}

	// There should be exactly one merged STAGED packfile
	stagedID := snapshot.StagedPackfileIds[0]
	stagedMeta, err := m.store.GetPackfile(ctx, repoID, stagedID)
	if err != nil {
		return fmt.Errorf("packfile: getting staged packfile: %w", err)
	}

	// Transition STAGED → COMMITTED with FIRST_CLASS
	stagedMeta.Status = PackfileStatus_COMMITTED
	stagedMeta.Classification = PackfileClassification_FIRST_CLASS

	if err := m.store.PutPackfile(ctx, repoID, stagedID, stagedMeta); err != nil {
		return fmt.Errorf("packfile: updating packfile status: %w", err)
	}

	// Update snapshot: move to active, clear staged
	now := time.Now()
	err = m.store.UpdateSnapshotWithRetry(ctx, repoID, func(snap *PackfileSnapshot) error {
		snap.ActivePackfileIds = []string{stagedID}
		snap.StagedPackfileIds = nil
		snap.LastCommitAt = timestamppb.New(now)
		return nil
	})
	if err != nil {
		return fmt.Errorf("packfile: updating snapshot: %w", err)
	}

	m.logger.Info("packfile: commit completed",
		"repo_id", repoID,
		"packfile_id", stagedID)

	return nil
}

// CleanupSuperseded deletes .pack and .sst files for packfiles with SUPERSEDED status.
// Called during garbage collection to reclaim disk space.
func (m *PackfileManager) CleanupSuperseded(ctx context.Context, repoID graveler.RepositoryID) error {
	m.logger.Info("packfile: cleanup superseded started",
		"repo_id", repoID)

	// Scan for packfiles with SUPERSEDED status
	iter, err := m.store.ScanPackfilesByStatus(ctx, repoID, PackfileStatus_SUPERSEDED)
	if err != nil {
		return fmt.Errorf("packfile: scanning superseded packfiles: %w", err)
	}
	defer iter.Close()

	var cleanedIDs []string
	for iter.Next() {
		meta := iter.Value()
		if meta == nil {
			continue
		}

		// Construct the packfile path
		packfilePath := filepath.Join(meta.StorageNamespace, "_packfiles", meta.RepositoryId, meta.PackfileId)

		// Delete .pack file
		packFile := filepath.Join(packfilePath, "data.pack")
		if err := os.Remove(packFile); err != nil && !os.IsNotExist(err) {
			m.logger.Warn("packfile: failed to delete pack file",
				"packfile_id", meta.PackfileId,
				"path", packFile,
				"error", err)
		}

		// Delete .sst file
		indexFile := filepath.Join(packfilePath, "data.sst")
		if err := os.Remove(indexFile); err != nil && !os.IsNotExist(err) {
			m.logger.Warn("packfile: failed to delete index file",
				"packfile_id", meta.PackfileId,
				"path", indexFile,
				"error", err)
		}

		// Remove the directory if empty
		os.Remove(packfilePath)

		// Transition status to DELETED in kv
		meta.Status = PackfileStatus_DELETED
		if err := m.store.PutPackfile(ctx, repoID, meta.PackfileId, meta); err != nil {
			m.logger.Warn("packfile: failed to update packfile status to DELETED",
				"packfile_id", meta.PackfileId,
				"error", err)
		}

		cleanedIDs = append(cleanedIDs, meta.PackfileId)
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("packfile: iterating superseded packfiles: %w", err)
	}

	m.logger.Info("packfile: cleanup superseded completed",
		"repo_id", repoID,
		"count", len(cleanedIDs))

	return nil
}