package packfile

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// PackfileManagerConfig holds dependencies for PackfileManager.
type PackfileManagerConfig struct {
	BlockAdapter block.Adapter
	Store        StoreInterface
}

// PackfileManager is the single facade for all packfile operations.
// Graveler and REST API handlers call this type, not individual sub-package types.
// PackfileManager owns all packfile state — Graveler is a client, not an owner.
type PackfileManager struct {
	blockAdapter block.Adapter
	store        StoreInterface
}

// NewPackfileManager creates a new PackfileManager with all dependencies wired.
func NewPackfileManager(cfg PackfileManagerConfig) *PackfileManager {
	return &PackfileManager{
		blockAdapter: cfg.BlockAdapter,
		store:        cfg.Store,
	}
}

// GetPackfile retrieves packfile metadata by repository ID and packfile ID.
func (m *PackfileManager) GetPackfile(ctx context.Context, repoID graveler.RepositoryID, packfileID string) (*PackfileMetadata, error) {
	return m.store.GetPackfile(ctx, repoID, packfileID)
}

// ListPackfiles returns a paginated list of packfile metadata for a repository.
// TODO: Implement pagination with KV scan.
func (m *PackfileManager) ListPackfiles(ctx context.Context, repoID graveler.RepositoryID, after string, limit int) ([]*PackfileMetadata, bool, error) {
	return []*PackfileMetadata{}, false, nil
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

// MergeStaged merges all STAGED packfiles in the repository snapshot.
// It deduplicates objects across staged packfiles and writes one merged STAGED packfile.
// TODO: Implement in Epic 3.
func (m *PackfileManager) MergeStaged(ctx context.Context, repoID graveler.RepositoryID) error {
	return nil
}

// Commit transitions merged STAGED packfiles to COMMITTED status.
// Graveler calls this after the commit transaction succeeds.
// TODO: Implement in Epic 3.
func (m *PackfileManager) Commit(ctx context.Context, repoID graveler.RepositoryID) error {
	return nil
}

// CleanupSuperseded deletes .pack and .sst files for packfiles with SUPERSEDED status.
// TODO: Implement in Epic 4.
func (m *PackfileManager) CleanupSuperseded(ctx context.Context, repoID graveler.RepositoryID) error {
	return nil
}