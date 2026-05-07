package packfile

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
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