package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/packfile"
)

// packfileManagerAdapter wraps a packfile.Manager to implement the graveler.PackfileManager interface.
// graveler expects MergeStaged(ctx, repoID) error, but packfile.Manager returns (*PackfileMetadata, error).
// This adapter discards the metadata return value.
type packfileManagerAdapter struct {
	mgr *packfile.Manager
}

// NewPackfileManagerAdapter creates an adapter that allows packfile.Manager to be used as a graveler.PackfileManager.
func NewPackfileManagerAdapter(mgr *packfile.Manager) *packfileManagerAdapter {
	return &packfileManagerAdapter{mgr: mgr}
}

// MergeStaged implements the graveler.PackfileManager interface.
func (a *packfileManagerAdapter) MergeStaged(ctx context.Context, repoID string) error {
	_, err := a.mgr.MergeStaged(ctx, repoID)
	return err
}

// Commit implements the graveler.PackfileManager interface.
func (a *packfileManagerAdapter) Commit(ctx context.Context, repoID string) error {
	return a.mgr.Commit(ctx, repoID)
}