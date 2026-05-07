package packfile

import (
	"context"

	"github.com/treeverse/lakefs/pkg/packfile"
)

// contextKey is a custom string type for context keys to avoid collisions
// with other packages using context.WithValue.
type contextKey string

const (
	// Context keys for packfile operations.
	// Use the helper functions (WithRepoID, GetRepoID, etc.) to set and retrieve values.
	contextKeyRepoID     contextKey = "repo_id"
	contextKeyUploadID    contextKey = "upload_id"
	contextKeyPackfileID  contextKey = "packfile_id"
)

// WithRepoID returns a new context with the repository ID stored.
func WithRepoID(ctx context.Context, repoID string) context.Context {
	return context.WithValue(ctx, contextKeyRepoID, repoID)
}

// GetRepoID retrieves the repository ID from the context.
// Returns the repo ID and true if found, or empty string and false if not set.
func GetRepoID(ctx context.Context) (string, bool) {
	val := ctx.Value(contextKeyRepoID)
	if val == nil {
		return "", false
	}
	repoID, ok := val.(string)
	return repoID, ok
}

// WithUploadID returns a new context with the upload ID stored.
func WithUploadID(ctx context.Context, uploadID packfile.UploadID) context.Context {
	return context.WithValue(ctx, contextKeyUploadID, uploadID)
}

// GetUploadID retrieves the upload ID from the context.
// Returns the upload ID and true if found, or empty UploadID and false if not set.
func GetUploadID(ctx context.Context) (packfile.UploadID, bool) {
	val := ctx.Value(contextKeyUploadID)
	if val == nil {
		return "", false
	}
	uploadID, ok := val.(packfile.UploadID)
	return uploadID, ok
}

// WithPackfileID returns a new context with the packfile ID stored.
func WithPackfileID(ctx context.Context, packfileID packfile.PackfileID) context.Context {
	return context.WithValue(ctx, contextKeyPackfileID, packfileID)
}

// GetPackfileID retrieves the packfile ID from the context.
// Returns the packfile ID and true if found, or empty PackfileID and false if not set.
func GetPackfileID(ctx context.Context) (packfile.PackfileID, bool) {
	val := ctx.Value(contextKeyPackfileID)
	if val == nil {
		return "", false
	}
	packfileID, ok := val.(packfile.PackfileID)
	return packfileID, ok
}