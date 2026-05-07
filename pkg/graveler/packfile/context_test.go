package packfile

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/packfile"
)

func TestWithRepoIDGetRepoID(t *testing.T) {
	tests := []struct {
		name   string
		repoID string
	}{
		{name: "valid repo ID", repoID: "my-repo"},
		{name: "empty repo ID", repoID: ""},
		{name: "complex repo ID", repoID: "aws-account-123:us-east-1:my-repo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctxWithID := WithRepoID(ctx, tt.repoID)

			got, ok := GetRepoID(ctxWithID)
			if !ok {
				t.Errorf("GetRepoID() ok = false, want true")
				return
			}
			if got != tt.repoID {
				t.Errorf("GetRepoID() = %q, want %q", got, tt.repoID)
			}
		})
	}
}

func TestGetRepoIDMissing(t *testing.T) {
	ctx := context.Background()
	_, ok := GetRepoID(ctx)
	if ok {
		t.Errorf("GetRepoID() ok = true, want false for missing key")
	}
}

func TestGetRepoIDWrongContext(t *testing.T) {
	// Set a string with the same key in plain context.Value
	ctx := context.WithValue(context.Background(), contextKeyRepoID, "some-repo")

	got, ok := GetRepoID(ctx)
	if !ok {
		t.Errorf("GetRepoID() ok = false, want true")
		return
	}
	if got != "some-repo" {
		t.Errorf("GetRepoID() = %q, want %q", got, "some-repo")
	}
}

func TestWithUploadIDGetUploadID(t *testing.T) {
	tests := []struct {
		name     string
		uploadID packfile.UploadID
	}{
		{name: "valid ULID", uploadID: packfile.UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV")},
		{name: "another ULID", uploadID: packfile.UploadID("AAAAAAAAAAAAAAAAAAAAAAAAAA")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctxWithID := WithUploadID(ctx, tt.uploadID)

			got, ok := GetUploadID(ctxWithID)
			if !ok {
				t.Errorf("GetUploadID() ok = false, want true")
				return
			}
			if got != tt.uploadID {
				t.Errorf("GetUploadID() = %q, want %q", got, tt.uploadID)
			}
		})
	}
}

func TestGetUploadIDMissing(t *testing.T) {
	ctx := context.Background()
	_, ok := GetUploadID(ctx)
	if ok {
		t.Errorf("GetUploadID() ok = true, want false for missing key")
	}
}

func TestWithPackfileIDGetPackfileID(t *testing.T) {
	tests := []struct {
		name        string
		packfileID  packfile.PackfileID
	}{
		{name: "valid packfile ID", packfileID: packfile.PackfileID("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")},
		{name: "all zeros", packfileID: packfile.PackfileID("0000000000000000000000000000000000000000000000000000000000000000")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctxWithID := WithPackfileID(ctx, tt.packfileID)

			got, ok := GetPackfileID(ctxWithID)
			if !ok {
				t.Errorf("GetPackfileID() ok = false, want true")
				return
			}
			if got != tt.packfileID {
				t.Errorf("GetPackfileID() = %q, want %q", got, tt.packfileID)
			}
		})
	}
}

func TestGetPackfileIDMissing(t *testing.T) {
	ctx := context.Background()
	_, ok := GetPackfileID(ctx)
	if ok {
		t.Errorf("GetPackfileID() ok = true, want false for missing key")
	}
}

func TestContextPropagation(t *testing.T) {
	// Simulate a chain of function calls
	ctx := context.Background()
	ctx = WithRepoID(ctx, "test-repo")
	ctx = WithUploadID(ctx, packfile.UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV"))
	ctx = WithPackfileID(ctx, packfile.PackfileID("abcdef123456789abcdef0123456789abcdef0123456789abcdef0123456789ab"))

	// At the "far end of the call chain" - verify all values are retrievable
	repoID, ok := GetRepoID(ctx)
	if !ok || repoID != "test-repo" {
		t.Errorf("GetRepoID() = %q, ok = %v, want %q, true", repoID, ok, "test-repo")
	}

	uploadID, ok := GetUploadID(ctx)
	if !ok || uploadID != packfile.UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV") {
		t.Errorf("GetUploadID() = %q, ok = %v, want %q, true", uploadID, ok, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	}

	packfileID, ok := GetPackfileID(ctx)
	if !ok || packfileID != packfile.PackfileID("abcdef123456789abcdef0123456789abcdef0123456789abcdef0123456789ab") {
		t.Errorf("GetPackfileID() = %q, ok = %v, want %q, true", packfileID, ok, "abcdef123456789abcdef0123456789abcdef0123456789abcdef0123456789ab")
	}
}

func BenchmarkGetRepoID(b *testing.B) {
	ctx := WithRepoID(context.Background(), "benchmark-repo")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetRepoID(ctx)
	}
}

func BenchmarkGetUploadID(b *testing.B) {
	ctx := WithUploadID(context.Background(), packfile.UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetUploadID(ctx)
	}
}

func BenchmarkGetPackfileID(b *testing.B) {
	ctx := WithPackfileID(context.Background(), packfile.PackfileID("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetPackfileID(ctx)
	}
}