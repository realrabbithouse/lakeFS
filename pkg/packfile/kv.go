package packfile

import (
	"github.com/treeverse/lakefs/pkg/kv"
)

const PackfilesPartition = "packfiles"

func init() {
	kv.MustRegisterType(PackfilesPartition, "packfile:*", (&PackfileMetadata{}).ProtoReflect().Type())
	kv.MustRegisterType(PackfilesPartition, "snapshot:*", (&PackfileSnapshot{}).ProtoReflect().Type())
	kv.MustRegisterType(PackfilesPartition, "upload:*", (&UploadSession{}).ProtoReflect().Type())
}

func PackfilePath(packfileID string) string {
	return kv.FormatPath("packfile", packfileID)
}

func SnapshotPath(repositoryID string) string {
	return kv.FormatPath("snapshot", repositoryID)
}

func UploadPath(uploadID string) string {
	return kv.FormatPath("upload", uploadID)
}
