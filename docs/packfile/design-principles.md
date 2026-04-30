# Design Principles

1. **Streaming IO only** — All operations use `io.Reader`/`io.Writer`; no in-memory `[]byte` for bulk data
2. **Local filesystem primary** — Packfiles live on local disk/NFS for merge efficiency; S3 is async backup
3. **Immutable once created** — Packfiles are never modified after finalization
4. **Server-side validation** — No pre-signed URL direct-to-S3 upload; all data streams through lakeFS server
5. **Block adapter agnostic** — Implementation depends on the existing block adapter, never modifies it

---
