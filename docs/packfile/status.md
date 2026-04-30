# Status

**Draft** — Reviewed and updated following editorial, adversarial, and prose reviews.

**Deferred scope:** Packfile download (server → client), S3 Gateway integration, metrics/monitoring, pre-signed URL evaluation, repository deletion (async orphan cleanup)

**Resolved open questions:**
- Footer checksum: excludes the 32 checksum bytes (computed over header + object data only)
- Per-object size: constrained by `bigFileThreshold` configuration, not the uint32 field limit
- PackfileSnapshot version field: to be added for optimistic concurrency control
- KV partition split: no atomic cross-partition update; accepted limitation with future improvement path
- L1 cache eviction: reference counting required before closing/deleting open handles
- Disk exhaustion cleanup: sort temp file deleted on Phase 2 failure before returning error
