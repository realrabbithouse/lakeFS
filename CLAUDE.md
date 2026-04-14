# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is lakeFS?

lakeFS is an open-source tool that transforms object storage into a Git-like repository. It provides Git-like version control for data lakes (Git for Data).

## Technology Stack

- **Backend:** Go (1.25+)
- **Frontend:** TypeScript, React 18.2, Vite
- **Storage:** AWS S3, Azure Blob Storage, Google Cloud Storage
- **Metadata DB:** PostgreSQL, DynamoDB, CosmosDB, local Pebble

## Key Architecture Components

### High-Level Overview

```
┌─────────────────────────────────────────────────────────┐
│ Client Layer: Web UI, CLI (lakectl), API clients        │
├─────────────────────────────────────────────────────────┤
│ API Server: REST API + S3-compatible gateway + auth     │
├─────────────────────────────────────────────────────────┤
│ Core Logic: Catalog, Git operations (Graveler), Actions │
├─────────────────────────────────────────────────────────┤
│ Storage Layer: Metadata (KV Store) + Objects (S3/GCS)   │
└─────────────────────────────────────────────────────────┘
```

### Core Packages (`pkg/`)

- **`graveler/`**: Core data model and Git-like operations (commits, branches, tags)
- **`catalog/`**: Filesystem namespace and path resolution
- **`api/`**: REST API server implementation
- **`gateway/`**: S3-compatible API gateway
- **`block/`**: Object storage abstraction layer (S3/GCS/Azure)
- **`kv/`**: Pluggable key-value store for metadata
- **`auth/` + `authentication/`**: RBAC and auth mechanisms
- **`actions/`**: Event-driven automation framework

### Entry Points (`cmd/`)

- **`lakefs/`**: Main lakeFS server
- **`lakectl/`**: Command-line interface

## Build & Test Commands

### Building

```bash
make build              # Build everything (UI + Go binaries)
make build-binaries     # Build only Go binaries (lakefs, lakectl)
make gen-ui             # Build only the web UI
make gen-code           # Run code generators (mocks, protobuf, API)
make gen                # Run all generators + build clients + docs
```

### Testing

```bash
make test               # Run all tests (Go + HadoopFS)
make test-go            # Run Go tests only
make run-test           # Run Go tests without regenerating (faster)
make fast-test          # Run Go tests without race detector (fastest)
make test-html          # Run tests and open HTML coverage report
make esti               # Run esti (system/integration tests)
make system-tests       # Run full system test suite locally
```

### Linting & Validation

```bash
make lint               # Lint Go and UI code
make gofmt              # Format Go code
make checks-validator   # Run all validation steps (lint, proto, clients, etc.)
make validate-proto     # Validate protobuf files
make validate-ui-format # Validate UI code formatting
```

### Docker & Docs

```bash
make build-docker       # Build Docker image
make docs-serve         # Serve docs locally (mkdocs on :4000)
make gen-docs           # Generate CLI documentation
```

### Client SDKs

```bash
make clients            # Generate all client SDKs
make client-python      # Generate Python SDK
make client-java        # Generate Java SDK
make sdk-rust           # Generate Rust SDK
make package-python     # Package Python SDK + wrapper
```

## Common Tasks

- Run a single Go test: `go test -count=1 -v ./pkg/path/to/package -run TestName`
- Build and run lakeFS locally: `make build && ./lakefs run --quickstart`
