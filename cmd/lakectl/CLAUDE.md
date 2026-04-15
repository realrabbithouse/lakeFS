# lakectl - lakeFS Command-Line Interface

`lakectl` is the official CLI tool for lakeFS, providing a command-line interface to manage data lakes with Git-like semantics. It enables users to interact with lakeFS repositories, branches, commits, and data objects directly from the terminal.

## Architecture Overview

### Technology Stack
- **Framework**: Cobra (Go CLI framework)
- **API Client**: Auto-generated from OpenAPI specification (`/pkg/api/apigen`)
- **Configuration**: Viper for config management (YAML + environment variables)
- **Output**: go-pretty for formatted table output

### Configuration
- **Default config path**: `~/.lakectl.yaml`
- **Environment variable prefix**: `LAKECTL_`
- **Key config options**:
  - `server.endpoint_url` - lakeFS server URL
  - `credentials.access_key_id` / `secret_access_key` - API credentials
  - `credentials.provider.type` - Auth method (basic, aws_iam)
  - `server.retries` - Retry configuration
  - `options.parallelism` - Default parallelism for operations
  - `local.*` - Local sync options (symlinks, non-regular files)

### URI Format
lakeFS uses `lakefs://` URIs in the format:
- `lakefs://repo` - Repository
- `lakefs://repo/branch` - Branch
- `lakefs://repo/branch/path/to/file` - Object path

The `--base-uri` flag can set a default prefix for shorter URIs.

## Command Groups

### 1. Repository Management (`repo`)

| Command | Description |
|---------|-------------|
| `repo create <repo> <storage-namespace>` | Create a new repository |
| `repo create-bare <repo> <storage-namespace>` | Create bare repository (no initial commit) |
| `repo delete <repo>` | Delete a repository |
| `repo list` | List all accessible repositories |

**Storage namespace**: S3 bucket, GCS bucket, or Azure blob container path where data will be stored.

### 2. Branch Management (`branch`)

| Command | Description |
|---------|-------------|
| `branch create <repo>/<source-ref> <new-branch>` | Create branch from ref |
| `branch delete <repo>/<branch>` | Delete a branch |
| `branch list <repo>` | List all branches |
| `branch show <repo>/<branch>` | Show branch details |
| `branch protect <repo>/<branch>` | Protect/unprotect branch |
| `branch reset <repo>/<branch> <ref>` | Reset branch to ref |
| `branch revert <repo>/<branch> <commit>` | Revert commits |

### 3. Commit Operations (`commit`)

| Command | Description |
|---------|-------------|
| `commit <repo>/<branch>` | Commit staged changes |

**Flags**:
- `-m, --message` - Commit message (required)
- `--allow-empty-message` - Allow commits with no changes
- `--meta key=value` - Add metadata
- `--epoch-time-seconds` - Custom commit timestamp

### 4. Filesystem Operations (`fs`)

| Command | Description |
|---------|-------------|
| `fs cat <lakefs-path>` | Display object contents |
| `fs download <lakefs-path> <local-path>` | Download objects |
| `fs ls <lakefs-path>` | List objects/directories |
| `fs presign <lakefs-path>` | Generate pre-signed URL |
| `fs rm <lakefs-path>` | Delete objects |
| `fs stage <local-path> <lakefs-path>` | Stage files for upload |
| `fs stat <lakefs-path>` | Show object metadata |
| `fs update-metadata <lakefs-path>` | Update object metadata |
| `fs upload <local-path> <lakefs-path>` | Upload files |

**Common flags**:
- `-r, --recursive` - Recursive operations
- `--pre-sign` - Use pre-signed URLs (recommended)
- `-p, --parallelism` - Concurrent operations

### 5. Local Sync (`local`)

The `local` commands provide bidirectional sync between local directories and lakeFS paths, similar to Git.

| Command | Description |
|---------|-------------|
| `local init <path>` | Initialize local lakeFS sync |
| `local clone <repo> <path>` | Clone repository locally |
| `local commit <path>` | Commit local changes |
| `local pull <lakefs-ref> <path>` | Pull from lakeFS |
| `local checkout <lakefs-ref> <path>` | Checkout branch to local |
| `local status <path>` | Show local changes |
| `local list <path>` | List synced files |

**Configuration** (`~/.lakectl.yaml`):
```yaml
local:
  skip_non_regular_files: false  # Ignore symlinks
  symlink_support: false         # Store symlink state

experimental:
  local:
    posix_permissions:
      enabled: false
      include_uid: false
      include_gid: false
```

### 6. Authentication & Authorization (`auth`)

#### Users (`auth users`)
| Command | Description |
|---------|-------------|
| `auth users create <user-id>` | Create user |
| `auth users delete <user-id>` | Delete user |
| `auth users list` | List users |
| `auth users credentials create <user-id>` | Create access credentials |
| `auth users credentials list <user-id>` | List credentials |
| `auth users credentials delete <user-id> <credential-id>` | Delete credential |
| `auth users policies attach <user-id> <policy>` | Attach policy |
| `auth users policies detach <user-id> <policy>` | Detach policy |
| `auth users policies list <user-id>` | List policies |
| `auth users groups list <user-id>` | List user groups |
| `auth users aws-iam attach <user-id> <role-arn>` | Attach IAM role |
| `auth users aws-iam detach <user-id> <role-arn>` | Detach IAM role |
| `auth users aws-iam list` | List IAM identities |
| `auth users aws-iam lookup` | Lookup current IAM identity |

#### Groups (`auth groups`)
| Command | Description |
|---------|-------------|
| `auth groups create <group>` | Create group |
| `auth groups delete <group>` | Delete group |
| `auth groups list` | List groups |
| `auth groups add-member <group> <user>` | Add user to group |
| `auth groups remove-member <group> <user>` | Remove user from group |
| `auth groups members list <group>` | List group members |
| `auth groups policies attach <group> <policy>` | Attach policy |
| `auth groups policies detach <group> <policy>` | Detach policy |
| `auth groups policies list <group>` | List group policies |
| `auth groups acl get <group>` | Get ACL |
| `auth groups acl set <group> <permission>` | Set ACL |

#### Policies (`auth policies`)
| Command | Description |
|---------|-------------|
| `auth policies create <policy>` | Create policy (JSON/YAML) |
| `auth policies delete <policy>` | Delete policy |
| `auth policies list` | List policies |
| `auth policies show <policy>` | Show policy details |

### 7. Tags (`tag`)

| Command | Description |
|---------|-------------|
| `tag create <repo>/<tag> <commit>` | Create tag |
| `tag delete <repo>/<tag>` | Delete tag |
| `tag list <repo>` | List tags |
| `tag show <repo>/<tag>` | Show tag details |

### 8. Actions (`actions`)

| Command | Description |
|---------|-------------|
| `actions runs list <repo>` | List action runs |
| `actions runs describe <repo> <run-id>` | Show run details |
| `actions validate <file>` | Validate action YAML |

### 9. Garbage Collection (`gc`)

| Command | Description |
|---------|-------------|
| `gc check <repo>` | Check GC eligible objects |
| `gc get-config <repo>` | Show GC configuration |
| `gc set-config <repo>` | Set GC configuration |
| `gc delete-config <repo>` | Delete GC configuration |
| `gc prepare <repo>` | Prepare GC run |

### 10. Maintenance (`refs`)

| Command | Description |
|---------|-------------|
| `refs dump <repo>` | Dump refs to file |
| `refs restore <repo> <file>` | Restore refs from file |

Useful for disaster recovery or migrating repositories.

### 11. Utility Commands

| Command | Description |
|---------|-------------|
| `login` | Web-based OAuth login |
| `config` | Interactive configuration |
| `doctor` | Diagnose configuration & connectivity |
| `diff <ref1> <ref2>` | Compare refs/paths |
| `log <repo>/<ref>` | Show commit history |
| `show <repo>/<ref>` | Show commit/object details |
| `find-merge-base <ref1> <ref2>` | Find common ancestor |
| `cherry-pick <commit> <branch>` | Apply commit to branch |
| `identity` | Show current user |
| `import <repo>` | Import data |
| `completion <shell>` | Generate shell completion |

### 12. Bisect (`bisect`)

Git-like bisect for finding commits that introduced issues.

| Command | Description |
|---------|-------------|
| `bisect start <bad> <good>` | Start bisect |
| `bisect good <ref>` | Mark commit as good |
| `bisect bad <ref>` | Mark commit as bad |
| `bisect run <command>` | Automated bisect |
| `bisect log` | Show bisect log |
| `bisect view` | Current bisect state |
| `bisect reset` | End bisect |

### 13. Abuse Commands (`abuse`)

Testing/benchmarking commands that simulate various workloads.

| Command | Description |
|---------|-------------|
| `abuse create-branches` | Create many branches |
| `abuse commit` | Make many commits |
| `abuse merge` | Merge branches |
| `abuse random-writes` | Random object writes |
| `abuse random-reads` | Random object reads |
| `abuse random-delete` | Random object deletes |
| `abuse link-same-object` | Link same object multiple times |

### 14. Plugins (`plugin`)

| Command | Description |
|---------|-------------|
| `plugin list` | List installed plugins |
| `plugin` | Plugin help |

Plugins are executables named `lakectl-<name>` in PATH.

## Common Workflows

### Initial Setup
```bash
# Interactive configuration
lakectl config

# Or manual config edit (~/.lakectl.yaml)
lakectl login                    # Web login
lakectl doctor                   # Verify setup
```

### Repository Operations
```bash
# Create repository
lakectl repo create my-repo s3://my-bucket/my-data

# List repos
lakectl repo list

# Create branch
lakectl branch create lakefs://my-repo/main feature-branch

# Commit changes
lakectl fs upload local-data/ lakefs://my-repo/feature-branch/
lakectl commit lakefs://my-repo/feature-branch -m "Add data"
```

### Data Operations
```bash
# List files
lakectl fs ls lakefs://my-repo/main/

# Download
lakectl fs download lakefs://my-repo/main/data/file.csv ./local/

# Upload
lakectl fs upload ./local/file.csv lakefs://my-repo/main/data/
```

### Local Sync
```bash
# Initialize local sync
lakectl local init ./my-data lakefs://my-repo/main/

# Make changes and commit
lakectl local commit ./my-data -m "Update data"

# Pull latest
lakectl local pull lakefs://my-repo/main/ ./my-data
```

### Access Control
```bash
# Create user and grant access
lakectl auth users create data-user
lakectl auth users credentials create data-user

# Create policy
lakectl auth policies create -f policy.json

# Attach policy
lakectl auth users policies attach data-user ReadWritePolicy
```

## Key Implementation Patterns

### URI Parsing
The `uri.Parse()` function parses lakeFS URIs and supports:
- Full URI: `lakefs://repo/branch/path`
- Short form with `--base-uri`: `repo/branch/path`
- Context-aware resolution based on current directory

### Error Handling
- `Die(msg, code)` - Print error and exit
- `DieErr(err)` - Print error and exit
- `DieOnErrorOrUnexpectedStatusCode(resp, err, expected)` - Handle API errors

### Output Formatting
- Tables with `go-pretty` for list outputs
- Colored text using ANSI codes
- Pagination support with `--after` and `--amount` flags

### Async Operations
Some commands support `--async` flag for non-blocking execution:
- Branch creation/deletion
- Commit operations
- Import operations

Run ID returned can be tracked with `async <run-id>`.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `LAKECTL_CONFIG_FILE` | Config file path |
| `LAKECTL_BASE_URI` | Default base URI |
| `LAKECTL_SKIP_ENTERPRISE_CHECK` | Skip Enterprise warning |
| `NO_COLOR` | Disable colors |
| `LAKECTL_INTERACTIVE` | Force interactive mode |