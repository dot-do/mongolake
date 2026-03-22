---
name: mongolake-agent-skill
version: 1.0.0
metadata:
  requires:
    bins: ["mongolake"]
---

# mongolake CLI - Agent Skill File

mongolake is a CLI for a MongoDB-compatible lakehouse backed by Parquet files. It provides local development, remote sync, compaction, proxying, tunneling, and authentication.

## Safety Invariants

### CRITICAL - Destructive Operations

1. **`push` overwrites remote data.** It uploads local database state to a remote endpoint and can delete remote collections/documents that do not exist locally. ALWAYS run `mongolake push <db> --remote <url> --dry-run` before the real push.

2. **`pull` overwrites local data.** It downloads remote database state and replaces local Parquet files. ALWAYS run `mongolake pull <db> --remote <url> --dry-run` before the real pull.

3. **`compact` rewrites Parquet files in-place.** It merges small row groups and removes tombstoned documents. This operation cannot be undone. ALWAYS run `mongolake compact <db> <collection> --dry-run` first.

### Network Security

4. **NEVER pass `--host 0.0.0.0`** to `dev` or `proxy` without explicit user confirmation. Binding to all interfaces exposes the server to the network. The defaults (`localhost` for dev, `127.0.0.1` for proxy) are safe.

5. **`--target` must not point to internal/private IPs** (10.x, 172.16-31.x, 192.168.x, 127.x, ::1, link-local). This is an SSRF vector. Validate the URL before passing it to `proxy`.

### Path Safety

6. **`--path` must not contain `../`** or resolve outside the project directory. This prevents path traversal attacks against the local filesystem.

### Authentication

7. **`auth login` requires browser interaction** (device flow via oauth.do). It cannot run in headless/CI environments without a pre-existing token. Use `auth whoami` to check if already authenticated.

## Command Reference

### Safe / Read-Only Commands

| Command | Description | Notes |
|---------|-------------|-------|
| `mongolake help` | Show help | Always safe |
| `mongolake version` | Show version | Always safe |
| `mongolake auth whoami` | Show current user | Read-only; exit code 1 if not authenticated |
| `mongolake auth whoami --profile <name>` | Check named profile | Read-only |

### Local-Only Commands (no remote side effects)

| Command | Description | Notes |
|---------|-------------|-------|
| `mongolake dev` | Start local dev server | Default port 3456, binds to localhost |
| `mongolake dev --port <port> --path <dir>` | Dev server with options | Validates port 1-65535 |
| `mongolake shell` | Interactive MongoDB-like shell | Operates on local `.mongolake` dir |
| `mongolake shell --path <dir>` | Shell with custom data dir | Local only |
| `mongolake tunnel` | Expose local server via Cloudflare tunnel | Requires `cloudflared` binary |

### Mutating Commands (require caution)

| Command | Description | Risk Level |
|---------|-------------|------------|
| `mongolake push <db> --remote <url>` | Upload local to remote | HIGH - overwrites + deletes remote data |
| `mongolake pull <db> --remote <url>` | Download remote to local | HIGH - overwrites local data |
| `mongolake compact <db> <collection>` | Rewrite Parquet files | MEDIUM - irreversible file rewrite |
| `mongolake auth login` | Start OAuth device flow | LOW - requires browser |
| `mongolake auth logout` | Clear stored credentials | LOW - removes local token file |
| `mongolake proxy --target <uri>` | Start wire protocol proxy | MEDIUM - routes traffic to target |

### Dry-Run Pattern

For all mutating remote/file operations, always preview first:

```bash
# Preview compaction
mongolake compact mydb users --dry-run

# Preview push
mongolake push mydb --remote https://api.mongolake.com --dry-run

# Preview pull
mongolake pull mydb --remote https://api.mongolake.com --dry-run
```

## Output Parsing

- All commands write user-facing output to stdout.
- Error messages go to stderr with prefix `Error parsing arguments:` or `Fatal error:`.
- Exit code 0 = success, 1 = failure.
- `auth whoami` exits 1 when not authenticated (useful for scripting).
- `dev`, `shell`, `proxy`, and `tunnel` are long-running processes; they do not exit until interrupted (Ctrl+C / SIGINT).

## Authentication

- Provider: oauth.do (device flow)
- Token storage: local filesystem, managed by the CLI
- Profiles: `--profile <name>` (default: "default")
- Check auth: `mongolake auth whoami`
- Commands: `login`, `logout`, `whoami`

## Environment Variables

| Variable | Purpose | Used By |
|----------|---------|---------|
| None documented | mongolake reads auth tokens from local filesystem | `auth` subcommands |

## CLI Options Quick Reference

### `dev`
- `-p, --port <port>` - Port (default: 3456)
- `-P, --path <path>` - Data directory (default: .mongolake)
- `-h, --host <host>` - Bind host (default: localhost)
- `--no-cors` - Disable CORS
- `-v, --verbose` - Verbose logging

### `shell`
- `-P, --path <path>` - Data directory (default: .mongolake)
- `-v, --verbose` - Show stack traces

### `proxy`
- `-t, --target <uri>` - Target connection string (REQUIRED)
- `-p, --port <port>` - Local port (default: 27017)
- `-h, --host <host>` - Local bind host (default: 127.0.0.1)
- `--pool` - Enable connection pooling
- `--pool-size <n>` - Max pool size (default: 10)
- `-v, --verbose` - Wire protocol logging

### `tunnel`
- `-p, --port <port>` - Port to tunnel (default: 3456)
- `-v, --verbose` - Verbose logging

### `auth`
- `--profile <name>` - Named profile (default: "default")
- `-v, --verbose` - Verbose output
