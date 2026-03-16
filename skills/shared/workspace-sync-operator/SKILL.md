# Workspace Sync Operator

Use this skill when working inside a superproject managed by `codex-workspace-sync`.

## Rules

- Treat the server as the canonical source of superproject state.
- Prefer updating managed Markdown instead of leaving architecture decisions undocumented.
- Never silently delete protected Markdown files.
- If the sync system reports a mismatch, resolve it before continuing live sync.
- If a thread refresh is pending, reopen the thread before assuming the local session is current.

