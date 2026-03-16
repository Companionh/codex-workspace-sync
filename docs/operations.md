# Operations

## Hetzner paths

- app code: `/opt/codex-workspace-sync/app`
- live sync state: `/opt/codex-workspace-sync/state`

## State layout

Each superproject gets:

- `baseline/`
- `ecosystem/`
- `subprojects/`
- `threads/`
- `generated/`
- `manifests/`
- `raw_codex/`
- `backups/`

## Running the shell

The Windows operator experience is a simple command shell started by a batch file.

Typical flow:

1. run `update-from-server` or `override-current-state`
2. run `turn-on-sync`
3. let the shell heartbeat and sync in the background
4. inspect `status` when changing devices
5. run `turn-off-sync` manually if desired, or allow the server lease to expire automatically

