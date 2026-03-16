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

## Publishing code from Windows

`codex-workspace-sync` uses the same safe publish pattern as `telegram-scraper-bot`.

1. Copy `scripts/windows/push-config.example.cmd` to `scripts/windows/push-config.local.cmd`.
2. Fill in the repo URL, branch, and optional temp checkout path.
3. Run `scripts/windows/push-repo.bat`.

The script exports only the tracked project tree needed for GitHub publishing into a temp checkout under `backups/push_tmp_repo`, then commits and pushes from there. Local runtime state, secrets, caches, and server backups are excluded from the export. Git authentication should already be configured on the Windows machine, ideally through SSH. The helper copies `user.name` and `user.email` from the main repo checkout when available, and you can override them in `push-config.local.cmd` with `GIT_USER_NAME` and `GIT_USER_EMAIL`.

## Updating code on Windows

Use `scripts/windows/pull-repo.bat` to pull the latest project code onto a Windows machine.

1. Make sure Git authentication is already configured on the machine, ideally through SSH.
2. Reuse `scripts/windows/push-config.local.cmd` if you already have it.
3. Run `scripts/windows/pull-repo.bat`.

By default the helper fetches `origin`, fast-forwards the current branch, refreshes the editable Python install, and runs `compileall` over `src`, `tests`, and `tools`. If the repo has uncommitted changes, it autostashes them first and restores them after the update.

## Updating code on Hetzner

1. Copy `scripts/server/github.env.template` to `scripts/server/github.env` or `/etc/codex-workspace-sync/github.env`.
2. Fill in the GitHub username, fine-grained token, and repo URL.
3. Run `scripts/server/update_from_github.sh`.

The updater creates a pre-update backup of the live sync state, fast-forwards the checkout, refreshes the editable Python install, and runs a compile check. Use `--restart` with `--restart-unit <unit>` if you also want it to restart a service after the update.
