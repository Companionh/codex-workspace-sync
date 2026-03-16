# codex-workspace-sync

`codex-workspace-sync` is a Python monorepo for keeping Codex workspace context portable across multiple Windows devices with a Hetzner-hosted sync hub.

The repo contains:

- a Windows-first client CLI and shell
- a FastAPI server with SQLite metadata and filesystem-backed state
- templates for new superprojects
- global shared skills that any superproject can pull
- system docs for how sync, leasing, recovery, and handoff work

## Core ideas

- The server is authoritative for live superproject state.
- Devices initiate heartbeats; the server does not poll clients.
- A global active lease prevents multiple devices from live-syncing at once.
- Missing heartbeats for 60 seconds automatically expire the active lease.
- Raw Codex artifacts are synced for resume fidelity.
- Normalized checkpoints and manifests are synced for validation and recovery.
- Suspicious deletes and destructive Markdown drift are quarantined instead of auto-propagated.

## Repo layout

- `src/cws/` application code
- `src/cws/templates/` scaffolding templates used when creating superprojects
- `docs/` system and protocol docs
- `skills/shared/` lightweight skills shared by all superprojects
- `scripts/windows/` convenience launchers for the sync shell
- `scripts/server/` convenience launchers for the Hetzner service
- `tools/` publishing helpers used to export a sanitized GitHub tree

## Quick start

1. Install Python 3.12.
2. Install the package in editable mode: `py -3.12 -m pip install -e .[dev]`
3. Initialize the server on Hetzner: `cws-server init`
4. Start the API on Hetzner: `cws-server serve`
5. Enroll a Windows device: `cws enroll-device`
6. Launch the sync shell: `scripts\\windows\\cws-shell.bat`

If you want a Windows launcher that stays open after enrollment succeeds or fails, use:

- `scripts\\windows\\cws-enroll-device.bat`

That launcher runs against the repo's local `src` tree, forces a persistent `cmd` window, and writes logs under `%LOCALAPPDATA%\\CodexWorkspaceSync\\logs\\`.

During enrollment:

- `Secondary passphrase` means the passphrase you set with `cws-server init` on Hetzner.
- `SSH password` means the Linux account password and can be left blank for key-based SSH.
- `SSH key passphrase` means the passphrase that unlocks your local private key.

## Windows repo push helper

Use `scripts\\windows\\push-repo.bat` to publish this repo from Windows.

- It mirrors the `telegram-scraper-bot` workflow instead of pushing from the live checkout.
- It exports a curated project tree into `backups\\push_tmp_repo`, commits there, and pushes from that temp checkout.
- If `scripts\\windows\\push-config.local.cmd` exists, it loads your repo URL, branch, and temp-checkout path from there.
- It assumes Git authentication is already configured on the machine, preferably with SSH.
- The temp checkout keeps `origin` on the normal repo URL and uses plain `git clone`, `git fetch`, and `git push`.
- It copies `user.name` and `user.email` from your main checkout into the temp publish checkout when available.
- It pauses at the end when launched by double-click so you can read the output.
- The local config file is ignored by Git.

## Windows repo pull helper

Use `scripts\\windows\\pull-repo.bat` to update this repo from GitHub on Windows.

- It fetches and fast-forwards the current checkout from `origin` by default.
- It can autostash local changes before the update and restore them afterward.
- If the local branch and remote branch diverge, it can prompt either to rebase local commits or to create a backup branch and sync the current branch to the fetched remote branch.
- It can refresh the editable Python install and run a compile check after the pull.
- It pauses at the end when launched by double-click so you can read the output.
- It uses the same local config file as the push helper.

## Hetzner repo update helper

Use `scripts/server/update_from_github.sh` on the server-side app checkout to fast-forward from GitHub.

- GitHub credentials can live in `scripts/server/github.env` or `/etc/codex-workspace-sync/github.env`.
- The script creates a pre-update backup of live sync state before pulling.
- It can autostash temporary code changes, reinstall the editable package, and optionally restart a service unit after updating.

If you install the server helper, you also get a single-command launcher:

- `update-codex-workspace`

That wrapper loads the server auth file, autostashes checkout changes, fast-forwards from GitHub, refreshes the package install, runs compile checks, reloads `systemd`, and restarts `codex-workspace-sync.service`. The older command name `update-codex-workspace-sync` remains as a compatibility alias.

## Hetzner service install helpers

Use these scripts to fit the project into the same `/opt` + `/etc` + `systemd` pattern as the sibling bots:

- `scripts/server/install_systemd.sh`
- `scripts/server/install_update_command.sh`
- `scripts/server/systemd/codex-workspace-sync.service`
- `scripts/server/systemd/codex-workspace-sync-update.service`
- `scripts/server/systemd/codex-workspace-sync-update.timer`
- `scripts/server/service.env.template`

They install:

- a persistent API service: `codex-workspace-sync.service`
- a one-command updater: `update-codex-workspace`
- an optional hourly update timer

## Key commands

- `cws shell`
- `cws enroll-device`
- `cws create-superproject`
- `cws disconnect-superproject --superproject <slug>`
- `cws delete-superproject-server --superproject <slug> [--force]`
- `cws update-from-server --superproject <slug>`
- `cws override-current-state --superproject <slug>`
- `cws turn-on-sync --superproject <slug>`
- `cws turn-off-sync`
- `cws status`
- `cws refresh-thread --superproject <slug> --thread <thread-id>`

## Notes

- This repo does not store live superproject data in Git.
- Shared skills live in the repo because they are lightweight and reusable.
- The current v1 stores synced payloads in plaintext on the server by design.
- The storage layer is intentionally modular so payload encryption can be added later.
