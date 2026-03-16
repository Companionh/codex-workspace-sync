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

## Quick start

1. Install Python 3.12.
2. Install the package in editable mode: `py -3.12 -m pip install -e .[dev]`
3. Initialize the server on Hetzner: `cws-server init`
4. Start the API on Hetzner: `cws-server serve`
5. Enroll a Windows device: `cws enroll-device`
6. Launch the sync shell: `scripts\\windows\\cws-shell.bat`

## Windows repo push helper

Use `scripts\\windows\\push-repo.bat` to stage, commit, and push this repo from Windows.

- It uses the current repo remote by default.
- If `scripts\\windows\\push-config.local.cmd` exists, it loads your token and defaults from there.
- It supports GitHub username + fine-grained token authentication for private repos.
- If the working tree is dirty, it prompts for a commit message unless `COMMIT_MESSAGE` is already set.
- The local config file is ignored by Git so your token stays out of the repo history.

## Key commands

- `cws shell`
- `cws enroll-device`
- `cws create-superproject`
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
