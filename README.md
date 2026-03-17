# codex-workspace-sync

`codex-workspace-sync` is a Python monorepo for keeping Codex workspace context portable across multiple Windows devices with a self-hosted sync hub.

> Status: experimental alpha. This project is usable for a single operator who is comfortable with SSH, self-hosting, and recovering from upstream Codex format changes. It is not production-grade sync middleware yet.

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

## Current status

- The current target setup is one person, multiple Windows machines, and one self-hosted Linux server.
- The sync model is designed for one active live-sync device at a time.
- The repo is intended to be transparent and hackable rather than fully automated or consumer-friendly.
- The codebase currently has automated tests, but the operational workflow still assumes a technically comfortable operator.

## Known limitations

- The project depends on Codex local state under `~/.codex`, including session files and index metadata that are not stable public APIs.
- Exact thread portability depends on Codex and VS Code continuing to store compatible local artifacts across devices and versions.
- Server-side synced payloads are stored in plaintext by design in v1.
- The system is optimized for a single user, not concurrent multi-user collaboration.
- Enrollment, device recovery, and server updates still involve manual operator steps.
- Windows client workflows and a Linux self-hosted server are the primary supported environment today.
- If upstream Codex changes its local file layout or thread metadata shape, parts of this project may need repairs.

## Repo layout

- `src/cws/` application code
- `src/cws/templates/` scaffolding templates used when creating superprojects
- `docs/` system and protocol docs
- `skills/shared/` lightweight skills shared by all superprojects
- `scripts/windows/` convenience launchers for the sync shell
- `scripts/server/` convenience launchers for the server-side service
- `tools/` publishing helpers used to export a sanitized GitHub tree

## Quick start

1. Install Python 3.12.
2. Install the package in editable mode: `py -3.12 -m pip install -e .[dev]`
3. Initialize the server: `cws-server init`
4. Start the API: `cws-server serve`
5. Enroll a Windows device: `cws enroll-device`
6. Launch the sync shell: `scripts\\windows\\cws-shell.bat`

If you want a Windows launcher that stays open after enrollment succeeds or fails, use:

- `scripts\\windows\\cws-enroll-device.bat`

That launcher runs against the repo's local `src` tree, forces a persistent `cmd` window, and writes logs under `%LOCALAPPDATA%\\CodexWorkspaceSync\\logs\\`.

Enrollment registers the device with the server, but it does not automatically bind existing server superprojects into the local client config. If the server already has the superproject you want, attach it locally before running `update-from-server`.
During enrollment:

- `Secondary passphrase` means the passphrase you set with `cws-server init` on the server.
- `SSH password` means the Linux account password and can be left blank for key-based SSH.
- `SSH key passphrase` means the passphrase that unlocks your local private key.

## Security notes

- The server is authoritative, so anyone with server access effectively has access to synced Codex state.
- Synced raw bundles can contain conversation history, local thread metadata, and synced skills.
- Payload encryption is intentionally out of scope for the current alpha, but the storage model was kept modular so encryption can be added later.
- Do not expose the sync API publicly without normal server hardening, firewall rules, and SSH hygiene.

## Windows repo push helper

Use `scripts\\windows\\push-repo.bat` to publish this repo from Windows.

- It uses the same sanitized temp-checkout publishing pattern as the other sibling repos instead of pushing from the live checkout.
- It exports a curated project tree into `backups\\push_tmp_repo`, commits there, and pushes from that temp checkout.
- If `scripts\\windows\\push-config.local.cmd` exists, it loads your repo URL, branch, and temp-checkout path from there.
- It assumes Git authentication is already configured on the machine, preferably with SSH.
- The temp checkout keeps `origin` on the normal repo URL and uses plain `git clone`, `git fetch`, and `git push`.
- It copies `user.name` and `user.email` from your main checkout into the temp publish checkout when available.
- Before publishing, it warns if the working branch is behind the latest published branch and uses a stronger warning when the working branch has diverged from it.
- After a successful publish, it can realign the main working branch to the published mirror and create a `backup/post_publish_*` safety branch first.
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

## Server repo update helper

Use `scripts/server/update_from_github.sh` on the server-side app checkout to fast-forward from GitHub.

- GitHub credentials can live in `scripts/server/github.env` or `/etc/codex-workspace-sync/github.env`.
- The script creates a pre-update backup of live sync state before pulling.
- It can autostash temporary code changes, reinstall the editable package, and optionally restart a service unit after updating.

If you install the server helper, you also get a single-command launcher:

- `update-codex-workspace`

That wrapper loads the server auth file, autostashes checkout changes, fast-forwards from GitHub, refreshes the package install, runs compile checks, reloads `systemd`, and restarts `codex-workspace-sync.service`. The older command name `update-codex-workspace-sync` remains as a compatibility alias.

## Server service install helpers

Use these scripts to fit the project into a standard `/opt` + `/etc` + `systemd` Linux deployment pattern:

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
- `cws attach-superproject`
- `cws disconnect-superproject <slug>`
- `cws delete-superproject-server <slug> [--force]`
- `cws update-from-server <slug>`
- `cws override-current-state <slug>`
- `cws turn-on-sync <slug>`
- `cws turn-off-sync`
- `cws status`
- `cws refresh-thread <slug> --thread <thread-id>`

The older `--superproject <slug>` form still works, but the positional form is now the default.

## Notes

- This repo does not store live superproject data in Git.
- Shared skills live in the repo because they are lightweight and reusable.
- The current v1 stores synced payloads in plaintext on the server by design.
- The storage layer is intentionally modular so payload encryption can be added later.
