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

1. enroll the device if needed with `enroll-device`
2. attach an existing server superproject with `attach-superproject`, or create a new one with `create-superproject`
3. run `update-from-server` or `override-current-state`
4. run `turn-on-sync`
5. let the shell heartbeat and sync in the background
6. inspect `status` when changing devices
7. run `turn-off-sync` manually if desired, or allow the server lease to expire automatically

For first-time device enrollment from Windows, use:

- `scripts/windows/cws-enroll-device.bat`

That launcher forces a persistent `cmd` window so the operator can read any enrollment error output. It also writes logs under `%LOCALAPPDATA%\CodexWorkspaceSync\logs\`.

Prompt meanings during enrollment:

- `Secondary passphrase`: the bootstrap passphrase set on the server with `cws-server init`
- `SSH password`: the Linux account password, usually blank when SSH keys are used
- `SSH key passphrase`: the passphrase that unlocks the local private key on Windows

Enrollment only registers the device and stores its credentials. It does not automatically create local bindings for server-side superprojects. If `telegram-bots-suite` already exists on the server, run `attach-superproject` once on each new machine before using `update-from-server`.
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

By default the helper fetches `origin`, fast-forwards the current branch, refreshes the editable Python install, and runs `compileall` over `src`, `tests`, and `tools`. If the repo has uncommitted changes, it autostashes them first and restores them after the update. If the local branch and remote branch diverge, it prompts either to rebase local commits or to create a backup branch and sync the current branch to the fetched remote branch. You can control that behavior with `REBASE_ON_DIVERGENCE` and `RESET_ON_DIVERGENCE` in `push-config.local.cmd`.

## Updating code on Hetzner

1. Copy `scripts/server/github.env.template` to `scripts/server/github.env` or `/etc/codex-workspace-sync/github.env`.
2. Fill in the GitHub username, fine-grained token, and repo URL.
3. Run `scripts/server/update_from_github.sh`.

The updater creates a pre-update backup of the live sync state, autostashes checkout changes, fast-forwards the checkout, refreshes the editable Python install, runs a compile check, reloads `systemd`, and can restart a service after the update.

For the normal server workflow, install the one-command wrapper and just run:

```bash
update-codex-workspace
```

That installed launcher also keeps `update-codex-workspace-sync` available as a compatibility alias.

## Installing the Hetzner service

The project now includes sibling-style server install helpers:

- `scripts/server/install_systemd.sh`
- `scripts/server/install_update_command.sh`
- `scripts/server/service.env.template`
- `scripts/server/systemd/codex-workspace-sync.service`
- `scripts/server/systemd/codex-workspace-sync-update.service`
- `scripts/server/systemd/codex-workspace-sync-update.timer`

Recommended server layout:

- app checkout: `/opt/codex-workspace-sync/app`
- live state: `/opt/codex-workspace-sync/state`
- auth file: `/etc/codex-workspace-sync/github.env`
- service env file: `/etc/codex-workspace-sync/service.env`

Typical install commands:

```bash
cd /opt/codex-workspace-sync/app
sudo mkdir -p /etc/codex-workspace-sync
sudo cp ./scripts/server/service.env.template /etc/codex-workspace-sync/service.env
sudo bash ./scripts/server/install_systemd.sh \
  --root /opt/codex-workspace-sync/app \
  --python /opt/codex-workspace-sync/app/.venv/bin/python \
  --user root \
  --state-root /opt/codex-workspace-sync/state \
  --port 8787
sudo bash ./scripts/server/install_update_command.sh \
  --root /opt/codex-workspace-sync/app \
  --python /opt/codex-workspace-sync/app/.venv/bin/python \
  --auth-file /etc/codex-workspace-sync/github.env \
  --state-root /opt/codex-workspace-sync/state
```

Then enable the API service:

```bash
sudo systemctl enable --now codex-workspace-sync.service
sudo systemctl status codex-workspace-sync.service
```
