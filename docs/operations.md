# Operations

## Recommended Linux paths

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
3. run `doctor <slug>` to catch stale revisions, queue problems, or lease conflicts before you start
4. run `update-from-server --dry-run <slug>` if you want a metadata-only preview
5. run `update-from-server <slug>` or `override-current-state <slug>`
6. run `turn-on-sync <slug>`
7. let the shell heartbeat and sync in the background
8. inspect `status` or `queue-status` when changing devices
9. run `turn-off-sync` manually if desired, or allow the server lease to expire automatically

`update-from-server` now prompts separately for:

- managed Markdown docs
- shared Codex runtime
- shared skills
- tracked thread payloads

That means the operator can preview or apply only the parts that matter instead of blindly bulk-pulling the whole server state.

For first-time device enrollment from Windows, use:

- `scripts/windows/cws-enroll-device.bat`

That launcher forces a persistent `cmd` window so the operator can read any enrollment error output. It also writes logs under `%LOCALAPPDATA%\CodexWorkspaceSync\logs\`.

Prompt meanings during enrollment:

- `Secondary passphrase`: the bootstrap passphrase set on the server with `cws-server init`
- `SSH password`: the Linux account password, usually blank when SSH keys are used
- `SSH key passphrase`: the passphrase that unlocks the local private key on Windows

Enrollment only registers the device and stores its credentials. It does not automatically create local bindings for server-side superprojects. If a superproject already exists on the server, run `attach-superproject` once on each new machine before using `update-from-server`.

## Operator commands worth knowing

- `status`: includes the configured lease scope and queue health summary
- `queue-status`: shows queued checkpoint items, pending conflict counts, retry counts, and last known queue error
- `doctor [slug]`: checks server reachability, schema version, current lease owner, local `.codex` readability, queue health, and stale superproject state
- `localthreads`: shows local Codex threads with names and last-user-turn previews
- `threadlist <slug>`: shows server-side tracked threads for one superproject
- `addthread`, `rename-thread`, `untrack-thread`, `remove-thread`: manage tracked threads explicitly instead of relying on blind thread discovery
- `set-lease-scope <global|superproject>`: keep the default single active device model or opt into per-superproject lease scoping
- `force-thread-updates <slug>`: push the currently tracked local thread payloads without turning on full live sync

## Publishing code from Windows

`codex-workspace-sync` uses the same safe publish pattern as the sibling repos that publish from a sanitized temp checkout.

1. Copy `scripts/windows/push-config.example.cmd` to `scripts/windows/push-config.local.cmd`.
2. Fill in the repo URL, branch, and optional temp checkout path.
3. Run `scripts/windows/push-repo.bat`.

The script exports only the tracked project tree needed for GitHub publishing into a temp checkout under `backups/push_tmp_repo`, then commits and pushes from there. Local runtime state, secrets, caches, and server backups are excluded from the export. Git authentication should already be configured on the Windows machine, ideally through SSH. The helper copies `user.name` and `user.email` from the main repo checkout when available, and you can override them in `push-config.local.cmd` with `GIT_USER_NAME` and `GIT_USER_EMAIL`.

Before it creates a publish commit, it checks the main working branch against the publish mirror. If the working branch is only behind, it warns and asks for confirmation. If the histories are desynced or diverged, it shows a stronger warning and asks again before continuing.

To reduce future Git-history drift, the pusher can also sync the main working branch to the published mirror after a successful push. When enabled, it creates a `backup/post_publish_*` safety branch before resetting the working branch to the just-published commit.

## Updating code on Windows

Use `scripts/windows/pull-repo.bat` to pull the latest project code onto a Windows machine.

1. Make sure Git authentication is already configured on the machine, ideally through SSH.
2. Reuse `scripts/windows/push-config.local.cmd` if you already have it.
3. Run `scripts/windows/pull-repo.bat`.

By default the helper fetches `origin`, fast-forwards the current branch, refreshes the editable Python install, and runs `compileall` over `src`, `tests`, and `tools`. If the repo has uncommitted changes, it autostashes them first and restores them after the update. If the local branch and remote branch diverge, it prompts either to rebase local commits or to create a backup branch and sync the current branch to the fetched remote branch. You can control that behavior with `REBASE_ON_DIVERGENCE` and `RESET_ON_DIVERGENCE` in `push-config.local.cmd`.

## Updating code on the server

1. Copy `scripts/server/github.env.template` to `scripts/server/github.env` or `/etc/codex-workspace-sync/github.env`.
2. Fill in the GitHub username, GitHub token, and repo URL.
3. Run `scripts/server/update_from_github.sh`.

The updater creates a pre-update backup of the live sync state, autostashes checkout changes, fast-forwards the checkout, refreshes the editable Python install, runs a compile check, reloads `systemd`, and can restart a service after the update.

For the normal server workflow, install the one-command wrapper and just run:

```bash
update-codex-workspace
```

That installed launcher also keeps `update-codex-workspace-sync` available as a compatibility alias.

## Installing the server service

The project now includes server install helpers:

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
