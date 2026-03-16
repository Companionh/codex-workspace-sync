#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BRANCH="main"
RESTART_AFTER="false"
AUTOSTASH="true"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
AUTH_FILE="${GITHUB_AUTH_FILE:-/etc/codex-workspace-sync/github.env}"
STATE_ROOT="${CWS_STATE_ROOT:-/opt/codex-workspace-sync/state}"
RESTART_UNIT="${CWS_RESTART_UNIT:-codex-workspace-sync.service}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)
      BRANCH="$2"
      shift 2
      ;;
    --restart)
      RESTART_AFTER="true"
      shift
      ;;
    --restart-unit)
      RESTART_UNIT="$2"
      shift 2
      ;;
    --no-autostash)
      AUTOSTASH="false"
      shift
      ;;
    --python)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --auth-file)
      AUTH_FILE="$2"
      shift 2
      ;;
    --state-root)
      STATE_ROOT="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--branch main] [--restart] [--restart-unit codex-workspace-sync.service] [--no-autostash] [--python /path/to/python] [--auth-file /etc/codex-workspace-sync/github.env] [--state-root /opt/codex-workspace-sync/state]" >&2
      exit 1
      ;;
  esac
done

if [[ ! -d "$ROOT/.git" ]]; then
  echo "This script must run inside a git checkout. Missing: $ROOT/.git" >&2
  exit 1
fi

if [[ -f "$AUTH_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$AUTH_FILE"
  set +a
fi

git_fetch_source="${GITHUB_REPO_URL:-origin}"

run_git() {
  if [[ -n "${GITHUB_USERNAME:-}" && -n "${GITHUB_TOKEN:-}" ]]; then
    local askpass
    local status
    askpass="$(mktemp)"
    chmod 700 "$askpass"
    cat > "$askpass" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  *Username*|*username*) printf '%s\n' "$GITHUB_USERNAME" ;;
  *Password*|*password*) printf '%s\n' "$GITHUB_TOKEN" ;;
  *) printf '%s\n' "$GITHUB_TOKEN" ;;
esac
EOF
    set +e
    GIT_TERMINAL_PROMPT=0 GIT_ASKPASS="$askpass" git -C "$ROOT" "$@"
    status=$?
    set -e
    rm -f "$askpass"
    return "$status"
  fi
  git -C "$ROOT" "$@"
}

has_local_changes() {
  if ! git -C "$ROOT" diff --quiet; then
    return 0
  fi
  if ! git -C "$ROOT" diff --cached --quiet; then
    return 0
  fi
  if [[ -n "$(git -C "$ROOT" ls-files --others --exclude-standard)" ]]; then
    return 0
  fi
  return 1
}

BACKUP_DIR="$ROOT/server_backups/pre_update_$(date -u +%Y%m%d_%H%M%S)"
echo "Creating pre-update live-state backup"
/usr/bin/env bash "$ROOT/scripts/server/backup_live_state.sh" "$ROOT" "$STATE_ROOT" "$BACKUP_DIR"

STASHED_LOCAL_CHANGES="false"
if has_local_changes; then
  git -C "$ROOT" status --short > "$BACKUP_DIR/git_status_before_update.txt"
  git -C "$ROOT" diff > "$BACKUP_DIR/local_changes.patch" || true
  git -C "$ROOT" diff --cached > "$BACKUP_DIR/local_changes_staged.patch" || true
  if [[ "$AUTOSTASH" == "true" ]]; then
    echo "Local checkout changes detected. Autostashing before merge."
    git -C "$ROOT" stash push --include-untracked -m "codex-workspace-sync pre-update autostash $(date -u +%Y%m%d_%H%M%S)"
    STASHED_LOCAL_CHANGES="true"
  else
    echo "Local checkout changes detected and --no-autostash was set." >&2
    echo "Commit, stash, or discard the local changes first." >&2
    exit 1
  fi
fi

if [[ "$git_fetch_source" == "origin" ]]; then
  echo "Fetching origin/$BRANCH"
else
  echo "Fetching $BRANCH from configured private GitHub URL"
fi
run_git fetch "$git_fetch_source" "$BRANCH"
run_git merge --ff-only FETCH_HEAD

echo "Refreshing Python package install"
"$PYTHON_BIN" -m pip install -e "$ROOT"

echo "Running syntax checks"
"$PYTHON_BIN" -m compileall "$ROOT/src" "$ROOT/tests"

if [[ "$RESTART_AFTER" == "true" ]]; then
  echo "Restarting service unit: $RESTART_UNIT"
  systemctl restart "$RESTART_UNIT"
fi

if [[ "$STASHED_LOCAL_CHANGES" == "true" ]]; then
  echo "Restoring stashed local changes"
  set +e
  git -C "$ROOT" stash pop --index
  STASH_POP_STATUS=$?
  set -e
  if [[ "$STASH_POP_STATUS" -ne 0 ]]; then
    echo "Update completed, but reapplying the local changes caused conflicts." >&2
    echo "The stash was kept. Resolve it manually with: git stash list" >&2
    exit 1
  fi
fi

echo "Update complete on branch: $BRANCH"
