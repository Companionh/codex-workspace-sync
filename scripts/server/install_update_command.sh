#!/usr/bin/env bash
set -euo pipefail

ROOT_ARG=""
TARGET_BIN="/usr/local/bin/update-codex-workspace"
LEGACY_TARGET_BIN="/usr/local/bin/update-codex-workspace-sync"
DEFAULT_BRANCH="main"
PYTHON_ARG=""
AUTH_FILE_ARG="/etc/codex-workspace-sync/github.env"
STATE_ROOT_ARG="/opt/codex-workspace-sync/state"
RESTART_UNIT_ARG="codex-workspace-sync.service"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      ROOT_ARG="$2"
      shift 2
      ;;
    --target)
      TARGET_BIN="$2"
      shift 2
      ;;
    --legacy-target)
      LEGACY_TARGET_BIN="$2"
      shift 2
      ;;
    --branch)
      DEFAULT_BRANCH="$2"
      shift 2
      ;;
    --python)
      PYTHON_ARG="$2"
      shift 2
      ;;
    --auth-file)
      AUTH_FILE_ARG="$2"
      shift 2
      ;;
    --state-root)
      STATE_ROOT_ARG="$2"
      shift 2
      ;;
    --restart-unit)
      RESTART_UNIT_ARG="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--root /opt/codex-workspace-sync/app] [--target /usr/local/bin/update-codex-workspace] [--legacy-target /usr/local/bin/update-codex-workspace-sync] [--branch main] [--python /path/to/python] [--auth-file /etc/codex-workspace-sync/github.env] [--state-root /opt/codex-workspace-sync/state] [--restart-unit codex-workspace-sync.service]" >&2
      exit 1
      ;;
  esac
done

ROOT_ARG="${ROOT_ARG:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
if [[ ! -d "$ROOT_ARG/.git" ]]; then
  echo "Root is not a git checkout: $ROOT_ARG/.git missing" >&2
  exit 1
fi

if [[ -z "$PYTHON_ARG" ]]; then
  if [[ -x "$ROOT_ARG/.venv/bin/python" ]]; then
    PYTHON_ARG="$ROOT_ARG/.venv/bin/python"
  else
    PYTHON_ARG="python3"
  fi
fi

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT

cat > "$tmpfile" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$ROOT_ARG"
export PYTHON_BIN="$PYTHON_ARG"
export GITHUB_AUTH_FILE="$AUTH_FILE_ARG"
export CWS_STATE_ROOT="$STATE_ROOT_ARG"
exec /usr/bin/env bash "$ROOT_ARG/scripts/server/update_from_github.sh" --branch "$DEFAULT_BRANCH" --python "$PYTHON_ARG" --auth-file "$AUTH_FILE_ARG" --state-root "$STATE_ROOT_ARG" --restart --restart-unit "$RESTART_UNIT_ARG" "\$@"
EOF

sudo install -m 0755 "$tmpfile" "$TARGET_BIN"
if [[ -n "$LEGACY_TARGET_BIN" && "$LEGACY_TARGET_BIN" != "$TARGET_BIN" ]]; then
  sudo ln -sfn "$TARGET_BIN" "$LEGACY_TARGET_BIN"
fi
echo "Installed update command: $TARGET_BIN"
if [[ -n "$LEGACY_TARGET_BIN" && "$LEGACY_TARGET_BIN" != "$TARGET_BIN" ]]; then
  echo "Installed compatibility alias: $LEGACY_TARGET_BIN -> $TARGET_BIN"
fi
echo "It runs: $ROOT_ARG/scripts/server/update_from_github.sh --branch $DEFAULT_BRANCH --python $PYTHON_ARG --auth-file $AUTH_FILE_ARG --state-root $STATE_ROOT_ARG --restart --restart-unit $RESTART_UNIT_ARG"
