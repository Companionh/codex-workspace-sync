#!/usr/bin/env bash
set -euo pipefail

ROOT_ARG=""
PYTHON_ARG=""
USER_ARG="root"
ENABLE_UPDATE_TIMER="false"
DEFAULT_BRANCH="main"
PORT_ARG="8787"
STATE_ROOT_ARG="/opt/codex-workspace-sync/state"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      ROOT_ARG="$2"
      shift 2
      ;;
    --python)
      PYTHON_ARG="$2"
      shift 2
      ;;
    --user)
      USER_ARG="$2"
      shift 2
      ;;
    --branch)
      DEFAULT_BRANCH="$2"
      shift 2
      ;;
    --port)
      PORT_ARG="$2"
      shift 2
      ;;
    --state-root)
      STATE_ROOT_ARG="$2"
      shift 2
      ;;
    --enable-update-timer)
      ENABLE_UPDATE_TIMER="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--root /opt/codex-workspace-sync/app] [--python /path/to/python] [--user root] [--branch main] [--port 8787] [--state-root /opt/codex-workspace-sync/state] [--enable-update-timer]" >&2
      exit 1
      ;;
  esac
done

ROOT_ARG="${ROOT_ARG:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
if [[ -z "$PYTHON_ARG" ]]; then
  if [[ -x "$ROOT_ARG/.venv/bin/python" ]]; then
    PYTHON_ARG="$ROOT_ARG/.venv/bin/python"
  else
    PYTHON_ARG="python3"
  fi
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

render_unit() {
  local src="$1"
  local dst="$2"
  sed \
    -e "s|__ROOT__|$ROOT_ARG|g" \
    -e "s|__PYTHON__|$PYTHON_ARG|g" \
    -e "s|__USER__|$USER_ARG|g" \
    -e "s|__BRANCH__|$DEFAULT_BRANCH|g" \
    -e "s|__PORT__|$PORT_ARG|g" \
    -e "s|__STATE_ROOT__|$STATE_ROOT_ARG|g" \
    "$src" > "$dst"
}

render_unit "$ROOT_ARG/scripts/server/systemd/codex-workspace-sync.service" "$tmpdir/codex-workspace-sync.service"
render_unit "$ROOT_ARG/scripts/server/systemd/codex-workspace-sync-update.service" "$tmpdir/codex-workspace-sync-update.service"
cp "$ROOT_ARG/scripts/server/systemd/codex-workspace-sync-update.timer" "$tmpdir/codex-workspace-sync-update.timer"

sudo install -m 0644 "$tmpdir/codex-workspace-sync.service" /etc/systemd/system/codex-workspace-sync.service
sudo install -m 0644 "$tmpdir/codex-workspace-sync-update.service" /etc/systemd/system/codex-workspace-sync-update.service
sudo install -m 0644 "$tmpdir/codex-workspace-sync-update.timer" /etc/systemd/system/codex-workspace-sync-update.timer

sudo systemctl daemon-reload

if [[ "$ENABLE_UPDATE_TIMER" == "true" ]]; then
  sudo systemctl enable --now codex-workspace-sync-update.timer
fi

echo "Installed systemd units."
echo "API service:      /etc/systemd/system/codex-workspace-sync.service"
echo "Update service:   /etc/systemd/system/codex-workspace-sync-update.service"
echo "Update timer:     /etc/systemd/system/codex-workspace-sync-update.timer"
if [[ "$ENABLE_UPDATE_TIMER" == "true" ]]; then
  echo "Update timer enabled."
fi
