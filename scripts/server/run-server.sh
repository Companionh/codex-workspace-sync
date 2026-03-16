#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
CWS_SERVER_BIN="${CWS_SERVER_BIN:-$ROOT/.venv/bin/cws-server}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8787}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

export CWS_APP_ROOT="${CWS_APP_ROOT:-$ROOT}"
export CWS_STATE_ROOT="${CWS_STATE_ROOT:-/opt/codex-workspace-sync/state}"

if [[ -x "$CWS_SERVER_BIN" ]]; then
  exec "$CWS_SERVER_BIN" serve --host "$HOST" --port "$PORT"
fi

exec "$PYTHON_BIN" -m cws.server.bootstrap serve --host "$HOST" --port "$PORT"
