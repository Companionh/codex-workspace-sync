#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
STATE_ROOT="${2:-${CWS_STATE_ROOT:-/opt/codex-workspace-sync/state}}"

if [[ -d "$ROOT" && -f "$ROOT/pyproject.toml" ]]; then
  ROOT="$(cd "$ROOT" && pwd)"
else
  ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

STAMP="$(date -u +%Y%m%d_%H%M%S)"
DEST="${3:-$ROOT/server_backups/live_$STAMP}"

mkdir -p "$DEST"

if [[ -f "$ROOT/.env" ]]; then
  cp -a "$ROOT/.env" "$DEST/.env"
fi

if [[ -d "$ROOT/.cws" ]]; then
  cp -a "$ROOT/.cws" "$DEST/.cws"
fi

if [[ -d "$STATE_ROOT" ]]; then
  cp -a "$STATE_ROOT" "$DEST/state"
fi

echo "Backed up live state to: $DEST"
