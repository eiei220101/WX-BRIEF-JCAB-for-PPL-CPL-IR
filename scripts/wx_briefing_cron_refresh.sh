#!/bin/bash
# 毎朝 08:00 JST 用: 動作中ポータルへ HTTP 更新 → 失敗時は単体スクリプトで再取得
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
LOG_DIR="$REPO_ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/cron_refresh.log"

PYTHON="$REPO_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="$(command -v python3)"
fi

read -r PORT TOKEN <<< "$(
  "$PYTHON" - <<'PY'
import json
from pathlib import Path
cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
port = int(cfg.get("port") or 18876)
secret = str((cfg.get("merged_pdf") or {}).get("cron_refresh_secret") or "").strip()
print(port, secret)
PY
)"

{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') cron refresh start (repo=$REPO_ROOT) ==="
} >> "$LOG"

URL="http://127.0.0.1:${PORT}/internal/refresh-materials"
CURL_OPTS=(-sfS -X POST --max-time 900)
if [[ -n "$TOKEN" ]]; then
  CURL_OPTS+=(-H "X-WX-Briefing-Cron-Token: ${TOKEN}")
fi

if curl "${CURL_OPTS[@]}" "$URL" >> "$LOG" 2>&1; then
  echo "=== HTTP refresh OK (portal on :${PORT}) ===" >> "$LOG"
  exit 0
fi

echo "=== HTTP refresh failed; running standalone script ===" >> "$LOG"
"$PYTHON" "$REPO_ROOT/scripts/wx_briefing_refresh_all.py" >> "$LOG" 2>&1
