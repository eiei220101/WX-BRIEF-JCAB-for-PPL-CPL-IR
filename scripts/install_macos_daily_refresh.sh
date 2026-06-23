#!/bin/bash
# macOS: 毎朝 08:00（システムのローカル時刻）に資料を自動更新する launchd を登録する。
# 日本で使う場合は「システム設定 → 一般 → 日付と時刻 → タイムゾーン」を東京にしてください。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.wxbriefing.daily-refresh"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/${LABEL}.plist"
SCRIPT="$REPO_ROOT/scripts/wx_briefing_cron_refresh.sh"
LOG_DIR="$REPO_ROOT/data/logs"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "このインストーラは macOS 用です。" >&2
  exit 1
fi

chmod +x "$SCRIPT" "$REPO_ROOT/scripts/wx_briefing_refresh_all.py"
mkdir -p "$LOG_DIR" "$PLIST_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${SCRIPT}</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>8</integer>
    <key>Minute</key>
    <integer>0</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/launchd_stdout.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/launchd_stderr.log</string>
  <key>WorkingDirectory</key>
  <string>${REPO_ROOT}</string>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/${LABEL}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}" 2>/dev/null || true

echo "登録しました: ${PLIST_PATH}"
echo "毎日 08:00（Mac のローカル時刻）に ${SCRIPT} を実行します。"
echo "ログ: ${LOG_DIR}/cron_refresh.log"
echo ""
echo "【重要】08:00 更新をポータルのメモリに反映するには、同時刻まで python app.py を常駐させてください。"
echo "  cd ${REPO_ROOT} && .venv/bin/python app.py"
echo ""
echo "手動テスト:"
echo "  ${SCRIPT}"
echo ""
echo "登録解除:"
echo "  launchctl bootout gui/$(id -u)/${LABEL}"
