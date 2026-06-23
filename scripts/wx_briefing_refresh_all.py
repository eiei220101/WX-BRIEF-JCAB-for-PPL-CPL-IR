#!/usr/bin/env python3
"""
WX Briefing: 全資料を再取得し、config に従い結合 PDF を温める（cron / launchd 用）。

例:
  .venv/bin/python scripts/wx_briefing_refresh_all.py
  .venv/bin/python scripts/wx_briefing_refresh_all.py --skip-merged-pdf
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import app as wx  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="WX Briefing 全資料の再取得（cron 用）")
    parser.add_argument(
        "--skip-merged-pdf",
        action="store_true",
        help="結合 PDF の事前生成をスキップする",
    )
    args = parser.parse_args()
    cfg = wx.load_config()
    summary = wx.run_materials_refresh_job(
        cfg,
        prebuild_merged=not args.skip_merged_pdf,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    ok = int(summary.get("fetch_ok") or 0)
    err = int(summary.get("fetch_err") or 0)
    if ok == 0 and err > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
