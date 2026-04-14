#!/usr/bin/env python3
"""
ローカル用: 設定したURLから毎回取得し、1クリックでPDF（結合）ダウンロード。
実行: python app.py
ブラウザ: http://127.0.0.1:<configのport>/
"""
from __future__ import annotations

import base64
import hashlib
import html
import io
import json
import math
import os
import re
import sys
import traceback
import urllib.error
import urllib.request
import urllib.parse
import zipfile
import concurrent.futures
import shutil
import subprocess

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from xml.sax.saxutils import escape as xml_escape
import secrets
import threading
import time
import webbrowser
from datetime import datetime, time as dt_time, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
USER_AGENT = "WXBriefingPortal/1.0 (+local)"
# 画面が古いときの切り分け用（更新したら数字を上げる）
PORTAL_BUILD = "20260414-75-streamlit-taf-merge-select"

_PORTAL_APP_PATH = Path(__file__).resolve()
_PORTAL_GIT_ONCE: str | None = None


def _portal_git_short_once() -> str:
    """リポジトリ内なら短いコミット（未コミット変更ありは -dirty）。失敗時は空（1 プロセスで 1 回だけ試行）。"""
    global _PORTAL_GIT_ONCE
    if _PORTAL_GIT_ONCE is not None:
        return _PORTAL_GIT_ONCE
    try:
        root = _PORTAL_APP_PATH.parent
        cp = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=2,
        )
        if cp.returncode != 0 or not (cp.stdout or "").strip():
            _PORTAL_GIT_ONCE = ""
            return _PORTAL_GIT_ONCE
        h = cp.stdout.strip()
        try:
            cp2 = subprocess.run(
                ["git", "diff", "--quiet"],
                cwd=root,
                timeout=2,
            )
            if cp2.returncode != 0:
                h += "-dirty"
        except (OSError, subprocess.TimeoutExpired):
            pass
        _PORTAL_GIT_ONCE = h
    except (OSError, subprocess.TimeoutExpired):
        _PORTAL_GIT_ONCE = ""
    if _PORTAL_GIT_ONCE is None:
        _PORTAL_GIT_ONCE = ""
    return _PORTAL_GIT_ONCE


def portal_build_stamp() -> str:
    """
    画面・HTTP ヘッダ用の実行識別子（毎回評価するときは app.py の mtime が更新される）。
    PORTAL_BUILD（手動ラベル）に加え、ファイル更新時刻（UTC）と、利用可能なら git の短いコミット。
    """
    try:
        m = _PORTAL_APP_PATH.stat().st_mtime
    except OSError:
        mtime_s = "?"
    else:
        mtime_s = datetime.fromtimestamp(m, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    parts = [PORTAL_BUILD, f"app.py {mtime_s}"]
    g = _portal_git_short_once()
    if g:
        parts.append(f"git {g}")
    return " | ".join(parts)


def portal_build_short_stamp() -> str:
    """Streamlit キャプション用: ``PORTAL_BUILD`` と ``app.py`` の更新時刻（UTC）のみ（git なし）。"""
    try:
        m = _PORTAL_APP_PATH.stat().st_mtime
    except OSError:
        mtime_s = "?"
    else:
        mtime_s = datetime.fromtimestamp(m, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return f"{PORTAL_BUILD} | app.py {mtime_s}"
# NOAA Aviation Weather Center（公開 API・METAR/TAF 用・source=noaa_awc のとき）
AWC_API_METAR = "https://aviationweather.gov/api/data/metar"
AWC_API_TAF = "https://aviationweather.gov/api/data/taf"
# 国際気象海洋㈱ SmartPhone 版（公式 API ではなく HTML。source=imoc のとき）
IMOC_SMARTPHONE_D = "https://www.imoc.co.jp/SmartPhone/d"
# metar.php の Area=（地域）一覧内の Port= リンク先
IMOC_METAR_AREA_BY_ICAO: dict[str, int] = {
    "RJSF": 1,
    "RJSS": 1,
    "RJSN": 4,
    "RJSC": 1,
    "RJSI": 1,
    "RJSY": 1,
    "RJTU": 2,
    "RJAH": 2,
    "RJSK": 1,
}
# taf.php は地域別の一覧ページ。当該 ICAO が掲載されている Area（同社ページに無い場合は None）
IMOC_TAF_AREA_BY_ICAO: dict[str, int | None] = {
    "RJSF": 0,
    "RJSS": 0,
    "RJSN": 1,
    "RJSC": None,
    "RJSI": 0,
    "RJSY": None,
    "RJTU": None,
    "RJAH": 1,
    "RJSK": 0,
}
# 既定の対象空港（config.metar_taf_fetch.airports が空のとき）
METAR_TAF_DEFAULT_AIRPORTS: list[dict[str, str]] = [
    {"icao": "RJSF", "label": "福島空港"},
    {"icao": "RJSS", "label": "仙台空港"},
    {"icao": "RJSN", "label": "新潟空港"},
    {"icao": "RJSC", "label": "山形空港"},
    {"icao": "RJSI", "label": "花巻空港"},
    {"icao": "RJSY", "label": "庄内空港"},
    {"icao": "RJTU", "label": "宇都宮飛行場"},
    {"icao": "RJAH", "label": "百里飛行場（茨城空港）"},
    {"icao": "RJSK", "label": "秋田空港"},
    {"icao": "RJFF", "label": "福岡空港"},
    {"icao": "RJFR", "label": "北九州空港"},
    {"icao": "RJFS", "label": "佐賀空港"},
    {"icao": "RJFU", "label": "長崎空港"},
    {"icao": "RJDB", "label": "壱岐空港"},
    {"icao": "RJDT", "label": "対馬空港（対馬やまねこ空港）"},
    {"icao": "RJFE", "label": "福江空港（五島つばき空港）"},
    {"icao": "RJDO", "label": "小値賀空港"},
    {"icao": "RJDK", "label": "上五島空港"},
    {"icao": "RJFT", "label": "熊本空港"},
    {"icao": "RJDA", "label": "天草空港"},
    {"icao": "RJFO", "label": "大分空港"},
    {"icao": "RJFM", "label": "宮崎空港"},
    {"icao": "RJFK", "label": "鹿児島空港"},
    {"icao": "RJFG", "label": "種子島空港"},
    {"icao": "RJFC", "label": "屋久島空港"},
]
METAR_TAF_ICAO_ALLOW = frozenset(a["icao"] for a in METAR_TAF_DEFAULT_AIRPORTS)
_METAR_TAF_PDF_FONT_OK = False

# Streamlit / HTTP ポータル共通: METAR・TAF 等の UI 地域枠（後から九州などを追加しやすい）
UI_REGION_GROUPS_METAR_TAF: list[dict] = [
    {
        "id": "tohoku_kanto",
        "title": "東北・関東",
        "icaos": (
            "RJSF",
            "RJSS",
            "RJSN",
            "RJSC",
            "RJSI",
            "RJSY",
            "RJSK",
            "RJTU",
            "RJAH",
        ),
    },
    {
        "id": "kyushu",
        "title": "九州",
        "icaos": (
            "RJFF",
            "RJFR",
            "RJFS",
            "RJFU",
            "RJDB",
            "RJDT",
            "RJFE",
            "RJDO",
            "RJDK",
            "RJFT",
            "RJDA",
            "RJFO",
            "RJFM",
            "RJFK",
            "RJFG",
            "RJFC",
        ),
    },
]
# 飛行場時系列予報の products 用（ICAO 並びは METAR・TAF と同じ地域定義）
UI_REGION_GROUPS_TAF_ICAO = UI_REGION_GROUPS_METAR_TAF
# 下層悪天予想図（詳細版）の Fig を地域枠に分類（現状は東北・関東のみ）。
# Fig### と画像は気象庁 awfo_low-level_detailed-sigwx.html の areano と同一
# （JMA_DETAILED_SIGWX_FIG_LABELS_JA を参照）。
UI_REGION_GROUPS_DETAILED_SIGWX: list[dict] = [
    {
        "id": "tohoku_kanto",
        "title": "東北・関東",
        "figs": (
            "Fig206",
            "Fig204",
            "Fig202",
            "Fig205",
            "Fig501",
            "Fig203",
            "Fig301",
            "Fig302",
        ),
    },
]
# 下層悪天予想図（地域別・時系列）: 東北を「東北・関東」枠、西日本を別枠
UI_REGION_GROUPS_SIGWX_AREA: list[dict] = [
    {"id": "tohoku_kanto", "title": "東北・関東", "areas": ("fbsn",)},
    {"id": "nishi_nihon", "title": "西日本", "areas": ("fbos",)},
]


def group_metar_taf_airports_by_region(
    airports: list[dict],
) -> list[tuple[str, list[dict]]]:
    """UI 用: (地域タイトル, 空港行) のリスト。未定義の空港は「その他」。"""
    seen: set[str] = set()
    blocks: list[tuple[str, list[dict]]] = []
    for g in UI_REGION_GROUPS_METAR_TAF:
        title = str(g.get("title") or "").strip() or "地域"
        want = [str(x).strip().upper() for x in (g.get("icaos") or ()) if str(x).strip()]
        if not want:
            continue
        order = {c: i for i, c in enumerate(want)}
        sub = [
            ap
            for ap in airports
            if str(ap.get("icao") or "").strip().upper() in order
        ]
        sub.sort(
            key=lambda ap: order.get(str(ap.get("icao") or "").strip().upper(), 99)
        )
        for ap in sub:
            seen.add(str(ap.get("icao") or "").strip().upper())
        if sub:
            blocks.append((title, sub))
    other = [
        ap
        for ap in airports
        if str(ap.get("icao") or "").strip().upper() not in seen
    ]
    if other:
        other.sort(key=lambda ap: str(ap.get("icao") or ""))
        blocks.append(("その他", other))
    return blocks


def group_taf_products_by_region(
    prows: list[dict],
) -> list[tuple[str, list[dict]]]:
    """飛行場時系列予報 products を地域枠ごとに並べ替え。"""
    rows: list[dict] = []
    for pr in prows:
        if not isinstance(pr, dict):
            continue
        c = str(pr.get("icao") or "").strip().upper()
        if c:
            rows.append(pr)
    seen: set[str] = set()
    blocks: list[tuple[str, list[dict]]] = []
    for g in UI_REGION_GROUPS_TAF_ICAO:
        title = str(g.get("title") or "").strip() or "地域"
        want = [str(x).strip().upper() for x in (g.get("icaos") or ()) if str(x).strip()]
        if not want:
            continue
        order = {c: i for i, c in enumerate(want)}
        sub = [pr for pr in rows if str(pr.get("icao") or "").strip().upper() in order]
        sub.sort(
            key=lambda pr: order.get(str(pr.get("icao") or "").strip().upper(), 99)
        )
        for pr in sub:
            seen.add(str(pr.get("icao") or "").strip().upper())
        if sub:
            blocks.append((title, sub))
    other = [
        pr
        for pr in rows
        if str(pr.get("icao") or "").strip().upper() not in seen
    ]
    if other:
        other.sort(key=lambda pr: str(pr.get("icao") or ""))
        blocks.append(("その他", other))
    return blocks


def group_detailed_sigwx_rows_by_region(
    drows: list[dict],
) -> list[tuple[str, list[dict]]]:
    """詳細版の行（fig_key）を地域枠に分類。"""
    seen: set[str] = set()
    blocks: list[tuple[str, list[dict]]] = []
    for g in UI_REGION_GROUPS_DETAILED_SIGWX:
        title = str(g.get("title") or "").strip() or "地域"
        want = [str(x).strip() for x in (g.get("figs") or ()) if str(x).strip()]
        if not want:
            continue
        order = {c: i for i, c in enumerate(want)}
        sub = [d for d in drows if str(d.get("fig_key") or "").strip() in order]
        sub.sort(
            key=lambda d: order.get(str(d.get("fig_key") or "").strip(), 99)
        )
        for d in sub:
            seen.add(str(d.get("fig_key") or "").strip())
        if sub:
            blocks.append((title, sub))
    other = [d for d in drows if str(d.get("fig_key") or "").strip() not in seen]
    if other:
        other.sort(key=lambda d: str(d.get("fig_key") or ""))
        blocks.append(("その他", other))
    return blocks


def group_sigwx_rows_by_region(srows: list[dict]) -> list[tuple[str, list[dict]]]:
    """下層悪天予想図（地域別）の行を UI 枠ごとに分類。"""
    seen: set[str] = set()
    blocks: list[tuple[str, list[dict]]] = []
    for g in UI_REGION_GROUPS_SIGWX_AREA:
        title = str(g.get("title") or "").strip() or "地域"
        want = [
            re.sub(r"[^a-z0-9]", "", str(x).lower())
            for x in (g.get("areas") or ())
            if str(x).strip()
        ]
        if not want:
            continue
        order = {a: i for i, a in enumerate(want)}
        sub = [s for s in srows if str(s.get("area") or "") in order]
        sub.sort(key=lambda s: order.get(str(s.get("area") or ""), 99))
        for s in sub:
            seen.add(str(s.get("area") or ""))
        if sub:
            blocks.append((title, sub))
    other = [s for s in srows if str(s.get("area") or "") not in seen]
    if other:
        other.sort(key=lambda s: str(s.get("area") or ""))
        blocks.append(("その他", other))
    return blocks


def _metar_taf_pdf_font_name() -> str:
    """日本語を含む PDF 用（reportlab 同梱の CID フォント）。"""
    global _METAR_TAF_PDF_FONT_OK
    if not _METAR_TAF_PDF_FONT_OK:
        name = "HeiseiKakuGo-W5"
        pdfmetrics.registerFont(UnicodeCIDFont(name))
        _METAR_TAF_PDF_FONT_OK = True
    return "HeiseiKakuGo-W5"


def _pdf_paragraph_text(raw: str) -> str:
    return xml_escape(str(raw)).replace("\n", "<br/>")
JMA_LIST_URL = "https://www.jma.go.jp/bosai/weather_map/data/list.json"
JMA_PNG_BASE = "https://www.jma.go.jp/bosai/weather_map/data/png/"
JMA_QUICKDAILY_BASE = "https://www.data.jma.go.jp/yoho/data/wxchart/quick"
JMA_SPAS_LATEST_URL = "https://www.data.jma.go.jp/yoho/data/wxchart/quick/spas_latest.txt"
# 防災天気図（アジア太平洋域）の「最新24時間予想図」PDF と同一（bosai フロントの PDF リンク先）
JMA_FSAS24_ASIA_PDF = "https://www.data.jma.go.jp/yoho/data/wxchart/quick/FSAS24_{}_ASIA.pdf"
JMA_NUMERICMAP_NWP_BASE = "https://www.jma.go.jp/bosai/numericmap/data/nwpmap/"
# 航空気象情報「国内悪天予想図（FBJP）」awfo_fbjp.html と同一の表示用 PNG（更新は気象庁側で上書き）
JMA_AIRINFO_FBJP_PNG = "https://www.data.jma.go.jp/airinfo/data/pict/fbjp/fbjp.png"
# 下層悪天予想図（awfo_low-level_sigwx.html / conf/functions.js の pict/low-level_sigwx/ 規則）
JMA_AIRINFO_LOW_LEVEL_SIGWX_BASE = "https://www.data.jma.go.jp/airinfo/data/pict/low-level_sigwx/"
JMA_AIRINFO_LOW_LEVEL_SIGWX_AREAS = frozenset(
    {"fbsp", "fbsn", "fbtk", "fbos", "fbkg", "fbok"}
)
# awfo_low-level_sigwx.html の areaArray 順（functions.js）に対応する表示名
LOW_LEVEL_SIGWX_AREA_LABELS: dict[str, str] = {
    "fbsp": "北海道",
    "fbsn": "東北",
    "fbtk": "東日本",
    "fbos": "西日本",
    "fbkg": "奄美",
    "fbok": "沖縄",
}
# FT3=03, FT6=06, FT9=09, 時系列=39（changeFT と同一）
JMA_AIRINFO_LOW_LEVEL_SIGWX_FT = frozenset({"03", "06", "09", "39"})
# 下層悪天予想図（詳細版）（awfo_low-level_detailed-sigwx.html / functions_low-level_detailed-sigwx.js）
JMA_AIRINFO_DETAILED_SIGWX_BASE = "https://www.data.jma.go.jp/airinfo/data/pict/low-level_sigwx_p/"
JMA_AIRINFO_DETAILED_SIGWX_PREFIX = "Lsigp_"
# 国内航空路6・12時間予想断面図 FXJP106（awfo_fxjp106.html → ../data/pict/nwp/fxjp106_HH.png）
JMA_AIRINFO_FXJP_NWP_BASE = "https://www.data.jma.go.jp/airinfo/data/pict/nwp/"
JMA_AIRINFO_FXJP106_INITIAL_UTC = frozenset(
    {"00", "03", "06", "09", "12", "15", "18", "21"}
)
# 飛行場時系列予報・情報（awfo_taf.html / conf/functions_taf.js の pict/taf/ 規則）
JMA_AIRINFO_TAF_PNG_BASE = "https://www.data.jma.go.jp/airinfo/data/pict/taf/"
# awfo_taf / functions_taf.js: 既定 pictType=QMCD98_、切替で QMCJ98_。
# PDF では PART1→PART2 の順とするため、1 ページ目=QMCD98_、2 ページ目=QMCJ98_ とする。
JMA_AIRINFO_TAF_PART1_PREFIX = "QMCD98_"
JMA_AIRINFO_TAF_PART2_PREFIX = "QMCJ98_"
JMA_HIMI_JP_TARGET_TIMES = (
    "https://www.jma.go.jp/bosai/himawari/data/satimg/targetTimes_jp.json"
)
JMA_HIMI_JP_SATIMG_BASE = "https://www.jma.go.jp/bosai/himawari/data/satimg"
# 防災ひまわり satimg（jp）の実タイルは z=6 まで。z=7 以降は 404 となるためモザイクも 6 で打ち止め。
HIMI_JP_TILE_MAX_ZOOM = 6
JMA_NOWC_TILE_ROOT = "https://www.jma.go.jp/bosai/jmatile/data/nowc"
JMA_NOWC_TARGET_N1 = f"{JMA_NOWC_TILE_ROOT}/targetTimes_N1.json"
JMA_GSI_PALE_TILE = "https://www.jma.go.jp/tile/gsi/pale"
WXBRIEFING_HRPNS_MOSAIC = "wxbriefing://jma-nowc-hrpns-mosaic"
WXBRIEFING_HIMI_JP_MOSAIC = "wxbriefing://jma-himi-jp-mosaic"
WXBRIEFING_HIMI_JP_MAP_SCREENSHOT = "wxbriefing://jma-himi-map-screenshot"
WXBRIEFING_AIRINFO_TAF_MERGED = "wxbriefing://jma-airinfo-taf-merged"
JST = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")
_jma_list_cache: tuple[float, dict] | None = None
JMA_LIST_CACHE_SEC = 60.0
_himawari_jp_times_cache: tuple[float, list] | None = None
HIMI_JP_TIMES_CACHE_SEC = 25.0
# 統合地図スクショ: 広告・ヘッダの外に出しがちな地図本体（Leaflet）を優先して切り出す
_HIMI_MAP_SCREENSHOT_DEFAULT_SELECTORS: tuple[str, ...] = (
    "#contents-inner .leaflet-container",
    ".leaflet-container",
    "#mapContents .leaflet-container",
    "#map .leaflet-container",
    "div#map",
    "#map",
)
_HIMI_MAP_SCREENSHOT_HIDE_ADS_JS = """
(() => {
  const sels = [
    'iframe[src*="googlesyndication"]','iframe[src*="doubleclick.net"]',
    'iframe[id^="google_ads"]','iframe[name^="google_ads"]','ins.adsbygoogle',
    'div[id^="div-gpt-ad"]','iframe[title*="広告"]','iframe[title*="Advertisement"]'
  ];
  for (const s of sels) {
    try {
      document.querySelectorAll(s).forEach((el) => {
        el.style.setProperty("display", "none", "important");
        el.style.setProperty("visibility", "hidden", "important");
      });
    } catch (_e) {}
  }
})();
"""
_playwright_chromium_lock = threading.Lock()
_playwright_chromium_ready = False


def _header_safe_ascii(text: str, max_len: int = 2000) -> str:
    """HTTP ヘッダは ASCII/Latin-1 前提のクライアントがあるため、日本語等は置換する。"""
    return text.encode("ascii", errors="replace").decode("ascii")[:max_len]


def load_config() -> dict:
    raw = CONFIG_PATH.read_text(encoding="utf-8")
    return json.loads(raw)


def fetch_url(url: str, timeout: int = 60) -> tuple[bytes, str | None]:
    headers = {"User-Agent": USER_AGENT}
    if "data.jma.go.jp/mscweb/data/himawari" in url or "jma.go.jp/bosai/himawari/data/satimg" in url:
        headers["Cache-Control"] = "max-age=0, no-cache"
        headers["Pragma"] = "no-cache"
    if "jma.go.jp/bosai/himawari/data/satimg" in url:
        headers["Referer"] = "https://www.jma.go.jp/"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        ctype = resp.headers.get("Content-Type")
    return data, ctype


def _parse_jma_nowc_ts14_utc(s: str) -> datetime:
    """jmatile targetTimes の 14 桁（YYYYMMDDHHmmss）を UTC とみなす。"""
    return datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _fmt_jst_utc(dt_utc: datetime) -> tuple[str, str]:
    """人向けラベル: (JST 文字列, UTC 文字列)"""
    jst = dt_utc.astimezone(JST)
    return (
        jst.strftime("%Y-%m-%d %H:%M JST"),
        dt_utc.strftime("%Y-%m-%d %H:%M UTC"),
    )


def _jma_parse_list_json(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


def _jma_fetch_list_cached() -> dict:
    global _jma_list_cache
    now = time.time()
    if _jma_list_cache is not None and (now - _jma_list_cache[0]) < JMA_LIST_CACHE_SEC:
        return _jma_list_cache[1]
    raw, _ = fetch_url(JMA_LIST_URL, timeout=30)
    data = _jma_parse_list_json(raw)
    _jma_list_cache = (now, data)
    return data


def _jma_area_and_valid(filename: str) -> tuple[str | None, str | None]:
    m = re.search(r"_(\d{6})_(\d{14})_MET_CHT_", filename)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def _jma_pick_surface_analysis(now_files: list[str], want_jst: datetime) -> str | None:
    want = want_jst.astimezone(JST)
    target_v = want.strftime("%Y%m%d%H") + "0000"
    matches: list[tuple[str, str]] = []
    for fn in now_files:
        if "MET_CHT_JCIspas" not in fn:
            continue
        _area, vt = _jma_area_and_valid(fn)
        if not vt:
            continue
        if vt == target_v:
            matches.append((fn[:14], fn))
    if not matches:
        return None
    return max(matches, key=lambda t: t[0])[1]


def _jma_forecast_files(block: dict) -> list[str]:
    out: list[str] = []
    for k in ("ft24", "ft48"):
        out.extend(block.get(k) or [])
    return out


def _jma_pick_forecast_surface_21jst(block: dict, now_jst: datetime) -> tuple[str | None, str | None]:
    """予想地上: ファイル名に当日21時(JST)が入るものがあれば採用。無ければ ft24 最新を使う（警告文付き）。"""
    ymd = now_jst.strftime("%Y%m%d")
    needle = f"_{ymd}210000_"
    cands: list[tuple[str, str]] = []
    for fn in _jma_forecast_files(block):
        if needle not in fn:
            continue
        if "JCIfsas" not in fn:
            continue
        cands.append((fn[:14], fn))
    if cands:
        return max(cands, key=lambda t: t[0])[1], None
    ft24 = block.get("ft24") or []
    if not ft24:
        return None, "ft24 が空のため予想図を選べませんでした"
    return ft24[-1], "当日21時を示すファイル名が見つからず、最新の24時間予想（日本域・PNG）を入れました"


def quickdaily_asas_pdf_url(
    show_ymd: str | None,
    hour_jst: int,
    color: bool,
) -> str:
    """
    data.jma.go.jp の quickdaily.js（makeASAS）と同じ規則で PDF URL を組み立てる。
    ページ上の「日本時間 HH 時」は JST のカレンダ日付に対し、ファイル名の時刻は UTC（-9h）。
    """
    allowed = (3, 9, 15, 21)
    if hour_jst not in allowed:
        raise ValueError(f"hour_jst は {allowed} のいずれかにしてください（アジア太平洋域の実況時刻）")

    if show_ymd:
        ymd = str(show_ymd).strip()
        if len(ymd) != 8 or not ymd.isdigit():
            raise ValueError("show は YYYYMMDD 8桁で指定してください")
    else:
        ymd = datetime.now(JST).strftime("%Y%m%d")

    day0 = datetime.strptime(ymd, "%Y%m%d").replace(tzinfo=JST)
    target_jst = day0.replace(hour=hour_jst, minute=0, second=0, microsecond=0)
    utc = target_jst.astimezone(UTC)
    ym = utc.strftime("%Y%m")
    ymdh = utc.strftime("%Y%m%d%H")
    prefix = "ASAS_COLOR_" if color else "ASAS_MONO_"
    return f"{JMA_QUICKDAILY_BASE}/{ym}/{prefix}{ymdh}00.pdf"


def _quickmonthly_latest_ymd_from_spas_latest() -> str:
    """
    quickmonthly.js と同じ spas_latest.txt を読み、カレンダ上の「いちばん新しい日」(YYYYMMDD) を返す。
    ファイル先頭12桁を yyyyMMddHHmm とみなし JST、そこに 9 時間を足してから日付部分を使う（JS と同趣旨）。
    """
    raw, _ = fetch_url(JMA_SPAS_LATEST_URL, timeout=20)
    text = raw.decode("utf-8", "replace").strip()
    m = re.match(r"^(\d{12})", text)
    if not m:
        raise ValueError(f"spas_latest.txt が想定外です: {text[:120]!r}")
    s = m.group(1)
    base = datetime.strptime(s, "%Y%m%d%H%M").replace(tzinfo=JST)
    shifted = base + timedelta(hours=9)
    return shifted.strftime("%Y%m%d")


def jma_fsas24_asia_pdf_url(*, color: bool) -> str:
    """アジア太平洋域の最新24時間予想（FSAS24）PDF。防災天気図ページの PDF リンクと同じ。"""
    mid = "COLOR" if color else "MONO"
    return JMA_FSAS24_ASIA_PDF.format(mid)


def quickmonthly_prevday_asas_21jst_pdf_url(color: bool) -> tuple[str, str, str]:
    """
    「月別一覧でいちばん新しい日」の前日の、実況天気図（アジア太平洋域）21時(JST) の PDF URL。
    戻り値: (pdf_url, show_ymd 前日, latest_ymd 一覧上の最新日)
    """
    latest_ymd = _quickmonthly_latest_ymd_from_spas_latest()
    d_latest = datetime.strptime(latest_ymd, "%Y%m%d").replace(tzinfo=JST).date()
    d_prev = d_latest - timedelta(days=1)
    show = d_prev.strftime("%Y%m%d")
    url = quickdaily_asas_pdf_url(show, 21, color)
    return url, show, latest_ymd


def jma_numericmap_upper_pdf_url(product_id: str, utc_hour: str) -> str:
    """
    防災「数値予報天気図」高層（#type=upper）の PDF。
    ページ内定数と同じ相対パス data/nwpmap/{id}_{00|12}.pdf を絶対URL化。
    """
    u = str(utc_hour).strip()
    if u not in ("00", "12"):
        raise ValueError('utc_hour は "00" または "12"（UTC発表時刻）')
    pid = re.sub(r"[^a-z0-9]", "", product_id.lower())
    if not pid:
        raise ValueError("product_id が空です")
    return f"{JMA_NUMERICMAP_NWP_BASE}{pid}_{u}.pdf"


def jma_airinfo_taf_part_png_url(icao: str, part: int) -> str:
    """
    航空気象情報「飛行場時系列予報・飛行場時系列情報」の画像 URL。
    awfo_taf.html が読み込む conf/functions_taf.js と同じ pict/taf/{prefix}{ICAO}.png。
    part は 1（QMCD98_=PART1・ページ既定）または 2（QMCJ98_=PART2・切替）。
    """
    code = re.sub(r"[^A-Za-z0-9]", "", str(icao)).upper()
    if len(code) != 4:
        raise ValueError("ICAO は英数字4文字（例: RJSS）を指定してください")
    if part == 1:
        pref = JMA_AIRINFO_TAF_PART1_PREFIX
    elif part == 2:
        pref = JMA_AIRINFO_TAF_PART2_PREFIX
    else:
        raise ValueError("part は 1 または 2 を指定してください")
    return f"{JMA_AIRINFO_TAF_PNG_BASE}{pref}{code}.png"


def build_airinfo_taf_merged_pdf_bytes(item: dict, timeout: int = 90) -> bytes:
    """飛行場時系列予報: PART1 / PART2 の PNG を取得し、1 本の PDF に連結（選択は item のフラグ）。"""
    from pypdf import PdfReader, PdfWriter

    icao = str(item.get("taf_icao") or "").strip()
    if not icao:
        raise ValueError("飛行場時系列予報: taf_icao（ICAO）がありません")
    inc1 = bool(item.get("taf_include_part1", True))
    inc2 = bool(item.get("taf_include_part2", True))
    if not inc1 and not inc2:
        raise ValueError("飛行場時系列予報: PART1 / PART2 のいずれかにチェックが必要です")
    res = float(item.get("taf_image_pdf_resolution") or 120)
    res = min(300.0, max(72.0, res))
    writer = PdfWriter()
    blobs: list[bytes] = []
    if inc1 and inc2:
        u1 = jma_airinfo_taf_part_png_url(icao, 1)
        u2 = jma_airinfo_taf_part_png_url(icao, 2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fut1 = ex.submit(fetch_url, u1, timeout)
            fut2 = ex.submit(fetch_url, u2, timeout)
            b1, _ = fut1.result()
            b2, _ = fut2.result()
        blobs.extend((b1, b2))
    elif inc1:
        b1, _ = fetch_url(jma_airinfo_taf_part_png_url(icao, 1), timeout=timeout)
        blobs.append(b1)
    elif inc2:
        b2, _ = fetch_url(jma_airinfo_taf_part_png_url(icao, 2), timeout=timeout)
        blobs.append(b2)
    for raw in blobs:
        pdf_one = _image_bytes_to_single_page_pdf(raw, resolution=res)
        reader = PdfReader(io.BytesIO(pdf_one))
        if len(reader.pages) == 0:
            raise ValueError("飛行場時系列予報: 中間 PDF にページがありません")
        writer.add_page(reader.pages[0])
    out_buf = io.BytesIO()
    writer.write(out_buf)
    return out_buf.getvalue()


def jma_airinfo_low_level_sigwx_png_url(area: str, ft: str) -> str:
    """
    航空気象情報「下層悪天予想図」の画像 URL。
    awfo_low-level_sigwx.html が読み込む conf/functions.js と同じ
    pict/low-level_sigwx/{area}{ft}.png（area は fbsp 等、ft は 03/06/09/39）。
    """
    a = re.sub(r"[^a-z0-9]", "", str(area).lower())
    f = re.sub(r"[^a-z0-9]", "", str(ft).lower())
    if f in ("timeseries", "series", "jikeiretsu"):
        f = "39"
    if a not in JMA_AIRINFO_LOW_LEVEL_SIGWX_AREAS:
        raise ValueError(
            f"area は {sorted(JMA_AIRINFO_LOW_LEVEL_SIGWX_AREAS)} のいずれかにしてください（東北=fbsn）"
        )
    if f not in JMA_AIRINFO_LOW_LEVEL_SIGWX_FT:
        raise ValueError(
            f"forecast_type(ft) は 03, 06, 09, 39(時系列) のいずれかにしてください: {f!r}"
        )
    return f"{JMA_AIRINFO_LOW_LEVEL_SIGWX_BASE}{a}{f}.png"


def jma_airinfo_detailed_sigwx_png_url(fig: str, *, prefix: str | None = None) -> str:
    """
    航空気象情報「下層悪天予想図（詳細版）」の画像 URL。
    awfo_low-level_detailed-sigwx.html の areano（Fig204 等）と同一で、
    pict/low-level_sigwx_p/{Lsigp_}{Fig###}.png。
    """
    raw = str(fig).strip()
    compact = re.sub(r"[\s_-]+", "", raw)
    m = re.fullmatch(r"(?i)fig(\d+)", compact)
    if not m:
        raise ValueError("fig は Fig + 数字（例: Fig204 宮城）で指定してください")
    canon = f"Fig{m.group(1)}"
    pref = str(prefix or JMA_AIRINFO_DETAILED_SIGWX_PREFIX).strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*_", pref):
        raise ValueError("prefix が想定外です（既定は Lsigp_）")
    return f"{JMA_AIRINFO_DETAILED_SIGWX_BASE}{pref}{canon}.png"


def detailed_sigwx_fig_canonical(fig: str) -> str | None:
    """Fig204 / fig204 / Fig_204 等を Fig204 形式に正規化。不正なら None。"""
    raw = str(fig).strip()
    compact = re.sub(r"[\s_-]+", "", raw)
    m = re.fullmatch(r"(?i)fig(\d+)", compact)
    if not m:
        return None
    return f"Fig{m.group(1)}"


# 下層悪天予想図（詳細版）: awfo_low-level_detailed-sigwx.html の value="Fig###" と
# ドロップダウン表示の対応（気象庁サイト実測）。UI・PDF 説明のラベル整合用。
# 未定義の Fig は config の label をそのまま使う。
JMA_DETAILED_SIGWX_FIG_LABELS_JA: dict[str, str] = {
    "Fig201": "青森県",
    "Fig202": "秋田県",
    "Fig203": "岩手県",
    "Fig204": "宮城県",
    "Fig205": "山形県",
    "Fig206": "福島県",
    "Fig301": "茨城県",
    "Fig302": "栃木県",
    "Fig501": "新潟県",
}


def detailed_sigwx_official_ja_label(fig: str) -> str | None:
    """Fig 番号に対応する公式サイト相当の日本語ラベル。辞書に無ければ None。"""
    fk = detailed_sigwx_fig_canonical(str(fig))
    if not fk:
        return None
    return JMA_DETAILED_SIGWX_FIG_LABELS_JA.get(fk)


def jma_airinfo_fxjp106_cross_section_png_url(utc_initial: str) -> str:
    """
    航空気象情報「国内航空路6・12時間予想断面図」（FXJP106）の PNG。
    awfo_fxjp106.html がリンクする ../data/pict/nwp/fxjp106_{HH}.png と同一（HH は初期値時刻 UTC）。
    """
    raw = str(utc_initial).strip().lower().replace("utc", "")
    if raw in ("0", "00"):
        u = "00"
    elif raw.isdigit():
        u = raw.zfill(2)
    else:
        u = raw
    if u not in JMA_AIRINFO_FXJP106_INITIAL_UTC:
        raise ValueError(
            f"initial_utc は {sorted(JMA_AIRINFO_FXJP106_INITIAL_UTC)} "
            f"のいずれかにしてください: {utc_initial!r}"
        )
    return f"{JMA_AIRINFO_FXJP_NWP_BASE}fxjp106_{u}.png"


def numericmap_nwp_utc_suffix_for_download_jst(now_jst: datetime | None = None) -> str:
    """
    数値予報天気図（NWP）の 00UTC / 12UTC 版の切り替え。

    ダウンロード時刻を日本時間とみなし、
    09:30〜21:30（端を含む）なら "00"、それ以外（21:31 以降または 09:29 以前）なら "12"。
    """
    jst = now_jst or datetime.now(JST)
    if jst.tzinfo is None:
        jst = jst.replace(tzinfo=JST)
    else:
        jst = jst.astimezone(JST)
    hm = jst.hour * 60 + jst.minute
    start = 9 * 60 + 30
    end = 21 * 60 + 30
    if start <= hm <= end:
        return "00"
    return "12"


def _deg2num(lat_deg: float, lon_deg: float, zoom: int) -> tuple[int, int]:
    """Web Mercator（XYZ）のタイル番号。北緯は y が小さくなる。"""
    lat_rad = math.radians(lat_deg)
    n = 2.0**zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int(
        (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n
    )
    return xtile, ytile


def _jma_tile_range_lonlat(
    zoom: int,
    lon_w: float,
    lon_e: float,
    lat_s: float,
    lat_n: float,
) -> tuple[int, int, int, int]:
    x_nw, y_nw = _deg2num(lat_n, lon_w, zoom)
    x_se, y_se = _deg2num(lat_s, lon_e, zoom)
    x_min, x_max = min(x_nw, x_se), max(x_nw, x_se)
    y_min, y_max = min(y_nw, y_se), max(y_nw, y_se)
    return x_min, x_max, y_min, y_max


def _himawari_world_px_latlon(wx: float, wy: float, z: int) -> tuple[float, float]:
    """Web Mercator（Slippy）世界画素座標 → (lon, lat) 度。"""
    n = 256 * (2**z)
    lon = wx / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * wy / n))))
    return lon, lat


def _himawari_world_px_from_latlon(lat: float, lon: float, z: int) -> tuple[float, float]:
    """緯度経度（度）→ 世界画素座標（連続値）。"""
    lat_rad = math.radians(lat)
    n = 256 * (2**z)
    wx = (lon + 180.0) / 360.0 * n
    wy = (
        (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi)
        / 2.0
        * n
    )
    return wx, wy


def _himawari_bbox_from_map_viewport(
    z: int,
    center_lat: float,
    center_lon: float,
    vp_w_px: float = 1280.0,
    vp_h_px: float = 820.0,
) -> tuple[float, float, float, float]:
    """
    防災統合地図 ``#z/lat/lon/`` と同系: ズーム z・中心 (lat,lon)・地図キャンバス画素 vp_w×vp_h で
    見えている範囲に近い lon_w, lon_e, lat_s, lat_n（南＝lat_s、北＝lat_n）。

    ``vp_*`` はブラウザの地図枠のおおよそ（既定 1280×820）。窓を広げると西の大陸側まで含みやすい。
    参照: https://www.jma.go.jp/bosai/map.html#5/37.545/145.415/&elem=color&contents=himawari
    """
    cx, cy = _himawari_world_px_from_latlon(center_lat, center_lon, z)
    corners = (
        (cx - vp_w_px / 2, cy - vp_h_px / 2),
        (cx + vp_w_px / 2, cy - vp_h_px / 2),
        (cx - vp_w_px / 2, cy + vp_h_px / 2),
        (cx + vp_w_px / 2, cy + vp_h_px / 2),
    )
    lons: list[float] = []
    lats: list[float] = []
    for wx, wy in corners:
        lo, la = _himawari_world_px_latlon(wx, wy, z)
        lons.append(lo)
        lats.append(la)
    return (min(lons), max(lons), min(lats), max(lats))


def _fetch_himawari_jp_target_times() -> list:
    """防災ひまわり日本域タイル用 targetTimes_jp.json（短時間キャッシュ）。"""
    global _himawari_jp_times_cache
    now = time.time()
    c = _himawari_jp_times_cache
    if c is not None and (now - c[0]) < HIMI_JP_TIMES_CACHE_SEC:
        return c[1]
    raw, _ = fetch_url(JMA_HIMI_JP_TARGET_TIMES, timeout=35)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, list):
        raise ValueError("targetTimes_jp.json: トップが配列ではありません")
    _himawari_jp_times_cache = (now, data)
    return data


def _himawari_jp_band_products(band: str) -> tuple[str, str, str]:
    b = re.sub(r"[^a-z0-9]", "", str(band).lower())
    if not b:
        raise ValueError("band が空です")
    if b in ("tre", "color", "rep", "truecolor", "vis", "rgb"):
        return "REP", "ETC", b
    if b in ("b13", "ir", "infrared", "tbb"):
        return "B13", "TBB", b
    raise ValueError(
        f"未対応の衛星 band: {band!r}（tre=可視相当・b13=赤外／統合地図 color・ir に対応）"
    )


def _himawari_jp_select_slot(ref_utc: datetime | None = None) -> tuple[str, str, datetime]:
    """targetTimes_jp.json から取得時刻 ref 以前で最新の basetime / validtime / slot(UTC) を返す。"""
    ref = ref_utc or datetime.now(UTC)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=UTC)
    else:
        ref = ref.astimezone(UTC)
    skew = timedelta(seconds=90)
    arr = _fetch_himawari_jp_target_times()
    picks: list[tuple[str, dict]] = []
    for it in arr:
        if not isinstance(it, dict):
            continue
        bt = str(it.get("basetime") or "")
        if len(bt) != 14 or not bt.isdigit():
            continue
        try:
            slot = _parse_jma_nowc_ts14_utc(bt)
        except ValueError:
            continue
        if slot > ref + skew:
            continue
        picks.append((bt, it))

    use: dict
    if picks:
        use = max(picks, key=lambda t: t[0])[1]
    else:
        fall: list[tuple[str, dict]] = []
        for it in arr:
            if not isinstance(it, dict):
                continue
            bt = str(it.get("basetime") or "")
            if len(bt) != 14 or not bt.isdigit():
                continue
            fall.append((bt, it))
        if not fall:
            raise ValueError("ひまわり targetTimes_jp.json に有効な basetime がありません")
        use = max(fall, key=lambda t: t[0])[1]

    bt = str(use["basetime"])
    vt = str(use.get("validtime") or bt)
    if len(vt) != 14 or not vt.isdigit():
        vt = bt
    slot_utc = _parse_jma_nowc_ts14_utc(bt)
    return bt, vt, slot_utc


def _himawari_jp_satimg_tile_url(
    bt: str,
    vt: str,
    prod_band: str,
    prod_name: str,
    z: int,
    x: int,
    y: int,
) -> str:
    return f"{JMA_HIMI_JP_SATIMG_BASE}/{bt}/jp/{vt}/{prod_band}/{prod_name}/{z}/{x}/{y}.jpg"


def _himawari_jp_fetch_tile_jpeg(
    bt: str,
    vt: str,
    prod_band: str,
    prod_name: str,
    z: int,
    x: int,
    y_xyz: int,
) -> bytes | None:
    """Web Mercator XYZ の (x,y) のみ使用（TMS の y 反転は別地域のタイルが返ることがあり結合ずれの原因になる）。"""
    n = 1 << z
    if y_xyz < 0 or y_xyz >= n or x < 0 or x >= n:
        return None
    u = _himawari_jp_satimg_tile_url(
        bt, vt, prod_band, prod_name, z, x, y_xyz
    )
    for _attempt in range(5):
        try:
            data, _ = fetch_url(u, timeout=32)
            if (
                isinstance(data, bytes)
                and len(data) > 500
                and data[:2] == b"\xff\xd8"
            ):
                return data
        except Exception:
            pass
        time.sleep(0.08 * (_attempt + 1))
    return None


def _himawari_is_shell_margin_rgb(
    p: tuple[int, int, int],
    *,
    bg: tuple[int, int, int],
    bg_tol: int,
    black_max: int,
) -> bool:
    """
    モザイク外周トリム用。欠損タイル色 (32,36,42) に近い画素、またはタイル JPEG 内の
    レターボックス（純黒〜近黒）をマージンとみなす。black_max < 0 のときは近黒扱いをしない。
    """
    r, g, b = p[0], p[1], p[2]
    br, bgc, bb = bg[0], bg[1], bg[2]
    if (
        abs(r - br) <= bg_tol
        and abs(g - bgc) <= bg_tol
        and abs(b - bb) <= bg_tol
    ):
        return True
    if black_max < 0:
        return False
    return r <= black_max and g <= black_max and b <= black_max


def _himawari_trim_uniform_border(
    im,
    *,
    bg: tuple[int, int, int] = (32, 36, 42),
    tol: int = 10,
    black_max: int = 12,
):
    """
    モザイク外周の「欠損タイル用の均一背景」およびタイル内の近黒レターボックス行・列を切り落とす。
    海・雲は通常その閾値より明るいため残る（赤外で縁の海面が極端に暗いときは margin_black_max を下げる）。
    """
    im = im.convert("RGB")
    w, h = im.size
    if w < 8 or h < 8:
        return im
    px = im.load()

    def is_margin_pixel(x: int, y: int) -> bool:
        return _himawari_is_shell_margin_rgb(
            px[x, y], bg=bg, bg_tol=tol, black_max=black_max
        )

    t = 0
    while t < h:
        if not all(is_margin_pixel(x, t) for x in range(w)):
            break
        t += 1
    b = h - 1
    while b >= t:
        if not all(is_margin_pixel(x, b) for x in range(w)):
            break
        b -= 1
    l = 0
    while l < w:
        if not all(is_margin_pixel(l, y) for y in range(t, b + 1)):
            break
        l += 1
    r = w - 1
    while r >= l:
        if not all(is_margin_pixel(r, y) for y in range(t, b + 1)):
            break
        r -= 1
    if r < l or b < t:
        return im
    cw, ch = r - l + 1, b - t + 1
    if cw < 32 or ch < 32:
        return im
    return im.crop((l, t, r + 1, b + 1))


def _himawari_crop_canvas_to_filled_tiles(
    canvas,
    xs: list[int],
    ys: list[int],
    tile_map: dict[tuple[int, int], bytes | None],
    tw: int,
    th: int,
):
    """
    取得に成功したタイルが占める格子の最小外接矩形に切り出す（欠損のみの行・列を落とし、外周をまっすぐにする）。
    Pillow の crop は (left, upper, right, lower) で right/lower は排他的。
    """
    nx = len(xs)
    ny = len(ys)
    min_i, max_i = nx, -1
    min_j, max_j = ny, -1
    for j in range(ny):
        ty = ys[j]
        for i in range(nx):
            tx = xs[i]
            if tile_map.get((tx, ty)):
                min_i = min(min_i, i)
                max_i = max(max_i, i)
                min_j = min(min_j, j)
                max_j = max(max_j, j)
    if max_i < min_i or max_j < min_j:
        return canvas
    left = min_i * tw
    upper = min_j * th
    right = (max_i + 1) * tw
    lower = (max_j + 1) * th
    if right <= left or lower <= upper:
        return canvas
    return canvas.crop((left, upper, right, lower))


def _himawari_tight_crop_non_background(
    im,
    *,
    bg: tuple[int, int, int] = (32, 36, 42),
    tol: int = 16,
    black_max: int = 12,
):
    """
    モザイク外周を画素単位で切り落とす。
    欠損タイル色 (32,36,42) に加え、タイル JPEG のレターボックス（純黒〜近黒）もマージンとして扱う。
    black_max < 0 のときは欠損色のみ（従来互換）。
    """
    from PIL import ImageChops, ImageOps

    rgb = im.convert("RGB")
    w, h = rgb.size
    if w < 8 or h < 8:
        return rgb
    r, g, b = rgb.split()
    br, bgc, bb = bg
    mr = r.point(lambda x, br=br, tol=tol: 255 if abs(x - br) <= tol else 0)
    mg = g.point(lambda x, bgc=bgc, tol=tol: 255 if abs(x - bgc) <= tol else 0)
    mb = b.point(lambda x, bb=bb, tol=tol: 255 if abs(x - bb) <= tol else 0)
    margin_bg = ImageChops.darker(ImageChops.darker(mr, mg), mb)
    if black_max >= 0:
        bm = max(0, min(40, int(black_max)))
        mbr = r.point(lambda x, bm=bm: 255 if x <= bm else 0)
        mbg = g.point(lambda x, bm=bm: 255 if x <= bm else 0)
        mbb = b.point(lambda x, bm=bm: 255 if x <= bm else 0)
        margin_blk = ImageChops.darker(ImageChops.darker(mbr, mbg), mbb)
        margin = ImageChops.lighter(margin_bg, margin_blk)
    else:
        margin = margin_bg
    m = ImageOps.invert(margin)
    bbox = m.getbbox()
    if not bbox:
        return rgb
    l, t, r2, b2 = bbox
    l = max(0, l - 1)
    t = max(0, t - 1)
    r2 = min(w, r2 + 1)
    b2 = min(h, b2 + 1)
    if r2 - l < 48 or b2 - t < 48:
        return rgb
    return rgb.crop((l, t, r2, b2))


def build_himawari_jp_mosaic_jpeg_bytes(opts: dict) -> bytes:
    """
    緯度経度の矩形範囲を Web Mercator（XYZ）タイルで覆い、最大ズームは satimg 実装上限（z=6）まで。

    描画範囲は lon_w〜lon_e / lat_s〜lat_n。config の bosai_himawari_like_map で統合地図 #z/lat/lon に合わせて自動算出も可。
    """
    from PIL import Image

    _mp = getattr(Image, "MAX_IMAGE_PIXELS", 0) or 178_956_970
    Image.MAX_IMAGE_PIXELS = max(_mp, 500_000_000)

    band = str(opts.get("band") or "")
    bt = str(opts.get("basetime") or "")
    vt = str(opts.get("validtime") or bt)
    prod_band = str(opts.get("prod_band") or "")
    prod_name = str(opts.get("prod_name") or "")
    if not prod_band or not prod_name:
        pb, pn, _ = _himawari_jp_band_products(band)
        prod_band, prod_name = pb, pn

    lon_w = float(opts.get("lon_w", 117.29))
    lon_e = float(opts.get("lon_e", 173.54))
    lat_s = float(opts.get("lat_s", 21.98))
    lat_n = float(opts.get("lat_n", 50.43))
    if lon_e <= lon_w or lat_n <= lat_s:
        raise ValueError("ひまわりモザイク: 緯度経度の範囲が不正です")

    max_tiles = int(opts.get("max_tiles", 400))
    max_tiles = max(16, min(900, max_tiles))

    z_req = int(opts.get("z_fetch", HIMI_JP_TILE_MAX_ZOOM))
    z_req = max(3, min(HIMI_JP_TILE_MAX_ZOOM, z_req))

    z = z_req
    x_min = x_max = y_min = y_max = 0
    ntot = 0
    while z >= 3:
        x_min, x_max, y_min, y_max = _jma_tile_range_lonlat(
            z, lon_w, lon_e, lat_s, lat_n
        )
        ntot = (x_max - x_min + 1) * (y_max - y_min + 1)
        if ntot <= max_tiles:
            break
        z -= 1
    if z < 3 or ntot > max_tiles:
        raise ValueError(
            "ひまわりモザイク: タイル枚数が多すぎます（範囲を狭めるか max_tiles を上げてください）"
        )

    xs = list(range(x_min, x_max + 1))
    ys = list(range(y_min, y_max + 1))
    coords: list[tuple[int, int]] = [(tx, ty) for ty in ys for tx in xs]

    def load_one(xy: tuple[int, int]) -> tuple[tuple[int, int], bytes | None]:
        tx, ty = xy
        blob = _himawari_jp_fetch_tile_jpeg(
            bt, vt, prod_band, prod_name, z, tx, ty
        )
        return (xy, blob)

    tile_map: dict[tuple[int, int], bytes | None] = {}
    n_workers = min(8, max(4, len(coords) // 6 + 1))
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = [ex.submit(load_one, xy) for xy in coords]
        for fut in concurrent.futures.as_completed(futs):
            xy, blob = fut.result()
            tile_map[xy] = blob

    failed = [xy for xy in coords if not tile_map.get(xy)]
    if failed:
        time.sleep(0.25)
        for xy in failed:
            blob = _himawari_jp_fetch_tile_jpeg(
                bt, vt, prod_band, prod_name, z, xy[0], xy[1]
            )
            if blob:
                tile_map[xy] = blob
    failed2 = [xy for xy in coords if not tile_map.get(xy)]
    for xy in failed2:
        time.sleep(0.12)
        blob = _himawari_jp_fetch_tile_jpeg(
            bt, vt, prod_band, prod_name, z, xy[0], xy[1]
        )
        if blob:
            tile_map[xy] = blob

    first: bytes | None = None
    for xy in coords:
        b = tile_map.get(xy)
        if b:
            first = b
            break
    if not first:
        raise ValueError(
            "ひまわりモザイク: 取得できたタイルがありません（ズームや時刻、通信を確認してください）"
        )

    im0 = Image.open(io.BytesIO(first))
    tw, th = im0.size
    tw = max(1, tw)
    th = max(1, th)
    out_w, out_h = tw * len(xs), th * len(ys)
    canvas = Image.new("RGB", (out_w, out_h), (32, 36, 42))

    for j, ty in enumerate(ys):
        for i, tx in enumerate(xs):
            blob = tile_map.get((tx, ty))
            ox, oy = i * tw, j * th
            if not blob:
                continue
            try:
                im = Image.open(io.BytesIO(blob)).convert("RGB")
                if im.size != (tw, th):
                    im = im.resize((tw, th), Image.Resampling.LANCZOS)
                canvas.paste(im, (ox, oy))
            except Exception:
                continue

    if bool(opts.get("crop_mosaic_to_filled_tiles", True)):
        canvas = _himawari_crop_canvas_to_filled_tiles(
            canvas, xs, ys, tile_map, tw, th
        )
    margin_black_max = int(opts.get("margin_black_max", 12))
    margin_black_max = max(-1, min(40, margin_black_max))
    trim_tol = int(opts.get("trim_mosaic_border_tol", 10))
    trim_tol = max(4, min(24, trim_tol))
    if bool(opts.get("trim_mosaic_border", True)):
        canvas = _himawari_trim_uniform_border(
            canvas, tol=trim_tol, black_max=margin_black_max
        )
    if bool(opts.get("tight_crop_mosaic", True)):
        tol = int(opts.get("tight_crop_mosaic_tol", 16))
        tol = max(6, min(40, tol))
        canvas = _himawari_tight_crop_non_background(
            canvas, tol=tol, black_max=margin_black_max
        )

    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=96, subsampling=0, optimize=True)
    return buf.getvalue()


def _himawari_map_screenshot_page_url(band: str, msc: dict, row: dict) -> str:
    """bosai_himawari_map_screenshot または products[].map_screenshot_url から取得 URL を決める。"""
    u = str(row.get("map_screenshot_url") or "").strip()
    if u:
        return u
    snap = msc.get("bosai_himawari_map_screenshot")
    if not isinstance(snap, dict):
        return ""
    bnorm = re.sub(r"[^a-z0-9]", "", str(band).lower())
    if bnorm in ("tre", "color", "rep", "truecolor", "vis", "rgb", "etc"):
        return str(snap.get("url_visible") or snap.get("url_color") or "").strip()
    if bnorm in ("b13", "ir", "infrared", "tbb"):
        return str(snap.get("url_infrared") or snap.get("url_ir") or "").strip()
    return ""


def _himawari_map_screenshot_element_png(
    page,
    *,
    clip_selector: str,
    fallback_selectors: list[str],
    goto_timeout: int,
) -> bytes:
    """
    地図パネル（Leaflet 等）だけを撮影。セレクタが無いときはビューポート全体。
    """
    to = min(120_000, max(5000, int(goto_timeout)))

    def _try_locator(locator) -> bytes | None:
        try:
            locator.wait_for(state="visible", timeout=min(12_000, to))
        except Exception:
            return None
        try:
            box = locator.bounding_box()
        except Exception:
            return None
        if not box or box["width"] < 180 or box["height"] < 180:
            return None
        try:
            return locator.screenshot(type="png")
        except Exception:
            return None

    if clip_selector:
        blob = _try_locator(page.locator(clip_selector).first)
        if blob:
            return blob
    for sel in fallback_selectors:
        blob = _try_locator(page.locator(sel).first)
        if blob:
            return blob
    return page.screenshot(type="png", full_page=False)


def _himawari_bosai_map_read_observation_time(page) -> datetime | None:
    """
    統合地図ページ上の「YYYY年M月D日H時mm分[ss秒]」（日本時間）を DOM から推定する。
    地図凡例と一致しやすいよう、「秒」付きの表記を優先し、同一テキスト内では末尾に近い一致を採用する。
    """
    pat = re.compile(
        r"(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2})時(\d{1,2})分(?:(\d{1,2})秒)?"
    )
    chunks: list[str] = []
    for sel in (".leaflet-container", "#contents-inner", "body"):
        try:
            t = page.locator(sel).first.inner_text(timeout=6000)
            if t:
                chunks.append(t)
        except Exception:
            continue
    try:
        extra = page.evaluate(
            """() => {
              const s = [];
              for (const e of document.querySelectorAll(
                  '[title],[aria-label],[data-time],[data-observation-time]'
              )) {
                s.push(
                  e.getAttribute('title') || '',
                  e.getAttribute('aria-label') || '',
                  e.getAttribute('data-time') || '',
                  e.getAttribute('data-observation-time') || ''
                );
              }
              return s.join('\\n');
            }"""
        )
        if isinstance(extra, str) and extra.strip():
            chunks.append(extra)
    except Exception:
        pass
    combined = "\n".join(chunks)
    hits = list(pat.finditer(combined))
    if not hits:
        return None
    best_m = hits[0]
    best_key: tuple[int, int] = (-1, -1)
    for m in hits:
        has_sec = m.group(6) is not None
        k = (2 if has_sec else 1, m.end())
        if k > best_key:
            best_key = k
            best_m = m
    y = int(best_m.group(1))
    mo = int(best_m.group(2))
    d = int(best_m.group(3))
    h = int(best_m.group(4))
    mi = int(best_m.group(5))
    sec = int(best_m.group(6) or 0)
    try:
        jst = datetime(y, mo, d, h, mi, sec, tzinfo=JST)
        return jst.astimezone(UTC)
    except ValueError:
        return None


def _himawari_msc_header_lines_for_obs_utc(obs_utc: datetime) -> list[str]:
    """MSC 衛星キャプション2行（日本時間表記＋UTC）。"""
    u = obs_utc.astimezone(UTC) if obs_utc.tzinfo else obs_utc.replace(tzinfo=UTC)
    jst = u.astimezone(JST)
    utc_main = u.strftime("%Y-%m-%d %H:%M")
    return [
        (
            f"観測（日本時間）{jst.year}年{jst.month}月{jst.day}日"
            f"{jst.hour}時{jst.minute:02d}分"
        ),
        f"UTC {utc_main}",
    ]


def _ensure_playwright_chromium_runtime() -> None:
    """
    ``pip install playwright`` だけではブラウザバイナリが無い環境（Streamlit Community Cloud 等）向けに、
    初回起動時に ``python -m playwright install chromium`` を実行する。

    自動実行を禁止する場合: 環境変数 ``WX_BRIEFING_PLAYWRIGHT_NO_AUTO_INSTALL=1``
    （その場合は手元またはビルドで chromium をインストール済みにすること）。
    """
    global _playwright_chromium_ready
    if _playwright_chromium_ready:
        return
    no_auto = os.environ.get("WX_BRIEFING_PLAYWRIGHT_NO_AUTO_INSTALL", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    from playwright.sync_api import sync_playwright

    with _playwright_chromium_lock:
        if _playwright_chromium_ready:
            return

        def _try_launch() -> bool:
            try:
                with sync_playwright() as p:
                    b = p.chromium.launch(headless=True)
                    b.close()
                return True
            except Exception:
                return False

        if _try_launch():
            _playwright_chromium_ready = True
            return
        if no_auto:
            raise ValueError(
                "Playwright の Chromium が未配置です。環境変数 "
                "WX_BRIEFING_PLAYWRIGHT_NO_AUTO_INSTALL=1 が付いているため自動インストールをスキップしました。"
                " `python -m playwright install chromium` を実行するか、当該環境変数を外してください。"
            )
        proc = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            tail = (proc.stderr or "")[-2000:]
            raise ValueError(
                "Playwright: `python -m playwright install chromium` が失敗しました。"
                " リポジトリ直下の packages.txt に Chromium 用のシステムパッケージがあるか確認し、再デプロイしてください。"
                f" stderr末尾: {tail!r}"
            )
        if not _try_launch():
            raise ValueError(
                "Playwright の Chromium を配置しましたが起動に失敗しました。"
                " OS の依存ライブラリ不足の可能性があります。packages.txt を見直すか、"
                " config の bosai_himawari_map_screenshot.enabled を false にしてタイルモザイクへ戻してください。"
            )
        _playwright_chromium_ready = True


def build_himawari_bosai_map_screenshot_jpeg_bytes(
    opts: dict,
) -> tuple[bytes, datetime | None]:
    """
    防災統合地図（map.html）を headless Chromium で開き、**地図パネル（Leaflet）中心**の JPEG を返す。

    戻り値の datetime はページ DOM から読んだ観測時刻（日本時間表記の解釈結果・UTC  aware）。
    取得できたときは fetch 側でキャプションとファイル名をこれに合わせる。

    既定では広告 iframe を非表示にし、``.leaflet-container`` 系セレクタで地図 DOM だけを切り出す。
    ``device_scale_factor``（既定 2）でピクセル密度を上げ、地図部分の解像感を高める。

    初回は Chromium バイナリの自動ダウンロードを試みる（Streamlit Cloud 向け）。
    """
    page_url = str(opts.get("page_url") or "").strip()
    if not page_url.startswith("https://www.jma.go.jp/bosai/map.html"):
        raise ValueError(
            "統合地図スクショ: page_url は https://www.jma.go.jp/bosai/map.html で始まる必要があります"
        )
    wait_ms = int(opts.get("wait_ms", 12000))
    wait_ms = max(2000, min(180_000, wait_ms))
    vw = int(opts.get("viewport_width", 1440))
    vh = int(opts.get("viewport_height", 900))
    vw = max(640, min(2560, vw))
    vh = max(480, min(1600, vh))
    goto_timeout = int(opts.get("goto_timeout_ms", 120_000))
    goto_timeout = max(30_000, min(300_000, goto_timeout))
    clip_selector = str(opts.get("clip_selector") or "").strip()
    raw_sel = opts.get("map_clip_selectors")
    if isinstance(raw_sel, list) and raw_sel:
        fallback_selectors = [str(x).strip() for x in raw_sel if str(x).strip()]
    else:
        fallback_selectors = list(_HIMI_MAP_SCREENSHOT_DEFAULT_SELECTORS)
    dsf = float(opts.get("device_scale_factor", 2))
    dsf = max(1.0, min(3.0, dsf))
    hide_ads = bool(opts.get("hide_ad_overlays", True))
    use_dom_time = bool(opts.get("use_dom_observation_time", True))

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ValueError(
            "統合地図スクショには Playwright が必要です: "
            "pip install playwright および playwright install chromium を実行してください。"
        ) from e

    _ensure_playwright_chromium_runtime()

    from PIL import Image

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                viewport={"width": vw, "height": vh},
                user_agent=USER_AGENT,
                locale="ja-JP",
                device_scale_factor=dsf,
            )
            page = context.new_page()
            page.goto(
                page_url,
                wait_until="domcontentloaded",
                timeout=goto_timeout,
            )
            page.wait_for_timeout(wait_ms)
            if hide_ads:
                try:
                    page.evaluate(_HIMI_MAP_SCREENSHOT_HIDE_ADS_JS)
                except Exception:
                    pass
                page.wait_for_timeout(600)
            obs_dom: datetime | None = None
            if use_dom_time:
                try:
                    obs_dom = _himawari_bosai_map_read_observation_time(page)
                except Exception:
                    obs_dom = None
            png = _himawari_map_screenshot_element_png(
                page,
                clip_selector=clip_selector,
                fallback_selectors=fallback_selectors,
                goto_timeout=goto_timeout,
            )
            context.close()
        finally:
            browser.close()

    im = Image.open(io.BytesIO(png)).convert("RGB")
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=93, subsampling=0, optimize=True)
    return buf.getvalue(), obs_dom


def msc_himawari_japan_jpg_url_for_ref_utc(
    band: str,
    ref_utc: datetime | None = None,
    opts: dict | None = None,
) -> tuple[str, str, str, datetime]:
    """
    気象庁防災「統合地図」のひまわり日本域に相当するタイル JPEG（satimg API）の URL。

    ブラウザの可視（elem=color）相当は REP/ETC、赤外（elem=ir）は B13/TBB。
    map.html は SPA のため直接は取得せず、targetTimes_jp.json とタイルパスで合成する。
    """
    opts = opts or {}
    prod_band, prod_name, _b = _himawari_jp_band_products(band)
    bt, vt, slot_utc = _himawari_jp_select_slot(ref_utc)

    z = int(opts.get("bosai_map_zoom", opts.get("bosai_zoom", 4)))
    lat = float(opts.get("bosai_map_lat", opts.get("bosai_lat", 37.545)))
    lon = float(opts.get("bosai_map_lon", opts.get("bosai_lon", 145.415)))
    z = max(0, min(HIMI_JP_TILE_MAX_ZOOM, z))
    xtile, ytile = _deg2num(lat, lon, z)

    url = _himawari_jp_satimg_tile_url(bt, vt, prod_band, prod_name, z, xtile, ytile)
    hhmm = slot_utc.strftime("%H%M")
    slot_iso = slot_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    return url, hhmm, slot_iso, slot_utc


def nowc_latest_hrpns_basetime_validtime() -> tuple[str, str]:
    raw, _ = fetch_url(JMA_NOWC_TARGET_N1, timeout=35)
    arr = json.loads(raw.decode("utf-8"))
    best_key: tuple[str, str] | None = None
    best_bt: str | None = None
    best_vt: str | None = None
    for item in arr:
        if not isinstance(item, dict):
            continue
        els = item.get("elements") or []
        if "hrpns" not in els:
            continue
        bt = item.get("basetime")
        vt = item.get("validtime")
        if not isinstance(bt, str) or not isinstance(vt, str):
            continue
        key = (bt, vt)
        if best_key is None or key > best_key:
            best_key = key
            best_bt, best_vt = bt, vt
    if not best_bt or not best_vt:
        raise ValueError("targetTimes_N1.json に hrpns の時刻がありません")
    return best_bt, best_vt


def _nowc_hrpns_tile_url(basetime: str, validtime: str, z: int, x: int, y: int) -> str:
    return (
        f"{JMA_NOWC_TILE_ROOT}/{basetime}/none/{validtime}/surf/hrpns/{z}/{x}/{y}.png"
    )


def _gsi_pale_tile_url(z: int, x: int, y: int) -> str:
    return f"{JMA_GSI_PALE_TILE}/{z}/{x}/{y}.png"


def _pil_open_rgba(data: bytes):
    from PIL import Image

    im = Image.open(io.BytesIO(data))
    if im.mode == "RGBA":
        return im
    if im.mode == "RGB":
        return im.convert("RGBA")
    return im.convert("RGBA")


def _try_pil_truetype(path: str, px: int):
    """
    Pillow で TrueType / TTC を開く。
    NotoSansCJK*.ttc は言語面が複数あるため index を試し、日本語キャプションが描ける面だけ採用する。
    """
    from PIL import ImageFont

    if not path or not os.path.isfile(path):
        return None
    lower = path.lower()
    base = os.path.basename(lower)
    is_cjk_ttc = lower.endswith(".ttc") and "notosanscjk" in base and "jp" not in base
    if lower.endswith(".ttc"):
        indices = list(range(18)) if is_cjk_ttc else list(range(10))
    else:
        indices = [0]
    for idx in indices:
        try:
            fnt = ImageFont.truetype(path, px, index=idx)
        except OSError:
            continue
        except Exception:
            continue
        if is_cjk_ttc:
            try:
                bbox = fnt.getbbox("観測（日本時間）")
                w = bbox[2] - bbox[0]
                h = bbox[3] - bbox[1]
                if w <= 1 or h <= 1:
                    continue
            except Exception:
                continue
        return fnt
    return None


def _linux_font_paths_fc_match() -> list[str]:
    """fontconfig が返す日本向きフォント（Streamlit Cloud / Debian）。"""
    if sys.platform == "win32":
        return []
    fc = shutil.which("fc-match")
    if not fc:
        return []
    bad_sub = ("dejavu", "liberation", "bitstream", "ubuntu", "arial", "verdana")
    patterns = (
        "Noto Sans CJK JP:style=Regular",
        "Noto Sans CJK JP",
        "Noto Sans JP:style=Regular",
        "Noto Sans JP",
        "sans-serif:lang=ja",
    )
    out: list[str] = []
    for pat in patterns:
        try:
            r = subprocess.run(
                [fc, "-f", "%{file}", pat],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            line = (r.stdout or "").strip().splitlines()
            p = line[0].strip() if line else ""
            if not p or not os.path.isfile(p):
                continue
            pl = p.lower()
            if any(s in pl for s in bad_sub):
                continue
            if p not in out:
                out.append(p)
        except Exception:
            continue
    return out


def _hrpns_caption_font(px: int):
    """
    衛星・ナウキャスト等の画像キャプション用（Streamlit でも同じ経路）。

    優先順: 環境変数 WX_BRIEFING_CAPTION_FONT → リポジトリ内 fonts/*.otf|ttf|ttc
    → Windows 標準 → Linux（fonts-noto-cjk 等）→ macOS。無いと日本語が豆腐になる。
    """
    from PIL import ImageFont

    candidates: list[str] = []
    env_p = (os.environ.get("WX_BRIEFING_CAPTION_FONT") or "").strip()
    if env_p:
        candidates.append(env_p)

    fonts_dir = Path(__file__).resolve().parent / "fonts"
    if fonts_dir.is_dir():
        for ext in (".otf", ".ttf", ".ttc"):
            for f in sorted(fonts_dir.glob(f"*{ext}")):
                candidates.append(str(f))

    candidates.extend(_linux_font_paths_fc_match())

    windir = os.environ.get("WINDIR", r"C:\Windows")
    for name in (
        "meiryo.ttc",
        "YuGothM.ttc",
        "YuGothR.ttc",
        "msgothic.ttc",
        "msyh.ttc",
        "BIZ-UDGothicR.ttc",
        "BIZ-UDGothicB.ttc",
    ):
        candidates.append(os.path.join(windir, "Fonts", name))
    # Debian/Ubuntu: 日本語単体 OTF を CJK 集合 TTC より先に（TTC は言語面の取り違えで欠字しやすい）。
    candidates.extend(
        (
            "/usr/share/fonts/opentype/noto/NotoSansJP-Regular.otf",
            "/usr/share/fonts/truetype/noto/NotoSansJP-Regular.otf",
            "/usr/share/fonts/truetype/noto/NotoSansJP-Regular.ttf",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        )
    )
    # macOS（パスは ASCII のみ。日本語名フォントは環境差が大きいので省略）
    candidates.extend(
        (
            "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
            "/Library/Fonts/Arial Unicode.ttf",
        )
    )
    seen: set[str] = set()
    for p in candidates:
        if not p or p in seen:
            continue
        seen.add(p)
        font = _try_pil_truetype(p, px)
        if font is not None:
            return font
    return ImageFont.load_default()


def _msc_postprocess_jpg(
    jpeg_data: bytes,
    lines: list[str],
    *,
    crop_right: int,
    crop_bottom: int,
) -> bytes:
    """
    MSC 衛星 JPG: 右下の Himawari 帯表記をクロップで除去し、上端に観測時刻（JST/UTC）の帯を付与する。
    帯内の文字は textbbox で幅に収まるまでフォントを縮小する。
    """
    from PIL import Image, ImageDraw

    _mp = getattr(Image, "MAX_IMAGE_PIXELS", 0) or 178_956_970
    Image.MAX_IMAGE_PIXELS = max(_mp, 500_000_000)

    im = Image.open(io.BytesIO(jpeg_data)).convert("RGB")
    w, h = im.size
    cr = max(0, min(int(crop_right), w // 3))
    cb = max(0, min(int(crop_bottom), h // 4))
    if cr > 0 or cb > 0:
        im = im.crop((0, 0, max(1, w - cr), max(1, h - cb)))
    w, h = im.size
    lines = [ln for ln in lines if str(ln).strip()]
    if not lines:
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=96, subsampling=0, optimize=True)
        return buf.getvalue()

    pad_x = max(10, w // 200)
    max_w = max(40, w - 2 * pad_x)
    probe = Image.new("RGB", (w, 4), (255, 255, 255))
    draw_p = ImageDraw.Draw(probe)
    fs_hi = min(42, max(20, w // 48))
    fs = 14
    for cand in range(fs_hi, 8, -1):
        font = _hrpns_caption_font(cand)
        ok = True
        for line in lines:
            bbox = draw_p.textbbox((0, 0), line, font=font)
            tw = bbox[2] - bbox[0]
            if tw > max_w:
                ok = False
                break
        if ok:
            fs = cand
            break
    else:
        fs = 10
    font = _hrpns_caption_font(fs)
    line_h = int(fs * 1.48)
    bar = line_h * len(lines) + max(22, w // 120)
    out = Image.new("RGB", (w, h + bar), (245, 247, 250))
    out.paste(im, (0, bar))
    draw = ImageDraw.Draw(out)
    y0 = 6
    for i, line in enumerate(lines):
        draw.text((pad_x, y0 + i * line_h), line, fill=(10, 14, 22), font=font)
    buf = io.BytesIO()
    out.save(buf, format="JPEG", quality=96, subsampling=0, optimize=True)
    return buf.getvalue()


def _hrpns_mosaic_with_caption(
    canvas_rgb, bt: str, vt: str
):  # returns PIL Image RGB
    from PIL import Image, ImageDraw

    bt_u = _parse_jma_nowc_ts14_utc(bt)
    vt_u = _parse_jma_nowc_ts14_utc(vt)
    if bt == vt:
        lines = [
            "降水ナウキャスト hrpns（解析＝表示時刻）",
            f"{bt_u.astimezone(JST):%Y-%m-%d %H:%M} JST ／  {bt_u:%Y-%m-%d %H:%M} UTC",
        ]
    else:
        lines = [
            "降水ナウキャスト hrpns",
            f"解析 {bt_u.astimezone(JST):%Y-%m-%d %H:%M} JST ／  {bt_u:%Y-%m-%d %H:%M} UTC",
            f"表示 {vt_u.astimezone(JST):%Y-%m-%d %H:%M} JST ／  {vt_u:%Y-%m-%d %H:%M} UTC",
        ]

    w, h = canvas_rgb.size
    fs = max(16, min(28, w // 48))
    line_h = int(fs * 1.35)
    cap_h = line_h * len(lines) + 28
    out_im = Image.new("RGB", (w, h + cap_h), (248, 248, 250))
    out_im.paste(canvas_rgb, (0, 0))
    draw = ImageDraw.Draw(out_im)
    font = _hrpns_caption_font(fs)
    y0 = h + 10
    for i, line in enumerate(lines):
        draw.text((12, y0 + i * line_h), line, fill=(20, 24, 32), font=font)
    return out_im


def build_hrpns_mosaic_png_bytes(opts: dict) -> bytes:
    """
    降水ナウキャスト（hrpns）タイルを日本域付近で取得し、1枚の PNG に結合する。
    opts: zoom, basemap, lon_w, lon_e, lat_s, lat_n、hrpns_basetime, hrpns_validtime（いずれも省略可）
    """
    from PIL import Image

    z = int(opts.get("zoom") if opts.get("zoom") is not None else 6)
    z = max(4, min(8, z))
    basemap = bool(opts.get("basemap", True))
    lon_w = float(opts.get("lon_w", 122.0))
    lon_e = float(opts.get("lon_e", 150.0))
    lat_s = float(opts.get("lat_s", 24.0))
    lat_n = float(opts.get("lat_n", 46.0))

    bt = opts.get("hrpns_basetime")
    vt = opts.get("hrpns_validtime")
    if (
        not isinstance(bt, str)
        or not isinstance(vt, str)
        or len(bt) != 14
        or len(vt) != 14
    ):
        bt, vt = nowc_latest_hrpns_basetime_validtime()
    x_min, x_max, y_min, y_max = _jma_tile_range_lonlat(z, lon_w, lon_e, lat_s, lat_n)
    tw = x_max - x_min + 1
    th = y_max - y_min + 1
    coords: list[tuple[int, int]] = [
        (x, y) for y in range(y_min, y_max + 1) for x in range(x_min, x_max + 1)
    ]

    def load_one(xy: tuple[int, int]) -> tuple[tuple[int, int], str, bytes | None]:
        x, y = xy
        u = _nowc_hrpns_tile_url(bt, vt, z, x, y)
        try:
            data, _ = fetch_url(u, timeout=30)
            return (xy, "hrpns", data)
        except Exception:
            return (xy, "hrpns", None)

    def load_gsi(xy: tuple[int, int]) -> tuple[tuple[int, int], str, bytes | None]:
        x, y = xy
        u = _gsi_pale_tile_url(z, x, y)
        try:
            data, _ = fetch_url(u, timeout=30)
            return (xy, "gsi", data)
        except Exception:
            return (xy, "gsi", None)

    hrpns_map: dict[tuple[int, int], bytes | None] = {}
    gsi_map: dict[tuple[int, int], bytes | None] = {}
    max_workers = min(16, max(4, len(coords)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(load_one, xy) for xy in coords]
        if basemap:
            futs.extend(ex.submit(load_gsi, xy) for xy in coords)
        for fut in concurrent.futures.as_completed(futs):
            xy, kind, blob = fut.result()
            if kind == "hrpns":
                hrpns_map[xy] = blob
            else:
                gsi_map[xy] = blob

    tile_w, tile_h = 256, 256
    out_w, out_h = tw * tile_w, th * tile_h
    canvas = Image.new("RGB", (out_w, out_h), (255, 255, 255))

    for j, ty in enumerate(range(y_min, y_max + 1)):
        for i, tx in enumerate(range(x_min, x_max + 1)):
            ox, oy = i * tile_w, j * tile_h
            xy = (tx, ty)
            if basemap:
                gb = gsi_map.get(xy)
                if gb:
                    try:
                        gim = _pil_open_rgba(gb).convert("RGB")
                        canvas.paste(gim, (ox, oy))
                    except Exception:
                        pass
            hb = hrpns_map.get(xy)
            if hb:
                try:
                    him = _pil_open_rgba(hb)
                    canvas.paste(him.convert("RGB"), (ox, oy), him.split()[-1])
                except Exception:
                    pass

    try:
        canvas = _hrpns_mosaic_with_caption(canvas, str(bt), str(vt))
    except Exception:
        pass

    buf = io.BytesIO()
    canvas.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def fetch_item_bytes(item: dict, timeout: int = 90) -> tuple[bytes, str | None]:
    """通常 URL 取得、または wxbriefing 内部生成（モザイク PNG 等）。"""
    url = item.get("url")
    if isinstance(url, str) and url.startswith(WXBRIEFING_HRPNS_MOSAIC):
        raw = build_hrpns_mosaic_png_bytes(item.get("hrpns_mosaic") or {})
        return raw, "image/png"
    if isinstance(url, str) and url.startswith(WXBRIEFING_AIRINFO_TAF_MERGED):
        raw = build_airinfo_taf_merged_pdf_bytes(item, timeout=timeout)
        return raw, "application/pdf"
    if not url:
        raise ValueError("URL がありません")

    if isinstance(url, str) and url.startswith(WXBRIEFING_HIMI_JP_MOSAIC):
        data, ctype = (
            build_himawari_jp_mosaic_jpeg_bytes(item.get("himawari_mosaic") or {}),
            "image/jpeg",
        )
    elif isinstance(url, str) and url.startswith(WXBRIEFING_HIMI_JP_MAP_SCREENSHOT):
        snap = item.get("himawari_map_screenshot")
        if not isinstance(snap, dict) or not str(snap.get("page_url") or "").strip():
            raise ValueError(
                "統合地図スクショ: item に himawari_map_screenshot.page_url がありません"
            )
        raw_jpg, obs_dom = build_himawari_bosai_map_screenshot_jpeg_bytes(snap)
        data, ctype = raw_jpg, "image/jpeg"
        if isinstance(obs_dom, datetime) and obs_dom.tzinfo is not None:
            item["msc_header_lines"] = _himawari_msc_header_lines_for_obs_utc(obs_dom)
            fn0 = str(item.get("filename") or "")
            fn1 = re.sub(
                r"_(\d{8})_(\d{4})JST(?=\.[A-Za-z0-9]+$)",
                f"_{obs_dom.astimezone(JST):%Y%m%d_%H%M}JST",
                fn0,
                count=1,
            )
            if fn1 != fn0:
                item["filename"] = fn1
    else:
        data, ctype = fetch_url(str(url), timeout=timeout)

    if item.get("msc_jst_header") and isinstance(data, bytes) and len(data) > 64:
        ctlow = (ctype or "").lower()
        if "jpeg" in ctlow or "jpg" in ctlow:
            raw_lines = item.get("msc_header_lines")
            if isinstance(raw_lines, list) and raw_lines:
                cap_lines = [str(x).strip() for x in raw_lines if str(x).strip()]
            else:
                cap = str(item.get("msc_jst_header_line") or "").strip()
                cap_lines = [cap] if cap else []
            if cap_lines:
                try:
                    data = _msc_postprocess_jpg(
                        data,
                        cap_lines,
                        crop_right=int(item.get("msc_crop_right") or 0),
                        crop_bottom=int(item.get("msc_crop_bottom") or 0),
                    )
                    ctype = "image/jpeg"
                except Exception:
                    pass
    return data, ctype


def _merged_pdf_max_fetch_workers(cfg: dict, n_jobs: int) -> int:
    """
    結合 PDF 取得の並列度。config の merged_pdf.fetch_workers（または merged_pdf.max_fetch_workers）、
    トップレベル merged_pdf_fetch_workers のいずれか。既定 8、1〜24、件数を超えない。
    """
    raw = None
    block = cfg.get("merged_pdf")
    if isinstance(block, dict):
        raw = block.get("fetch_workers", block.get("max_fetch_workers"))
    if raw is None:
        raw = cfg.get("merged_pdf_fetch_workers")
    try:
        w = int(raw) if raw is not None else 8
    except (TypeError, ValueError):
        w = 8
    w = max(1, min(24, w))
    if n_jobs > 0:
        w = min(w, n_jobs)
    return w


def _merged_pdf_fetch_one(ix_item: tuple[int, dict]) -> tuple[int, dict, str, str, bytes | None, str | None, str | None]:
    """
    結合 PDF 用の 1 件取得。戻り値:
    (index, item, name, kind, data, ctype, message)
    kind は ok | no_url | svg | err
    """
    i, item = ix_item
    name = str(item.get("filename") or f"file_{i + 1}.bin")
    url = item.get("url")
    low = name.lower()
    if not url:
        return i, item, name, "no_url", None, None, f"{name}: URL なし"
    if low.endswith(".svg") or low.endswith(".svgz"):
        return (
            i,
            item,
            name,
            "svg",
            None,
            None,
            f"{name}: SVG は結合対象外のためスキップしました",
        )
    try:
        data, ctype = fetch_item_bytes(item)
        return i, item, name, "ok", data, ctype, None
    except urllib.error.HTTPError as e:
        return i, item, name, "err", None, None, f"{name}: HTTP {e.code}"
    except urllib.error.URLError as e:
        return i, item, name, "err", None, None, f"{name}: {e.reason}"
    except Exception as e:  # noqa: BLE001
        return i, item, name, "err", None, None, f"{name}: {e}"


def expand_download_items(
    cfg: dict,
    *,
    merged_taf_selection: dict | None = None,
    merged_sigwx_areas: list[str] | None = None,
    merged_detailed_sigwx_figs: list[str] | None = None,
) -> tuple[list[dict], list[str]]:
    """
    config の items に加え、jma_weather_map が有効なら気象庁 bosai の list.json から
    実況・予想のURLを組み立てる。warnings は ZIP とは別に HTTP ヘッダ用。

    merged_taf_selection: 結合 PDF 用。``{"icaos": ["RJSF", ...], "part1": true, "part2": true}`` のとき、
    飛行場時系列予報は該当 ICAO のみ・PART フラグを item に載せる。None で従来どおり全件・PART1+2。

    merged_sigwx_areas: 下層悪天予想図の地域コード（例: fbsn, fbos）のリスト。
        None は全件（config の products または従来の単一 area）。空リストは同ブロックを出さない。

    merged_detailed_sigwx_figs: 下層悪天予想図（詳細版）の Fig 番号（例: Fig206）のリスト。
        None は products 全件。空リストは同ブロックを出さない。
    """
    out: list[dict] = []
    warnings: list[str] = []
    taf_allow: set[str] | None = None
    taf_part1 = taf_part2 = True
    if isinstance(merged_taf_selection, dict):
        raw_icaos = merged_taf_selection.get("icaos")
        if isinstance(raw_icaos, (list, tuple, set)):
            taf_allow = {
                re.sub(r"[^A-Za-z0-9]", "", str(x)).upper()
                for x in raw_icaos
                if str(x).strip()
            }
        taf_part1 = bool(merged_taf_selection.get("part1", True))
        taf_part2 = bool(merged_taf_selection.get("part2", True))
    sigwx_allow: frozenset[str] | None = None
    if merged_sigwx_areas is not None:
        sigwx_allow = frozenset(
            re.sub(r"[^a-z0-9]", "", str(x)).lower()
            for x in merged_sigwx_areas
            if str(x).strip()
        )
    detailed_fig_allow: frozenset[str] | None = None
    if merged_detailed_sigwx_figs is not None:
        figs: set[str] = set()
        for raw in merged_detailed_sigwx_figs:
            c = detailed_sigwx_fig_canonical(str(raw))
            if c:
                figs.add(c)
        detailed_fig_allow = frozenset(figs)
    for it in cfg.get("items") or []:
        if isinstance(it, dict):
            out.append(it)

    jm = cfg.get("jma_weather_map")
    if isinstance(jm, dict) and jm.get("enabled"):
        series = jm.get("series") or "near"
        if jm.get("monochrome"):
            series = "near_monochrome"

        try:
            lst = _jma_fetch_list_cached()
        except Exception as e:  # noqa: BLE001
            warnings.append(f"気象庁 list.json 取得失敗: {e}")
        else:
            block = lst.get(series)
            if not isinstance(block, dict):
                warnings.append(f"気象庁 list.json に series={series!r} がありません")
            else:
                _expand_jma_weather_map_items(out, warnings, jm, block)

    qm = cfg.get("jma_quickmonthly_prevnight21_asas")
    if isinstance(qm, dict) and qm.get("enabled"):
        if "color" in qm:
            qm_color = bool(qm.get("color"))
        else:
            qm_color = not qm.get("monochrome")
        try:
            pdf_url, show_ymd, latest_ymd = quickmonthly_prevday_asas_21jst_pdf_url(qm_color)
        except Exception as e:  # noqa: BLE001
            warnings.append(f"昨晩21時(月別一覧基準) ASAS PDF: {e}")
        else:
            fn = qm.get("filename") or "ASAS　実況地上天気図_昨晩21時JST_アジア太平洋域_過去.pdf"
            out.append(
                {
                    "filename": fn,
                    "url": pdf_url,
                    "comment": (
                        "気象庁 過去の実況天気図（月別一覧・data/wxchart/quick/spas_latest.txt 基準）。"
                        f"一覧上の最新日 {latest_ymd} の前日 {show_ymd} のアジア太平洋域 21時(JST) PDF"
                    ),
                }
            )

    qd = cfg.get("jma_quickdaily_asas")
    if isinstance(qd, dict) and qd.get("enabled"):
        show = qd.get("show")
        if isinstance(show, str) and show.lower() == "today":
            show = None
        hour = int(qd.get("hour_jst") if qd.get("hour_jst") is not None else 3)
        if "color" in qd:
            use_color = bool(qd.get("color"))
        else:
            use_color = not qd.get("monochrome")
        try:
            pdf_url = quickdaily_asas_pdf_url(show, hour, use_color)
        except ValueError as e:
            warnings.append(f"quickdaily ASAS: {e}")
        else:
            fn = qd.get("filename") or f"ASAS　実況地上天気図_今朝{hour:02d}時JST_アジア太平洋域_実況.pdf"
            out.append(
                {
                    "filename": fn,
                    "url": pdf_url,
                    "comment": "気象庁 過去の実況天気図（1日表示）と同じ PDF（data.jma.go.jp / quickdaily.js 準拠）",
                }
            )

    fs = cfg.get("jma_fsas24_asia_pdf")
    if isinstance(fs, dict) and fs.get("enabled"):
        if "color" in fs:
            fs_color = bool(fs.get("color"))
        else:
            fs_color = not fs.get("monochrome")
        pdf_url = jma_fsas24_asia_pdf_url(color=fs_color)
        fn = fs.get("filename") or "FSAS　予想地上天気図_今夜21時JST_アジア太平洋域_24h予想.pdf"
        out.append(
            {
                "filename": fn,
                "url": pdf_url,
                "comment": (
                    "気象庁 防災天気図（アジア太平洋域・カラー/白黒）の「最新24時間予想図」と同一 PDF。"
                    "固定URLのため取得時点の最新版が入ります（bosai/weather_map の PDF リンク先）。"
                ),
            }
        )

    nm = cfg.get("jma_numericmap_upper")
    if isinstance(nm, dict) and nm.get("enabled"):
        utc_h = str(nm.get("utc_hour") or "12").strip()
        if utc_h in ("0", "00"):
            utc_h = "00"
        elif utc_h in ("12",):
            utc_h = "12"
        prows = nm.get("products")
        if not prows:
            prows = [
                {"id": "aupq35", "filename": f"AUPQ35_{utc_h}UTC.pdf"},
                {"id": "aupq78", "filename": f"AUPQ78_{utc_h}UTC.pdf"},
            ]
        for pr in prows:
            if not isinstance(pr, dict):
                continue
            pid = pr.get("id") or pr.get("product")
            fn = pr.get("filename")
            if not pid:
                warnings.append("jma_numericmap_upper: id なしの行をスキップしました")
                continue
            try:
                u = jma_numericmap_upper_pdf_url(str(pid), utc_h)
            except ValueError as e:
                warnings.append(f"jma_numericmap_upper ({pid}): {e}")
                continue
            out.append(
                {
                    "filename": fn or f"{str(pid).upper()}_{utc_h}UTC.pdf",
                    "url": u,
                    "comment": "気象庁 数値予報天気図・高層（numericmap / UPPER_PROPS）",
                }
            )

    msc = cfg.get("jma_msc_himawari_japan")
    if isinstance(msc, dict) and msc.get("enabled"):
        rows = msc.get("products")
        if not rows:
            rows = [
                {
                    "band": "tre",
                    "filename": "衛星画像可視.jpg",
                    "label": "可視（真彩色）",
                },
                {
                    "band": "b13",
                    "filename": "衛星画像赤外.jpg",
                    "label": "赤外（B13）",
                },
            ]
        for row in rows:
            if not isinstance(row, dict):
                continue
            band = row.get("band") or row.get("id")
            fn = row.get("filename")
            label = row.get("label") or f"band={band}"
            if not band:
                warnings.append("jma_msc_himawari_japan: band なしの行をスキップしました")
                continue
            him_mosaic: dict | None = None
            him_snap: dict | None = None
            try:
                ref_utc = datetime.now(UTC)
                snap = msc.get("bosai_himawari_map_screenshot")
                use_snap = isinstance(snap, dict) and bool(snap.get("enabled"))

                if use_snap:
                    page_url = _himawari_map_screenshot_page_url(str(band), msc, row)
                    if not page_url:
                        raise ValueError(
                            "bosai_himawari_map_screenshot.enabled のときは "
                            "url_visible / url_infrared、または products[].map_screenshot_url を設定してください。"
                        )
                    _bt, _vt, slot_utc = _himawari_jp_select_slot(ref_utc)
                    mcs = snap.get("map_clip_selectors")
                    him_snap = {
                        "page_url": page_url,
                        "wait_ms": int(snap.get("wait_ms", 12000)),
                        "viewport_width": int(snap.get("viewport_width", 1680)),
                        "viewport_height": int(snap.get("viewport_height", 980)),
                        "goto_timeout_ms": int(snap.get("goto_timeout_ms", 120000)),
                        "clip_selector": str(snap.get("clip_selector") or "").strip(),
                        "device_scale_factor": float(snap.get("device_scale_factor", 2)),
                        "hide_ad_overlays": bool(snap.get("hide_ad_overlays", True)),
                        "use_dom_observation_time": bool(
                            snap.get("use_dom_observation_time", True)
                        ),
                    }
                    if isinstance(mcs, list) and mcs:
                        him_snap["map_clip_selectors"] = [
                            str(x).strip() for x in mcs if str(x).strip()
                        ]
                    him_mosaic = None
                    jpg_url = WXBRIEFING_HIMI_JP_MAP_SCREENSHOT
                else:
                    max_tiles = int(msc.get("bosai_himawari_mosaic_max_tiles", 400))
                    max_tiles = max(16, min(900, max_tiles))
                    bbox = msc.get("bosai_himawari_bbox")
                    like = msc.get("bosai_himawari_like_map")
                    if isinstance(like, dict) and like.get("enabled", False):
                        z_lm = int(like.get("z", 5))
                        lat_lm = float(like.get("lat", 37.545))
                        lon_lm = float(like.get("lon", 145.415))
                        vp_w = float(like.get("vp_w", 1280))
                        vp_h = float(like.get("vp_h", 820))
                        lon_w, lon_e, lat_s, lat_n = _himawari_bbox_from_map_viewport(
                            z_lm, lat_lm, lon_lm, vp_w, vp_h
                        )
                    elif isinstance(bbox, dict):
                        lon_w = float(bbox.get("lon_w", 117.29))
                        lon_e = float(bbox.get("lon_e", 173.54))
                        lat_s = float(bbox.get("lat_s", 21.98))
                        lat_n = float(bbox.get("lat_n", 50.43))
                    else:
                        lon_w = float(msc.get("bosai_him_lon_w", 117.29))
                        lon_e = float(msc.get("bosai_him_lon_e", 173.54))
                        lat_s = float(msc.get("bosai_him_lat_s", 21.98))
                        lat_n = float(msc.get("bosai_him_lat_n", 50.43))

                    if msc.get("bosai_himawari_single_tile"):
                        jpg_url, _hhmm, _slot_iso, slot_utc = (
                            msc_himawari_japan_jpg_url_for_ref_utc(
                                str(band), ref_utc, msc
                            )
                        )
                    else:
                        bt, vt, slot_utc = _himawari_jp_select_slot(ref_utc)
                        prod_band, prod_name, _ = _himawari_jp_band_products(
                            str(band)
                        )
                        if isinstance(like, dict) and like.get("enabled", False):
                            z_lm = int(like.get("z", 5))
                            z_fetch = int(like.get("fetch_zoom", z_lm))
                        else:
                            z_fetch = int(
                                msc.get("bosai_fetch_zoom", HIMI_JP_TILE_MAX_ZOOM)
                            )
                        z_fetch = max(3, min(HIMI_JP_TILE_MAX_ZOOM, z_fetch))
                        him_mosaic = {
                            "band": str(band),
                            "basetime": bt,
                            "validtime": vt,
                            "prod_band": prod_band,
                            "prod_name": prod_name,
                            "lon_w": lon_w,
                            "lon_e": lon_e,
                            "lat_s": lat_s,
                            "lat_n": lat_n,
                            "z_fetch": z_fetch,
                            "max_tiles": max_tiles,
                            "trim_mosaic_border": bool(
                                msc.get("trim_mosaic_border", True)
                            ),
                            "crop_mosaic_to_filled_tiles": bool(
                                msc.get("crop_mosaic_to_filled_tiles", True)
                            ),
                            "tight_crop_mosaic": bool(msc.get("tight_crop_mosaic", True)),
                            "tight_crop_mosaic_tol": int(
                                msc.get("tight_crop_mosaic_tol", 16)
                            ),
                            "margin_black_max": int(msc.get("margin_black_max", 12)),
                            "trim_mosaic_border_tol": int(
                                msc.get("trim_mosaic_border_tol", 10)
                            ),
                        }
                        qd: dict[str, str] = {
                            "band": str(band),
                            "bt": bt,
                            "lw": f"{lon_w:.3f}",
                            "le": f"{lon_e:.3f}",
                            "ls": f"{lat_s:.3f}",
                            "ln": f"{lat_n:.3f}",
                            "zf": str(z_fetch),
                        }
                        if isinstance(like, dict) and like.get("enabled", False):
                            qd["lm"] = "1"
                            qd["lz"] = str(int(like.get("z", 5)))
                        q = urllib.parse.urlencode(qd)
                        jpg_url = f"{WXBRIEFING_HIMI_JP_MOSAIC}?{q}"
            except Exception as e:  # noqa: BLE001
                warnings.append(f"jma_msc_himawari_japan ({band}): {e}")
                continue
            slot_jst = slot_utc.astimezone(JST)
            utc_main = slot_utc.strftime("%Y-%m-%d %H:%M")
            crop_r = int(msc.get("crop_logo_right", 108))
            crop_b = int(msc.get("crop_logo_bottom", 44))
            header_lines = [
                (
                    f"観測（日本時間）{slot_jst.year}年{slot_jst.month}月{slot_jst.day}日"
                    f"{slot_jst.hour}時{slot_jst.minute:02d}分"
                ),
                f"UTC {utc_main}",
            ]
            dpi = float(msc.get("pdf_merge_resolution") or 600)
            dpi = min(600.0, max(96.0, dpi))
            up_edge = msc.get("upscale_long_edge")
            if up_edge is None:
                up_edge_i: int | None = 16384
            else:
                up_edge_i = int(up_edge)
                if up_edge_i <= 0:
                    up_edge_i = None
                else:
                    up_edge_i = min(16384, max(801, up_edge_i))
            up_note = f"長辺最大 {up_edge_i}px 相当まで" if up_edge_i else "長辺拡大オフ"
            stem = Path(fn).stem if fn else f"himawari_jpn_{band}"
            ext = Path(fn).suffix if fn and Path(fn).suffix else ".jpg"
            if not ext.startswith("."):
                ext = ".jpg"
            out_fn = f"{stem}_{slot_jst:%Y%m%d_%H%M}JST{ext}"
            if him_snap is not None:
                him_comment = (
                    "気象庁防災「統合地図」"
                    "[map.html](https://www.jma.go.jp/bosai/map.html) を **Playwright（Chromium）** で開き、"
                    "広告 iframe を抑えつつ **Leaflet 地図コンテナ**（`map_clip_selectors`）だけを切り出した JPEG。"
                    "`device_scale_factor` で解像度を上げられる。地図の描画待ちは `wait_ms`。"
                    "初回は Chromium バイナリを自動ダウンロードする（`packages.txt` の OS ライブラリが必要）。"
                    "手元では `playwright install chromium` でも可。`WX_BRIEFING_PLAYWRIGHT_NO_AUTO_INSTALL=1` で自動 DL を止められる。"
                    "上端の観測時刻は **ページ DOM から読み取った値**（地図凡例に近い表記）に合わせ、取得時にキャプション・ファイル名も更新する（`use_dom_observation_time`）。"
                    f"一覧上の仮時刻（targetTimes ベース）: **日本時間 {slot_jst.year}年{slot_jst.month}月{slot_jst.day}日"
                    f"{slot_jst.hour}時{slot_jst.minute:02d}分** ／ **UTC {utc_main}**（ダウンロード時は DOM が優先）。{label}。"
                    "**結合 PDF では A4 に収め印刷向け**。"
                    f"印刷 dpi={float(msc.get('print_dpi', 300)):g}、余白約 {float(msc.get('print_margin_mm', 5)):g}mm。"
                    f"拡大（結合前）: {up_note}。"
                )
            else:
                him_comment = (
                    "気象庁防災「統合地図」ひまわり日本域に相当するタイル画像（"
                    "https://www.jma.go.jp/bosai/himawari/data/satimg/ ）。"
                    "ブラウザ表示の可視は https://www.jma.go.jp/bosai/map.html#5/37.545/145.415/&elem=color&contents=himawari 、"
                    "赤外は https://www.jma.go.jp/bosai/map.html#5/37.545/145.415/&elem=ir&contents=himawari と同系のデータ。"
                    "モザイクは bosai_himawari_like_map（統合地図 #5/37.545/145.415 相当の画角）または bosai_himawari_bbox を Web Mercator タイルで結合。"
                    "取得できたタイルの外周だけを長方形に切り出し（crop_mosaic_to_filled_tiles）、余白は trim_mosaic_border で削る。"
                    "satimg は z=6 まで（それ以上は 404）。単タイルのみは bosai_himawari_single_tile: true。"
                    "右下ロゴ等は控えめにトリミング（crop_logo_* で調整）。"
                    f"観測 **日本時間 {slot_jst.year}年{slot_jst.month}月{slot_jst.day}日"
                    f"{slot_jst.hour}時{slot_jst.minute:02d}分** ／ **UTC {utc_main}**。"
                    f"{label}。上端の帯は2行（日本時間／UTC）、幅に収まるよう字サイズを自動調整。"
                    "**結合 PDF では A4（縦横は画像比で自動）1ページに全体を収め、余白付きで印刷向け**。"
                    f"印刷 dpi={float(msc.get('print_dpi', 300)):g}、余白約 {float(msc.get('print_margin_mm', 5)):g}mm。"
                    f"拡大（結合前）: {up_note}。"
                )
            row_item: dict = {
                "filename": out_fn,
                "url": jpg_url,
                "pdf_image_resolution": dpi,
                "pdf_upscale_long_edge": up_edge_i,
                "msc_jst_header": True,
                "msc_header_lines": header_lines,
                "msc_crop_right": crop_r,
                "msc_crop_bottom": crop_b,
                "pdf_a4_fit": True,
                "pdf_a4_dpi": float(msc.get("print_dpi", 300)),
                "pdf_a4_margin_mm": float(msc.get("print_margin_mm", 5)),
                "comment": him_comment,
            }
            if him_mosaic is not None:
                row_item["himawari_mosaic"] = him_mosaic
            if him_snap is not None:
                row_item["himawari_map_screenshot"] = him_snap
            out.append(row_item)

    zm = cfg.get("jma_nowc_hrpns_mosaic")
    if isinstance(zm, dict) and zm.get("enabled"):
        moz: dict = {}
        if zm.get("zoom") is not None:
            moz["zoom"] = int(zm["zoom"])
        if "basemap" in zm:
            moz["basemap"] = bool(zm.get("basemap"))
        for k in ("lon_w", "lon_e", "lat_s", "lat_n"):
            if k in zm and zm[k] is not None:
                moz[k] = float(zm[k])
        try:
            bt_raw, vt_raw = nowc_latest_hrpns_basetime_validtime()
        except Exception as e:  # noqa: BLE001
            warnings.append(f"jma_nowc_hrpns_mosaic: 時刻取得失敗 {e}")
            bt_raw, vt_raw = "", ""
        if bt_raw and vt_raw:
            moz["hrpns_basetime"] = bt_raw
            moz["hrpns_validtime"] = vt_raw
            bt_u = _parse_jma_nowc_ts14_utc(bt_raw)
            vt_u = _parse_jma_nowc_ts14_utc(vt_raw)
            bt_j, bt_uu = _fmt_jst_utc(bt_u)
            vt_j, vt_uu = _fmt_jst_utc(vt_u)
            time_note = (
                f"解析基準時刻: **{bt_j}**（{bt_uu}）。"
                f"表示（valid）時刻: **{vt_j}**（{vt_uu}）。"
                "targetTimes の 14 桁は UTC（気象庁 jmatile 仕様）。"
            )
            stem = Path(zm.get("filename") or "降水ナウキャスト_hrpns_日本域").stem
            fn = f"{stem}_vt{vt_u.astimezone(JST):%Y%m%d_%H%M}JST.png"
        else:
            time_note = "時刻表記: 取得時に targetTimes を参照できませんでした。"
            fn = zm.get("filename") or "降水ナウキャスト_hrpns_日本域モザイク.png"
        out.append(
            {
                "filename": fn,
                "url": WXBRIEFING_HRPNS_MOSAIC,
                "hrpns_mosaic": moz,
                "comment": (
                    "気象庁 ナウキャストの降水強度（hrpns）を、防災 jmatile と同じタイル規則で取得し"
                    "日本域周辺でタイル結合した PNG。地理院淡色地図を下敷きに重ねる設定が既定。"
                    f"{time_note}"
                    "画像下に同じ内容のキャプションを焼き付けています。"
                    "ブラウザの IMAGE 出力と同一ファイルではありません。"
                ),
            }
        )

    nwp = cfg.get("jma_numericmap_nwp")
    if isinstance(nwp, dict) and nwp.get("enabled"):
        utc_h = numericmap_nwp_utc_suffix_for_download_jst()
        prows = nwp.get("products")
        if not prows:
            prows = [
                {"id": "fxfe502", "filename": f"FXFE502_{utc_h}UTC.pdf"},
                {"id": "fxfe5782", "filename": f"FXFE5782_{utc_h}UTC.pdf"},
                {"id": "fxjp854", "filename": f"FXJP854_{utc_h}UTC.pdf"},
            ]
        for pr in prows:
            if not isinstance(pr, dict):
                continue
            pid = pr.get("id") or pr.get("product")
            fn = pr.get("filename")
            if not pid:
                warnings.append("jma_numericmap_nwp: id なしの行をスキップしました")
                continue
            try:
                u = jma_numericmap_upper_pdf_url(str(pid), utc_h)
            except ValueError as e:
                warnings.append(f"jma_numericmap_nwp ({pid}): {e}")
                continue
            out.append(
                {
                    "filename": fn or f"{str(pid).upper()}_{utc_h}UTC.pdf",
                    "url": u,
                    "comment": (
                        "気象庁 数値予報天気図・NWP（numericmap / NWP_PROPS）。"
                        f"取得時の日本時間が 9:30〜21:30 なら 00UTC 版、それ以外は 12UTC 版（今回 {utc_h}UTC）。"
                    ),
                }
            )

    fbjp = cfg.get("jma_airinfo_fbjp")
    if isinstance(fbjp, dict) and fbjp.get("enabled"):
        url = str(fbjp.get("url") or JMA_AIRINFO_FBJP_PNG).strip()
        fn = fbjp.get("filename") or "FBJP_国内悪天予想図.png"
        out.append(
            {
                "filename": fn,
                "url": url,
                "comment": (
                    "気象庁 航空気象情報の国内悪天予想図（FBJP）。"
                    "data.jma.go.jp/airinfo の awfo_fbjp.html が参照する pict/fbjp/fbjp.png と同一。"
                    "固定パスのため取得時点で気象庁が公開している最新画像が入ります。"
                ),
            }
        )

    taf = cfg.get("jma_airinfo_taf")
    if isinstance(taf, dict) and taf.get("enabled"):
        dpi_block = float(taf.get("image_pdf_resolution") or 120)
        prows = taf.get("products")
        if not prows:
            prows = [
                {"icao": "RJSF", "filename": "飛行場時系列予報_福島空港_PART1-2.pdf"},
                {"icao": "RJSS", "filename": "飛行場時系列予報_仙台空港_PART1-2.pdf"},
                {"icao": "RJSN", "filename": "飛行場時系列予報_新潟空港_PART1-2.pdf"},
            ]
        for pr in prows:
            if not isinstance(pr, dict):
                continue
            icao = pr.get("icao") or pr.get("id")
            fn = pr.get("filename")
            label = (pr.get("label") or pr.get("name") or "").strip()
            if not icao:
                warnings.append("jma_airinfo_taf: ICAO なしの行をスキップしました")
                continue
            code = re.sub(r"[^A-Za-z0-9]", "", str(icao)).upper()
            if taf_allow is not None and code not in taf_allow:
                continue
            try:
                jma_airinfo_taf_part_png_url(str(icao), 1)
                jma_airinfo_taf_part_png_url(str(icao), 2)
            except ValueError as e:
                warnings.append(f"jma_airinfo_taf ({icao}): {e}")
                continue
            if not taf_part1 and not taf_part2:
                continue
            dpi = float(pr.get("image_pdf_resolution") or dpi_block)
            base_fn = fn or f"TAF_{code}_PART1-2.pdf"
            ext = Path(base_fn).suffix or ".pdf"
            lab_short = (label or code).replace(" ", "").replace("　", "")
            if isinstance(merged_taf_selection, dict):
                if taf_part1 and taf_part2:
                    part_suffix = "PART1-2"
                    part_desc = "PART1（QMCD98_）の次に PART2（QMCJ98_）を 1 本の PDF（2 ページ）に連結。"
                elif taf_part1:
                    part_suffix = "PART1"
                    part_desc = "PART1（QMCD98_）のみ 1 ページの PDF。"
                else:
                    part_suffix = "PART2"
                    part_desc = "PART2（QMCJ98_）のみ 1 ページの PDF。"
                out_fn = f"飛行場時系列予報_{lab_short}_{code}_{part_suffix}{ext}"
            else:
                out_fn = base_fn
                part_desc = (
                    "PART1（QMCD98_）の次に PART2（QMCJ98_）の PNG を 1 本の PDF（2 ページ）に連結。"
                )
            out.append(
                {
                    "filename": out_fn,
                    "url": WXBRIEFING_AIRINFO_TAF_MERGED,
                    "taf_icao": code,
                    "taf_include_part1": taf_part1,
                    "taf_include_part2": taf_part2,
                    "taf_image_pdf_resolution": dpi,
                    "comment": (
                        "気象庁 航空気象情報「飛行場時系列予報・飛行場時系列情報」（awfo_taf.html）。"
                        f"ICAO **{code}**"
                        + (f"（{label}）" if label else "")
                        + f"。{part_desc}"
                    ),
                }
            )

    sigwx = cfg.get("jma_airinfo_low_level_sigwx")
    if isinstance(sigwx, dict) and sigwx.get("enabled"):
        url_override = str(sigwx.get("url") or "").strip()
        ft_raw = sigwx.get("forecast_type")
        if ft_raw is None:
            ft_raw = sigwx.get("ft")
        ft = str(ft_raw if ft_raw is not None else "39").strip().lower()
        if url_override:
            if sigwx_allow is not None and len(sigwx_allow) == 0:
                pass
            else:
                fn = sigwx.get("filename") or "下層悪天予想図.png"
                out.append(
                    {
                        "filename": fn,
                        "url": url_override,
                        "comment": (
                            "気象庁 航空気象情報「下層悪天予想図」（URL 直指定）。"
                            "固定パスのため取得時点の最新画像が入ります。"
                        ),
                    }
                )
        else:
            if sigwx_allow is not None and len(sigwx_allow) == 0:
                pass
            else:
                prows_sig = sigwx.get("products")
                if isinstance(prows_sig, list) and prows_sig:
                    rows: list[dict] = [p for p in prows_sig if isinstance(p, dict)]
                else:
                    area0 = str(sigwx.get("area") or "fbsn").strip().lower()
                    rows = [
                        {
                            "area": area0,
                            "filename": sigwx.get("filename")
                            or "下層悪天予想図_東北_時系列.png",
                            "label": LOW_LEVEL_SIGWX_AREA_LABELS.get(area0, area0),
                        }
                    ]
                for pr in rows:
                    area = str(pr.get("area") or "").strip().lower()
                    area = re.sub(r"[^a-z0-9]", "", area)
                    if not area:
                        warnings.append(
                            "jma_airinfo_low_level_sigwx: area なしの行をスキップしました"
                        )
                        continue
                    if sigwx_allow is not None and area not in sigwx_allow:
                        continue
                    try:
                        u = jma_airinfo_low_level_sigwx_png_url(area, ft)
                    except ValueError as e:
                        warnings.append(f"jma_airinfo_low_level_sigwx ({area}): {e}")
                        continue
                    lab = (pr.get("label") or pr.get("name") or "").strip()
                    if not lab:
                        lab = LOW_LEVEL_SIGWX_AREA_LABELS.get(area, area)
                    fn = pr.get("filename") or (
                        f"下層悪天予想図_{lab}_ft{ft}.png"
                    )
                    out.append(
                        {
                            "filename": fn,
                            "url": u,
                            "comment": (
                                "気象庁 航空気象情報「下層悪天予想図」"
                                "（awfo_low-level_sigwx.html / functions.js）。"
                                f"地域 **{lab}**（area={area}）、予報区分 **{ft}**"
                                "（39=時系列）。固定パスのため取得時点の最新画像が入ります。"
                            ),
                        }
                    )

    dsig = cfg.get("jma_airinfo_low_level_detailed_sigwx")
    if isinstance(dsig, dict) and dsig.get("enabled"):
        if detailed_fig_allow is not None and len(detailed_fig_allow) == 0:
            pass
        else:
            prows = dsig.get("products")
            if not prows:
                prows = [
                    {
                        "fig": "Fig206",
                        "filename": "下層悪天予想図_詳細_福島.png",
                        "label": "福島",
                    },
                    {
                        "fig": "Fig204",
                        "filename": "下層悪天予想図_詳細_宮城.png",
                        "label": "宮城",
                    },
                    {
                        "fig": "Fig501",
                        "filename": "下層悪天予想図_詳細_新潟.png",
                        "label": "新潟",
                    },
                ]
            pref_default = str(dsig.get("image_prefix") or "").strip() or None
            for pr in prows:
                if not isinstance(pr, dict):
                    continue
                fig = pr.get("fig") or pr.get("areano") or pr.get("value")
                fn = pr.get("filename")
                label = (pr.get("label") or pr.get("name") or "").strip()
                url_override = str(pr.get("url") or "").strip()
                if not fig:
                    warnings.append(
                        "jma_airinfo_low_level_detailed_sigwx: fig なしの行をスキップしました"
                    )
                    continue
                fig_key = detailed_sigwx_fig_canonical(str(fig))
                if not fig_key:
                    warnings.append(
                        f"jma_airinfo_low_level_detailed_sigwx: fig 不正のためスキップ: {fig!r}"
                    )
                    continue
                if detailed_fig_allow is not None and fig_key not in detailed_fig_allow:
                    continue
                official_lab = detailed_sigwx_official_ja_label(fig_key)
                if official_lab:
                    if label and label != official_lab:
                        warnings.append(
                            "jma_airinfo_low_level_detailed_sigwx: "
                            f"{fig_key} の label が公式と異なります "
                            f"(config={label!r} → 表示・説明は {official_lab!r} に合わせます)"
                        )
                    label = official_lab
                elif not label:
                    label = fig_key
                pref = str(pr.get("image_prefix") or "").strip() or pref_default
                pref_arg = pref if pref else None
                if url_override:
                    u = url_override
                else:
                    try:
                        u = jma_airinfo_detailed_sigwx_png_url(
                            str(fig), prefix=pref_arg
                        )
                    except ValueError as e:
                        warnings.append(
                            f"jma_airinfo_low_level_detailed_sigwx ({fig}): {e}"
                        )
                        continue
                fig_slug = re.sub(r"[^A-Za-z0-9]+", "_", str(fig)).strip("_")
                out.append(
                    {
                        "filename": fn or f"下層悪天予想図_詳細_{fig_slug}.png",
                        "url": u,
                        "comment": (
                            "気象庁 航空気象情報「下層悪天予想図（詳細版）」"
                            "（awfo_low-level_detailed-sigwx.html / "
                            "functions_low-level_detailed-sigwx.js）。"
                            f"地域選択 **{fig_key}**"
                            + (f"（{label}）" if label else "")
                            + "。固定パスのため取得時点の最新画像が入ります。"
                        ),
                    }
                )

    fxjp106 = cfg.get("jma_airinfo_fxjp106_cross_section")
    if isinstance(fxjp106, dict) and fxjp106.get("enabled"):
        url_override = str(fxjp106.get("url") or "").strip()
        utc_raw = fxjp106.get("utc_hour")
        if utc_raw is None:
            utc_raw = fxjp106.get("initial_utc")
        utc_h = str(utc_raw if utc_raw is not None else "00").strip()
        fn = fxjp106.get("filename") or "FXJP106_国内航空路予想断面図.png"
        if url_override:
            u = url_override
        else:
            try:
                u = jma_airinfo_fxjp106_cross_section_png_url(utc_h)
            except ValueError as e:
                warnings.append(f"jma_airinfo_fxjp106_cross_section: {e}")
                u = ""
        if u:
            m_url = re.search(r"fxjp106_(\d{2})\.png$", u)
            u_disp = m_url.group(1) if m_url else str(utc_h).strip()
            out.append(
                {
                    "filename": fn,
                    "url": u,
                    "comment": (
                        "気象庁 航空気象情報「国内航空路6・12時間予想断面図」（FXJP106・"
                        "awfo_fxjp106.html）。"
                        f"初期値時刻 **{u_disp} UTC** の画像（pict/nwp/fxjp106_{u_disp}.png）。"
                        "固定パスのため取得時点の最新画像が入ります。"
                    ),
                }
            )

    return out, warnings


def _expand_jma_weather_map_items(out: list[dict], warnings: list[str], jm: dict, block: dict) -> None:
    now_jst = datetime.now(JST)
    prev = now_jst - timedelta(days=1)
    t_prev_21 = prev.replace(hour=21, minute=0, second=0, microsecond=0)
    t_today_03 = now_jst.replace(hour=3, minute=0, second=0, microsecond=0)
    t_today_21 = now_jst.replace(hour=21, minute=0, second=0, microsecond=0)

    now_files = block.get("now") or []

    f1 = _jma_pick_surface_analysis(now_files, t_prev_21)
    if f1:
        out.append(
            {
                "filename": jm.get("filename_prev_day_21")
                or f"地上天気図_前日21時JST_{t_prev_21:%Y%m%d}.png",
                "url": JMA_PNG_BASE + f1,
                "comment": "気象庁 防災天気図（日本周辺・実況）",
            }
        )
    else:
        warnings.append(
            f"前日21時(JST)の実況図が list に見つかりませんでした（対象 {t_prev_21:%Y-%m-%d %H}時）"
        )

    f2 = _jma_pick_surface_analysis(now_files, t_today_03)
    if f2:
        out.append(
            {
                "filename": jm.get("filename_today_03")
                or f"地上天気図_当日03時JST_{t_today_03:%Y%m%d}.png",
                "url": JMA_PNG_BASE + f2,
                "comment": "気象庁 防災天気図（日本周辺・実況）",
            }
        )
    else:
        warnings.append(
            f"当日03時(JST)の実況図が list に見つかりませんでした（未発表の可能性）"
        )

    f3, w3 = _jma_pick_forecast_surface_21jst(block, now_jst)
    if f3:
        item = {
            "filename": jm.get("filename_today_21_forecast")
            or f"予想地上天気図_当日21時JST_{t_today_21:%Y%m%d}.png",
            "url": JMA_PNG_BASE + f3,
            "comment": "気象庁 防災天気図（日本周辺・予想）",
        }
        out.append(item)
        if w3:
            warnings.append(w3)
    else:
        warnings.append("予想地上天気図を list から選べませんでした")


def _guess_content_type(filename: str) -> str:
    fn = filename.lower()
    if fn.endswith(".pdf"):
        return "application/pdf"
    if fn.endswith(".png"):
        return "image/png"
    if fn.endswith(".jpg") or fn.endswith(".jpeg"):
        return "image/jpeg"
    if fn.endswith(".gif"):
        return "image/gif"
    if fn.endswith(".svg"):
        return "image/svg+xml"
    return "application/octet-stream"


def fetch_one_expanded_item(
    cfg: dict, index: int | None, name: str | None
) -> tuple[dict | None, str | None]:
    items, _warn = expand_download_items(cfg)
    if name:
        for it in items:
            if isinstance(it, dict) and it.get("filename") == name:
                return it, None
        return None, f"filename が見つかりません: {name}"
    if index is not None:
        if not (0 <= index < len(items)):
            return None, f"index が範囲外です（0〜{len(items) - 1}）"
        it = items[index]
        return (it, None) if isinstance(it, dict) else (None, "不正な項目です")
    return None, "クエリ i（番号）または name（ZIP内ファイル名）を指定してください"


def build_zip(cfg: dict) -> tuple[bytes, list[str], list[str], int]:
    buf = io.BytesIO()
    errors: list[str] = []
    warnings: list[str] = []
    items, jma_warnings = expand_download_items(cfg)
    warnings.extend(jma_warnings)
    ok = 0
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for i, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            name = item.get("filename") or f"file_{i + 1}.bin"
            if not url:
                errors.append(f"{name}: URL なし")
                continue
            try:
                data, _ = fetch_item_bytes(item)
                zi = zipfile.ZipInfo(filename=name)
                zi.compress_type = zipfile.ZIP_DEFLATED
                zf.writestr(zi, data)
                ok += 1
            except urllib.error.HTTPError as e:
                errors.append(f"{name}: HTTP {e.code}")
            except urllib.error.URLError as e:
                errors.append(f"{name}: {e.reason}")
            except Exception as e:  # noqa: BLE001
                errors.append(f"{name}: {e}")
    return buf.getvalue(), errors, warnings, ok


def _a4_canvas_pixels(*, dpi: float, landscape: bool) -> tuple[int, int]:
    """A4 用のピクセルサイズ（210×297 mm）。landscape のとき長辺を横に。"""
    short_mm, long_mm = 210.0, 297.0
    if landscape:
        pw = int(round(long_mm / 25.4 * dpi))
        ph = int(round(short_mm / 25.4 * dpi))
    else:
        pw = int(round(short_mm / 25.4 * dpi))
        ph = int(round(long_mm / 25.4 * dpi))
    return max(1, pw), max(1, ph)


def _image_bytes_to_single_page_pdf(
    data: bytes,
    *,
    resolution: float = 100.0,
    upscale_long_edge: int | None = None,
) -> bytes:
    from PIL import Image

    im = Image.open(io.BytesIO(data))
    if im.mode in ("RGBA", "LA"):
        base = im.convert("RGBA")
        bg = Image.new("RGB", base.size, (255, 255, 255))
        bg.paste(base, mask=base.split()[-1])
        im = bg
    elif im.mode == "P":
        base = im.convert("RGBA")
        bg = Image.new("RGB", base.size, (255, 255, 255))
        bg.paste(base, mask=base.split()[-1])
        im = bg
    elif im.mode != "RGB":
        im = im.convert("RGB")
    if upscale_long_edge and upscale_long_edge > 0:
        w, h = im.size
        long_edge = max(w, h)
        if long_edge < upscale_long_edge:
            scale = upscale_long_edge / long_edge
            nw = max(1, int(round(w * scale)))
            nh = max(1, int(round(h * scale)))
            im = im.resize((nw, nh), Image.Resampling.LANCZOS)
    out = io.BytesIO()
    im.save(out, format="PDF", resolution=resolution)
    return out.getvalue()


def _image_bytes_to_single_page_pdf_a4(
    data: bytes,
    *,
    resolution: float = 200.0,
    upscale_long_edge: int | None = None,
    margin_mm: float = 12.0,
) -> bytes:
    """
    画像を A4（縦／横はアスペクトで自動）1ページに収め、余白を付けて印刷しやすくする。
    MediaBox は実質 A4 になるよう dpi とキャンバス寸法を揃える。
    """
    from PIL import Image

    dpi = float(resolution)
    dpi = min(300.0, max(120.0, dpi))
    margin_mm = max(2.5, min(18.0, float(margin_mm)))

    im = Image.open(io.BytesIO(data))
    if im.mode in ("RGBA", "LA"):
        base = im.convert("RGBA")
        bg = Image.new("RGB", base.size, (255, 255, 255))
        bg.paste(base, mask=base.split()[-1])
        im = bg
    elif im.mode == "P":
        base = im.convert("RGBA")
        bg = Image.new("RGB", base.size, (255, 255, 255))
        bg.paste(base, mask=base.split()[-1])
        im = bg
    elif im.mode != "RGB":
        im = im.convert("RGB")

    iw, ih = im.size
    # 衛星は横長が多い → 横長なら A4 横（ランドスケープ）で余白を減らす
    landscape = iw >= ih
    cw, ch = _a4_canvas_pixels(dpi=dpi, landscape=landscape)
    mpx = int(round(margin_mm / 25.4 * dpi))
    inner_w = max(1, cw - 2 * mpx)
    inner_h = max(1, ch - 2 * mpx)

    if upscale_long_edge and upscale_long_edge > 0:
        long_edge = max(iw, ih)
        cap_up = int(max(inner_w, inner_h) * 2.2)
        target = min(int(upscale_long_edge), cap_up)
        if long_edge < target:
            s = target / long_edge
            im = im.resize(
                (max(1, int(round(iw * s))), max(1, int(round(ih * s)))),
                Image.Resampling.LANCZOS,
            )
            iw, ih = im.size

    scale = min(inner_w / iw, inner_h / ih)
    nw = max(1, int(round(iw * scale)))
    nh = max(1, int(round(ih * scale)))
    fitted = im.resize((nw, nh), Image.Resampling.LANCZOS)

    page = Image.new("RGB", (cw, ch), (255, 255, 255))
    ox = (cw - nw) // 2
    oy = (ch - nh) // 2
    page.paste(fitted, (ox, oy))
    out = io.BytesIO()
    page.save(out, format="PDF", resolution=dpi)
    return out.getvalue()


def build_merged_pdf(
    cfg: dict,
    *,
    merged_taf_selection: dict | None = None,
    merged_sigwx_areas: list[str] | None = None,
    merged_detailed_sigwx_figs: list[str] | None = None,
) -> tuple[bytes, list[str], list[str], int]:
    """
    取得対象をすべて取得し、1つの PDF に連結する。
    PDF はページとして追加、PNG/JPEG/GIF は1枚1ページの PDF にしてから追加。
    未対応形式（SVG 等）はスキップして warnings に記録。

    取得は HTTP 等が独立している限り ``ThreadPoolExecutor`` で並列化し、
    **ページ順は expand_download_items の items 順のまま**（正確性を維持）。
    並列度は ``config["merged_pdf"]["fetch_workers"]``（既定 8、上限 24）。

    merged_taf_selection: Streamlit 等から飛行場時系列予報の ICAO / PART を絞るときに指定。

    merged_sigwx_areas / merged_detailed_sigwx_figs: 下層悪天予想図・詳細版を結合 PDF だけで絞るとき。
        省略時は expand_download_items の既定（config どおり全件）。
    """
    try:
        from pypdf import PdfReader, PdfWriter
        from PIL import Image  # noqa: F401 — 依存確認用
    except ImportError as e:
        raise RuntimeError(
            "1つのPDFにまとめるには pypdf と Pillow が必要です。"
            'フォルダで次を実行してください: pip install -r requirements.txt'
        ) from e

    errors: list[str] = []
    warnings: list[str] = []
    items, jma_warnings = expand_download_items(
        cfg,
        merged_taf_selection=merged_taf_selection,
        merged_sigwx_areas=merged_sigwx_areas,
        merged_detailed_sigwx_figs=merged_detailed_sigwx_figs,
    )
    warnings.extend(jma_warnings)

    writer = PdfWriter()
    pages_added = 0

    fetch_jobs: list[tuple[int, dict]] = [
        (i, it) for i, it in enumerate(items) if isinstance(it, dict)
    ]
    max_w = _merged_pdf_max_fetch_workers(cfg, len(fetch_jobs))
    if max_w <= 1 or len(fetch_jobs) <= 1:
        fetched_rows = [_merged_pdf_fetch_one(job) for job in fetch_jobs]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as ex:
            fetched_rows = list(ex.map(_merged_pdf_fetch_one, fetch_jobs))

    for _i, item, name, kind, data, _ctype, msg in fetched_rows:
        if kind == "no_url":
            if msg:
                errors.append(msg)
            continue
        if kind == "svg":
            if msg:
                warnings.append(msg)
            continue
        if kind == "err":
            if msg:
                errors.append(msg)
            continue
        if kind != "ok" or data is None:
            continue

        low = name.lower()

        try:
            if low.endswith(".pdf"):
                reader = PdfReader(io.BytesIO(data))
                n = len(reader.pages)
                if n == 0:
                    warnings.append(f"{name}: PDF にページがありません")
                writer.append_pages_from_reader(reader)
                pages_added += n
            elif low.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff")):
                try:
                    dpi = float(item.get("pdf_image_resolution") or 100)
                    up = item.get("pdf_upscale_long_edge")
                    up_i: int | None
                    if up is None:
                        up_i = None
                    else:
                        up_i = int(up)
                        if up_i <= 0:
                            up_i = None
                    if item.get("pdf_a4_fit"):
                        a4_dpi = float(item.get("pdf_a4_dpi") or 200)
                        a4_mm = float(item.get("pdf_a4_margin_mm") or 5)
                        pdf_one = _image_bytes_to_single_page_pdf_a4(
                            data,
                            resolution=a4_dpi,
                            upscale_long_edge=up_i,
                            margin_mm=a4_mm,
                        )
                    else:
                        pdf_one = _image_bytes_to_single_page_pdf(
                            data,
                            resolution=dpi,
                            upscale_long_edge=up_i,
                        )
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{name}: 画像→PDF 変換失敗 {e}")
                    continue
                reader = PdfReader(io.BytesIO(pdf_one))
                writer.append_pages_from_reader(reader)
                pages_added += len(reader.pages)
            else:
                warnings.append(
                    f"{name}: 拡張子が PDF/画像以外のためスキップしました（結合は .pdf .png .jpg .gif 等のみ）"
                )
        except Exception as e:  # noqa: BLE001
            errors.append(f"{name}: PDF 結合処理エラー {e}")

    if pages_added == 0:
        return b"", errors, warnings, 0

    out_buf = io.BytesIO()
    writer.write(out_buf)
    return out_buf.getvalue(), errors, warnings, pages_added


def metar_taf_airports_from_config(cfg: dict) -> list[dict[str, str]]:
    """
    metar_taf_fetch が有効なとき、既定9空港を固定順で返す。
    config の airports にはラベル上書きのみ使う（ICAO は allowlist 内のみ反映）。
    """
    block = cfg.get("metar_taf_fetch")
    if not isinstance(block, dict) or not block.get("enabled"):
        return []
    label_over: dict[str, str] = {}
    rows = block.get("airports")
    if isinstance(rows, list):
        for ap in rows:
            if not isinstance(ap, dict):
                continue
            icao = ap.get("icao") or ap.get("id")
            if not icao:
                continue
            code = re.sub(r"[^A-Za-z0-9]", "", str(icao)).upper()
            if code not in METAR_TAF_ICAO_ALLOW:
                continue
            lab = str(ap.get("label") or ap.get("name") or code).strip()
            if lab:
                label_over[code] = lab
    out: list[dict[str, str]] = []
    for row in METAR_TAF_DEFAULT_AIRPORTS:
        icao = row["icao"]
        out.append({"icao": icao, "label": label_over.get(icao, row["label"])})
    return out


def _fetch_awc_metar_raw(icao: str, timeout: int = 25) -> str | None:
    q = urllib.parse.urlencode({"ids": icao.upper(), "format": "json"})
    url = f"{AWC_API_METAR}?{q}"
    raw, _ = fetch_url(url, timeout=timeout)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, list) or not data:
        return None
    first = data[0]
    if not isinstance(first, dict):
        return None
    ob = first.get("rawOb")
    return str(ob).strip() if ob else None


def _fetch_awc_taf_raw(icao: str, timeout: int = 25) -> str | None:
    q = urllib.parse.urlencode({"ids": icao.upper(), "format": "json"})
    url = f"{AWC_API_TAF}?{q}"
    raw, _ = fetch_url(url, timeout=timeout)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, list) or not data:
        return None
    first = data[0]
    if not isinstance(first, dict):
        return None
    t = first.get("rawTAF")
    return str(t).strip() if t else None


def metar_taf_source(cfg: dict) -> str:
    """metar_taf_fetch.source: imoc（既定） / noaa_awc"""
    block = cfg.get("metar_taf_fetch")
    if not isinstance(block, dict):
        return "imoc"
    raw = str(block.get("source") or "imoc").strip().lower().replace("-", "_")
    if raw in ("noaa", "awc", "noaa_awc", "aviationweather"):
        return "noaa_awc"
    return "imoc"


_IMOC_LISTVIEW_UL = re.compile(
    r"<ul[^>]*data-role=[\"']listview[\"'][^>]*>(.*?)</ul>", re.I | re.S
)


def _parse_imoc_metar_station_html(html: str, icao: str) -> str | None:
    """SmartPhone metar.php の listview から最新1件（最初の <hr> 手前）を抜粋。"""
    code = icao.upper()
    for um in _IMOC_LISTVIEW_UL.finditer(html):
        inner = um.group(1)
        if f"({code})</li>" not in inner:
            continue
        dm = re.search(rf"\({re.escape(code)}\)</li>", inner, re.I)
        if not dm:
            continue
        tail = inner[dm.end() :]
        hm = re.search(r"<hr\s*/?>", tail, re.I)
        segment = tail[: hm.start()] if hm else tail
        plain = re.sub(r"<br\s*/?>", "\n", segment, flags=re.I)
        plain = re.sub(r"<[^>]+>", "", plain)
        lines = [ln.strip() for ln in plain.splitlines() if ln.strip()]
        body: list[str] = []
        for ln in lines:
            if re.match(r"^\d{1,2}/\d{1,2}", ln):
                continue
            body.append(ln)
            if ln.rstrip().endswith("="):
                break
        if not body:
            continue
        text = re.sub(r"\s+", " ", " ".join(body)).strip()
        if code in text.upper() and (
            "METAR" in text.upper() or re.search(rf"{re.escape(code)}\s+\d{{6}}Z", text)
        ):
            return text
    return None


def _parse_imoc_taf_station_html(html: str, icao: str) -> str | None:
    """SmartPhone taf.php の listview から該当空港の TAF 1本を抜粋。"""
    code = icao.upper()
    for um in _IMOC_LISTVIEW_UL.finditer(html):
        block = um.group(1)
        if f"({code})</li>" not in block:
            continue
        if not re.search(rf"TAF\s+{re.escape(code)}\s+", block, re.I):
            continue
        tm = re.search(rf"(TAF\s+{re.escape(code)}\s+[\s\S]+?=)", block, re.I)
        if not tm:
            continue
        one = tm.group(1)
        one = re.sub(r"<br\s*/?>", " ", one, flags=re.I)
        one = re.sub(r"<[^>]+>", " ", one)
        return re.sub(r"\s+", " ", one).strip()
    return None


def _fetch_imoc_metar_raw(icao: str, cache: dict[str, str], timeout: int = 25) -> str | None:
    code = icao.upper()
    area = IMOC_METAR_AREA_BY_ICAO.get(code)
    if area is None:
        return None
    url = (
        f"{IMOC_SMARTPHONE_D}/metar.php?"
        f"{urllib.parse.urlencode({'Lang': 'Jpn', 'Area': str(area), 'Port': code})}"
    )
    if url not in cache:
        raw, _ = fetch_url(url, timeout=timeout)
        cache[url] = raw.decode("utf-8", errors="replace")
    return _parse_imoc_metar_station_html(cache[url], code)


def _fetch_imoc_taf_raw(icao: str, cache: dict[str, str], timeout: int = 25) -> str | None:
    code = icao.upper()
    area = IMOC_TAF_AREA_BY_ICAO.get(code)
    if area is None:
        return None
    url = (
        f"{IMOC_SMARTPHONE_D}/taf.php?"
        f"{urllib.parse.urlencode({'Lang': 'Jpn', 'Area': str(area)})}"
    )
    if url not in cache:
        raw, _ = fetch_url(url, timeout=timeout)
        cache[url] = raw.decode("utf-8", errors="replace")
    return _parse_imoc_taf_station_html(cache[url], code)


def _metar_taf_ddhhmmz_to_jst_display(raw: str | None) -> str | None:
    """
    生報文中で最初に現れる DDHHMMZ を UTC の観測・発報時刻とみなし、JST で表示用文字列にする。
    年月は現在 UTC 月を基準に前後1ヶ月の候補から報文時刻に最も近いものを採用。
    """
    if not raw:
        return None
    m = re.search(r"\b(\d{2})(\d{2})(\d{2})Z\b", raw.strip())
    if not m:
        return None
    d_day, hour, minute = int(m.group(1)), int(m.group(2)), int(m.group(3))
    now = datetime.now(timezone.utc)
    best: datetime | None = None
    best_diff: float | None = None
    for mo_off in (-1, 0, 1):
        y, mo = now.year, now.month + mo_off
        while mo < 1:
            mo += 12
            y -= 1
        while mo > 12:
            mo -= 12
            y += 1
        try:
            dt = datetime(y, mo, d_day, hour, minute, tzinfo=timezone.utc)
        except ValueError:
            continue
        diff = abs((dt - now).total_seconds())
        if best is None or (best_diff is not None and diff < best_diff):
            best = dt
            best_diff = diff
    if best is None:
        return None
    return best.astimezone(JST).strftime("%Y/%m/%d %H:%M JST")


def _format_taf_becmg_tempo_lines(raw: str | None) -> str | None:
    """TAF 本文で BECMG / TEMPO の直前に改行を入れ、行頭に揃える。"""
    if not raw:
        return raw
    t = re.sub(r"\s+", " ", raw.strip())
    t = re.sub(r" (?=BECMG\b)", "\n", t)
    t = re.sub(r" (?=TEMPO\b)", "\n", t)
    return t


def _airport_heading_para_markup(code: str, lab: str, time_bits: list[str]) -> str:
    """ICAO・空港名＋（小さめ）METAR/TAF 発行時刻ラベル用 Paragraph マークアップ。"""
    esc = xml_escape
    base = f"{esc(str(code))}　{esc(str(lab))}"
    if time_bits:
        frag = esc("　".join(time_bits))
        base += f'<font size="6.5" color="#5a5f66">　{frag}</font>'
    return base


def build_metar_taf_pdf_bytes(
    cfg: dict,
    requested_icaos: list[str],
    include_metar: bool,
    include_taf: bool,
) -> tuple[bytes, list[str], int]:
    """
    選択空港について METAR・TAF（生報文）を PDF にまとめる。
    戻り値: (PDF バイト列, 警告メッセージ, 対象に含めた空港数)
    """
    allowed_order = [a["icao"] for a in METAR_TAF_DEFAULT_AIRPORTS]
    allow_set = METAR_TAF_ICAO_ALLOW
    warnings: list[str] = []
    norm: list[str] = []
    for raw in requested_icaos:
        c = re.sub(r"[^A-Za-z0-9]", "", str(raw)).upper()
        if len(c) != 4:
            warnings.append(f"スキップ（ICAO 不正）: {raw!r}")
            continue
        if c not in allow_set:
            warnings.append(f"スキップ（一覧外の空港）: {c}")
            continue
        if c not in norm:
            norm.append(c)
    ordered = [c for c in allowed_order if c in norm]
    labels = {a["icao"]: a["label"] for a in metar_taf_airports_from_config(cfg)}
    for code in METAR_TAF_ICAO_ALLOW:
        labels.setdefault(code, code)

    source = metar_taf_source(cfg)
    imoc_html_cache: dict[str, str] = {}

    font = _metar_taf_pdf_font_name()
    styles = getSampleStyleSheet()
    sid = str(time.time_ns())
    title_style = ParagraphStyle(
        name=f"MtTitle_{sid}",
        parent=styles["Heading2"],
        fontName=font,
        fontSize=14,
        leading=18,
        alignment=TA_LEFT,
    )
    head_style = ParagraphStyle(
        name=f"MtHead_{sid}",
        parent=styles["Normal"],
        fontName=font,
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#333333"),
        alignment=TA_LEFT,
    )
    body_style = ParagraphStyle(
        name=f"MtBody_{sid}",
        parent=styles["Normal"],
        fontName=font,
        fontSize=9,
        leading=12,
        alignment=TA_LEFT,
        wordWrap="CJK",
    )
    sub_style = ParagraphStyle(
        name=f"MtSub_{sid}",
        parent=styles["Normal"],
        fontName=font,
        fontSize=10.5,
        leading=14,
        spaceBefore=10,
        spaceAfter=4,
        alignment=TA_LEFT,
    )
    sub_head_style = ParagraphStyle(
        name=f"MtSubHead_{sid}",
        parent=sub_style,
        spaceBefore=0,
        spaceAfter=3,
    )
    lbl_style = ParagraphStyle(
        name=f"MtLbl_{sid}",
        parent=body_style,
        fontSize=8.2,
        leading=10,
        textColor=colors.HexColor("#4a5568"),
        spaceBefore=3,
        spaceAfter=2,
    )

    content_w = float(A4[0] - 36 * mm)
    airport_tables: list = []
    for code in ordered:
        lab = labels.get(code, code)
        met: str | None = None
        taf: str | None = None
        if include_metar:
            try:
                if source == "noaa_awc":
                    met = _fetch_awc_metar_raw(code)
                else:
                    met = _fetch_imoc_metar_raw(code, imoc_html_cache)
            except Exception as e:  # noqa: BLE001
                warnings.append(f"{code} METAR: {e}")
                met = None
        if include_taf:
            try:
                if source == "noaa_awc":
                    taf = _fetch_awc_taf_raw(code)
                else:
                    taf = _fetch_imoc_taf_raw(code, imoc_html_cache)
            except Exception as e:  # noqa: BLE001
                warnings.append(f"{code} TAF: {e}")
                taf = None

        time_bits: list[str] = []
        if include_metar:
            ts = _metar_taf_ddhhmmz_to_jst_display(met)
            if ts:
                time_bits.append(f"METAR {ts}")
        if include_taf:
            ts = _metar_taf_ddhhmmz_to_jst_display(taf)
            if ts:
                time_bits.append(f"TAF {ts}")

        rows: list[list] = []
        rows.append(
            [Paragraph(_airport_heading_para_markup(code, str(lab), time_bits), sub_head_style)]
        )
        if include_metar:
            rows.append([Paragraph(_pdf_paragraph_text("METAR"), lbl_style)])
            rows.append(
                [
                    Paragraph(
                        _pdf_paragraph_text(met or "（取得できませんでした）"),
                        body_style,
                    )
                ]
            )
        if include_metar and include_taf:
            rows.append([Spacer(1, 1.5 * mm)])
        if include_taf:
            taf_disp = _format_taf_becmg_tempo_lines(taf) if taf else None
            rows.append([Paragraph(_pdf_paragraph_text("TAF"), lbl_style)])
            rows.append(
                [
                    Paragraph(
                        _pdf_paragraph_text(taf_disp or "（取得できませんでした）"),
                        body_style,
                    )
                ]
            )

        tbl = Table(rows, colWidths=[content_w])
        tbl.setStyle(
            TableStyle(
                [
                    ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#b8c0cc")),
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f6f8fb")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ]
            )
        )
        airport_tables.append(tbl)
        airport_tables.append(Spacer(1, 3.5 * mm))

    story: list = []
    story.append(Paragraph(_pdf_paragraph_text("METAR・TAF"), title_style))
    if source == "noaa_awc":
        src_txt = "出典: NOAA Aviation Weather Center（aviationweather.gov）。"
    else:
        src_txt = "出典: 国際気象海洋株式会社（imoc.co.jp）SmartPhone ページの HTML 表示を抜粋しています。"
    story.append(Paragraph(_pdf_paragraph_text(src_txt), head_style))
    loaded_at = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S JST")
    story.append(
        Paragraph(_pdf_paragraph_text(f"この PDF を作成した日時（ロード）: {loaded_at}"), head_style)
    )
    story.append(Spacer(1, 4 * mm))
    if warnings:
        story.append(Paragraph(_pdf_paragraph_text("-- 注意 --\n" + "\n".join(warnings)), head_style))
        story.append(Spacer(1, 3 * mm))
    if airport_tables:
        story.extend(airport_tables)
    else:
        story.append(Paragraph(_pdf_paragraph_text("（対象空港がありません）"), body_style))

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title="METAR TAF",
    )
    doc.build(story)
    return buf.getvalue(), warnings, len(ordered)


def html_metar_taf_panel(cfg: dict) -> str:
    """トップページ用: 空港・METAR/TAF を選び PDF でダウンロード（有効時のみ）。"""
    airports = metar_taf_airports_from_config(cfg)
    block = cfg.get("metar_taf_fetch")
    if not isinstance(block, dict) or not block.get("enabled") or not airports:
        return ""
    rows_html_parts: list[str] = []
    for title, aps in group_metar_taf_airports_by_region(airports):
        title_e = html.escape(title)
        inner: list[str] = []
        for ap in aps:
            icao = ap["icao"]
            lab = html.escape(ap["label"])
            cid = html.escape(re.sub(r"[^A-Za-z0-9_]", "_", icao))
            ic_e = html.escape(icao)
            inner.append(
                f'<label class="metar-taf-label" for="mt_{cid}">'
                f'<input type="checkbox" name="icao" value="{ic_e}" id="mt_{cid}" class="metar-taf-ap"> '
                f'<span class="metar-taf-name">{lab}</span>'
                f' <span class="metar-taf-icao">({ic_e})</span></label>'
            )
        if not inner:
            continue
        inner_joined = "\n".join(inner)
        rows_html_parts.append(
            f'<fieldset class="metar-taf-region"><legend>{title_e}</legend>'
            f'<div class="metar-taf-checks metar-taf-checks-region">'
            f"{inner_joined}</div></fieldset>"
        )
    rows_html = "\n".join(rows_html_parts)
    src = metar_taf_source(cfg)
    raw_note = block.get("note")
    if raw_note:
        note = str(raw_note)
    elif src == "noaa_awc":
        note = (
            "データは NOAA Aviation Weather Center（米国政府）の公開 API です。"
            "日本の官報・運航判断は必ず公式情報で確認してください。"
        )
    else:
        note = (
            "データは国際気象海洋株式会社（imoc.co.jp）の SmartPhone ページから読み取っています（公式 API ではありません）。"
            "RJSC・RJSY・RJTU の TAF は同サイトの一覧に掲載がなく取得できません。"
            "官報・運航判断は必ず公式情報で確認してください。"
        )
    note_e = html.escape(str(note))
    checks = rows_html
    script = """
<script>
(function () {
  var form = document.getElementById("metar-taf-form");
  if (!form) return;
  var kindRow = document.getElementById("metar-taf-kind-row");
  var aps = form.querySelectorAll("input.metar-taf-ap");
  var met = form.querySelector("#mt_inc_metar");
  var taf = form.querySelector("#mt_inc_taf");
  function anyAp() {
    for (var i = 0; i < aps.length; i++) { if (aps[i].checked) return true; }
    return false;
  }
  function syncKinds() {
    var ok = anyAp();
    if (kindRow) {
      kindRow.style.opacity = ok ? "1" : "0.5";
      kindRow.querySelectorAll("input[type=checkbox]").forEach(function (el) {
        el.disabled = !ok;
        if (!ok) el.checked = false;
      });
    }
  }
  aps.forEach(function (el) { el.addEventListener("change", syncKinds); });
  syncKinds();
  form.addEventListener("submit", function (ev) {
    if (!anyAp()) {
      ev.preventDefault();
      alert("空港を1つ以上選んでください。");
      return;
    }
    if (met && taf && !met.checked && !taf.checked) {
      ev.preventDefault();
      alert("METAR と TAF のどちらか（または両方）にチェックを入れてください。");
      return;
    }
    var parts = [];
    aps.forEach(function (el) { if (el.checked) parts.push(el.value); });
    var kinds = [];
    if (met && met.checked) kinds.push("METAR");
    if (taf && taf.checked) kinds.push("TAF");
    var msg = "次の内容で PDF をダウンロードします。よろしいですか？\\n\\n空港: " + parts.join(", ")
      + "\\n種別: " + kinds.join("・");
    if (!confirm(msg)) ev.preventDefault();
  });
})();
</script>
"""
    return f"""
      <div class="metar-taf" aria-labelledby="metar-taf-heading">
        <h3 id="metar-taf-heading" class="metar-taf-title">生報文のPDFダウンロード</h3>
        <p class="metar-taf-lead">① 空港にチェック　② <strong>METAR</strong> / <strong>TAF</strong> を選ぶ　③「PDFをダウンロード」で別タブに PDF が開きます。</p>
        <form id="metar-taf-form" class="metar-taf-form" method="post" action="/metar_taf_pdf" target="_blank" rel="noopener noreferrer">
          <p class="metar-taf-subtitle">対象空港</p>
          <div class="metar-taf-regions-wrap">
{checks}
          </div>
          <div id="metar-taf-kind-row" class="metar-taf-kind-row" aria-live="polite">
            <p class="metar-taf-subtitle">報文の種類</p>
            <label class="metar-taf-label" for="mt_inc_metar"><input type="checkbox" name="include_metar" value="1" id="mt_inc_metar"> <span>METAR</span></label>
            <label class="metar-taf-label" for="mt_inc_taf"><input type="checkbox" name="include_taf" value="1" id="mt_inc_taf"> <span>TAF</span></label>
          </div>
          <div class="metar-taf-actions">
            <button type="submit" class="btn btn-secondary">PDFをダウンロード</button>
          </div>
        </form>
        <p class="metar-taf-note">{note_e}</p>
      </div>
{script}"""


def page_html(cfg: dict) -> str:
    title_raw = cfg.get("title") or "WX Briefing"
    title_e = html.escape(str(title_raw))
    port = int(cfg.get("port") or 18876)
    rows: list[str] = []
    items_expanded, _warn = expand_download_items(cfg)
    n_valid = 0
    for idx, item in enumerate(items_expanded):
        if not isinstance(item, dict):
            continue
        u = item.get("url") or ""
        n = html.escape(item.get("filename") or "(名前なし)")
        n_valid += 1
        num = html.escape(str(n_valid))
        if u:
            name_cell = f'<a class="item-link" href="/file?i={idx}">{n}</a>'
        else:
            name_cell = f'<span class="item-name">{n}</span><span class="item-na">（URLなし）</span>'
        rows.append(
            f'<li class="item"><div class="item-row">'
            f'<span class="item-idx" aria-hidden="true">{num}</span>'
            f'<div class="item-name-wrap">{name_cell}</div></div></li>'
        )
    if not rows:
        rows.append('<li class="item empty">設定（config.json）に資料がありません。</li>')
    body_list = "\n".join(rows)

    auto_lines: list[str] = []
    jm = cfg.get("jma_weather_map")
    if isinstance(jm, dict) and jm.get("enabled"):
        auto_lines.append("防災天気図（日本周辺）: 前日21時・当日3時の実況、当日21時の予想（必要に応じて24h予想PNG）")
    qm = cfg.get("jma_quickmonthly_prevnight21_asas")
    if isinstance(qm, dict) and qm.get("enabled"):
        auto_lines.append(
            "過去の実況天気図（月別一覧）と同じ spas_latest 基準で「最新日の前日」のアジア太平洋域 21時(JST) ASAS PDF（昨晩21時地上として利用）"
        )
    qd = cfg.get("jma_quickdaily_asas")
    if isinstance(qd, dict) and qd.get("enabled"):
        auto_lines.append("実況天気図・アジア太平洋域（ASAS）PDF（1日表示と同じ取り方）")
    fs = cfg.get("jma_fsas24_asia_pdf")
    if isinstance(fs, dict) and fs.get("enabled"):
        auto_lines.append(
            "防災天気図（アジア太平洋域）の「最新24時間予想図」FSAS24 PDF（data.jma.go.jp 固定URL・取得時点の最新）"
        )
    nm = cfg.get("jma_numericmap_upper")
    if isinstance(nm, dict) and nm.get("enabled"):
        auto_lines.append("数値予報・高層（AUPQ35 / AUPQ78 など、config の 12UTC 設定）")
    msc = cfg.get("jma_msc_himawari_japan")
    if isinstance(msc, dict) and msc.get("enabled"):
        auto_lines.append(
            "防災統合地図相当のひまわり（bbox モザイク・satimg z≤6・上端 JST/UTC・A4 結合 PDF）"
        )
    zm = cfg.get("jma_nowc_hrpns_mosaic")
    if isinstance(zm, dict) and zm.get("enabled"):
        auto_lines.append(
            "降水ナウキャスト hrpns（タイルを日本域でモザイク化・targetTimes の UTC を JST 併記し画像下にキャプション）"
        )
    nwp = cfg.get("jma_numericmap_nwp")
    if isinstance(nwp, dict) and nwp.get("enabled"):
        auto_lines.append(
            "数値予報天気図・NWP（FXFE502 / FXFE5782 / FXJP854 など）："
            "日本時間 9:30〜21:30 は 00UTC 版、21:31 以降または 9:29 以前は 12UTC 版"
        )
    fbjp = cfg.get("jma_airinfo_fbjp")
    if isinstance(fbjp, dict) and fbjp.get("enabled"):
        auto_lines.append(
            "航空気象情報・国内悪天予想図 FBJP（airinfo / pict/fbjp/fbjp.png・取得時点の最新）"
        )
    taf = cfg.get("jma_airinfo_taf")
    if isinstance(taf, dict) and taf.get("enabled"):
        auto_lines.append(
            "航空気象情報・飛行場時系列予報（福島 RJSF / 仙台 RJSS / 新潟 RJSN など）："
            "PART1（QMCD98_）の次に PART2（QMCJ98_）の PNG を 2 ページの 1 本 PDF に連結"
        )
    sigwx = cfg.get("jma_airinfo_low_level_sigwx")
    if isinstance(sigwx, dict) and sigwx.get("enabled"):
        auto_lines.append(
            "航空気象情報・下層悪天予想図（config の地域コードごと・既定 ft=39 時系列 / pict/low-level_sigwx/）"
        )
    dsig = cfg.get("jma_airinfo_low_level_detailed_sigwx")
    if isinstance(dsig, dict) and dsig.get("enabled"):
        auto_lines.append(
            "航空気象情報・下層悪天予想図（詳細版）（config の Fig ごと / pict/low-level_sigwx_p/）"
        )
    fxjp106 = cfg.get("jma_airinfo_fxjp106_cross_section")
    if isinstance(fxjp106, dict) and fxjp106.get("enabled"):
        auto_lines.append(
            "航空気象情報・国内航空路6・12時間予想断面図 FXJP106（pict/nwp/fxjp106_HH.png・既定 00UTC）"
        )
    auto_block = ""
    if auto_lines:
        lis = "".join(f"<li>{html.escape(line)}</li>" for line in auto_lines)
        auto_block = (
            f'<details class="panel"><summary>気象庁から自動取得する資料</summary><ul class="list-tight">{lis}</ul>'
            f'<p class="panel-note">出典の詳細は各公式ページの仕様に準じます。</p></details>'
        )

    list_head = (
        f"資料一覧（{n_valid}件）" if n_valid else "資料一覧"
    )
    build_e = html.escape(portal_build_stamp())
    app_path_e = html.escape(str(Path(__file__).resolve()))
    metar_taf_block = html_metar_taf_panel(cfg)
    metar_taf_section = ""
    if metar_taf_block.strip():
        metar_taf_section = f"""
    <div class="card card-section card-metar-taf">
      <h2 class="section-head">METAR・TAF</h2>
      {metar_taf_block}
    </div>"""

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title_e}</title>
  <style>
    :root {{
      --bg: #f6f7f9;
      --card: #fff;
      --text: #1a1d26;
      --muted: #5c6578;
      --border: #e2e5eb;
      --accent: #2563eb;
      --accent-h: #1d4ed8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: "Segoe UI", system-ui, sans-serif;
      margin: 0; padding: 1.5rem 1rem 2.5rem;
      background: var(--bg); color: var(--text);
      line-height: 1.55;
    }}
    .wrap {{ max-width: 42rem; margin: 0 auto; }}
    .build-strip {{
      font-size: 0.75rem; line-height: 1.45; color: #e2e8f0;
      background: #1e293b; border-radius: 10px; padding: 0.55rem 0.85rem; margin: 0 0 1rem;
      word-break: break-all;
    }}
    .build-strip-line {{ margin: 0 0 0.45rem; }}
    .build-strip-path {{ margin: 0; font-size: 0.72rem; line-height: 1.4; color: #cbd5e1; }}
    .build-strip-path code {{
      display: block; margin-top: 0.25rem; font-size: 0.88em; color: #f8fafc;
      word-break: break-all; font-weight: 400;
    }}
    .build-strip strong {{ color: #fff; }}
    .build-strip kbd {{
      font-family: inherit; font-size: 0.9em;
      border: 1px solid #475569; padding: 0.05em 0.3em; border-radius: 4px;
      background: #334155;
    }}
    .build-strip a.build-strip-link {{ color: #93c5fd; }}
    .build-strip a.build-strip-link:hover {{ color: #bfdbfe; }}
    header {{
      margin-bottom: 1.25rem;
    }}
    h1 {{
      font-size: 1.6rem; font-weight: 700; margin: 0 0 0.4rem;
      letter-spacing: -0.02em;
    }}
    .lead {{ color: var(--muted); margin: 0; font-size: 0.98rem; max-width: 36rem; }}
    .card {{
      background: var(--card); border: 1px solid var(--border);
      border-radius: 12px; padding: 1.25rem 1.35rem;
      margin-bottom: 1rem; box-shadow: 0 1px 2px rgba(0,0,0,.04);
    }}
    .card-metar-taf .metar-taf {{
      margin-top: 0; padding-top: 0; border-top: none;
    }}
    .card-charts .charts-actions {{ text-align: center; padding: 0.15rem 0 0.25rem; }}
    .card h2 {{
      font-size: 0.75rem; text-transform: uppercase;
      letter-spacing: 0.06em; color: var(--muted);
      margin: 0 0 0.65rem; font-weight: 600;
    }}
    .card h2.list-head {{
      text-transform: none; letter-spacing: 0;
      font-size: 1.05rem; font-weight: 600; color: var(--text);
      margin: 0 0 0.4rem;
    }}
    .card h2.section-head {{
      text-transform: none; letter-spacing: 0;
      font-size: 1.05rem; font-weight: 600; color: var(--text);
      margin: 0 0 0.75rem; padding-bottom: 0.55rem;
      border-bottom: 1px solid var(--border);
    }}
    .list-hint {{
      font-size: 0.8rem; color: var(--muted); margin: 0 0 1rem;
      line-height: 1.45;
    }}
    .steps {{ margin: 0; padding-left: 1.1rem; color: var(--text); font-size: 0.9rem; }}
    .steps li {{ margin: 0.35rem 0; }}
    .btn {{
      display: inline-block; padding: 0.85rem 1.5rem;
      background: var(--accent); color: #fff !important;
      text-decoration: none; border-radius: 10px; font-weight: 600;
      font-size: 1.05rem; box-shadow: 0 2px 6px rgba(37,99,235,.25);
    }}
    .btn:hover {{ background: var(--accent-h); }}
    .btn-wrap {{ margin-top: 0.5rem; }}
    .btn-secondary {{
      display: inline-block; padding: 0.65rem 1.25rem;
      background: #475569; color: #fff !important; border: none; border-radius: 10px; font-weight: 600;
      font-size: 0.95rem; cursor: pointer; font-family: inherit;
      box-shadow: 0 2px 5px rgba(71,85,105,.22);
    }}
    .btn-secondary:hover {{ background: #334155; }}
    .metar-taf {{ margin-top: 1.2rem; padding-top: 1.15rem; border-top: 1px solid var(--border); text-align: left; }}
    .metar-taf-title {{ font-size: 1.02rem; font-weight: 600; margin: 0 0 0.4rem; color: var(--text); }}
    .metar-taf-lead {{ font-size: 0.82rem; color: var(--muted); margin: 0 0 0.5rem; line-height: 1.45; }}
    .metar-taf-subtitle {{ font-size: 0.88rem; font-weight: 600; margin: 0.85rem 0 0.35rem; color: var(--text); }}
    .metar-taf-kind-row {{ margin: 0.35rem 0 0.5rem; display: flex; flex-direction: column; align-items: flex-start; gap: 0.35rem; }}
    .metar-taf-regions-wrap {{ display: flex; flex-direction: column; gap: 0.65rem; margin: 0.35rem 0 0.75rem; }}
    .metar-taf-region {{
      border: 1px solid var(--border); border-radius: 10px; padding: 0.5rem 0.65rem 0.65rem;
      background: #fafbfc;
    }}
    .metar-taf-region > legend {{
      padding: 0 0.35rem; font-size: 0.82rem; font-weight: 600; color: var(--text);
    }}
    .metar-taf-checks {{ display: flex; flex-direction: column; align-items: flex-start; gap: 0.45rem; margin: 0.35rem 0 0.75rem; }}
    .metar-taf-checks-region {{ margin: 0.25rem 0 0; gap: 0.4rem; }}
    .metar-taf-label {{ font-size: 0.92rem; cursor: pointer; display: flex; align-items: center; gap: 0.4rem; }}
    .metar-taf-icao {{ color: var(--muted); font-size: 0.86em; font-weight: 500; }}
    .metar-taf-actions {{ margin-top: 0.25rem; }}
    .metar-taf-note {{ font-size: 0.75rem; color: var(--muted); margin: 0.75rem 0 0; line-height: 1.45; }}
    .primary-note {{ font-size: 0.82rem; color: var(--muted); margin: 0.75rem 0 0; line-height: 1.45; }}
    .primary-note code {{ font-size: 0.85em; background: #eef1f6; padding: 0.1em 0.35em; border-radius: 4px; }}
    .details-usage {{ margin-top: 1rem; text-align: left; }}
    .details-usage > summary {{ cursor: pointer; font-size: 0.88rem; color: var(--muted); }}
    .details-usage > summary:hover {{ color: var(--accent); }}
    .details-usage .steps {{ margin-top: 0.5rem; }}
    .card-restart-mini h2 {{ margin-bottom: 0.35rem; }}
    .restart-one {{
      font-size: 0.88rem; margin: 0 0 0.75rem; line-height: 1.55; color: var(--text);
    }}
    .restart-one kbd {{
      font-family: inherit; font-size: 0.88em;
      border: 1px solid var(--border); border-bottom-width: 2px;
      padding: 0.06em 0.32em; border-radius: 4px; background: #fff;
    }}
    .restart-one code {{ font-size: 0.9em; background: #eef1f6; padding: 0.06em 0.3em; border-radius: 4px; }}
    .restart-details > summary {{
      cursor: pointer; font-size: 0.88rem; font-weight: 600; color: var(--accent);
      padding: 0.35rem 0;
    }}
    .restart-details > summary:hover {{ color: var(--accent-h); }}
    .restart-details .restart-body {{ margin-top: 0.5rem; }}
    .card-restart h2 {{ margin-bottom: 0.5rem; }}
    .restart-steps {{ margin: 0; padding-left: 1.1rem; font-size: 0.88rem; color: var(--text); line-height: 1.55; }}
    .restart-steps li {{ margin: 0.4rem 0; }}
    .restart-steps ul.sub {{
      margin: 0.4rem 0 0; padding-left: 1.15rem;
      list-style: disc; font-size: 0.96em; color: var(--muted);
    }}
    .restart-steps ul.sub li {{ margin: 0.35rem 0; }}
    .restart-steps ul.sub strong {{ color: var(--text); font-weight: 600; }}
    .restart-steps code {{ font-size: 0.85em; background: #eef1f6; padding: 0.08em 0.35em; border-radius: 4px; }}
    .restart-steps kbd {{
      font-family: inherit; font-size: 0.88em;
      border: 1px solid var(--border); border-bottom-width: 2px;
      padding: 0.08em 0.35em; border-radius: 4px; background: #fff;
    }}
    .restart-note {{ font-size: 0.8rem; color: var(--muted); margin: 0.6rem 0 0; }}
    .hint {{ font-size: 0.8rem; color: var(--muted); margin: 0.6rem 0 0; }}
    .hint code {{ font-size: 0.85em; background: #eef1f6; padding: 0.1em 0.35em; border-radius: 4px; }}
    .panel {{ margin-bottom: 1rem; }}
    .panel > summary {{
      cursor: pointer; font-size: 0.9rem; color: var(--muted);
      padding: 0.35rem 0;
    }}
    .panel > summary:hover {{ color: var(--accent); }}
    .list-tight {{ margin: 0.4rem 0 0; padding-left: 1.1rem; font-size: 0.85rem; color: var(--muted); }}
    .panel-note {{ font-size: 0.78rem; color: var(--muted); margin: 0.5rem 0 0; }}
    ul.file-list {{ list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 0.5rem; }}
    li.item {{
      background: #f8fafc; border: 1px solid var(--border); border-radius: 10px;
      padding: 0.65rem 0.85rem;
    }}
    li.item.empty {{
      color: var(--muted); font-size: 0.92rem; background: #fafafa;
    }}
    .item-row {{
      display: grid;
      grid-template-columns: 1.75rem 1fr;
      gap: 0.5rem 0.75rem;
      align-items: start;
    }}
    .item-idx {{ font-size: 0.85rem; color: var(--muted); font-variant-numeric: tabular-nums; text-align: right; padding-top: 0.18rem; }}
    .item-name-wrap {{ min-width: 0; }}
    .item-name {{ font-weight: 600; font-size: 0.98rem; line-height: 1.4; word-break: break-word; color: var(--text); }}
    .item-na {{ font-size: 0.78rem; font-weight: 400; color: var(--muted); margin-left: 0.35rem; }}
    a.item-link {{
      font-weight: 600; font-size: 0.98rem; line-height: 1.4; word-break: break-word;
      color: var(--accent); text-decoration: none;
      border-bottom: 1px solid transparent;
    }}
    a.item-link:hover {{ border-bottom-color: rgba(37,99,235,.45); }}
    footer {{
      margin-top: 1.25rem; padding-top: 0.85rem; border-top: 1px solid var(--border);
      font-size: 0.72rem; color: var(--muted); line-height: 1.5;
    }}
    footer a {{ color: var(--muted); }}
    footer a:hover {{ color: var(--accent); }}
  </style>
</head>
<body>
  <!-- WXBriefing {build_e} -->
  <div class="wrap">
    <header>
      <div class="build-strip">
        <p class="build-strip-line">このサーバーの版: <strong>{build_e}</strong> · ポート <strong>{port}</strong>。<kbd>Ctrl</kbd>+<kbd>F5</kbd> か <a class="build-strip-link" href="/?_reload=1">開き直す</a>。表示の <strong>app.py</strong> の更新時刻（UTC）が、いま編集したファイルの保存時刻と一致するか確認してください。版が古いときは「別フォルダの app.py が動いている」ことが多いです。</p>
        <p class="build-strip-path">いま応答している <strong>app.py</strong>（Cursor で編集しているフォルダの <code>app.py</code> と<strong>文字列が完全一致</strong>するか確認）:<code>{app_path_e}</code></p>
      </div>
      <h1>{title_e}</h1>
      <p class="lead">config.json の内容どおりに取り込み、1本のPDFにまとめます。</p>
    </header>

    {metar_taf_section}
    <div class="card card-section card-charts">
      <h2 class="section-head">各種天気図・予報図</h2>
      <div class="charts-actions">
        <div class="btn-wrap"><a class="btn" href="/download_merged">各種天気図予報図を取得</a></div>
        <p class="primary-note">PNG などの画像は1枚1ページ。元がPDFならページがそのまま続きます。</p>
      </div>
      <details class="details-usage panel">
        <summary>使い方（手順・初回セットアップ）</summary>
        <ol class="steps">
          <li><strong>各種天気図予報図を取得</strong>のボタンで、一覧の資料を1本のPDFに保存</li>
          <li>1件だけ欲しいときは一覧の<strong>資料名</strong>をクリック（元ファイルの形式のまま保存）</li>
          <li>初回のみ <code>pip install -r requirements.txt</code> が必要です</li>
        </ol>
      </details>
    </div>

    <div class="card card-list">
      <h2 class="list-head">{html.escape(list_head)}</h2>
      <p class="list-hint">名前だけの一覧です。1件だけ保存するときは<strong>青い資料名</strong>をクリック。</p>
      <ul class="file-list">
        {body_list}
      </ul>
    </div>

    {auto_block}

    <div class="card card-restart-mini">
      <h2 class="list-head">サーバー再起動（設定を反映）</h2>
      <p class="restart-one"><strong>いちばんよく使う手順:</strong> コマンドプロンプトで <kbd>Ctrl</kbd>+<kbd>C</kbd> → 同じ窓で <code>python app.py</code> → ブラウザで <code>http://127.0.0.1:{port}/</code> を開き直し、必要なら <kbd>Ctrl</kbd>+<kbd>F5</kbd>。</p>
      <details class="restart-details panel">
        <summary>再始動の手順（全文・start.bat など）</summary>
        <div class="restart-body">
          <p class="restart-note"><code>config.json</code> を直したあと・挙動がおかしいときの詳細です。</p>
          <ol class="restart-steps">
            <li>サーバーが動いているウィンドウ（コマンドプロンプト）を前面にし、<kbd>Ctrl</kbd> + <kbd>C</kbd> を押して止める</li>
            <li>もう一度起動する（どれか一つ）
              <ul class="sub">
                <li><strong>コマンドプロンプトで起動している場合</strong>：止めた<strong>同じウィンドウ</strong>で、プロンプトが戻ったら <code>python app.py</code> を打って Enter。別のフォルダにいるときは先に <code>cd /d</code> でツールのフォルダへ移動してから実行</li>
                <li><strong>ウィンドウを閉じた場合</strong>：コマンドプロンプトを新しく開き、<code>cd /d</code> でツールのフォルダへ移動してから <code>python app.py</code></li>
                <li><strong>start.bat を使う場合</strong>：エクスプローラーでフォルダの <code>start.bat</code> をダブルクリック</li>
              </ul>
            </li>
            <li>ブラウザで <code>http://127.0.0.1:{port}/</code> を開き直す。画面が古いときは <kbd>Ctrl</kbd> + <kbd>F5</kbd> で強制再読み込み</li>
          </ol>
        </div>
      </details>
    </div>

    <footer>
      ポート {port} · <a href="/debug">動作確認</a> · ビルド {build_e}<br>
      ブラウザのアドレスは <strong>http://</strong>（https ではない）で開いてください。
    </footer>
  </div>
</body>
</html>
"""


def _http_auth_digest(s: str) -> bytes:
    return hashlib.sha256(s.encode("utf-8")).digest()


def http_auth_settings(cfg: dict) -> tuple[bool, str, str, str]:
    """
    http_auth 設定を返す。
    戻り値: (有効か, realm ASCII, username, password)
    """
    block = cfg.get("http_auth")
    if not isinstance(block, dict) or not block.get("enabled"):
        return False, "WX Briefing", "", ""
    realm = str(block.get("realm") or "WX Briefing").strip() or "WX Briefing"
    realm_ascii = realm.encode("ascii", errors="replace").decode("ascii") or "WX Briefing"
    user = str(block.get("username") or "").strip()
    pw = str(block.get("password") or "")
    return True, realm_ascii, user, pw


def _http_basic_credentials_ok(given_user: str, given_pw: str, expect_user: str, expect_pw: str) -> bool:
    if not expect_user or not expect_pw:
        return False
    return secrets.compare_digest(
        _http_auth_digest(given_user), _http_auth_digest(expect_user)
    ) and secrets.compare_digest(_http_auth_digest(given_pw), _http_auth_digest(expect_pw))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def _send_http_basic_unauthorized(self, realm_ascii: str, message: str) -> None:
        body = (message or "認証が必要です。").encode("utf-8")
        safe_realm = realm_ascii.replace('"', "'")[:200]
        self.send_response(401)
        self.send_header("WWW-Authenticate", f'Basic realm="{safe_realm}"')
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _require_http_auth(self, cfg: dict) -> bool:
        """
        認証不要または成功なら True。
        401 を返した場合は False（呼び出し側は return する）。
        """
        enabled, realm, eu, ep = http_auth_settings(cfg)
        if not enabled:
            return True
        if not eu or not ep:
            self._send_http_basic_unauthorized(
                realm,
                "http_auth.enabled が true ですが username または password が空です。"
                "config.json を修正してください。",
            )
            return False
        auth = self.headers.get("Authorization")
        if not auth or not auth.startswith("Basic "):
            self._send_http_basic_unauthorized(realm, "認証が必要です。")
            return False
        try:
            decoded = base64.b64decode(auth[6:].strip(), validate=True).decode("utf-8")
        except Exception:
            self._send_http_basic_unauthorized(realm, "認証に失敗しました。")
            return False
        if ":" not in decoded:
            self._send_http_basic_unauthorized(realm, "認証に失敗しました。")
            return False
        u, _, p = decoded.partition(":")
        if not _http_basic_credentials_ok(u, p, eu, ep):
            self._send_http_basic_unauthorized(realm, "認証に失敗しました。")
            return False
        return True

    def do_GET(self) -> None:  # noqa: N802
        try:
            self._do_get()
        except Exception:
            sys.stderr.write(traceback.format_exc())
            try:
                self.send_response(500)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                msg = b"Internal Server Error. See the console where python app.py is running."
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
            except Exception:
                pass

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._do_post()
        except Exception:
            sys.stderr.write(traceback.format_exc())
            try:
                self.send_response(500)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                msg = b"Internal Server Error. See the console where python app.py is running."
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
            except Exception:
                pass

    def _do_post(self) -> None:
        path = self.path.split("?", 1)[0]
        if path != "/metar_taf_pdf":
            self.send_error(404, "Not Found")
            return
        try:
            cfg = load_config()
        except Exception as e:  # noqa: BLE001
            self.send_error(500, f"config error: {e}")
            return
        if not self._require_http_auth(cfg):
            return
        mt = cfg.get("metar_taf_fetch")
        if not isinstance(mt, dict) or not mt.get("enabled"):
            self.send_error(404, "METAR/TAF は config で無効になっています")
            return
        raw_len = (self.headers.get("Content-Length") or "0").strip()
        try:
            n = int(raw_len)
        except ValueError:
            self.send_error(400, "Content-Length が不正です")
            return
        if n < 0 or n > 2_000_000:
            self.send_error(400, "リクエストが大きすぎます")
            return
        body = self.rfile.read(n)
        try:
            form = urllib.parse.parse_qs(body.decode("utf-8"), keep_blank_values=True)
        except UnicodeDecodeError:
            self.send_error(400, "フォームの文字コードを解釈できません")
            return
        icaos = [str(x).strip() for x in form.get("icao", []) if str(x).strip()]
        include_metar = bool(form.get("include_metar"))
        include_taf = bool(form.get("include_taf"))
        if not icaos:
            msg = (
                "空港が1つも選ばれていません。\n"
                "前の画面に戻り、空港にチェックを入れてください。"
            )
            b = msg.encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.send_header("X-WXBriefing-Build", portal_build_stamp())
            self.end_headers()
            self.wfile.write(b)
            return
        if not include_metar and not include_taf:
            msg = "METAR または TAF のいずれか（または両方）にチェックを入れてください。"
            b = msg.encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.send_header("X-WXBriefing-Build", portal_build_stamp())
            self.end_headers()
            self.wfile.write(b)
            return
        try:
            pdf, warns, _n_ok = build_metar_taf_pdf_bytes(cfg, icaos, include_metar, include_taf)
        except Exception as e:  # noqa: BLE001
            self.send_error(502, f"METAR/TAF PDF エラー: {e}")
            return
        jst = ZoneInfo("Asia/Tokyo")
        fname = f"metar_taf_{datetime.now(jst).strftime('%Y%m%d_%H%M')}.pdf"
        self.send_response(200)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
        self.send_header("Content-Length", str(len(pdf)))
        self.send_header("X-WXBriefing-Build", portal_build_stamp())
        self.send_header("Cache-Control", "no-store")
        if warns:
            self.send_header("X-Download-Warnings", _header_safe_ascii("; ".join(warns)))
        self.end_headers()
        self.wfile.write(pdf)

    def _do_get(self) -> None:
        path = self.path.split("?", 1)[0]
        parsed = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(parsed.query)
        try:
            cfg = load_config()
        except Exception as e:  # noqa: BLE001
            self.send_error(500, f"config error: {e}")
            return
        if not self._require_http_auth(cfg):
            return

        if path in ("/", "/index.html"):
            html = page_html(cfg).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("X-WXBriefing-Build", portal_build_stamp())
            self.send_header("Connection", "close")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return

        if path == "/debug":
            app_p = Path(__file__).resolve()
            lines = [
                f"build={portal_build_stamp()}",
                f"app={app_p}",
                f"config={CONFIG_PATH.resolve()}",
                f"cwd={Path.cwd()}",
                f"argv={sys.argv}",
                "",
                "この内容が表示されれば、このプロセスがブラウザに応答しています。",
                "8765 のまま古い画面しか見えない場合は、別プロセスがポートを占有している可能性があります。",
            ]
            body = "\n".join(lines).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-WXBriefing-Build", portal_build_stamp())
            self.send_header("Connection", "close")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/download":
            data, errs, warns, ok = build_zip(cfg)
            hdr_warns = list(warns)
            hdr_warns.extend(f"取得失敗: {e}" for e in errs)
            if ok == 0:
                msg = "すべての取得に失敗しました: " + "; ".join(errs) if errs else "対象ファイルがありません"
                self.send_error(502, msg)
                return
            fname = "wx_briefing_latest.zip"
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
            self.send_header("Content-Length", str(len(data)))
            if hdr_warns:
                self.send_header("X-Download-Warnings", _header_safe_ascii("; ".join(hdr_warns)))
            self.end_headers()
            self.wfile.write(data)
            return

        if path == "/download_merged":
            try:
                data, errs, warns, pages = build_merged_pdf(cfg)
            except RuntimeError as e:
                self.send_error(500, str(e))
                return
            hdr_warns = list(warns)
            hdr_warns.extend(f"取得・結合失敗: {e}" for e in errs)
            if pages == 0 or not data:
                msg = "PDF にできるページがありません: " + "; ".join(errs) if errs else "対象がありません"
                self.send_error(502, msg)
                return
            fname = "wx_briefing_merged.pdf"
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
            self.send_header("Content-Length", str(len(data)))
            hdr_warns.append(f"結合ページ数: {pages}")
            if hdr_warns:
                self.send_header("X-Download-Warnings", _header_safe_ascii("; ".join(hdr_warns)))
            self.end_headers()
            self.wfile.write(data)
            return

        if path == "/file":
            raw_i = (q.get("i") or [None])[0]
            raw_name = (q.get("name") or [None])[0]
            idx: int | None
            if raw_i is not None and str(raw_i).strip() != "":
                try:
                    idx = int(str(raw_i).strip())
                except ValueError:
                    self.send_error(400, "i は整数で指定してください")
                    return
            else:
                idx = None
            name = str(raw_name).strip() if raw_name else None
            if not name:
                name = None
            item, err = fetch_one_expanded_item(cfg, idx, name)
            if err or not item:
                self.send_error(400, err or "項目がありません")
                return
            url = item.get("url")
            fname = item.get("filename") or "download.bin"
            if not url:
                self.send_error(400, "URL がありません")
                return
            try:
                data, ctype = fetch_item_bytes(item)
            except urllib.error.HTTPError as e:
                self.send_error(502, f"取得失敗 HTTP {e.code}")
                return
            except urllib.error.URLError as e:
                self.send_error(502, f"取得失敗: {e.reason}")
                return
            except Exception as e:  # noqa: BLE001
                self.send_error(502, str(e))
                return
            ct = (ctype or "").split(";")[0].strip() if ctype else ""
            if not ct or ct == "application/octet-stream":
                ct = _guess_content_type(fname)
            disp = fname.replace('"', "'")
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Disposition", f'attachment; filename="{disp}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self.send_error(404, "Not Found")


def main() -> None:
    app_path = Path(__file__).resolve()
    os.chdir(app_path.parent)
    cfg_path = CONFIG_PATH.resolve()
    if not cfg_path.is_file():
        print(f"config.json が見つかりません: {cfg_path}", file=sys.stderr)
        raise SystemExit(1)

    cfg = load_config()
    auth_on, _, auth_user, auth_pw = http_auth_settings(cfg)
    if auth_on and (not auth_user or not auth_pw):
        print(
            "*** http_auth.enabled が true ですが username または password が空です。***\n"
            "config.json の http_auth を修正してから再起動してください。",
            file=sys.stderr,
        )
        raise SystemExit(1)
    port = int(cfg.get("port") or 18876)
    listen_raw = str(cfg.get("listen_host") or "127.0.0.1").strip()
    if listen_raw.lower() == "localhost":
        listen_host = "127.0.0.1"
    elif listen_raw in ("127.0.0.1", "0.0.0.0"):
        listen_host = listen_raw
    else:
        print(
            f'警告: config.json の listen_host="{listen_raw}" は未対応のため 127.0.0.1 にします（'
            "使えるのは 127.0.0.1 または 0.0.0.0 のみです）。",
            file=sys.stderr,
        )
        listen_host = "127.0.0.1"
    url_local = f"http://127.0.0.1:{port}/"
    class _ReuseHTTPServer(ThreadingHTTPServer):
        allow_reuse_address = True

    try:
        server = _ReuseHTTPServer((listen_host, port), Handler)
    except OSError as e:
        if getattr(e, "winerror", None) == 10048 or e.errno == 98:  # Address already in use
            print(
                f"\n*** ポート {port} はすでに使用中です ***\n"
                f"別の（古い）python app.py が動いていると、ブラウザはそちらに繋がり画面が更新されません。\n"
                "対処: タスクマネージャーで「Python」を終了するか、config.json の \"port\" を別の番号に変えてください。\n"
                f"（このプログラムの場所: {app_path}）\n"
                f"詳細: {e}\n",
                file=sys.stderr,
            )
        else:
            print(f"サーバーを起動できませんでした: {e}", file=sys.stderr)
        raise SystemExit(1) from e

    print("=" * 60)
    print(" WX Briefing ポータル")
    print("=" * 60)
    print("このプログラム（必ずこのパスが動いているか確認）:")
    print(" ", app_path)
    print("設定ファイル:")
    print(" ", cfg_path)
    print(f"ビルド: {portal_build_stamp()}  待受: {listen_host}:{port}")
    print("※ ブラウザのアドレスは必ず http:// で始めてください（https:// だと ERR_EMPTY_RESPONSE になります）。")
    print()
    if listen_host == "0.0.0.0":
        print("外部（LAN・VPS）からアクセス可能なモードです（listen_host=0.0.0.0）。")
        print("ブラウザでは次のように、このマシンの IP またはドメインを指定してください:")
        print(f"   http://<このサーバーのホスト名またはIP>:{port}/")
        print("同一マシン上では次の URL でも開けます:")
        print(" ", url_local)
        print("  診断:", f"http://127.0.0.1:{port}/debug")
        print()
        if auth_on:
            print(
                "【HTTP 認証】http_auth が有効です（ブラウザの Basic 認証）。"
                "すべてのページ・ダウンロード・METAR/TAF PDF で ID/パスワードが必要です。"
            )
        else:
            print(
                "【警告】http_auth が無効です。インターネットに晒す場合は config.json で "
                '"http_auth": { "enabled": true, "username": "...", "password": "..." } を設定してください。',
                file=sys.stderr,
            )
            print(
                "（リバースプロキシで HTTPS を終端する運用も推奨です。パスワードは平文で config に保存されます。）"
            )
    else:
        print("次のURLをブラウザにコピーして開いてください（同一PCのみ）:")
        print(" ", url_local)
        print("  診断（このサーバーが応答しているか）:", f"http://127.0.0.1:{port}/debug")
    print("  古い画面のとき: Ctrl+F5 またはシークレットウィンドウ")
    print()
    print("このウィンドウは閉じないでください。終了は Ctrl+C")
    print("=" * 60)

    if cfg.get("open_browser", True):
        bust = urllib.parse.urlencode({"_": f"{PORTAL_BUILD}-{int(time.time())}"})
        open_url = f"{url_local}?{bust}"

        def open_browser() -> None:
            time.sleep(0.5)
            webbrowser.open(open_url)

        threading.Thread(target=open_browser, daemon=True).start()
        print("数秒後にブラウザを開きます（キャッシュ回避用クエリ付き）。止めたいときは config.json で \"open_browser\": false。")
    else:
        print('ブラウザは自動で開きません（config.json の "open_browser": false）。')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n終了しました。")


if __name__ == "__main__":
    main()
