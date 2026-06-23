"""
Streamlit 版 WX Briefing（app.py のロジックを再利用）。

手順の詳細は「Streamlit手順.md」（具体手順・だれでも版）を開いてください。

最短（ローカル・Windows 推奨）:
  python -m pip install -r requirements.txt
  python -m playwright install chromium
  python -m streamlit run streamlit_app.py
"""
from __future__ import annotations

import html
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# app.py と同じディレクトリをカレントに（config.json・相対パス）
_ROOT = Path(__file__).resolve().parent
os.chdir(_ROOT)
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import app as wx  # noqa: E402

# UI 地域見出し（app.py の UI_REGION_GROUPS_* の title と一致させる）
TOHOKU_KANTO_UI_TITLE = "東北・関東"
KYUSHU_UI_TITLE = "九州"
# METAR・TAF: 東北・関東のクイック選択（福島・仙台・新潟）
METAR_TAF_TOHOKU_FSSN_ICAOS = ("RJSF", "RJSS", "RJSN")
METAR_TAF_TOHOKU_FSSN_PRESET_KEY = "mt_tohoku_fssn"
METAR_TAF_COLLAPSIBLE_REGIONS = frozenset({TOHOKU_KANTO_UI_TITLE, KYUSHU_UI_TITLE})


def _metar_taf_fssn_checkbox_keys() -> list[str]:
    return [f"mt_ap_{icao}" for icao in METAR_TAF_TOHOKU_FSSN_ICAOS]


def _metar_taf_fssn_preset_callback() -> None:
    """「福島・仙台・新潟」: 3 空港の個別チェックを一括オン/オフ。"""
    v = bool(st.session_state.get(METAR_TAF_TOHOKU_FSSN_PRESET_KEY, False))
    for k in _metar_taf_fssn_checkbox_keys():
        st.session_state[k] = v


def _sync_fssn_preset_from_children() -> None:
    """3 空港がすべてオンならプリセットもオン、1 つでもオフならプリセットもオフ。"""
    keys = _metar_taf_fssn_checkbox_keys()
    if not keys:
        return
    st.session_state[METAR_TAF_TOHOKU_FSSN_PRESET_KEY] = all(
        bool(st.session_state.get(k, False)) for k in keys
    )


def _append_metar_taf_fssn_preset(selected: list[str]) -> None:
    if not st.session_state.get(METAR_TAF_TOHOKU_FSSN_PRESET_KEY, False):
        return
    for icao in METAR_TAF_TOHOKU_FSSN_ICAOS:
        if icao not in selected:
            selected.append(icao)


def _group_select_all_callback(sel_key: str, target_keys: list[str]):
    """「すべての項目を選択する」用: sel_key の真偽で target_keys を一括オン/オフ。"""

    def _sync() -> None:
        v = bool(st.session_state.get(sel_key, False))
        for k in target_keys:
            st.session_state[k] = v

    return _sync


def _sync_select_all_from_children(sel_key: str, target_keys: list[str]) -> None:
    """子チェックがすべてオンなら親もオン、1つでもオフなら親もオフ（親→子は on_change 側）。"""
    if not target_keys:
        return
    all_on = all(bool(st.session_state.get(k, False)) for k in target_keys)
    st.session_state[sel_key] = all_on


def _region_title_heading(title: str) -> None:
    """地域ブロック先頭の見出し（HTML エスケープ済み）。"""
    st.markdown(
        f'<p class="wx-region-title"><strong>{html.escape(title)}</strong></p>',
        unsafe_allow_html=True,
    )


def _region_select_all_header(
    sel_key: str,
    target_keys: list[str],
    *,
    keyed_container: bool = True,
) -> None:
    """見出しの直後に「すべての項目を選択する」。コンテナ key は sel_key ごとに一意。"""
    _sync_select_all_from_children(sel_key, target_keys)
    row_key = re.sub(r"[^0-9a-zA-Z_\-]", "_", sel_key).strip("_") or "selall"

    def _draw() -> None:
        st.checkbox(
            "すべての項目を選択する",
            key=sel_key,
            on_change=_group_select_all_callback(sel_key, target_keys),
        )

    if keyed_container:
        with st.container(key=f"wx_selall_header_{row_key}"):
            _draw()
    else:
        _draw()


def _norm_sigwx_area(area: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(area).lower())


def _sigwx_product_rows(sig: dict) -> list[dict]:
    """結合 PDF 用チェックボックス行（area 正規化済み）。"""
    area_labels = getattr(wx, "LOW_LEVEL_SIGWX_AREA_LABELS", {})
    prows = sig.get("products")
    if isinstance(prows, list) and prows:
        rows: list[dict] = []
        for p in prows:
            if not isinstance(p, dict):
                continue
            a = _norm_sigwx_area(str(p.get("area") or ""))
            if not a:
                continue
            lab = str(p.get("label") or p.get("name") or "").strip()
            if not lab:
                lab = area_labels.get(a, a)
            rows.append({"area": a, "label": lab})
        if rows:
            return rows
    a0 = _norm_sigwx_area(str(sig.get("area") or "fbsn"))
    lab0 = area_labels.get(a0, a0)
    return [{"area": a0, "label": lab0}]


def _detailed_sigwx_product_rows(dsig: dict) -> list[dict]:
    out: list[dict] = []
    dcf = getattr(wx, "detailed_sigwx_fig_canonical", None)
    if not callable(dcf):
        return out
    prows = dsig.get("products")
    if not isinstance(prows, list):
        return out
    for p in prows:
        if not isinstance(p, dict):
            continue
        fig = p.get("fig") or p.get("areano") or p.get("value")
        if not fig:
            continue
        fk = dcf(str(fig))
        if not fk:
            continue
        lab = str(p.get("label") or p.get("name") or "").strip() or fk
        off = getattr(wx, "detailed_sigwx_official_ja_label", None)
        if callable(off):
            o = off(fk)
            if o:
                lab = o
        out.append({"fig_key": fk, "label": lab})
    return out


def _typhoon_product_rows(cfg_typhoon: dict) -> list[dict]:
    fn = getattr(wx, "typhoon_product_rows", None)
    if callable(fn):
        return fn(cfg_typhoon)
    return []


def _typhoon_product_lookup(cfg_typhoon: dict) -> dict[str, dict]:
    fn = getattr(wx, "typhoon_product_lookup", None)
    if callable(fn):
        return fn(cfg_typhoon)
    norm = getattr(wx, "typhoon_product_id_norm", None)
    prows = cfg_typhoon.get("products")
    if not isinstance(prows, list):
        return {}
    out: dict[str, dict] = {}
    for p in prows:
        if not isinstance(p, dict):
            continue
        raw_id = str(p.get("id") or "")
        pid = norm(raw_id) if callable(norm) else raw_id.strip().lower()
        if pid:
            out[pid] = p
    return out


def _typhoon_product_fetch_ready(product: dict) -> bool:
    """page_url / url の有無（app.py 未再起動時も Streamlit 側で判定）。"""
    if not isinstance(product, dict):
        return False
    fn = getattr(wx, "typhoon_product_fetch_ready", None)
    if callable(fn):
        return bool(fn(product))
    page_url = str(product.get("page_url") or "").strip()
    url = str(product.get("url") or "").strip()
    return page_url.startswith("https://") or url.startswith("https://")


def _inject_wx_streamlit_ui_styles() -> None:
    """METAR 枠＝青、天気図枠＝赤、資料一覧枠＝緑。生成ボタン＝青。文字サイズは読みやすめに拡大。"""
    st.markdown(
        """
        <style>
          :root {
            --wx-font-base: 17px;
            --wx-font-body: 1.08rem;
            --wx-font-caption: 1rem;
            --wx-font-checkbox: 1.08rem;
            --wx-font-button: 1.06rem;
            --wx-font-subheader: 1.42rem;
            --wx-font-region-title: 1.18rem;
          }
          .stApp {
            font-size: var(--wx-font-base);
          }
          .stApp h1 {
            font-size: 2.15rem !important;
            line-height: 1.25 !important;
          }
          .stApp h2,
          .stApp h3 {
            font-size: var(--wx-font-subheader) !important;
            line-height: 1.35 !important;
          }
          .stApp [data-testid="stMarkdownContainer"] p,
          .stApp [data-testid="stMarkdownContainer"] li,
          .stApp [data-testid="stText"] {
            font-size: var(--wx-font-body) !important;
            line-height: 1.55 !important;
          }
          .stApp [data-testid="stMarkdownContainer"] strong {
            font-size: 1.14rem !important;
          }
          .stApp [data-testid="stCaptionContainer"] p,
          .stApp [data-testid="stCaptionContainer"] {
            font-size: var(--wx-font-caption) !important;
            line-height: 1.5 !important;
          }
          .stApp [data-testid="stCheckbox"] label p,
          .stApp [data-testid="stCheckbox"] label span,
          .stApp [data-testid="stCheckbox"] label div {
            font-size: var(--wx-font-checkbox) !important;
            line-height: 1.45 !important;
          }
          .stApp [data-testid="stButton"] button,
          .stApp [data-testid="stDownloadButton"] button {
            font-size: var(--wx-font-button) !important;
            padding: 0.5rem 1.05rem !important;
            line-height: 1.4 !important;
          }
          .stApp [data-testid="stExpander"] summary p,
          .stApp [data-testid="stExpander"] summary span {
            font-size: 1.12rem !important;
            line-height: 1.45 !important;
          }
          .stApp [data-testid="stSidebar"] {
            font-size: var(--wx-font-base);
          }
          .stApp [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
            font-size: var(--wx-font-body) !important;
          }
          .stApp .wx-region-title {
            font-size: var(--wx-font-region-title) !important;
            line-height: 1.45 !important;
            margin: 0 0 0.35rem 0 !important;
          }
          /* METAR / TAF 種別チェック: 横並び・ラベル強調（label p のみ。span も触ると二重表示になりやすい） */
          .st-key-wx_metar_taf_kind_row [data-testid="stHorizontalBlock"] {
            width: fit-content !important;
            max-width: 100% !important;
            gap: 1.75rem !important;
            align-items: center !important;
            margin-bottom: 0.35rem !important;
          }
          .st-key-wx_metar_taf_kind_row [data-testid="column"] {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: auto !important;
          }
          .st-key-wx_metar_taf_kind_row [data-testid="stCheckbox"] label {
            white-space: nowrap !important;
          }
          .st-key-wx_metar_taf_kind_row [data-testid="stCheckbox"] label p {
            font-size: 1.38rem !important;
            font-weight: 700 !important;
            letter-spacing: 0.03em !important;
            line-height: 1.3 !important;
            margin: 0 !important;
            white-space: nowrap !important;
          }
          .st-key-wx_metar_taf_kind_row [data-testid="stCheckbox"] [data-testid="stMarkdownContainer"] p {
            white-space: nowrap !important;
          }
          .st-key-wx_metar_taf_kind_row [data-testid="stCheckbox"] [data-baseweb="checkbox"]:has(input:checked)
            > span:first-of-type {
            background-color: #2563eb !important;
            border-color: #2563eb !important;
          }
          .st-key-wx_metar_taf_frame {
            border: 2px solid #2563eb !important;
            border-radius: 14px;
            padding: 10px 12px 12px !important;
            margin: 8px 2px 18px 2px !important;
            background: transparent !important;
            overflow: visible !important;
          }
          .st-key-wx_charts_frame {
            border: 2px solid #dc2626 !important;
            border-radius: 14px;
            padding: 10px 12px 12px !important;
            margin: 8px 2px 18px 2px !important;
            background: transparent !important;
            overflow: visible !important;
          }
          .st-key-wx_file_list_frame {
            border: 2px solid #16a34a !important;
            border-radius: 14px;
            padding: 10px 12px 12px !important;
            margin: 8px 2px 18px 2px !important;
            background: transparent !important;
            overflow: visible !important;
          }
          [class*="st-key-mt_go"] button {
            background-color: #2563eb !important;
            border: 1px solid #1d4ed8 !important;
            color: #ffffff !important;
          }
          [class*="st-key-mt_go"] button:hover {
            background-color: #1d4ed8 !important;
            border-color: #1e40af !important;
            color: #ffffff !important;
          }
          [class*="st-key-mt_go"] button:focus-visible {
            box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.35) !important;
          }
          /* 「すべての項目」先頭行のチェック位置 */
          [class*="st-key-wx_selall_header_"] [data-testid="stCheckbox"] [data-baseweb="checkbox"] {
            align-items: center !important;
          }
          [class*="st-key-wx_selall_header_"]
            [data-testid="stCheckbox"]
            [data-baseweb="checkbox"]
            > span:first-of-type {
            margin-top: 0 !important;
          }
          /* METAR/TAF 枠内のみチェックオン時を青（各種天気図枠は既定の赤系のまま） */
          .st-key-wx_metar_taf_frame [data-testid="stCheckbox"] [data-baseweb="checkbox"]:has(input:checked)
            > span:first-of-type {
            background-color: #2563eb !important;
            border-left-color: #2563eb !important;
            border-right-color: #2563eb !important;
            border-top-color: #2563eb !important;
            border-bottom-color: #2563eb !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _wx_build_display() -> str:
    """キャプション用の短いビルド行（PORTAL_BUILD | app.py UTC）。古い app.py では PORTAL_BUILD のみ。"""
    fn = getattr(wx, "portal_build_short_stamp", None)
    if callable(fn):
        try:
            return str(fn())
        except Exception:  # noqa: BLE001
            pass
    return str(getattr(wx, "PORTAL_BUILD", "unknown"))


def _wx_runtime_diag() -> dict[str, str | bool]:
    fn = getattr(wx, "portal_runtime_diagnostics", None)
    if callable(fn):
        try:
            raw = fn()
            if isinstance(raw, dict):
                return raw
        except Exception:  # noqa: BLE001
            pass
    return {
        "app_py": str(_ROOT / "app.py"),
        "repo_root": str(_ROOT),
        "portal_build": str(getattr(wx, "PORTAL_BUILD", "unknown")),
        "build_stamp": _wx_build_display(),
    }


def _duplicate_repo_warning(cur_root: Path) -> str | None:
    """GitHub Desktop 等で別フォルダの古いコピーを使っていないか。"""
    dup = (Path.home() / "Documents/GitHub/WX-BRIEF-JCAB-for-PPL-CPL-IR").resolve()
    cur = cur_root.resolve()
    if not dup.is_dir() or dup == cur:
        return None
    dup_app = dup / "app.py"
    if not dup_app.is_file():
        return None
    cur_build = str(getattr(wx, "PORTAL_BUILD", ""))
    m = re.search(r'PORTAL_BUILD\s*=\s*"([^"]+)"', dup_app.read_text(encoding="utf-8", errors="replace"))
    dup_build = m.group(1) if m else "?"
    if dup_build == cur_build:
        return None
    return (
        f"別フォルダに古いコピーがあります: `{dup}`（PORTAL_BUILD={dup_build}）。"
        f" いま起動中: `{cur}`（{cur_build}）。"
        " GitHub Desktop / ターミナルの作業ディレクトリがこちらか確認してください。"
    )


def _runtime_health_warnings() -> list[str]:
    """起動直後・画面に出す切り分け用の注意。"""
    diag = _wx_runtime_diag()
    warns: list[str] = []
    dup = _duplicate_repo_warning(_ROOT)
    if dup:
        warns.append(dup)
    if diag.get("coastline_overlay_png") and not diag.get("coastline_overlay_png_ok"):
        warns.append(
            "海岸線オーバーレイ PNG が見つかりません: "
            f"{diag.get('coastline_overlay_png')}（repo: {diag.get('repo_root')}）"
        )
    if diag.get("coastline_overlay_png") and not diag.get("coastline_overlay_fn_ok"):
        warns.append("海岸線オーバーレイ処理が未ロードの app.py です。Streamlit を再起動してください。")
    return warns


def _render_runtime_sidebar() -> None:
    """いま動いているコードのパス・機能状態（切り分け用）。"""
    diag = _wx_runtime_diag()
    with st.sidebar.expander("実行環境（切り分け）", expanded=False):
        st.caption(f"PORTAL_BUILD: {diag.get('portal_build', '?')}")
        st.code(str(diag.get("app_py") or "?"), language=None)
        st.caption(f"repo: {diag.get('repo_root', '?')}")
        if diag.get("coastline_overlay_png"):
            ok = bool(diag.get("coastline_overlay_png_ok"))
            st.caption(
                f"海岸線オーバーレイ: {'OK' if ok else 'NG'} — {diag.get('coastline_overlay_png')}"
            )
        bg_fn = getattr(wx, "background_refresh_snapshot", None)
        if callable(bg_fn):
            bg = bg_fn()
            if bg.get("scheduler_running"):
                daily = bg.get("daily_refresh_jst") or ""
                iv = bg.get("interval_minutes") or 0
                if daily and iv:
                    st.caption(f"自動更新: 毎日 {daily} ＋ {iv} 分ごと")
                elif daily:
                    st.caption(f"自動更新: 毎日 {daily}")
                elif iv:
                    st.caption(f"自動更新: {iv} 分ごと")
                if bg.get("next_refresh_jst"):
                    st.caption(f"次回更新予定: {bg['next_refresh_jst']}")
                if bg.get("refresh_running"):
                    st.caption("いま資料を自動取得中…")
                if bg.get("last_refresh_utc"):
                    st.caption(f"最終自動取得: {bg['last_refresh_utc']}")
                if bg.get("last_merged_pdf_utc"):
                    st.caption(f"結合PDF温め: {bg['last_merged_pdf_utc']}")
        stamp_path = _ROOT / "data" / "last_materials_refresh.json"
        if stamp_path.is_file():
            try:
                stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
                if isinstance(stamp, dict) and stamp.get("at_jst"):
                    st.caption(f"cron/launchd 最終更新: {stamp['at_jst']}")
            except (OSError, json.JSONDecodeError):
                pass
        st.caption(
            "変更が反映されないとき: ①上の app.py パスが編集したフォルダか "
            "②結合PDFを再生成したか ③Streamlit を再起動したか を確認。"
        )


# 衛星などのキャプション文字は app.py の Pillow（_hrpns_caption_font）で描画する。
# Streamlit Cloud: リポジトリ直下の packages.txt で fonts-noto-cjk を入れる。
# 自前フォント: wx-briefing-portal/fonts/ に .otf/.ttf を置くか、環境変数
# WX_BRIEFING_CAPTION_FONT に絶対パスを指定する。


def _auth_expected() -> tuple[str, str]:
    """
    Streamlit Cloud: Secrets にユーザー名・パスワードが両方あるときだけ採用。
    それ以外は config.json の http_auth（ローカルと同じルール）。
    """
    try:
        sec = st.secrets
        u = str(sec["AUTH_USERNAME"]).strip()
        p = str(sec["AUTH_PASSWORD"])
        if u and p:
            return u, p
    except Exception:
        pass
    cfg = wx.load_config()
    block = cfg.get("http_auth")
    if isinstance(block, dict):
        return str(block.get("username") or "").strip(), str(block.get("password") or "")
    return "", ""


def _ensure_login() -> bool:
    if st.session_state.get("_auth_ok"):
        return True
    cfg = wx.load_config()
    block = cfg.get("http_auth")
    if isinstance(block, dict) and not bool(block.get("enabled")):
        # app.py の HTTP サーバと同様: 認証オフならログイン不要
        return True
    st.title("WX Briefing")
    st.caption("ログインしてください。")
    u_in = st.text_input("Username", key="login_u")
    p_in = st.text_input("Password", type="password", key="login_p")
    if st.button("ログイン", type="primary"):
        eu, ep = _auth_expected()
        if not eu or not ep:
            st.error(
                "認証情報がありません。ローカルでは config.json の http_auth、"
                "Streamlit Cloud では Secrets に AUTH_USERNAME / AUTH_PASSWORD を設定してください。"
            )
            return False
        if wx._http_basic_credentials_ok(u_in, p_in, eu, ep):  # noqa: SLF001
            st.session_state["_auth_ok"] = True
            st.rerun()
        else:
            st.error("ユーザー名またはパスワードが違います。")
    return False


@st.cache_data(ttl=30)
def _cfg_cached():
    return wx.load_config()


def _collect_selected_metar_taf_icaos(airports: list[dict]) -> list[str]:
    """METAR・TAF 欄でオンになっている ICAO（福島・仙台・新潟プリセット含む）。"""
    selected: list[str] = []
    for ap in airports:
        icao = str(ap.get("icao") or "").strip().upper()
        if not icao:
            continue
        if st.session_state.get(f"mt_ap_{icao}", False):
            selected.append(icao)
    for icao in METAR_TAF_TOHOKU_FSSN_ICAOS:
        if st.session_state.get(METAR_TAF_TOHOKU_FSSN_PRESET_KEY, False) and icao not in selected:
            selected.append(icao)
    return selected


def _sync_charts_from_metar_taf_selection(cfg: dict) -> None:
    """
    METAR・TAF で選んだ空港に応じ、結合 PDF 用の
    飛行場時系列予報・下層悪天予想図・詳細版のチェックを同期する。

    METAR・TAF の選択が変わったときだけ実行し、
    「各種天気図・予報図」での手動チェックは上書きしない。
    """
    fn = getattr(wx, "chart_links_for_metar_taf_icaos", None)
    if not callable(fn):
        return
    airports = wx.metar_taf_airports_from_config(cfg)
    current = tuple(sorted(_collect_selected_metar_taf_icaos(airports)))
    prev = st.session_state.get("_metar_taf_sync_snapshot")
    if prev == current:
        return
    st.session_state["_metar_taf_sync_snapshot"] = current

    links = fn(list(current), cfg)
    taf_sel = set(links.get("taf_icaos") or [])
    sigwx_sel = set(links.get("sigwx_areas") or [])
    fig_sel = set(links.get("detailed_figs") or [])

    taf = cfg.get("jma_airinfo_taf")
    if isinstance(taf, dict) and taf.get("enabled"):
        prows = [
            p
            for p in (taf.get("products") or [])
            if isinstance(p, dict) and str(p.get("icao") or "").strip()
        ]
        for pr in prows:
            icao = str(pr.get("icao")).strip().upper()
            st.session_state[f"merge_taf_ap_{icao}"] = icao in taf_sel
        if prows:
            st.session_state["merge_taf_p1"] = bool(links.get("taf_part1"))
            st.session_state["merge_taf_p2"] = bool(links.get("taf_part2"))

    sig = cfg.get("jma_airinfo_low_level_sigwx")
    if (
        isinstance(sig, dict)
        and sig.get("enabled")
        and not str(sig.get("url") or "").strip()
    ):
        for row in _sigwx_product_rows(sig):
            area = row["area"]
            st.session_state[f"merge_sigwx_{area}"] = area in sigwx_sel

    dsig = cfg.get("jma_airinfo_low_level_detailed_sigwx")
    if isinstance(dsig, dict) and dsig.get("enabled"):
        for row in _detailed_sigwx_product_rows(dsig):
            fk = row["fig_key"]
            st.session_state[f"merge_dsig_{fk}"] = fk in fig_sel


def _render_metar_taf_region_airports(
    title: str,
    aps: list[dict],
    selected: list[str],
    *,
    mt_keys: list[str] | None,
    mt_selall_key: str | None,
    vertical: bool = False,
) -> None:
    """METAR・TAF 地域ブロック内: 全選択・プリセット・空港チェック。"""
    if mt_keys is not None and mt_selall_key is not None:
        _region_select_all_header(
            mt_selall_key,
            mt_keys,
            keyed_container=not vertical,
        )
        if title == TOHOKU_KANTO_UI_TITLE:
            _sync_fssn_preset_from_children()
            st.checkbox(
                "福島・仙台・新潟",
                value=False,
                key=METAR_TAF_TOHOKU_FSSN_PRESET_KEY,
                on_change=_metar_taf_fssn_preset_callback,
                help="福島空港・仙台空港・新潟空港（RJSF / RJSS / RJSN）を PDF に含めます。",
            )
        if vertical:
            for ap in aps:
                icao = ap["icao"]
                lab = ap["label"]
                if st.checkbox(
                    f"{lab} ({icao})",
                    value=False,
                    key=f"mt_ap_{icao}",
                ):
                    selected.append(icao)
        else:
            cols = st.columns(3)
            for i, ap in enumerate(aps):
                icao = ap["icao"]
                lab = ap["label"]
                with cols[i % 3]:
                    if st.checkbox(
                        f"{lab} ({icao})",
                        value=False,
                        key=f"mt_ap_{icao}",
                    ):
                        selected.append(icao)
    elif vertical:
        for ap in aps:
            icao = ap["icao"]
            lab = ap["label"]
            if st.checkbox(
                f"{lab} ({icao})",
                value=False,
                key=f"mt_ap_{icao}",
            ):
                selected.append(icao)
    else:
        cols = st.columns(3)
        for i, ap in enumerate(aps):
            icao = ap["icao"]
            lab = ap["label"]
            with cols[i % 3]:
                if st.checkbox(
                    f"{lab} ({icao})",
                    value=False,
                    key=f"mt_ap_{icao}",
                ):
                    selected.append(icao)


def _render_metar_taf(cfg: dict) -> None:
    airports = wx.metar_taf_airports_from_config(cfg)
    block = cfg.get("metar_taf_fetch")
    if not isinstance(block, dict) or not block.get("enabled") or not airports:
        return
    st.subheader("METAR・TAF")
    st.caption(
        "まず METAR / TAF の種別を選び、続けて空港を選んで PDF を生成します（初期状態はすべてオフ）。"
        " 東北・関東・九州は折りたたみから展開できます。"
        " 空港を選ぶと、下の「各種天気図・予報図」の飛行場時系列予報・下層悪天予想図にも"
        " 対応する項目が自動でオンになります。"
    )
    with st.container(key="wx_metar_taf_kind_row"):
        col_met, col_taf = st.columns(2, gap="medium")
        with col_met:
            want_met = st.checkbox("METAR", value=False, key="mt_met")
        with col_taf:
            want_taf = st.checkbox("TAF", value=False, key="mt_taf")
    selected: list[str] = []
    for title, aps in wx.group_metar_taf_airports_by_region(airports):
        if not aps:
            continue
        _mt_keys: list[str] | None = None
        _mt_selall_key: str | None = None
        if title == TOHOKU_KANTO_UI_TITLE:
            _mt_keys = [f"mt_ap_{str(ap['icao']).strip()}" for ap in aps]
            _mt_selall_key = "mt_selall_tohoku_kanto"
        elif title == KYUSHU_UI_TITLE:
            _mt_keys = [f"mt_ap_{str(ap['icao']).strip()}" for ap in aps]
            _mt_selall_key = "mt_selall_kyushu"
        if title in METAR_TAF_COLLAPSIBLE_REGIONS:
            with st.expander(title, expanded=False):
                _render_metar_taf_region_airports(
                    title,
                    aps,
                    selected,
                    mt_keys=_mt_keys,
                    mt_selall_key=_mt_selall_key,
                    vertical=True,
                )
        else:
            with st.container(border=True):
                _region_title_heading(title)
                _render_metar_taf_region_airports(
                    title,
                    aps,
                    selected,
                    mt_keys=_mt_keys,
                    mt_selall_key=_mt_selall_key,
                )
    _append_metar_taf_fssn_preset(selected)
    if st.button("METAR/TAF PDF を生成", type="secondary", key="mt_go"):
        if not selected:
            st.warning("空港を1つ以上選んでください。")
        elif not want_met and not want_taf:
            st.warning("METAR と TAF のどちらかにチェックを入れてください。")
        else:
            with st.spinner("取得・PDF 作成中…"):
                try:
                    pdf, warns, _n = wx.build_metar_taf_pdf_bytes(cfg, selected, want_met, want_taf)
                except Exception as e:  # noqa: BLE001
                    st.error(f"エラー: {e}")
                else:
                    if warns:
                        st.warning("\n".join(warns))
                    fn = f"metar_taf_{datetime.now(wx.JST).strftime('%Y%m%d_%H%M')}.pdf"
                    st.session_state["_mt_pdf"] = pdf
                    st.session_state["_mt_fn"] = fn
                    st.rerun()

    if st.session_state.get("_mt_pdf"):
        st.download_button(
            label="直近で生成した METAR/TAF PDF をダウンロード",
            data=st.session_state["_mt_pdf"],
            file_name=st.session_state.get("_mt_fn") or "metar_taf.pdf",
            mime="application/pdf",
            key="mt_dl",
        )
        if st.button("生成済み PDF をクリア", key="mt_clear"):
            st.session_state.pop("_mt_pdf", None)
            st.session_state.pop("_mt_fn", None)
            st.rerun()


def _render_charts_zip(cfg: dict) -> None:
    st.subheader("各種天気図・予報図")
    _sync_charts_from_metar_taf_selection(cfg)

    taf = cfg.get("jma_airinfo_taf")
    if isinstance(taf, dict) and taf.get("enabled"):
        with st.expander("飛行場時系列予報（結合 PDF に含める範囲）", expanded=False):
            st.caption(
                "空港と PART1 / PART2 を選び、「結合 PDF を生成」に反映されます。"
                " 初期状態はすべてオフです。全空港かつ PART1+2 をオンにすると従来どおりの展開になります。"
            )
            prows = [
                p
                for p in (taf.get("products") or [])
                if isinstance(p, dict) and str(p.get("icao") or "").strip()
            ]
            if not prows:
                st.info("config の `jma_airinfo_taf.products` に ICAO を追加してください。")
            else:
                gfn = getattr(wx, "group_taf_products_by_region", None)
                blocks = gfn(prows) if callable(gfn) else [("対象空港", prows)]
                for title, plist in blocks:
                    if not plist:
                        continue
                    _taf_keys: list[str] | None = None
                    _taf_selall_key: str | None = None
                    if title == TOHOKU_KANTO_UI_TITLE:
                        _taf_keys = [
                            f"merge_taf_ap_{str(pr.get('icao')).strip().upper()}"
                            for pr in plist
                        ]
                        _taf_selall_key = "merge_taf_selall_tohoku_kanto"
                    elif title == KYUSHU_UI_TITLE:
                        _taf_keys = [
                            f"merge_taf_ap_{str(pr.get('icao')).strip().upper()}"
                            for pr in plist
                        ]
                        _taf_selall_key = "merge_taf_selall_kyushu"
                    _region_title_heading(title)
                    if _taf_keys is not None and _taf_selall_key is not None:
                        _region_select_all_header(
                            _taf_selall_key,
                            _taf_keys,
                            keyed_container=False,
                        )
                    for pr in plist:
                        icao = str(pr.get("icao")).strip().upper()
                        lab = str(pr.get("label") or pr.get("name") or icao).strip()
                        st.checkbox(
                            f"{lab}（{icao}）",
                            value=False,
                            key=f"merge_taf_ap_{icao}",
                        )
                st.checkbox("PART1（QMCD98_）", value=False, key="merge_taf_p1")
                st.checkbox("PART2（QMCJ98_）", value=False, key="merge_taf_p2")

    sigwx_cfg = cfg.get("jma_airinfo_low_level_sigwx")
    if (
        isinstance(sigwx_cfg, dict)
        and sigwx_cfg.get("enabled")
        and not str(sigwx_cfg.get("url") or "").strip()
    ):
        with st.expander("下層悪天予想図（結合 PDF・時系列 ft=39）", expanded=False):
            st.caption(
                "地域を選び、「結合 PDF を生成」に反映されます。初期状態はすべてオフです。"
                " すべてオンで従来どおり全地域を含めます。"
            )
            srows = _sigwx_product_rows(sigwx_cfg)
            if not srows:
                st.info("config の `jma_airinfo_low_level_sigwx.products` に `area` を追加してください。")
            else:
                for sr in srows:
                    a = sr["area"]
                    st.checkbox(
                        f"{sr['label']}（{a}）",
                        value=False,
                        key=f"merge_sigwx_{a}",
                    )

    dsig_cfg = cfg.get("jma_airinfo_low_level_detailed_sigwx")
    if isinstance(dsig_cfg, dict) and dsig_cfg.get("enabled"):
        with st.expander("下層悪天予想図（詳細版）（結合 PDF）", expanded=False):
            st.caption(
                "県を選び、「結合 PDF を生成」に反映されます。初期状態はすべてオフです。"
                " すべてオンで従来どおり全件を含めます。"
            )
            drows = _detailed_sigwx_product_rows(dsig_cfg)
            if not drows:
                st.info(
                    "config の `jma_airinfo_low_level_detailed_sigwx.products` に `fig` を追加してください。"
                )
            else:
                gdet = getattr(wx, "group_detailed_sigwx_rows_by_region", None)
                dblocks = gdet(drows) if callable(gdet) else [("地域", drows)]
                for title, dlist in dblocks:
                    if not dlist:
                        continue
                    _ds_keys: list[str] | None = None
                    _ds_selall_key: str | None = None
                    if title == TOHOKU_KANTO_UI_TITLE:
                        _ds_keys = [f"merge_dsig_{dr['fig_key']}" for dr in dlist]
                        _ds_selall_key = "merge_dsig_selall_tohoku_kanto"
                    elif title == KYUSHU_UI_TITLE:
                        _ds_keys = [f"merge_dsig_{dr['fig_key']}" for dr in dlist]
                        _ds_selall_key = "merge_dsig_selall_kyushu"
                    _region_title_heading(title)
                    if _ds_keys is not None and _ds_selall_key is not None:
                        _region_select_all_header(
                            _ds_selall_key,
                            _ds_keys,
                            keyed_container=False,
                        )
                    for dr in dlist:
                        fk = dr["fig_key"]
                        st.checkbox(
                            str(dr["label"]).strip(),
                            value=False,
                            key=f"merge_dsig_{fk}",
                        )

    typhoon_cfg = cfg.get("jma_typhoon")
    if isinstance(typhoon_cfg, dict) and typhoon_cfg.get("enabled"):
        with st.expander("台風関連（結合 PDF）", expanded=False):
            st.caption(
                "資料を選び、「結合 PDF を生成」に反映されます。\n\n"
                "注意：海水温は人工衛星とブイ・船舶による観測値から解析された解析図です。\n\n"
                "気象庁の海水温図は毎日11時頃、前日の解析図を掲載されます。"
            )
            trows = _typhoon_product_rows(typhoon_cfg)
            if not trows:
                st.info(
                    "config の `jma_typhoon.products` に "
                    "`id` / `label` / `filename` を追加してください。"
                )
            else:
                t_keys = [f"merge_typhoon_{tr['id']}" for tr in trows]
                _region_select_all_header(
                    "merge_typhoon_selall",
                    t_keys,
                    keyed_container=False,
                )
                for tr in trows:
                    tid = tr["id"]
                    st.checkbox(
                        str(tr["label"]).strip(),
                        value=False,
                        key=f"merge_typhoon_{tid}",
                    )

    st.divider()
    c1, c2 = st.columns([3, 1])
    with c1:
        merge_col, refresh_col, utc_col = st.columns([2, 1, 2])
        with merge_col:
            if st.button("結合 PDF を生成", type="primary", key="btn_merged"):
                cfg_live = wx.load_config()
                typhoon_cfg_live = cfg_live.get("jma_typhoon")
                sigwx_cfg_live = cfg_live.get("jma_airinfo_low_level_sigwx")
                dsig_cfg_live = cfg_live.get("jma_airinfo_low_level_detailed_sigwx")
                errs: list[str] = []
                warns: list[str] = []
                data, pages = b"", 0
                merged_taf: dict | None = None
                skip_merged_pdf = False
                taf2 = cfg_live.get("jma_airinfo_taf")
                if isinstance(taf2, dict) and taf2.get("enabled"):
                    prows2 = [
                        p
                        for p in (taf2.get("products") or [])
                        if isinstance(p, dict) and str(p.get("icao") or "").strip()
                    ]
                    if prows2:
                        all_icaos = [str(p.get("icao")).strip().upper() for p in prows2]
                        sel = [
                            icao
                            for icao in all_icaos
                            if st.session_state.get(f"merge_taf_ap_{icao}", False)
                        ]
                        p1 = bool(st.session_state.get("merge_taf_p1", False))
                        p2 = bool(st.session_state.get("merge_taf_p2", False))
                        if sel and not p1 and not p2:
                            st.warning(
                                "飛行場時系列予報: 空港を選んだときは PART1 / PART2 の"
                                "どちらかにチェックを入れてください。"
                            )
                            skip_merged_pdf = True
                        else:
                            merged_taf = {"icaos": sel, "part1": p1, "part2": p2}
                merged_sigwx_areas: list[str] | None = None
                use_sigwx_kw = False
                if (
                    isinstance(sigwx_cfg_live, dict)
                    and sigwx_cfg_live.get("enabled")
                    and not str(sigwx_cfg_live.get("url") or "").strip()
                ):
                    srows_m = _sigwx_product_rows(sigwx_cfg_live)
                    if srows_m:
                        all_sa = [r["area"] for r in srows_m]
                        sel_sa = [
                            a for a in all_sa if st.session_state.get(f"merge_sigwx_{a}", False)
                        ]
                        if not sel_sa:
                            merged_sigwx_areas = []
                            use_sigwx_kw = True
                        elif set(sel_sa) != set(all_sa):
                            merged_sigwx_areas = sel_sa
                            use_sigwx_kw = True
                merged_detailed_figs: list[str] | None = None
                use_det_kw = False
                if isinstance(dsig_cfg_live, dict) and dsig_cfg_live.get("enabled"):
                    drows_m = _detailed_sigwx_product_rows(dsig_cfg_live)
                    if drows_m:
                        all_fk = [r["fig_key"] for r in drows_m]
                        sel_fk = [
                            fk
                            for fk in all_fk
                            if st.session_state.get(f"merge_dsig_{fk}", False)
                        ]
                        if not sel_fk:
                            merged_detailed_figs = []
                            use_det_kw = True
                        elif set(sel_fk) != set(all_fk):
                            merged_detailed_figs = sel_fk
                            use_det_kw = True
                merged_typhoon_ids: list[str] | None = None
                use_typhoon_kw = False
                sel_typhoon_for_warn: list[str] = []
                trows_m: list[dict] = []
                if isinstance(typhoon_cfg_live, dict) and typhoon_cfg_live.get("enabled"):
                    trows_m = _typhoon_product_rows(typhoon_cfg_live)
                    if trows_m:
                        all_tid = [r["id"] for r in trows_m]
                        sel_tid = [
                            tid
                            for tid in all_tid
                            if st.session_state.get(f"merge_typhoon_{tid}", False)
                        ]
                        sel_typhoon_for_warn = sel_tid
                        if not sel_tid:
                            merged_typhoon_ids = []
                            use_typhoon_kw = True
                        elif set(sel_tid) != set(all_tid):
                            merged_typhoon_ids = sel_tid
                            use_typhoon_kw = True
                if not skip_merged_pdf:
                    if sel_typhoon_for_warn and trows_m:
                        by_id = _typhoon_product_lookup(typhoon_cfg_live)
                        no_src = [
                            str(r["label"]).strip()
                            for r in trows_m
                            if r["id"] in sel_typhoon_for_warn
                            and not _typhoon_product_fetch_ready(by_id.get(r["id"], {}))
                        ]
                        if no_src:
                            st.warning(
                                "台風関連（取得先未設定）: "
                                + "、".join(no_src)
                                + " — 上記のみ config.json の `jma_typhoon.products` に"
                                " `page_url` または `url` が必要です。"
                                " 他の項目は取得を続行します。"
                            )
                    pdf_kw: dict = {"merged_taf_selection": merged_taf}
                    if use_sigwx_kw:
                        pdf_kw["merged_sigwx_areas"] = merged_sigwx_areas
                    if use_det_kw:
                        pdf_kw["merged_detailed_sigwx_figs"] = merged_detailed_figs
                    if use_typhoon_kw:
                        pdf_kw["merged_typhoon_ids"] = merged_typhoon_ids
                    build_fn = getattr(wx, "build_merged_pdf_cached", None) or wx.build_merged_pdf
                    with st.spinner("取得・結合中（キャッシュがあれば数秒）…"):
                        try:
                            data, errs, warns, pages = build_fn(cfg_live, **pdf_kw)
                        except RuntimeError as e:
                            st.error(str(e))
                        except Exception as e:  # noqa: BLE001
                            st.error(str(e))
                    st.session_state["_merged_pdf"] = data
                    st.session_state["_merged_pages"] = pages
                    st.session_state["_merged_errs"] = errs
                    st.session_state["_merged_warns"] = warns
                    st.session_state["_merged_pdf_build"] = str(getattr(wx, "PORTAL_BUILD", ""))
        with refresh_col:
            if st.button("資料を更新", type="secondary", key="btn_refresh_charts"):
                _request_materials_refresh()
                st.rerun()
        with utc_col:
            charts_refreshed_at_utc = st.session_state.get("_charts_refreshed_at_utc")
            if charts_refreshed_at_utc:
                st.caption(f"再取得: {charts_refreshed_at_utc}")
    with c2:
        if st.button("ZIP を生成", key="btn_zip"):
            _clear_individual_item_caches()
            with st.spinner("ZIP 作成中…"):
                zdata, errs, warns, ok = wx.build_zip(cfg)
            st.session_state["_zip"] = zdata
            st.session_state["_zip_ok"] = ok
            st.session_state["_zip_errs"] = errs
            st.session_state["_zip_warns"] = warns

    if st.session_state.get("_merged_pdf"):
        b = st.session_state["_merged_pdf"]
        pgs = st.session_state.get("_merged_pages", 0)
        cur_build = str(getattr(wx, "PORTAL_BUILD", ""))
        gen_build = str(st.session_state.get("_merged_pdf_build") or "")
        if gen_build and gen_build != cur_build:
            st.warning(
                f"表示中の結合 PDF は旧ビルド（{gen_build}）で生成されています。"
                f" 現在のコードは {cur_build} です。「結合 PDF を生成」を押して再生成してください。"
            )
        if b:
            st.success(f"結合 PDF 準備完了（約 {pgs} ページ）")
            st.download_button(
                "wx_briefing_merged.pdf をダウンロード",
                data=b,
                file_name="wx_briefing_merged.pdf",
                mime="application/pdf",
                key="dl_merged",
            )
        for e in st.session_state.get("_merged_errs", []) or []:
            st.error(e)
        for w in st.session_state.get("_merged_warns", []) or []:
            st.warning(w)

    if st.session_state.get("_zip") is not None:
        zb = st.session_state["_zip"]
        ok = st.session_state.get("_zip_ok", 0)
        st.success(f"ZIP 準備完了（{ok} 件入り）")
        st.download_button(
            "wx_briefing_latest.zip をダウンロード",
            data=zb,
            file_name="wx_briefing_latest.zip",
            mime="application/zip",
            key="dl_zip",
        )
        for e in st.session_state.get("_zip_errs", []) or []:
            st.error(e)
        for w in st.session_state.get("_zip_warns", []) or []:
            st.warning(w)


@st.cache_data(ttl=120)
def _cached_item_bytes(index: int, url: str) -> tuple[bytes | None, str | None, str]:
    """単体資料のバイト列（失敗時は None, エラー文）。`url` はキャッシュキー用（衛星の可変 URL で古い結果を避ける）。"""
    cfg = wx.load_config()
    item, err = wx.fetch_one_expanded_item(cfg, index, None)
    if err or not item:
        return None, None, err or "項目がありません"
    item_url = item.get("url")
    fname = item.get("filename") or "download.bin"
    if not item_url:
        return None, fname, "URL なし"
    try:
        data, _ct = wx.fetch_item_bytes(item)
    except Exception as e:  # noqa: BLE001
        return None, fname, str(e)
    return data, fname, ""


def _clear_individual_item_caches() -> None:
    """個別資料ダウンロード用: 取得キャッシュと Streamlit 側キャッシュを破棄。"""
    wx.clear_fetch_bytes_cache()
    _cfg_cached.clear()
    _cached_item_bytes.clear()


def _clear_stored_material_outputs() -> None:
    """生成済みの結合 PDF / ZIP をセッションから削除。"""
    for key in (
        "_merged_pdf",
        "_merged_pages",
        "_merged_errs",
        "_merged_warns",
        "_merged_pdf_build",
        "_zip",
        "_zip_ok",
        "_zip_errs",
        "_zip_warns",
    ):
        st.session_state.pop(key, None)


def _request_materials_refresh() -> None:
    st.session_state["_materials_force_refresh"] = True


def _run_materials_refresh_if_requested() -> None:
    """「資料を更新」押下後: 取得済み資料を破棄し、全件を最初から再取得。"""
    if not st.session_state.pop("_materials_force_refresh", False):
        return
    _clear_stored_material_outputs()
    _clear_individual_item_caches()
    refetch_fn = getattr(wx, "refetch_all_download_items", None)
    ok, err, notes = 0, 0, []
    with st.spinner("全資料を再取得中…（時間がかかることがあります）"):
        if callable(refetch_fn):
            ok, err, notes = refetch_fn(wx.load_config())
        else:
            wx.clear_fetch_bytes_cache()
    ts = datetime.now(wx.UTC).strftime("%Y-%m-%d %H:%M UTC")
    st.session_state["_charts_refreshed_at_utc"] = ts
    st.session_state["_items_refreshed_at_utc"] = ts
    st.session_state["_items_expanded_after_refresh"] = True
    st.session_state["_materials_just_refetched"] = True
    if ok or err:
        if err:
            st.warning(f"資料の再取得: {ok} 件成功、{err} 件失敗")
        else:
            st.success(f"資料の再取得: {ok} 件を更新しました（{ts}）")
    for note in notes[:8]:
        st.caption(f"⚠ {note}")
    if len(notes) > 8:
        st.caption(f"⚠ …他 {len(notes) - 8} 件")


def _render_individual_download_rows(items: list) -> None:
    """展開リスト内: 各資料の取得とダウンロードボタン。"""
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        name = item.get("filename") or f"(#{idx})"
        url = item.get("url")
        if not url:
            st.write(f"**{name}** （URLなし）")
            continue
        data, fn, err = _cached_item_bytes(idx, str(url))
        if err:
            st.write(f"**{name}** — {err}")
        elif data:
            st.download_button(
                label=f"⬇ {name}",
                data=data,
                file_name=fn or name,
                key=f"item_dl_{idx}",
            )


def _render_file_list(cfg: dict) -> None:
    items, warns = wx.expand_download_items(cfg)
    for w in warns:
        st.caption(f"⚠ {w}")
    if not items:
        st.info("config.json に資料がありません。")
        return
    st.subheader("資料一覧：個別ダウンロード")
    btn_col, note_col = st.columns([1, 3])
    with btn_col:
        if st.button("資料の更新", type="secondary", key="btn_refresh_items"):
            _request_materials_refresh()
            st.rerun()
    with note_col:
        refreshed_at_utc = st.session_state.get("_items_refreshed_at_utc")
        if refreshed_at_utc:
            st.caption(f"再取得: {refreshed_at_utc}")
    expand_after_refresh = bool(st.session_state.pop("_items_expanded_after_refresh", False))
    with st.expander("展開", expanded=expand_after_refresh):
        _render_individual_download_rows(items)


def main() -> None:
    st.set_page_config(
        page_title="WX Briefing",
        page_icon="🌤",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    if not _ensure_login():
        return

    _inject_wx_streamlit_ui_styles()

    cfg = _cfg_cached()
    title = cfg.get("title") or "WX Briefing"
    st.title(str(title))
    st.caption(f"ビルド: {_wx_build_display()}　15期　Ishikawa")
    for _rw in _runtime_health_warnings():
        st.warning(_rw)

    _run_materials_refresh_if_requested()

    if not st.session_state.pop("_materials_just_refetched", False):
        sched_fn = getattr(wx, "start_wx_briefing_background_scheduler", None)
        if callable(sched_fn):
            sched_fn(cfg)
        else:
            start_fn = getattr(wx, "start_wx_briefing_prefetch", None)
            if callable(start_fn):
                start_fn(cfg)

    with st.sidebar:
        _render_runtime_sidebar()
        ha = cfg.get("http_auth")
        if isinstance(ha, dict) and bool(ha.get("enabled")):
            st.subheader("アカウント")
            if st.button("ログアウト"):
                st.session_state["_auth_ok"] = False
                st.rerun()

    with st.container(key="wx_metar_taf_frame"):
        _render_metar_taf(cfg)
    st.divider()
    with st.container(key="wx_charts_frame"):
        _render_charts_zip(cfg)
    st.divider()
    with st.container(key="wx_file_list_frame"):
        _render_file_list(cfg)


if __name__ == "__main__":
    main()
