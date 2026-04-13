"""
Streamlit 版 WX Briefing（app.py のロジックを再利用）。

手順の詳細は「Streamlit手順.md」（具体手順・だれでも版）を開いてください。

最短（ローカル・Windows 推奨）:
  python -m pip install -r requirements.txt
  python -m playwright install chromium
  python -m streamlit run streamlit_app.py
"""
from __future__ import annotations

import os
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


def _render_metar_taf(cfg: dict) -> None:
    airports = wx.metar_taf_airports_from_config(cfg)
    block = cfg.get("metar_taf_fetch")
    if not isinstance(block, dict) or not block.get("enabled") or not airports:
        return
    st.subheader("METAR・TAF")
    st.caption("空港と種別を選び、PDF を生成します。")
    cols = st.columns(3)
    selected: list[str] = []
    for i, ap in enumerate(airports):
        icao = ap["icao"]
        lab = ap["label"]
        with cols[i % 3]:
            if st.checkbox(f"{lab} ({icao})", key=f"mt_ap_{icao}"):
                selected.append(icao)
    c1, c2 = st.columns(2)
    with c1:
        want_met = st.checkbox("METAR", value=True, key="mt_met")
    with c2:
        want_taf = st.checkbox("TAF", value=True, key="mt_taf")
    if st.button("METAR/TAF PDF を生成", key="mt_go"):
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
    taf = cfg.get("jma_airinfo_taf")
    if isinstance(taf, dict) and taf.get("enabled"):
        st.markdown("**飛行場時系列予報**（結合 PDF に含める範囲）")
        st.caption(
            "空港と PART1 / PART2 を選び、下の「結合 PDF を生成」に反映されます（全空港・PART1+2 のときは従来と同じ扱い）。"
        )
        prows = [
            p
            for p in (taf.get("products") or [])
            if isinstance(p, dict) and str(p.get("icao") or "").strip()
        ]
        if not prows:
            st.info("config の `jma_airinfo_taf.products` に ICAO を追加してください。")
        else:
            cols = st.columns(3)
            for i, pr in enumerate(prows):
                icao = str(pr.get("icao")).strip().upper()
                lab = str(pr.get("label") or pr.get("name") or icao).strip()
                with cols[i % 3]:
                    st.checkbox(f"{lab}（{icao}）", value=True, key=f"merge_taf_ap_{icao}")
            pc1, pc2 = st.columns(2)
            with pc1:
                st.checkbox("PART1（QMCD98_）", value=True, key="merge_taf_p1")
            with pc2:
                st.checkbox("PART2（QMCJ98_）", value=True, key="merge_taf_p2")
        st.divider()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("結合 PDF を生成", type="primary", key="btn_merged"):
            errs: list[str] = []
            warns: list[str] = []
            data, pages = b"", 0
            merged_taf: dict | None = None
            skip_merge = False
            taf2 = cfg.get("jma_airinfo_taf")
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
                        if st.session_state.get(f"merge_taf_ap_{icao}", True)
                    ]
                    p1 = bool(st.session_state.get("merge_taf_p1", True))
                    p2 = bool(st.session_state.get("merge_taf_p2", True))
                    if not sel:
                        st.warning(
                            "飛行場時系列予報: 空港を1つ以上選んでください。"
                        )
                        skip_merge = True
                    elif not p1 and not p2:
                        st.warning(
                            "飛行場時系列予報: PART1 と PART2 のどちらかにチェックを入れてください。"
                        )
                        skip_merge = True
                    elif not (
                        set(sel) == set(all_icaos)
                        and len(sel) == len(all_icaos)
                        and p1
                        and p2
                    ):
                        merged_taf = {"icaos": sel, "part1": p1, "part2": p2}
            if not skip_merge:
                with st.spinner("取得・結合中（時間がかかることがあります）…"):
                    try:
                        data, errs, warns, pages = wx.build_merged_pdf(
                            cfg, merged_taf_selection=merged_taf
                        )
                    except RuntimeError as e:
                        st.error(str(e))
                    except Exception as e:  # noqa: BLE001
                        st.error(str(e))
                st.session_state["_merged_pdf"] = data
                st.session_state["_merged_pages"] = pages
                st.session_state["_merged_errs"] = errs
                st.session_state["_merged_warns"] = warns
    with c2:
        if st.button("ZIP を生成", key="btn_zip"):
            with st.spinner("ZIP 作成中…"):
                zdata, errs, warns, ok = wx.build_zip(cfg)
            st.session_state["_zip"] = zdata
            st.session_state["_zip_ok"] = ok
            st.session_state["_zip_errs"] = errs
            st.session_state["_zip_warns"] = warns

    if st.session_state.get("_merged_pdf"):
        b = st.session_state["_merged_pdf"]
        pgs = st.session_state.get("_merged_pages", 0)
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


def _render_file_list(cfg: dict) -> None:
    items, warns = wx.expand_download_items(cfg)
    for w in warns:
        st.caption(f"⚠ {w}")
    if not items:
        st.info("config.json に資料がありません。")
        return
    with st.expander("資料一覧（1件ずつダウンロード）", expanded=False):
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


def main() -> None:
    st.set_page_config(
        page_title="WX Briefing",
        page_icon="🌤",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    if not _ensure_login():
        return

    cfg = _cfg_cached()
    title = cfg.get("title") or "WX Briefing"
    st.title(str(title))
    st.caption(
        f"ビルド: {wx.portal_build_stamp()} · Streamlit 版"
        " — 手動ラベル（`PORTAL_BUILD`）・**app.py の最終更新（UTC）**・（リポジトリ内なら）**git の短いコミット**。"
        " 更新時刻が直近の保存と一致すれば、このアプリが読み込んでいる **app.py** は新しいです。"
    )

    with st.sidebar:
        ha = cfg.get("http_auth")
        if isinstance(ha, dict) and bool(ha.get("enabled")):
            st.subheader("アカウント")
            if st.button("ログアウト"):
                st.session_state["_auth_ok"] = False
                st.rerun()

    _render_metar_taf(cfg)
    st.divider()
    _render_charts_zip(cfg)
    st.divider()
    _render_file_list(cfg)


if __name__ == "__main__":
    main()
