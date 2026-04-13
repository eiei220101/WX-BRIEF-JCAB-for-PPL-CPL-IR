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
        out.append({"fig_key": fk, "label": lab})
    return out


def _wx_build_display() -> str:
    """Streamlit Cloud が古い app.py のときも落ちない（portal_build_stamp 未実装なら PORTAL_BUILD のみ）。"""
    fn = getattr(wx, "portal_build_stamp", None)
    if callable(fn):
        try:
            return str(fn())
        except Exception:  # noqa: BLE001
            pass
    return str(getattr(wx, "PORTAL_BUILD", "unknown"))


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
    st.caption("空港と種別を選び、PDF を生成します（初期状態はすべてオフ）。")
    selected: list[str] = []
    for title, aps in wx.group_metar_taf_airports_by_region(airports):
        if not aps:
            continue
        with st.container(border=True):
            st.markdown(f"**{title}**")
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
    c1, c2 = st.columns(2)
    with c1:
        want_met = st.checkbox("METAR", value=False, key="mt_met")
    with c2:
        want_taf = st.checkbox("TAF", value=False, key="mt_taf")
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
                with st.container(border=True):
                    st.markdown(f"**{title}**")
                    cols = st.columns(3)
                    for i, pr in enumerate(plist):
                        icao = str(pr.get("icao")).strip().upper()
                        lab = str(pr.get("label") or pr.get("name") or icao).strip()
                        with cols[i % 3]:
                            st.checkbox(
                                f"{lab}（{icao}）",
                                value=False,
                                key=f"merge_taf_ap_{icao}",
                            )
            pc1, pc2 = st.columns(2)
            with pc1:
                st.checkbox("PART1（QMCD98_）", value=False, key="merge_taf_p1")
            with pc2:
                st.checkbox("PART2（QMCJ98_）", value=False, key="merge_taf_p2")
        st.divider()

    sigwx_cfg = cfg.get("jma_airinfo_low_level_sigwx")
    if (
        isinstance(sigwx_cfg, dict)
        and sigwx_cfg.get("enabled")
        and not str(sigwx_cfg.get("url") or "").strip()
    ):
        st.markdown("**下層悪天予想図**（結合 PDF・時系列 ft=39）")
        st.caption(
            "地域を選び、「結合 PDF を生成」に反映されます。初期状態はすべてオフです。"
            " すべてオンで従来どおり全地域を含めます。"
        )
        srows = _sigwx_product_rows(sigwx_cfg)
        if not srows:
            st.info("config の `jma_airinfo_low_level_sigwx.products` に `area` を追加してください。")
        else:
            gsig = getattr(wx, "group_sigwx_rows_by_region", None)
            sblocks = gsig(srows) if callable(gsig) else [("地域", srows)]
            for title, slist in sblocks:
                if not slist:
                    continue
                with st.container(border=True):
                    st.markdown(f"**{title}**")
                    sc = st.columns(min(3, max(1, len(slist))))
                    for i, sr in enumerate(slist):
                        a = sr["area"]
                        with sc[i % len(sc)]:
                            st.checkbox(
                                f"{sr['label']}（{a}）",
                                value=False,
                                key=f"merge_sigwx_{a}",
                            )
        st.divider()

    dsig_cfg = cfg.get("jma_airinfo_low_level_detailed_sigwx")
    if isinstance(dsig_cfg, dict) and dsig_cfg.get("enabled"):
        st.markdown("**下層悪天予想図（詳細版）**（結合 PDF）")
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
                with st.container(border=True):
                    st.markdown(f"**{title}**")
                    dc = st.columns(4)
                    for i, dr in enumerate(dlist):
                        fk = dr["fig_key"]
                        with dc[i % 4]:
                            st.checkbox(
                                f"{dr['label']}（{fk}）",
                                value=False,
                                key=f"merge_dsig_{fk}",
                            )
        st.divider()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("結合 PDF を生成", type="primary", key="btn_merged"):
            errs: list[str] = []
            warns: list[str] = []
            data, pages = b"", 0
            merged_taf: dict | None = None
            skip_merged_pdf = False
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
                        full = (
                            bool(sel)
                            and set(sel) == set(all_icaos)
                            and len(sel) == len(all_icaos)
                            and p1
                            and p2
                        )
                        merged_taf = None if full else {"icaos": sel, "part1": p1, "part2": p2}
            merged_sigwx_areas: list[str] | None = None
            use_sigwx_kw = False
            if (
                isinstance(sigwx_cfg, dict)
                and sigwx_cfg.get("enabled")
                and not str(sigwx_cfg.get("url") or "").strip()
            ):
                srows_m = _sigwx_product_rows(sigwx_cfg)
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
            if isinstance(dsig_cfg, dict) and dsig_cfg.get("enabled"):
                drows_m = _detailed_sigwx_product_rows(dsig_cfg)
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
            if not skip_merged_pdf:
                pdf_kw: dict = {"merged_taf_selection": merged_taf}
                if use_sigwx_kw:
                    pdf_kw["merged_sigwx_areas"] = merged_sigwx_areas
                if use_det_kw:
                    pdf_kw["merged_detailed_sigwx_figs"] = merged_detailed_figs
                with st.spinner("取得・結合中（時間がかかることがあります）…"):
                    try:
                        data, errs, warns, pages = wx.build_merged_pdf(cfg, **pdf_kw)
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
        f"ビルド: {_wx_build_display()} · Streamlit 版"
        " — 手動ラベル（`PORTAL_BUILD`）・（最新の **app.py** では）**app.py の最終更新（UTC）**・（リポジトリ内なら）**git の短いコミット**。"
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
