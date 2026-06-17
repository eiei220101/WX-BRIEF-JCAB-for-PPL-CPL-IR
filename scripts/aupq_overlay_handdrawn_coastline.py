#!/usr/bin/env python3
"""
AUPQ 高層天気図 PDF に「手書き海岸線画像」を重ねて、海岸線だけをオレンジで強調する。

この方式は「自動検出で海岸線を推定」ではなく、ユーザーが用意した海岸線（正解データ）を
そのまま重ねるため、等圧線・文字が誤ってオレンジ化する問題を避けやすい。

----------------------------------------------------------------------
必要な環境（README）
----------------------------------------------------------------------

Python パッケージ::

    pip install pdf2image Pillow numpy

システム依存（PDF → 画像 rasterize に必須）:

* **macOS (Homebrew):** ``brew install poppler``
* **Ubuntu / Debian:** ``sudo apt-get install poppler-utils``
* **Windows:** Poppler for Windows を PATH に通す
  https://github.com/oschwartz10612/poppler-windows/releases

使い方（例）::

    python scripts/aupq_overlay_handdrawn_coastline.py AUPQ35_12UTC.pdf \
      --overlay assets/overlays/nihon_coastline_overlay.png

出力:

* ``<stem>_coastline_overlay.pdf`` … 重ね合わせ後の PDF（既定）
* ``--png-dir ./out_png`` … ページ画像も保存する場合

注意:

* 手書き PNG は **700×1024**（``assets/overlays/aupq_upper_coastline_reference.png`` と同じサイズ）で作ると
  ズレが最小になります。Pages など別サイズでも **700×1024 に自動変換**してから重ねます（既定）。
* 位置が少しズレる場合は ``--overlay-x`` / ``--overlay-y`` で微調整できます。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageFilter

try:
    from pdf2image import convert_from_path
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "pdf2image が必要です: pip install pdf2image\n"
        "また poppler-utils（macOS: brew install poppler）を入れてください。"
    ) from exc


# AUPQ35 / AUPQ78（700×1024 相当）での地図パネル枠（参照座標）
_REF_PANELS = (
    (20, 42, 679, 499),  # 上段
    (20, 500, 679, 977),  # 下段
)
_REF_SIZE = (700, 1024)

ORANGE_RGB = (255, 136, 0)


def _panel_pixel_boxes(width: int, height: int) -> list[tuple[int, int, int, int]]:
    sx = width / _REF_SIZE[0]
    sy = height / _REF_SIZE[1]
    boxes: list[tuple[int, int, int, int]] = []
    for x0, y0, x1, y1 in _REF_PANELS:
        boxes.append(
            (
                max(0, int(x0 * sx)),
                max(0, int(y0 * sy)),
                min(width, int(x1 * sx)),
                min(height, int(y1 * sy)),
            )
        )
    return boxes


def _corner_brightness(overlay_rgb: np.ndarray) -> float:
    h, w = overlay_rgb.shape[:2]
    corners = (
        overlay_rgb[0, 0],
        overlay_rgb[0, w - 1],
        overlay_rgb[h - 1, 0],
        overlay_rgb[h - 1, w - 1],
    )
    return float(np.mean([c.mean() for c in corners]))


def _orange_line_mask(r: np.ndarray, g: np.ndarray, b: np.ndarray) -> np.ndarray:
    """オレンジ系の線（手書き海岸線）を拾う。"""
    return (
        (r > 80)
        & (g > 35)
        & (b < 120)
        & (r >= g)
        & (g >= b)
        & ((r.astype(np.int16) + g + b) > 60)
    )


def _overlay_to_mask(
    overlay_rgb: np.ndarray,
    *,
    alpha: np.ndarray | None = None,
    threshold: int = 235,
) -> np.ndarray:
    """
    オーバーレイ画像から「海岸線の線だけ」のマスクを作る。

    対応形式:
    - 透過 PNG（Pages 書き出し）… alpha あり & オレンジ線
    - 白背景 + オレンジ線
    - 黒背景 + オレンジ線
    """
    ov = overlay_rgb.astype(np.int16)
    r, g, b = ov[:, :, 0], ov[:, :, 1], ov[:, :, 2]
    gray = (r + g + b) // 3
    orange = _orange_line_mask(r, g, b)

    # 透過 PNG（背景が透明で、線だけ alpha があるタイプ）
    if alpha is not None and alpha.shape[:2] == overlay_rgb.shape[:2]:
        vis = alpha > 32
        if float(vis.mean()) < 0.5:
            return vis & orange

    if _corner_brightness(overlay_rgb) >= 128:
        # 白背景: 白以外の薄い線（alpha だけ見ると全面が線扱いになるので色で判定）
        return (gray < threshold) & (gray > 30)

    # 黒背景: オレンジ線だけ
    return orange


def extract_overlay_from_pages(pages_path: Path, out_png: Path) -> Path:
    """Pages (.pages) から埋め込み PNG を取り出す。"""
    import zipfile

    out_png.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(pages_path) as zf:
        png_names = [
            name
            for name in zf.namelist()
            if name.lower().endswith(".png") and "small" not in name.lower()
        ]
        if not png_names:
            raise ValueError(f"PNG が見つかりません: {pages_path}")
        best = max(png_names, key=lambda n: zf.getinfo(n).file_size)
        out_png.write_bytes(zf.read(best))
    return out_png


def _paste_orange_lines(
    base_rgb: np.ndarray,
    mask: np.ndarray,
    *,
    orange: tuple[int, int, int] = ORANGE_RGB,
    alpha: float = 0.95,
) -> np.ndarray:
    """
    base_rgb の mask 部分をオレンジでブレンドする。
    alpha=1.0 で完全置換、0.0 で変化なし。
    """
    out = base_rgb.copy().astype(np.float32)
    o = np.array(orange, dtype=np.float32)
    out[mask] = out[mask] * (1.0 - alpha) + o * alpha
    return np.clip(out, 0, 255).astype(np.uint8)


def _marker_mask_from_rgba(arr: np.ndarray) -> np.ndarray:
    """十字マーカー候補（小さなオレンジ塊）を拾う。"""
    r = arr[:, :, 0].astype(np.int16)
    g = arr[:, :, 1].astype(np.int16)
    b = arr[:, :, 2].astype(np.int16)
    if arr.shape[2] == 4:
        a = arr[:, :, 3].astype(np.int16)
        if int(a.max()) > 0:
            return (a > 32) & _orange_line_mask(r, g, b)
    gray = (r + g + b) // 3
    if _corner_brightness(arr[:, :, :3]) >= 128:
        return ((r + g + b) < 720) & (r > 150) & (g > 80) & (b > 60)
    return _orange_line_mask(r, g, b)


def _small_orange_components(
    marker_mask: np.ndarray,
    *,
    min_area: int = 12,
    max_area: int = 500,
) -> list[tuple[int, int, int]]:
    """(area, cx, cy) の小コンポーネント一覧。"""
    h, w = marker_mask.shape
    visited = np.zeros((h, w), dtype=bool)
    comps: list[tuple[int, int, int]] = []
    for y in range(h):
        for x in range(w):
            if not marker_mask[y, x] or visited[y, x]:
                continue
            stack = [(y, x)]
            visited[y, x] = True
            pts: list[tuple[int, int]] = []
            while stack:
                cy, cx = stack.pop()
                pts.append((cy, cx))
                for dy, dx in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                    ny, nx = cy + dy, cx + dx
                    if (
                        0 <= ny < h
                        and 0 <= nx < w
                        and marker_mask[ny, nx]
                        and not visited[ny, nx]
                    ):
                        visited[ny, nx] = True
                        stack.append((ny, nx))
            if min_area <= len(pts) <= max_area:
                cy = int(np.mean([p[0] for p in pts]))
                cx = int(np.mean([p[1] for p in pts]))
                comps.append((len(pts), cx, cy))
    return comps


def find_crosshair_anchors(
    ov_crop: Image.Image,
) -> tuple[tuple[int, int], tuple[int, int]]:
    """
    パネル半分の画像から、右上十字・左下十字の中心 (x,y) を返す。

    右上十字 → 図の右上角、左下十字 → 図の左下角 に合わせる前提。
    """
    arr = np.array(ov_crop.convert("RGBA"))
    sh, sw = arr.shape[:2]
    comps = _small_orange_components(_marker_mask_from_rgba(arr))
    if len(comps) < 2:
        raise ValueError(
            "十字マーカーが2つ見つかりません。"
            " PNG の右上・左下にオレンジの十字があるか確認してください。"
        )

    tr_cands = [c for c in comps if c[1] > 0.55 * sw and c[2] < 0.35 * sh]
    bl_cands = [c for c in comps if c[1] < 0.55 * sw and c[2] > 0.65 * sh]
    if not tr_cands or not bl_cands:
        raise ValueError(
            "十字マーカーの位置が想定外です。"
            " 各パネル（上段/下段）の右上・左下に十字がある PNG を使ってください。"
        )

    tr = min(tr_cands, key=lambda c: (sw - c[1]) ** 2 + c[2] ** 2)
    bl = min(bl_cands, key=lambda c: c[1] ** 2 + (sh - c[2]) ** 2)
    return (tr[1], tr[2]), (bl[1], bl[2])


def _exclude_crosshair_pixels(
    mask: np.ndarray,
    cross_points: list[tuple[int, int]],
    *,
    radius: int,
) -> np.ndarray:
    """十字マーカー部分は海岸線として描画しない。"""
    if not cross_points:
        return mask
    out = mask.copy()
    h, w = out.shape
    r2 = radius * radius
    for cx, cy in cross_points:
        y0 = max(0, cy - radius)
        y1 = min(h, cy + radius + 1)
        x0 = max(0, cx - radius)
        x1 = min(w, cx + radius + 1)
        yy, xx = np.ogrid[y0:y1, x0:x1]
        circle = (xx - cx) ** 2 + (yy - cy) ** 2 <= r2
        out[y0:y1, x0:x1] &= ~circle
    return out


def _crosshair_uniform_transform(
    tr: tuple[int, int],
    bl: tuple[int, int],
    panel_box: tuple[int, int, int, int],
) -> tuple[float, int, int]:
    """
    十字2点から uniform scale と paste 位置を返す（画像は切り詰めない）。

    左下十字 → パネル左下角、スケールは縦横比を保つ。
    """
    trx, try_ = tr
    blx, bly = bl
    x0, y0, x1, y1 = panel_box
    panel_w = max(1, x1 - x0)
    panel_h = max(1, y1 - y0)

    span_x = trx - blx
    span_y = bly - try_
    if span_x < 20 or span_y < 20:
        raise ValueError(f"十字間隔が狭すぎます: span_x={span_x}, span_y={span_y}")

    sx = panel_w / span_x
    sy = panel_h / span_y
    scale = min(sx, sy)
    paste_x = int(round(x0 - blx * scale))
    paste_y = int(round(y1 - bly * scale))
    return scale, paste_x, paste_y


def _sobel_edge_strength(gray: np.ndarray) -> np.ndarray:
    """Sobel エッジ強度（簡易、外部依存なし）。"""
    g = gray.astype(np.float32)
    p = np.pad(g, 1, mode="edge")
    gx = (
        (-1 * p[:-2, :-2])
        + (1 * p[:-2, 2:])
        + (-2 * p[1:-1, :-2])
        + (2 * p[1:-1, 2:])
        + (-1 * p[2:, :-2])
        + (1 * p[2:, 2:])
    )
    gy = (
        (-1 * p[:-2, :-2])
        + (-2 * p[:-2, 1:-1])
        + (-1 * p[:-2, 2:])
        + (1 * p[2:, :-2])
        + (2 * p[2:, 1:-1])
        + (1 * p[2:, 2:])
    )
    e = np.sqrt(gx * gx + gy * gy)
    m = float(e.max() + 1e-6)
    return e / m


def _best_shift_by_edge_overlap(
    edge: np.ndarray,
    overlay_mask: np.ndarray,
    *,
    search: int,
) -> tuple[int, int]:
    """overlay_mask を edge に最も重ねる dx,dy（スコア最大）を返す。"""
    h, w = edge.shape
    best_score = -1.0
    best_dx = 0
    best_dy = 0
    for dy in range(-search, search + 1):
        for dx in range(-search, search + 1):
            y0 = max(0, dy)
            y1 = min(h, h + dy)
            x0 = max(0, dx)
            x1 = min(w, w + dx)
            if y1 <= y0 or x1 <= x0:
                continue
            e = edge[y0:y1, x0:x1]
            m = overlay_mask[y0 - dy : y1 - dy, x0 - dx : x1 - dx]
            score = float((e * m).sum())
            if score > best_score:
                best_score = score
                best_dx, best_dy = dx, dy
    return best_dx, best_dy


def estimate_panel_offsets(
    page: Image.Image,
    overlay_img: Image.Image,
    *,
    overlay_scale: float,
    overlay_threshold: int,
    downsample: int = 6,
    search_px: int = 90,
) -> list[tuple[int, int]]:
    """1ページ目を使って、各パネルの dx,dy を自動推定する。"""
    base_gray = np.array(page.convert("L"))
    h, w = base_gray.shape[:2]
    boxes = _panel_pixel_boxes(w, h)
    if len(boxes) != 2:
        return [(0, 0), (0, 0)]

    ov_rgba = overlay_img.convert("RGBA")
    ov_w, ov_h = ov_rgba.size
    half_h = ov_h // 2

    offsets: list[tuple[int, int]] = []
    ds = max(1, int(downsample))
    for i, (x0, y0, x1, y1) in enumerate(boxes):
        panel_w = max(1, x1 - x0)
        panel_h = max(1, y1 - y0)

        ov_crop = (
            ov_rgba.crop((0, 0, ov_w, half_h))
            if i == 0
            else ov_rgba.crop((0, half_h, ov_w, ov_h))
        )
        target_w = int(panel_w * overlay_scale)
        target_h = int(panel_h * overlay_scale)
        ov_resized = ov_crop.resize((target_w, target_h), Image.Resampling.LANCZOS)
        ov_arr = np.array(ov_resized)
        ov_rgb = ov_arr[:, :, :3]
        ov_alpha = ov_arr[:, :, 3] if ov_arr.shape[2] == 4 else None
        ov_mask = _overlay_to_mask(ov_rgb, alpha=ov_alpha, threshold=overlay_threshold)

        edge = _sobel_edge_strength(base_gray[y0:y1, x0:x1])[::ds, ::ds]
        edge = np.where(edge >= 0.25, edge, 0.0)
        ov_ds = ov_mask[::ds, ::ds]

        dx_ds, dy_ds = _best_shift_by_edge_overlap(
            edge,
            ov_ds,
            search=max(1, int(search_px / ds)),
        )
        offsets.append((dx_ds * ds, dy_ds * ds))

    return offsets


_ORANGE_REF_RGB = (255, 178, 100)
_COASTLINE_SOFT_THRESHOLD = 145
_COASTLINE_CONNECT = True
_COASTLINE_CONNECT_DILATE = 5
_COASTLINE_CONNECT_ERODE = 3


def _coastline_soft_mask(
    ref_arr: np.ndarray,
    base_arr: np.ndarray | None = None,
) -> np.ndarray:
    """
    完成見本 PNG 上の海岸線をソフトマスク（0–255）として返す。

    base があれば差分でオレンジ追加部分を拾い、参照 PNG の色情報も合成する。
    """
    s = ref_arr.sum(axis=2)
    rb = ref_arr[:, :, 0].astype(np.float32) - ref_arr[:, :, 2].astype(np.float32)
    on_chart = (s > 250) & (s < 720)

    ref_soft = np.zeros(ref_arr.shape[:2], dtype=np.float32)
    ref_soft[on_chart] = np.clip((rb[on_chart] - 5.0) * 12.0, 0, 255)

    if base_arr is None or base_arr.shape[:2] != ref_arr.shape[:2]:
        return ref_soft.astype(np.uint8)

    dr = ref_arr[:, :, 0].astype(np.int16) - base_arr[:, :, 0].astype(np.int16)
    db = ref_arr[:, :, 2].astype(np.int16) - base_arr[:, :, 2].astype(np.int16)
    dg = ref_arr[:, :, 1].astype(np.int16) - base_arr[:, :, 1].astype(np.int16)
    hard = (dr > 5) & (dr - db > 12) & (dr > dg) & on_chart
    soft = np.zeros(ref_arr.shape[:2], dtype=np.float32)
    soft[hard] = 255.0
    fringe = hard | (
        on_chart & (rb > 6) & (ref_arr[:, :, 0].astype(np.int16) > ref_arr[:, :, 1].astype(np.int16) - 5)
    )
    soft[fringe] = np.maximum(soft[fringe], np.clip(rb[fringe] * 10.0, 0, 255))
    return np.maximum(soft, ref_soft).astype(np.uint8)


def _coastline_soft_mask_from_reference_png(
    reference_png: Image.Image,
    base_page: Image.Image | None = None,
) -> np.ndarray:
    """700×1024 基準でソフトマスクを作る（拡大前の参照解像度）。"""
    ref_w, ref_h = _REF_SIZE
    ref = reference_png.convert("RGB")
    if ref.size != (ref_w, ref_h):
        ref = ref.resize((ref_w, ref_h), Image.Resampling.LANCZOS)
    ref_arr = np.array(ref)
    base_arr = None
    if base_page is not None:
        base = base_page.convert("RGB").resize((ref_w, ref_h), Image.Resampling.LANCZOS)
        base_arr = np.array(base)
    return _coastline_soft_mask(ref_arr, base_arr)


def _connect_coastline_mask(
    thick: np.ndarray,
    *,
    dilate: int = _COASTLINE_CONNECT_DILATE,
    erode: int = _COASTLINE_CONNECT_ERODE,
) -> np.ndarray:
    """隙間をつないで線幅を均一化（膨張→収縮のクロージング）。"""
    img = Image.fromarray(thick.astype(np.uint8) * 255, mode="L")
    if dilate >= 3:
        img = img.filter(ImageFilter.MaxFilter(dilate))
    if erode >= 3:
        img = img.filter(ImageFilter.MinFilter(erode))
    return np.array(img) > 0


def _render_coastline_layer_from_soft_mask(
    soft_mask: np.ndarray,
    line_rgb: tuple[int, int, int],
    *,
    target_w: int | None = None,
    target_h: int | None = None,
    threshold: int = _COASTLINE_SOFT_THRESHOLD,
    connect: bool = _COASTLINE_CONNECT,
    connect_dilate: int = _COASTLINE_CONNECT_DILATE,
    connect_erode: int = _COASTLINE_CONNECT_ERODE,
) -> Image.Image:
    """参照解像度のソフトマスクをページサイズへ拡大し、均一な連続線の RGBA にする。"""
    if target_w is not None and target_h is not None:
        soft_mask = np.array(
            Image.fromarray(soft_mask, mode="L").resize(
                (max(1, target_w), max(1, target_h)), Image.Resampling.BILINEAR
            )
        )
    thick = soft_mask >= threshold
    if connect:
        thick = _connect_coastline_mask(
            thick, dilate=connect_dilate, erode=connect_erode
        )
    h, w = soft_mask.shape[:2]
    lr, lg, lb = line_rgb
    out = np.zeros((h, w, 4), dtype=np.uint8)
    out[thick] = (lr, lg, lb, 255)
    return Image.fromarray(out, "RGBA")


def process_page_from_reference_png(
    page: Image.Image,
    reference_png: Image.Image,
    *,
    overlay_x: int = 0,
    overlay_y: int = 0,
    overlay_scale: float = 1.0,
) -> Image.Image:
    """
    完成見本 PNG から抜いたオレンジ線を、PDF ページに 700×1024 基準で比例重ねする。

    参照 PNG と JMA AUPQ PDF は同一レイアウト（700×1024）のため、
    完成見本どおりの位置に海岸線だけ載せられる。
    """
    base = page.convert("RGBA")
    pw, ph = base.size
    target_w = max(1, int(round(pw * overlay_scale)))
    target_h = max(1, int(round(ph * overlay_scale)))

    soft_mask = _coastline_soft_mask_from_reference_png(reference_png, page)
    layer = _render_coastline_layer_from_soft_mask(
        soft_mask, _ORANGE_REF_RGB, target_w=target_w, target_h=target_h
    )
    canvas = Image.new("RGBA", (pw, ph), (0, 0, 0, 0))
    canvas.paste(layer, (overlay_x, overlay_y), layer)
    merged = Image.alpha_composite(base, canvas)
    return merged.convert("RGB")


def normalize_overlay_to_reference(overlay_img: Image.Image) -> tuple[Image.Image, tuple[int, int]]:
    """オーバーレイを AUPQ 参照サイズ 700×1024 に合わせる。"""
    ref_w, ref_h = _REF_SIZE
    ov = overlay_img.convert("RGBA")
    orig = ov.size
    if orig != (ref_w, ref_h):
        ov = ov.resize((ref_w, ref_h), Image.Resampling.LANCZOS)
    return ov, orig


def process_page_reference_fit(
    page: Image.Image,
    overlay_img: Image.Image,
    *,
    overlay_x: int,
    overlay_y: int,
    overlay_scale: float,
    overlay_threshold: int,
    overlay_alpha: float,
) -> Image.Image:
    """
    参照座標 700×1024 のオーバーレイを、PDF ページ全体に比例拡大して重ねる。

    AUPQ35 PDF のページ比率は 700:1024 と一致するため、位置ズレの主因である
    「画像サイズ不一致」を解消できる。
    """
    base_rgb = np.array(page.convert("RGB"))
    h, w = base_rgb.shape[:2]
    ref_w, ref_h = _REF_SIZE

    ov, _orig = normalize_overlay_to_reference(overlay_img)
    target_w = max(1, int(round(ref_w * (w / ref_w) * overlay_scale)))
    target_h = max(1, int(round(ref_h * (h / ref_h) * overlay_scale)))
    ov_resized = ov.resize((target_w, target_h), Image.Resampling.LANCZOS)

    ov_arr = np.array(ov_resized)
    ov_rgb = ov_arr[:, :, :3]
    ov_alpha = ov_arr[:, :, 3] if ov_arr.shape[2] == 4 else None
    ov_mask = _overlay_to_mask(ov_rgb, alpha=ov_alpha, threshold=overlay_threshold)

    px0 = overlay_x
    py0 = overlay_y
    px1 = min(w, px0 + target_w)
    py1 = min(h, py0 + target_h)
    if px1 <= px0 or py1 <= py0:
        return Image.fromarray(base_rgb, "RGB")

    sx0 = sy0 = 0
    if px0 < 0:
        sx0 = -px0
        px0 = 0
    if py0 < 0:
        sy0 = -py0
        py0 = 0
    sx1 = sx0 + (px1 - px0)
    sy1 = sy0 + (py1 - py0)

    sub_mask = ov_mask[sy0:sy1, sx0:sx1]
    if not sub_mask.any():
        return Image.fromarray(base_rgb, "RGB")

    out_rgb = base_rgb.copy()
    out_rgb[py0:py1, px0:px1] = _paste_orange_lines(
        out_rgb[py0:py1, px0:px1],
        sub_mask,
        orange=ORANGE_RGB,
        alpha=overlay_alpha,
    )
    return Image.fromarray(out_rgb, "RGB")


def process_page(
    page: Image.Image,
    overlay_img: Image.Image,
    *,
    overlay_x: int,
    overlay_y: int,
    overlay_scale: float,
    overlay_threshold: int,
    overlay_alpha: float,
    panel_offsets: list[tuple[int, int]] | None = None,
    align_crosshairs: bool = False,
) -> Image.Image:
    base = page.convert("RGB")
    base_rgb = np.array(base)
    h, w = base_rgb.shape[:2]

    boxes = _panel_pixel_boxes(w, h)
    if len(boxes) != 2:
        return base

    ov_rgba = overlay_img.convert("RGBA")
    ov_w, ov_h = ov_rgba.size
    half_h = ov_h // 2

    out_rgb = base_rgb
    for i, panel_box in enumerate(boxes):
        x0, y0, x1, y1 = panel_box
        panel_w = max(1, x1 - x0)
        panel_h = max(1, y1 - y0)

        ov_crop = (
            ov_rgba.crop((0, 0, ov_w, half_h))
            if i == 0
            else ov_rgba.crop((0, half_h, ov_w, ov_h))
        )

        if align_crosshairs:
            tr, bl = find_crosshair_anchors(ov_crop)
            scale, px0, py0 = _crosshair_uniform_transform(tr, bl, panel_box)
            px0 += overlay_x
            py0 += overlay_y
            crop_w, crop_h = ov_crop.size
            target_w = max(1, int(round(crop_w * scale)))
            target_h = max(1, int(round(crop_h * scale)))
            cross_scaled = [
                (int(round(tr[0] * scale)), int(round(tr[1] * scale))),
                (int(round(bl[0] * scale)), int(round(bl[1] * scale))),
            ]
            ov_resized = ov_crop.resize((target_w, target_h), Image.Resampling.LANCZOS)
        else:
            target_w = int(panel_w * overlay_scale)
            target_h = int(panel_h * overlay_scale)
            dx_i, dy_i = (
                panel_offsets[i] if panel_offsets and i < len(panel_offsets) else (0, 0)
            )
            px0 = x0 + overlay_x + dx_i
            py0 = y0 + overlay_y + dy_i
            cross_scaled = []
            ov_resized = ov_crop.resize((target_w, target_h), Image.Resampling.LANCZOS)

        ov_arr = np.array(ov_resized)
        ov_rgb = ov_arr[:, :, :3]
        ov_alpha = ov_arr[:, :, 3] if ov_arr.shape[2] == 4 else None
        ov_mask = _overlay_to_mask(
            ov_rgb,
            alpha=ov_alpha,
            threshold=overlay_threshold,
        )
        if cross_scaled:
            radius = max(8, int(round(max(target_w, target_h) * 0.015)))
            ov_mask = _exclude_crosshair_pixels(ov_mask, cross_scaled, radius=radius)

        px1 = min(w, px0 + target_w)
        py1 = min(h, py0 + target_h)
        if px1 <= px0 or py1 <= py0:
            continue

        sx0 = 0
        sy0 = 0
        if px0 < 0:
            sx0 = -px0
            px0 = 0
        if py0 < 0:
            sy0 = -py0
            py0 = 0
        sx1 = sx0 + (px1 - px0)
        sy1 = sy0 + (py1 - py0)

        sub_mask = ov_mask[sy0:sy1, sx0:sx1]
        if not sub_mask.any():
            continue

        base_sub = out_rgb[py0:py1, px0:px1]
        base_sub2 = _paste_orange_lines(
            base_sub,
            sub_mask,
            orange=ORANGE_RGB,
            alpha=overlay_alpha,
        )
        out_rgb = out_rgb.copy()
        out_rgb[py0:py1, px0:px1] = base_sub2

    return Image.fromarray(out_rgb, "RGB")


def save_images_as_pdf(images: list[Image.Image], out_pdf: Path, dpi: int) -> None:
    if not images:
        raise ValueError("画像がありません")
    first, *rest = images
    first.save(
        out_pdf,
        format="PDF",
        resolution=float(dpi),
        save_all=True,
        append_images=rest,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AUPQ PDF に手書き海岸線を重ねます。")
    p.add_argument("input_pdf", help="入力 PDF（例: AUPQ35_12UTC.pdf）")
    p.add_argument(
        "--reference-overlay",
        type=Path,
        default=Path("assets/overlays/aupq_upper_coastline_reference.png"),
        help="完成見本 PNG からオレンジ海岸線だけを重ねる（推奨・既定）",
    )
    p.add_argument(
        "--handdrawn-overlay",
        action="store_true",
        help="手書き PNG モード（--overlay / --from-pages）を使う",
    )
    p.add_argument(
        "--overlay",
        type=Path,
        help="手書き海岸線 PNG（--handdrawn-overlay 時）",
    )
    p.add_argument(
        "--from-pages",
        type=Path,
        help="Pages ファイル (.pages) から海岸線 PNG を自動抽出して使う",
    )
    p.add_argument(
        "-o",
        "--output",
        help="出力 PDF パス（省略時: <入力名>_coastline_overlay.pdf）",
    )
    p.add_argument("--dpi", type=int, default=300, help="ラスタライズ解像度（既定 300）")
    p.add_argument("--png-dir", type=Path, help="ページ PNG も保存するディレクトリ")

    # 微調整（まずは 0/1.0 のままで OK）
    p.add_argument("--overlay-x", type=int, default=0, help="重ね合わせXオフセット(px)")
    p.add_argument("--overlay-y", type=int, default=0, help="重ね合わせYオフセット(px)")
    p.add_argument("--overlay-scale", type=float, default=1.0, help="重ね合わせスケール")
    p.add_argument(
        "--overlay-threshold",
        type=int,
        default=235,
        help="オーバーレイの白背景を落とす閾値（大きいほど線が細く残る）",
    )
    p.add_argument(
        "--overlay-alpha",
        type=float,
        default=0.95,
        help="オレンジの濃さ（1.0=完全置換, 0.5=半透明）",
    )
    p.add_argument(
        "--per-panel",
        action="store_true",
        help="旧方式: 上下パネルごとに分割して重ねる（通常は不要）",
    )
    p.add_argument(
        "--align-crosshairs",
        action="store_true",
        help="PNG内の右上・左下の十字を、図の右上角・左下角に合わせて拡大縮小",
    )
    p.add_argument(
        "--auto-align",
        action="store_true",
        help="1ページ目から各パネルのズレ(dx,dy)を自動推定して補正する（--align-crosshairs と併用不可）",
    )
    p.add_argument("--align-search", type=int, default=90, help="自動位置合わせ探索範囲(px)")
    p.add_argument("--align-downsample", type=int, default=6, help="自動位置合わせの縮小率(大きいほど高速)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    in_pdf = Path(args.input_pdf).expanduser().resolve()
    if not in_pdf.is_file():
        print(f"入力 PDF が見つかりません: {in_pdf}", file=sys.stderr)
        return 1

    use_reference = not args.handdrawn_overlay and not args.from_pages and not args.overlay

    if args.from_pages:
        pages_path = args.from_pages.expanduser().resolve()
        if not pages_path.is_file():
            print(f"Pages ファイルが見つかりません: {pages_path}", file=sys.stderr)
            return 1
        overlay_path = in_pdf.with_name(f"{in_pdf.stem}_overlay_from_pages.png")
        try:
            extract_overlay_from_pages(pages_path, overlay_path)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        print(f"Pages から抽出: {overlay_path}")
        use_reference = False
    elif args.overlay:
        overlay_path = args.overlay.expanduser().resolve()
        use_reference = False
    elif use_reference:
        overlay_path = args.reference_overlay.expanduser().resolve()
    else:
        overlay_path = Path("assets/overlays/nihon_coastline_overlay.png").resolve()
        use_reference = False

    if not overlay_path.is_file():
        print(f"オーバーレイ画像が見つかりません: {overlay_path}", file=sys.stderr)
        return 1

    out_pdf = (
        Path(args.output).expanduser().resolve()
        if args.output
        else in_pdf.with_name(f"{in_pdf.stem}_coastline_overlay.pdf")
    )

    print(f"入力: {in_pdf}")
    print(f"DPI: {args.dpi}")

    reference_png: Image.Image | None = None
    overlay_img: Image.Image | None = None
    if use_reference:
        reference_png = Image.open(overlay_path)
        print(f"完成見本モード: {overlay_path}")
        print("  → 参照 PNG からオレンジ海岸線だけを抽出して重ねます")
    else:
        overlay_img = Image.open(overlay_path)
        ov_norm, ov_orig = normalize_overlay_to_reference(overlay_img)
        if ov_orig != _REF_SIZE:
            print(f"オーバーレイを参照サイズに変換: {ov_orig[0]}×{ov_orig[1]} → {_REF_SIZE[0]}×{_REF_SIZE[1]}")
        overlay_img = ov_norm

    pages = convert_from_path(str(in_pdf), dpi=args.dpi)

    use_reference_fit = (
        not use_reference and not args.per_panel and not args.align_crosshairs
    )
    if use_reference_fit:
        print("配置: 参照サイズ 700×1024 → PDF ページ全体に比例拡大")

    panel_offsets: list[tuple[int, int]] | None = None
    if args.auto_align and args.align_crosshairs:
        print("--auto-align と --align-crosshairs は同時には使えません。", file=sys.stderr)
        return 1

    if args.per_panel or args.align_crosshairs:
        if args.align_crosshairs:
            ov_rgba = overlay_img.convert("RGBA")
            ov_w, ov_h = ov_rgba.size
            half_h = ov_h // 2
            for label, crop in (
                ("upper", ov_rgba.crop((0, 0, ov_w, half_h))),
                ("lower", ov_rgba.crop((0, half_h, ov_w, ov_h))),
            ):
                try:
                    tr, bl = find_crosshair_anchors(crop)
                    print(f"crosshairs ({label}): TR={tr} BL={bl}")
                except ValueError as exc:
                    print(str(exc), file=sys.stderr)
                    return 1
        elif args.auto_align and pages:
            panel_offsets = estimate_panel_offsets(
                pages[0],
                overlay_img,
                overlay_scale=args.overlay_scale,
                overlay_threshold=args.overlay_threshold,
                downsample=args.align_downsample,
                search_px=args.align_search,
            )
            print(f"auto-align offsets: upper={panel_offsets[0]} lower={panel_offsets[1]}")

    out_images: list[Image.Image] = []
    for page in pages:
        if use_reference:
            assert reference_png is not None
            out_images.append(
                process_page_from_reference_png(
                    page,
                    reference_png,
                    overlay_x=args.overlay_x,
                    overlay_y=args.overlay_y,
                    overlay_scale=args.overlay_scale,
                )
            )
        elif use_reference_fit:
            assert overlay_img is not None
            out_images.append(
                process_page_reference_fit(
                    page,
                    overlay_img,
                    overlay_x=args.overlay_x,
                    overlay_y=args.overlay_y,
                    overlay_scale=args.overlay_scale,
                    overlay_threshold=args.overlay_threshold,
                    overlay_alpha=args.overlay_alpha,
                )
            )
        else:
            assert overlay_img is not None
            out_images.append(
                process_page(
                    page,
                    overlay_img,
                    overlay_x=args.overlay_x,
                    overlay_y=args.overlay_y,
                    overlay_scale=args.overlay_scale,
                    overlay_threshold=args.overlay_threshold,
                    overlay_alpha=args.overlay_alpha,
                    panel_offsets=panel_offsets,
                    align_crosshairs=args.align_crosshairs,
                )
            )

    if args.png_dir:
        args.png_dir.mkdir(parents=True, exist_ok=True)
        for i, im in enumerate(out_images, start=1):
            png_path = args.png_dir / f"{in_pdf.stem}_p{i:02d}.png"
            im.save(png_path, format="PNG")
            print(f"PNG: {png_path}")

    save_images_as_pdf(out_images, out_pdf, args.dpi)
    print(f"出力 PDF: {out_pdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

