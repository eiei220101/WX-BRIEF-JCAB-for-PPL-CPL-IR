#!/usr/bin/env python3
"""
AUPQ 高層天気図 PDF の海岸線（日本・大陸）を自動検出し、オレンジ色で強調する。

既定入力: カレントディレクトリの ``AUPQ35_12UTC.pdf``

----------------------------------------------------------------------
必要な環境（README）
----------------------------------------------------------------------

Python パッケージ::

    pip install pdf2image Pillow numpy

任意（形態学フィルタを OpenCV で行う場合）::

    pip install opencv-python

システム依存（PDF → 画像 rasterize に必須）:

* **macOS (Homebrew):** ``brew install poppler``
* **Ubuntu / Debian:** ``sudo apt-get install poppler-utils``
* **Windows:** Poppler for Windows を PATH に通す
  https://github.com/oschwartz10612/poppler-windows/releases

使い方::

    python scripts/aupq_coastline_orange.py
    python scripts/aupq_coastline_orange.py AUPQ78_12UTC.pdf -o AUPQ78_orange.pdf --dpi 300

出力:

* ``<stem>_coastline_orange.pdf`` … 色替え後の PDF（既定）
* ``--png-dir ./out_png`` … ページ画像も保存する場合

注意:

* 気象庁 AUPQ 図は海岸線が**薄いグレー**、等圧線は**黒**で描かれている。
  本スクリプトはグレー帯＋「海（白）に接する細線」で海岸線を推定する。
* 図面や発表時刻で閾値が合わない場合は ``--gray-min`` / ``--gray-max`` を調整してください。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
from PIL import Image

try:
    from pdf2image import convert_from_path
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "pdf2image が必要です: pip install pdf2image\n"
        "また poppler-utils（macOS: brew install poppler）を入れてください。"
    ) from exc

try:
    import cv2  # type: ignore

    _HAS_CV2 = True
except ImportError:
    _HAS_CV2 = False

# 気象庁 AUPQ35 / AUPQ78（700×1024 相当レイアウト）の地図枠（参照座標）
_REF_PANELS = (
    (20, 42, 679, 499),   # 上段（例: 300hPa）
    (20, 500, 679, 977),  # 下段（例: 500hPa）
)
_REF_SIZE = (700, 1024)

ORANGE_RGB = (255, 136, 0)


def _panel_pixel_boxes(width: int, height: int) -> list[tuple[int, int, int, int]]:
    """参照レイアウトを実画像サイズへスケールした地図枠 (x0,y0,x1,y1)。"""
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


def _neighbor_masks(
    gray: np.ndarray,
    *,
    white_threshold: int = 248,
    land_threshold: int = 220,
) -> tuple[np.ndarray, np.ndarray]:
    """8 近傍に白（海）／陸側（非白）があるか。"""
    g = gray.astype(np.int16)
    h, w = g.shape
    has_white = np.zeros((h, w), dtype=bool)
    has_land = np.zeros((h, w), dtype=bool)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            shifted = np.pad(g, 1, mode="edge")[1 + dy : 1 + dy + h, 1 + dx : 1 + dx + w]
            has_white |= shifted >= white_threshold
            has_land |= shifted < land_threshold
    return has_white, has_land


def _white_neighbor_mask(gray: np.ndarray) -> np.ndarray:
    """8 近傍に白（海）があるピクセル = 海岸線候補になりやすい。"""
    has_white, _ = _neighbor_masks(gray)
    return has_white


def _dark_neighbor_count(gray: np.ndarray) -> np.ndarray:
    """3×3 内の黒っぽいピクセル数（等圧線・文字の除外用）。"""
    g = gray.astype(np.int16)
    h, w = g.shape
    cnt = np.zeros((h, w), dtype=np.int16)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            shifted = np.pad(g, 1, mode="edge")[1 + dy : 1 + dy + h, 1 + dx : 1 + dx + w]
            cnt += (shifted < 95).astype(np.int16)
    return cnt


def _thin_dilate(mask: np.ndarray) -> np.ndarray:
    """1px だけ膨張（MaxFilter(3) より控えめ）。"""
    if not mask.any():
        return mask
    h, w = mask.shape
    out = mask.copy()
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            shifted = np.pad(mask, 1, mode="constant", constant_values=False)[
                1 + dy : 1 + dy + h, 1 + dx : 1 + dx + w
            ]
            out |= shifted
    return out


def _refine_coastline_mask(mask: np.ndarray, *, thicken: int = 1) -> np.ndarray:
    """
    細い海岸線を少し太らせて見やすくする（侵食はしない）。

    MinFilter は 1px の海岸線を丸ごと消してしまうため使わない。
    """
    if not mask.any() or thicken <= 0:
        return mask

    refined = mask
    for _ in range(thicken):
        if _HAS_CV2:
            m = (refined.astype(np.uint8) * 255)
            m = cv2.dilate(m, np.ones((2, 2), np.uint8), iterations=1)
            refined = m > 0
        else:
            refined = _thin_dilate(refined)
    return refined


def detect_coastline_mask(
    gray: np.ndarray,
    *,
    gray_min: int = 150,
    gray_max: int = 240,
    coast_gray_min: int = 70,
    coast_gray_max: int = 160,
    max_dark_neighbors: int = 2,
    thicken: int = 1,
) -> np.ndarray:
    """
    地図パネル内の海岸線らしいピクセルを推定する。

    気象庁 AUPQ はラスタライズ後、海岸線が次のどちらか（または両方）になる:

    - 薄いグレー（おおよそ 150–240）
    - やや濃いグレー（おおよそ 70–175）で、白い海と陸側の境界

    等圧線（黒）や文字は、黒近傍が多い画素として除外する。
    """
    has_white, has_land = _neighbor_masks(gray)

    light_coast = (gray >= gray_min) & (gray <= gray_max) & has_white

    boundary_coast = (
        (gray >= coast_gray_min)
        & (gray <= coast_gray_max)
        & has_white
        & has_land
    )

    coast = (light_coast | boundary_coast) & (gray >= 45)

    dark_cnt = _dark_neighbor_count(gray)
    coast &= dark_cnt <= max_dark_neighbors

    return _refine_coastline_mask(coast, thicken=thicken)


def apply_orange_coastlines(
    rgb: np.ndarray,
    mask: np.ndarray,
    orange: tuple[int, int, int] = ORANGE_RGB,
) -> np.ndarray:
    """マスク位置をオレンジに置換（他ピクセルは維持）。"""
    out = rgb.copy()
    out[mask] = orange
    return out


def process_page(
    pil_image: Image.Image,
    *,
    gray_min: int,
    gray_max: int,
    coast_gray_min: int,
    coast_gray_max: int,
    max_dark_neighbors: int,
    thicken: int,
) -> tuple[Image.Image, int]:
    """1 ページ分を処理。戻り値は (画像, オレンジ化ピクセル数)。"""
    rgb = np.array(pil_image.convert("RGB"))
    gray = np.array(pil_image.convert("L"))
    h, w = gray.shape
    full_mask = np.zeros((h, w), dtype=bool)

    for x0, y0, x1, y1 in _panel_pixel_boxes(w, h):
        if x1 <= x0 or y1 <= y0:
            continue
        sub_gray = gray[y0:y1, x0:x1]
        sub_mask = detect_coastline_mask(
            sub_gray,
            gray_min=gray_min,
            gray_max=gray_max,
            coast_gray_min=coast_gray_min,
            coast_gray_max=coast_gray_max,
            max_dark_neighbors=max_dark_neighbors,
            thicken=thicken,
        )
        full_mask[y0:y1, x0:x1] |= sub_mask

    orange_pixels = int(full_mask.sum())
    out_rgb = apply_orange_coastlines(rgb, full_mask)
    return Image.fromarray(out_rgb, "RGB"), orange_pixels


def pdf_to_orange_images(
    pdf_path: Path,
    *,
    dpi: int = 300,
    gray_min: int = 150,
    gray_max: int = 240,
    coast_gray_min: int = 70,
    coast_gray_max: int = 160,
    max_dark_neighbors: int = 2,
    thicken: int = 1,
) -> tuple[list[Image.Image], list[int]]:
    pages = convert_from_path(str(pdf_path), dpi=dpi)
    images: list[Image.Image] = []
    counts: list[int] = []
    for page in pages:
        image, count = process_page(
            page,
            gray_min=gray_min,
            gray_max=gray_max,
            coast_gray_min=coast_gray_min,
            coast_gray_max=coast_gray_max,
            max_dark_neighbors=max_dark_neighbors,
            thicken=thicken,
        )
        images.append(image)
        counts.append(count)
    return images, counts


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
    p = argparse.ArgumentParser(
        description="AUPQ 天気図 PDF の海岸線をオレンジ色に自動変換します。",
    )
    p.add_argument(
        "input_pdf",
        nargs="?",
        default="AUPQ35_12UTC.pdf",
        help="入力 PDF（既定: AUPQ35_12UTC.pdf）",
    )
    p.add_argument(
        "-o",
        "--output",
        help="出力 PDF パス（省略時: <入力名>_coastline_orange.pdf）",
    )
    p.add_argument("--dpi", type=int, default=300, help="ラスタライズ解像度（既定 300）")
    p.add_argument("--gray-min", type=int, default=150, help="薄いグレー海岸線の下限")
    p.add_argument("--gray-max", type=int, default=240, help="薄いグレー海岸線の上限")
    p.add_argument("--coast-gray-min", type=int, default=70, help="濃いグレー海岸線の下限")
    p.add_argument("--coast-gray-max", type=int, default=160, help="濃いグレー海岸線の上限")
    p.add_argument(
        "--max-dark-neighbors",
        type=int,
        default=2,
        help="3×3 内の黒ピクセル許容数（超えると等圧線扱いで除外）",
    )
    p.add_argument(
        "--thicken",
        type=int,
        default=1,
        help="海岸線を太らせる回数（0=そのまま、1=推奨）",
    )
    p.add_argument(
        "--png-dir",
        type=Path,
        help="ページ PNG も保存するディレクトリ",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    in_pdf = Path(args.input_pdf).expanduser().resolve()
    if not in_pdf.is_file():
        print(f"入力 PDF が見つかりません: {in_pdf}", file=sys.stderr)
        return 1

    out_pdf = (
        Path(args.output).expanduser().resolve()
        if args.output
        else in_pdf.with_name(f"{in_pdf.stem}_coastline_orange.pdf")
    )

    print(f"入力: {in_pdf}")
    print(
        f"DPI: {args.dpi}  薄灰: {args.gray_min}–{args.gray_max}  "
        f"濃灰: {args.coast_gray_min}–{args.coast_gray_max}"
    )
    if _HAS_CV2:
        print("線の強調: OpenCV dilate")
    else:
        print("線の強調: 細線を1px太らせる（opencv-python 未導入）")

    images, orange_counts = pdf_to_orange_images(
        in_pdf,
        dpi=args.dpi,
        gray_min=args.gray_min,
        gray_max=args.gray_max,
        coast_gray_min=args.coast_gray_min,
        coast_gray_max=args.coast_gray_max,
        max_dark_neighbors=args.max_dark_neighbors,
        thicken=args.thicken,
    )
    print(f"ページ数: {len(images)}")
    for i, count in enumerate(orange_counts, start=1):
        print(f"  ページ {i}: オレンジ化ピクセル {count:,}")
        if count == 0:
            print(
                "  ※ 0 件です。--gray-min / --gray-max / --coast-gray-min を広げて再実行してください。",
                file=sys.stderr,
            )

    if args.png_dir:
        args.png_dir.mkdir(parents=True, exist_ok=True)
        for i, im in enumerate(images, start=1):
            png_path = args.png_dir / f"{in_pdf.stem}_p{i:02d}.png"
            im.save(png_path, format="PNG")
            print(f"PNG: {png_path}")

    save_images_as_pdf(images, out_pdf, args.dpi)
    print(f"出力 PDF: {out_pdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
