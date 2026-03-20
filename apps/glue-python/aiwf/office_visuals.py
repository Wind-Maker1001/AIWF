from __future__ import annotations

import os
from typing import Any, Dict, Optional

from aiwf.office_style import office_lang, office_text, office_theme_settings, pil_font


def write_profile_illustration_png(
    path: str,
    profile: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
    *,
    utc_now_str,
) -> bool:
    try:
        from PIL import Image, ImageDraw  # type: ignore
    except Exception:
        return False

    image_params = dict(params or {})
    if office_lang(params) == "zh" and pil_font(20, params) is None:
        image_params["office_lang"] = "en"
    theme = office_theme_settings(image_params)
    width = 1200
    height = 720
    img = Image.new("RGB", (width, height), color=theme["bg_rgb"])
    draw = ImageDraw.Draw(img)
    font_title = pil_font(40, image_params, bold=True)
    font_time = pil_font(20, image_params)
    font_card_label = pil_font(24, image_params)
    font_card_value = pil_font(34, image_params, bold=True)
    font_chart = pil_font(22, image_params)
    font_bar_label = pil_font(18, image_params)
    font_bar_value = pil_font(20, image_params, bold=True)

    head_hex = str(theme.get("primary_hex", "1F4E78"))
    head_rgb = tuple(int(head_hex[i : i + 2], 16) for i in (0, 2, 4))
    draw.rectangle([(0, 0), (width, 88)], fill=head_rgb)
    draw.text(
        (32, 28),
        f"{theme.get('report_title')} - {office_text('质量快照', 'Snapshot', image_params)}",
        fill=(255, 255, 255),
        font=font_title,
    )
    draw.text((820, 32), utc_now_str(), fill=(220, 230, 240), font=font_time)

    quality = profile.get("quality") if isinstance(profile.get("quality"), dict) else {}
    kpis = [
        (office_text("总行数", "Rows", image_params), int(profile.get("rows", 0) or 0)),
        (office_text("输入行", "Input", image_params), int(quality.get("input_rows", 0) or 0)),
        (office_text("输出行", "Output", image_params), int(quality.get("output_rows", 0) or 0)),
        (office_text("无效行", "Invalid", image_params), int(quality.get("invalid_rows", 0) or 0)),
        (office_text("过滤行", "Filtered", image_params), int(quality.get("filtered_rows", 0) or 0)),
    ]

    card_w = 210
    card_h = 116
    gap = 18
    start_x = 36
    y = 118
    for i, (name, val) in enumerate(kpis):
        x = start_x + i * (card_w + gap)
        draw.rounded_rectangle(
            [(x, y), (x + card_w, y + card_h)],
            radius=12,
            fill=(255, 255, 255),
            outline=(210, 218, 230),
        )
        draw.text((x + 16, y + 18), name, fill=(80, 90, 110), font=font_card_label)
        draw.text((x + 16, y + 58), str(val), fill=(26, 34, 50), font=font_card_value)

    chart_x = 72
    chart_y = 320
    chart_w = 1050
    chart_h = 300
    draw.rectangle(
        [(chart_x, chart_y), (chart_x + chart_w, chart_y + chart_h)],
        fill=(255, 255, 255),
        outline=(210, 218, 230),
    )
    draw.text(
        (chart_x + 18, chart_y + 12),
        office_text("流程数据量", "Pipeline Volume", image_params),
        fill=(55, 65, 85),
        font=font_chart,
    )

    bar_vals = [
        (office_text("输入", "Input", image_params), int(quality.get("input_rows", 0) or 0), (70, 130, 180)),
        (office_text("输出", "Output", image_params), int(quality.get("output_rows", 0) or 0), (46, 164, 79)),
        (office_text("无效", "Invalid", image_params), int(quality.get("invalid_rows", 0) or 0), (215, 70, 70)),
        (office_text("过滤", "Filtered", image_params), int(quality.get("filtered_rows", 0) or 0), (235, 160, 60)),
    ]
    max_v = max([v for _, v, _ in bar_vals] + [1])
    usable_h = chart_h - 90
    bar_w = 160
    bar_gap = 68
    bx = chart_x + 80
    base_y = chart_y + chart_h - 38
    for name, val, color in bar_vals:
        h = int((float(val) / float(max_v)) * usable_h)
        draw.rectangle([(bx, base_y - h), (bx + bar_w, base_y)], fill=color)
        draw.text((bx + 20, base_y + 8), name, fill=(70, 80, 100), font=font_bar_label)
        draw.text((bx + 20, base_y - h - 24), str(val), fill=(40, 50, 70), font=font_bar_value)
        bx += bar_w + bar_gap

    os.makedirs(os.path.dirname(path), exist_ok=True)
    img.save(path)
    return True
