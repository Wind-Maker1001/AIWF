from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from aiwf.office_writer_docx import write_audit_docx as _write_audit_docx_impl
from aiwf.office_writer_pptx import write_deck_pptx as _write_deck_pptx_impl
from aiwf.office_writer_xlsx import write_fin_xlsx as _write_fin_xlsx_impl
from aiwf.office_visuals import (
    write_profile_illustration_png as _write_profile_illustration_png_impl,
)
from aiwf.office_style import (
    office_text,
)


def write_profile_illustration_png(
    path: str,
    profile: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
    *,
    utc_now_str,
) -> bool:
    return _write_profile_illustration_png_impl(
        path,
        profile,
        params,
        utc_now_str=utc_now_str,
    )


def write_fin_xlsx(
    xlsx_path: str,
    rows: List[Dict[str, Any]],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    *,
    to_decimal,
    build_profile,
    utc_now_str,
) -> None:
    _write_fin_xlsx_impl(
        xlsx_path,
        rows,
        image_path,
        params,
        to_decimal=to_decimal,
        build_profile=build_profile,
        utc_now_str=utc_now_str,
    )


def write_audit_docx(
    docx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    *,
    utc_now_str,
) -> None:
    _write_audit_docx_impl(
        docx_path,
        job_id,
        profile,
        image_path,
        params,
        utc_now_str=utc_now_str,
    )


def write_deck_pptx(
    pptx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    *,
    utc_now_str,
) -> None:
    _write_deck_pptx_impl(
        pptx_path,
        job_id,
        profile,
        image_path,
        params,
        utc_now_str=utc_now_str,
    )
