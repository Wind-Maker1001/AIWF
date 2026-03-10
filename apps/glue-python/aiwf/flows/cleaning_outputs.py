from __future__ import annotations

import csv
import json
from typing import Any, Callable, Dict, List, Optional


def write_cleaned_csv_impl(csv_path: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    if rows:
        columns = list(rows[0].keys())
        seen = set(columns)
        for row in rows[1:]:
            for key in row.keys():
                if key not in seen:
                    columns.append(key)
                    seen.add(key)
    else:
        columns = ["id", "amount"]

    with open(csv_path, "w", encoding="utf-8", newline="\n") as file:
        writer = csv.DictWriter(file, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})
    return {"rows": len(rows), "cols": len(columns)}


def write_cleaned_parquet_impl(parquet_path: str, rows: List[Dict[str, Any]]) -> None:
    try:
        import pandas as pd  # type: ignore

        dataframe = pd.DataFrame(rows)
        dataframe.to_parquet(parquet_path, index=False)
    except Exception:
        with open(parquet_path, "wb") as file:
            file.write(b"PARQUET_PLACEHOLDER\n")


def write_fin_xlsx_impl(
    xlsx_path: str,
    rows: List[Dict[str, Any]],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    *,
    office_write_fin_xlsx: Callable[..., None],
    to_decimal: Callable[[Any], Any],
    build_profile: Callable[[List[Dict[str, Any]], Dict[str, Any], str], Dict[str, Any]],
    utc_now_str: Callable[[], str],
) -> None:
    office_write_fin_xlsx(
        xlsx_path,
        rows,
        image_path,
        params,
        to_decimal=to_decimal,
        build_profile=build_profile,
        utc_now_str=utc_now_str,
    )


def write_audit_docx_impl(
    docx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    *,
    office_write_audit_docx: Callable[..., None],
    utc_now_str: Callable[[], str],
) -> None:
    office_write_audit_docx(docx_path, job_id, profile, image_path, params, utc_now_str=utc_now_str)


def write_deck_pptx_impl(
    pptx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    *,
    office_write_deck_pptx: Callable[..., None],
    utc_now_str: Callable[[], str],
) -> None:
    office_write_deck_pptx(pptx_path, job_id, profile, image_path, params, utc_now_str=utc_now_str)


def write_profile_json_impl(profile_path: str, profile: Dict[str, Any], params: Dict[str, Any]) -> None:
    payload = {
        "profile": profile,
        "params": params or {},
    }
    with open(profile_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
