from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Tuple

from aiwf.preprocess_enrichment import enrich_standardized_evidence_rows, normalize_rows_with_ftfy


_STANDARDIZED_EVIDENCE_DEDUP_FIELD_ALIASES = {
    "text": "claim_text",
    "content": "claim_text",
    "body": "claim_text",
    "paragraph": "claim_text",
    "claim": "claim_text",
    "author": "speaker",
    "name": "speaker",
    "speaker_name": "speaker",
    "url": "source_url",
    "link": "source_url",
    "source_link": "source_url",
    "title": "source_title",
    "source_name": "source_title",
    "topic": "debate_topic",
    "lang": "language",
}

_SOURCE_CONTEXT_FIELDS = ("source_title", "source_url", "source_domain", "published_at", "citation_text")
_EMBEDDED_TURN_LABEL_RE = re.compile(
    r"(?<!\w)(?:Speaker\s+[A-Za-z0-9_.-]+|Moderator|Host|Judge|[A-Z])\s*[:：]",
    flags=re.I,
)
_SECTION_HEADING_BRACKETS = (("【", "】"), ("[", "]"), ("(", ")"), ("（", "）"))


def _normalized_runtime_text(value: Any) -> str:
    return str(value or "").strip()


def _row_group_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _normalized_runtime_text(row.get("source_path")),
        _normalized_runtime_text(row.get("page")),
        _normalized_runtime_text(row.get("sheet_name")),
    )


def _looks_section_heading_row(row: Dict[str, Any]) -> bool:
    if _normalized_runtime_text(row.get("argument_role")).lower() == "section":
        return True
    claim_text = _normalized_runtime_text(row.get("claim_text"))
    if not claim_text or len(claim_text) > 80:
        return False
    if _normalized_runtime_text(row.get("speaker")):
        return False
    if any(_normalized_runtime_text(row.get(field)) for field in _SOURCE_CONTEXT_FIELDS):
        return False
    return any(
        claim_text.startswith(left) and claim_text.endswith(right)
        for left, right in _SECTION_HEADING_BRACKETS
    )


def _looks_source_context_row(row: Dict[str, Any]) -> bool:
    return any(_normalized_runtime_text(row.get(field)) for field in _SOURCE_CONTEXT_FIELDS)


def _claim_like_row(row: Dict[str, Any]) -> bool:
    claim_text = _normalized_runtime_text(row.get("claim_text"))
    if not claim_text or _looks_section_heading_row(row):
        return False
    role = _normalized_runtime_text(row.get("argument_role")).lower()
    if role in {"quote", "evidence", "citation", "moderation", "question", "metadata", "section"}:
        return False
    if _looks_source_context_row(row) and not _normalized_runtime_text(row.get("speaker")):
        source_title = _normalized_runtime_text(row.get("source_title"))
        source_url = _normalized_runtime_text(row.get("source_url"))
        if claim_text in {source_title, source_url}:
            return False
    return True


def _source_context_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        field: row.get(field)
        for field in _SOURCE_CONTEXT_FIELDS
        if _normalized_runtime_text(row.get(field))
    }


def _normalized_source_key(value: Any) -> str:
    text = _normalized_runtime_text(value).lower()
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"\[[^\]]{1,40}\]", " ", text)
    text = re.sub(r"[^0-9a-z\u4e00-\u9fff\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _merge_source_context(row: Dict[str, Any], context: Dict[str, Any]) -> bool:
    row_title = _normalized_runtime_text(row.get("source_title"))
    context_title = _normalized_runtime_text(context.get("source_title"))
    if row_title and context_title and _normalized_source_key(row_title) != _normalized_source_key(context_title):
        return False
    row_url = _normalized_runtime_text(row.get("source_url")).rstrip("/")
    context_url = _normalized_runtime_text(context.get("source_url")).rstrip("/")
    if row_url and context_url and row_url.lower() != context_url.lower():
        return False
    changed = False
    for field, value in context.items():
        if not _normalized_runtime_text(row.get(field)) and _normalized_runtime_text(value):
            row[field] = value
            changed = True
    return changed


def _propagate_trailing_source_context(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return rows
    propagated = [dict(row) for row in rows]
    pending_claim_indices: List[int] = []
    contiguous_source_indices: List[int] = []
    current_source_context: Dict[str, Any] = {}
    current_group: Tuple[str, str, str] | None = None
    source_block_open = False

    for index, row in enumerate(propagated):
        group_key = _row_group_key(row)
        if group_key != current_group:
            current_group = group_key
            pending_claim_indices = []
            contiguous_source_indices = []
            current_source_context = {}
            source_block_open = False

        if _looks_section_heading_row(row):
            pending_claim_indices = []
            contiguous_source_indices = []
            current_source_context = {}
            source_block_open = False
            continue

        if _normalized_runtime_text(row.get("argument_role")).lower() in {"metadata", "section"}:
            pending_claim_indices = []
            contiguous_source_indices = []
            current_source_context = {}
            source_block_open = False
            continue

        if _claim_like_row(row):
            if source_block_open:
                pending_claim_indices = []
                contiguous_source_indices = []
                current_source_context = {}
            pending_claim_indices.append(index)
            source_block_open = False
            continue

        if _looks_source_context_row(row):
            if not source_block_open:
                contiguous_source_indices = []
                current_source_context = {}
            contiguous_source_indices.append(index)
            current_source_context.update(_source_context_from_row(row))
            _merge_source_context(propagated[index], current_source_context)
            for target_index in pending_claim_indices + contiguous_source_indices[:-1]:
                _merge_source_context(propagated[target_index], current_source_context)
            source_block_open = True
            continue

        source_block_open = False

    return propagated


def _effective_deduplicate_fields(deduplicate_by: List[str], *, standardize_evidence: bool) -> List[str]:
    if not standardize_evidence:
        return [str(field) for field in deduplicate_by if str(field).strip()]
    remapped: List[str] = []
    seen: set[str] = set()
    for field in deduplicate_by:
        key = str(field).strip()
        if not key:
            continue
        effective = _STANDARDIZED_EVIDENCE_DEDUP_FIELD_ALIASES.get(key.lower(), key)
        if effective in seen:
            continue
        seen.add(effective)
        remapped.append(effective)
    return remapped


def _first_text_field(row: Dict[str, Any], preferred_field: str) -> str:
    if preferred_field and _normalized_runtime_text(row.get(preferred_field)):
        return preferred_field
    for candidate in ("claim_text", "text", "content", "body", "paragraph", "note"):
        if _normalized_runtime_text(row.get(candidate)):
            return candidate
    return preferred_field


def _split_embedded_debate_turns(text: Any) -> List[str]:
    normalized = _normalized_runtime_text(text)
    if not normalized:
        return []
    matches = list(_EMBEDDED_TURN_LABEL_RE.finditer(normalized))
    if len(matches) <= 1:
        return [normalized]
    out: List[str] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        segment = normalized[start:end].strip()
        if segment:
            out.append(segment)
    return out or [normalized]


def preprocess_rows_impl(
    rows: List[Dict[str, Any]],
    spec: Dict[str, Any],
    *,
    normalize_header: Callable[[str], str],
    normalize_amount: Callable[[Any, int], Any],
    normalize_date: Callable[[Any, str, List[str]], Any],
    apply_field_transform: Callable[[Any, str, Dict[str, Any]], Tuple[Any, bool]],
    filter_match: Callable[[Dict[str, Any], Dict[str, Any]], bool],
    chunk_text: Callable[[str, str, int], List[str]],
    to_canonical_evidence_row: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    apply_conflict_detection: Callable[[List[Dict[str, Any]], Dict[str, Any]], Tuple[List[Dict[str, Any]], int]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows, encoding_stats = normalize_rows_with_ftfy(rows, spec)
    header_map = spec.get("header_map") if isinstance(spec.get("header_map"), dict) else {}
    null_values = [str(x).strip().lower() for x in (spec.get("null_values") or ["null", "none", "na", "n/a"])]
    amount_fields = [str(x) for x in (spec.get("amount_fields") or ["amount"])]
    date_fields = [str(x) for x in (spec.get("date_fields") or [])]
    amount_round_digits = int(spec.get("amount_round_digits", 2))
    trim_strings = bool(spec.get("trim_strings", True))
    drop_empty_rows = bool(spec.get("drop_empty_rows", True))
    date_output_format = str(spec.get("date_output_format", "%Y-%m-%d"))
    date_input_formats = spec.get("date_input_formats") or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
    ]
    defaults = spec.get("default_values") if isinstance(spec.get("default_values"), dict) else {}
    include_fields = [str(x) for x in (spec.get("include_fields") or [])]
    exclude_fields = [str(x) for x in (spec.get("exclude_fields") or [])]
    field_transforms = spec.get("field_transforms") if isinstance(spec.get("field_transforms"), list) else []
    row_filters = spec.get("row_filters") if isinstance(spec.get("row_filters"), list) else []
    deduplicate_by = [str(x) for x in (spec.get("deduplicate_by") or [])]
    deduplicate_keep = str(spec.get("deduplicate_keep") or "first").strip().lower()
    standardize_evidence = bool(spec.get("standardize_evidence", False))
    evidence_schema = spec.get("evidence_schema") if isinstance(spec.get("evidence_schema"), dict) else {}
    chunk_mode = str(spec.get("chunk_mode") or "none").strip().lower()
    chunk_field = str(spec.get("chunk_field") or ("claim_text" if standardize_evidence else "text")).strip()
    chunk_max_chars = int(spec.get("chunk_max_chars", 500))

    out: List[Dict[str, Any]] = []
    dropped_empty = 0
    normalized_amount_cells = 0
    normalized_date_cells = 0
    transformed_cells = 0
    dropped_by_filters = 0
    duplicate_rows_removed = 0
    standardized_rows = 0
    chunked_rows_created = 0
    source_context_backfilled_rows = 0
    source_title_enriched_rows = 0
    citation_candidate_rows = 0
    citation_backend_success_rows = 0
    url_metadata_candidate_rows = 0
    url_metadata_enriched_rows = 0
    structural_duplicate_rows_removed = 0
    block_source_context_backfilled_rows = 0
    structural_source_context_backfilled_rows = 0
    adjacent_page_source_context_backfilled_rows = 0
    section_topic_rows_propagated = 0
    metadata_topic_rows_propagated = 0
    metadata_stance_rows_propagated = 0
    metadata_speaker_role_rows_propagated = 0
    heading_argument_role_rows_propagated = 0
    citation_token_source_backfilled_rows = 0
    leading_source_context_backfilled_rows = 0
    adjacent_citation_url_rows_collapsed = 0
    wrapped_claim_rows_merged = 0
    source_url_normalized_rows = 0
    citation_text_url_normalized_rows = 0
    source_marker_claim_replaced_rows = 0
    multi_source_citation_appended_rows = 0
    engine_trace: List[Dict[str, Any]] = list(encoding_stats.get("engine_trace") or [])

    for raw in rows:
        row: Dict[str, Any] = {}
        for key, value in dict(raw or {}).items():
            normalized_key = header_map.get(key, normalize_header(key))
            normalized_value = value
            if isinstance(normalized_value, str) and trim_strings:
                normalized_value = normalized_value.strip()
            if isinstance(normalized_value, str) and normalized_value.strip().lower() in null_values:
                normalized_value = None
            row[normalized_key] = normalized_value

        for key, default_value in defaults.items():
            if row.get(key) is None:
                row[key] = default_value

        for field in amount_fields:
            if field in row and row[field] is not None:
                normalized_value = normalize_amount(row[field], amount_round_digits)
                if normalized_value != row[field]:
                    normalized_amount_cells += 1
                row[field] = normalized_value

        for field in date_fields:
            if field in row and row[field] is not None:
                normalized_value = normalize_date(row[field], date_output_format, [str(x) for x in date_input_formats])
                if normalized_value != row[field]:
                    normalized_date_cells += 1
                row[field] = normalized_value

        for transform in field_transforms:
            if not isinstance(transform, dict):
                continue
            field = str(transform.get("field") or "")
            op = str(transform.get("op") or "")
            if not field or not op:
                continue
            source_field = str(transform.get("source_field") or field)
            normalized_value, changed = apply_field_transform(row.get(source_field), op, transform)
            if changed:
                transformed_cells += 1
            if (
                changed
                or field == source_field
                or row.get(field) is None
                or str(row.get(field)).strip() == ""
            ):
                row[field] = normalized_value

        if include_fields:
            row = {key: row.get(key) for key in include_fields}
        for key in exclude_fields:
            row.pop(key, None)

        if row_filters and any(not filter_match(row, item if isinstance(item, dict) else {}) for item in row_filters):
            dropped_by_filters += 1
            continue

        if drop_empty_rows and all(value is None or str(value).strip() == "" for value in row.values()):
            dropped_empty += 1
            continue

        effective_chunk_field = _first_text_field(row, chunk_field) if standardize_evidence else chunk_field
        source_text_for_chunking = str(row.get(effective_chunk_field) or "")
        embedded_turns = _split_embedded_debate_turns(source_text_for_chunking) if standardize_evidence else []
        chunk_targets = embedded_turns if len(embedded_turns) > 1 else chunk_text(source_text_for_chunking, chunk_mode, chunk_max_chars)
        if not chunk_targets:
            chunk_targets = [None]
        chunked_rows_created += max(0, len(chunk_targets) - 1)
        for chunk_index, chunk_value in enumerate(chunk_targets):
            result_row = dict(row)
            if chunk_value is not None:
                result_row[effective_chunk_field] = chunk_value
                result_row["chunk_seq"] = chunk_index
                result_row["chunk_index"] = chunk_index
            if standardize_evidence:
                result_row = to_canonical_evidence_row(result_row, evidence_schema)
                standardized_rows += 1
            out.append(result_row)

    if standardize_evidence:
        out, enrichment_stats = enrich_standardized_evidence_rows(out, spec)
        source_context_backfilled_rows = int(enrichment_stats.get("source_context_backfilled_rows", 0) or 0)
        source_title_enriched_rows = int(enrichment_stats.get("source_title_enriched_rows", 0) or 0)
        citation_candidate_rows = int(enrichment_stats.get("citation_candidate_rows", 0) or 0)
        citation_backend_success_rows = int(enrichment_stats.get("citation_backend_success_rows", 0) or 0)
        url_metadata_candidate_rows = int(enrichment_stats.get("url_metadata_candidate_rows", 0) or 0)
        url_metadata_enriched_rows = int(enrichment_stats.get("url_metadata_enriched_rows", 0) or 0)
        structural_duplicate_rows_removed = int(enrichment_stats.get("structural_duplicate_rows_removed", 0) or 0)
        block_source_context_backfilled_rows = int(enrichment_stats.get("block_source_context_backfilled_rows", 0) or 0)
        structural_source_context_backfilled_rows = int(enrichment_stats.get("structural_source_context_backfilled_rows", 0) or 0)
        adjacent_page_source_context_backfilled_rows = int(enrichment_stats.get("adjacent_page_source_context_backfilled_rows", 0) or 0)
        section_topic_rows_propagated = int(enrichment_stats.get("section_topic_rows_propagated", 0) or 0)
        metadata_topic_rows_propagated = int(enrichment_stats.get("metadata_topic_rows_propagated", 0) or 0)
        metadata_stance_rows_propagated = int(enrichment_stats.get("metadata_stance_rows_propagated", 0) or 0)
        metadata_speaker_role_rows_propagated = int(enrichment_stats.get("metadata_speaker_role_rows_propagated", 0) or 0)
        heading_argument_role_rows_propagated = int(enrichment_stats.get("heading_argument_role_rows_propagated", 0) or 0)
        citation_token_source_backfilled_rows = int(enrichment_stats.get("citation_token_source_backfilled_rows", 0) or 0)
        leading_source_context_backfilled_rows = int(enrichment_stats.get("leading_source_context_backfilled_rows", 0) or 0)
        adjacent_citation_url_rows_collapsed = int(enrichment_stats.get("adjacent_citation_url_rows_collapsed", 0) or 0)
        wrapped_claim_rows_merged = int(enrichment_stats.get("wrapped_claim_rows_merged", 0) or 0)
        source_url_normalized_rows = int(enrichment_stats.get("source_url_normalized_rows", 0) or 0)
        citation_text_url_normalized_rows = int(enrichment_stats.get("citation_text_url_normalized_rows", 0) or 0)
        source_marker_claim_replaced_rows = int(enrichment_stats.get("source_marker_claim_replaced_rows", 0) or 0)
        multi_source_citation_appended_rows = int(enrichment_stats.get("multi_source_citation_appended_rows", 0) or 0)
        engine_trace.extend(list(enrichment_stats.get("engine_trace") or []))
        out = _propagate_trailing_source_context(out)

    effective_deduplicate_by = _effective_deduplicate_fields(
        deduplicate_by,
        standardize_evidence=standardize_evidence,
    )

    if effective_deduplicate_by:
        unique_rows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        if deduplicate_keep == "last":
            for row in out:
                key = tuple(row.get(field) for field in effective_deduplicate_by)
                unique_rows[key] = row
            deduped = list(unique_rows.values())
        else:
            for row in out:
                key = tuple(row.get(field) for field in effective_deduplicate_by)
                if key not in unique_rows:
                    unique_rows[key] = row
            deduped = list(unique_rows.values())
        duplicate_rows_removed = len(out) - len(deduped)
        out = deduped

    out, conflict_rows_marked = apply_conflict_detection(out, spec)

    summary = {
        "input_rows": len(rows),
        "output_rows": len(out),
        "dropped_empty_rows": dropped_empty,
        "dropped_by_filters": dropped_by_filters,
        "duplicate_rows_removed": duplicate_rows_removed,
        "normalized_amount_cells": normalized_amount_cells,
        "normalized_date_cells": normalized_date_cells,
        "transformed_cells": transformed_cells,
        "standardized_rows": standardized_rows,
        "chunked_rows_created": chunked_rows_created,
        "conflict_rows_marked": conflict_rows_marked,
        "encoding_rows_repaired": int(encoding_stats.get("encoding_rows_repaired", 0) or 0),
        "encoding_cells_repaired": int(encoding_stats.get("encoding_cells_repaired", 0) or 0),
        "source_context_backfilled_rows": source_context_backfilled_rows,
        "source_title_enriched_rows": source_title_enriched_rows,
        "citation_candidate_rows": citation_candidate_rows,
        "citation_backend_success_rows": citation_backend_success_rows,
        "url_metadata_candidate_rows": url_metadata_candidate_rows,
        "url_metadata_enriched_rows": url_metadata_enriched_rows,
        "structural_duplicate_rows_removed": structural_duplicate_rows_removed,
        "block_source_context_backfilled_rows": block_source_context_backfilled_rows,
        "structural_source_context_backfilled_rows": structural_source_context_backfilled_rows,
        "adjacent_page_source_context_backfilled_rows": adjacent_page_source_context_backfilled_rows,
        "section_topic_rows_propagated": section_topic_rows_propagated,
        "metadata_topic_rows_propagated": metadata_topic_rows_propagated,
        "metadata_stance_rows_propagated": metadata_stance_rows_propagated,
        "metadata_speaker_role_rows_propagated": metadata_speaker_role_rows_propagated,
        "heading_argument_role_rows_propagated": heading_argument_role_rows_propagated,
        "citation_token_source_backfilled_rows": citation_token_source_backfilled_rows,
        "leading_source_context_backfilled_rows": leading_source_context_backfilled_rows,
        "adjacent_citation_url_rows_collapsed": adjacent_citation_url_rows_collapsed,
        "wrapped_claim_rows_merged": wrapped_claim_rows_merged,
        "source_url_normalized_rows": source_url_normalized_rows,
        "citation_text_url_normalized_rows": citation_text_url_normalized_rows,
        "source_marker_claim_replaced_rows": source_marker_claim_replaced_rows,
        "multi_source_citation_appended_rows": multi_source_citation_appended_rows,
        "engine_trace": engine_trace,
    }
    return out, summary
