from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _chunk_text(text: str, mode: str, max_chars: int) -> List[str]:
    s = (text or "").strip()
    if not s:
        return []
    m = (mode or "none").strip().lower()
    if m in {"none", "off"}:
        return [s]
    if m == "paragraph":
        parts = [p.strip() for p in re.split(r"\n\s*\n", s) if p.strip()]
        return parts if parts else [s]
    if m == "sentence":
        parts = [p.strip() for p in re.split(r"(?<=[.!?。！？])\s+", s) if p.strip()]
        return parts if parts else [s]
    if m == "fixed":
        size = max(1, int(max_chars))
        return [s[i : i + size].strip() for i in range(0, len(s), size) if s[i : i + size].strip()]
    return [s]


def _infer_topic_key(text: Any, ignore_words: Optional[List[str]] = None) -> str:
    s = str(text or "").lower()
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff\s]+", " ", s)
    tokens = [t for t in s.split() if t]
    if not tokens:
        return ""
    stop = {
        "the",
        "a",
        "an",
        "is",
        "are",
        "of",
        "to",
        "and",
        "or",
        "in",
        "on",
        "for",
        "we",
        "should",
        "it",
        "this",
        "that",
        "these",
        "those",
        "they",
        "them",
    }
    if ignore_words:
        stop.update({str(x).lower() for x in ignore_words})
    topic = [t for t in tokens if t not in stop][:8]
    return " ".join(topic)


def _detect_polarity(text: Any, positive_words: List[str], negative_words: List[str]) -> str:
    s = str(text or "").lower()
    pos = any(w in s for w in positive_words)
    neg = any(w in s for w in negative_words)
    if pos and not neg:
        return "pro"
    if neg and not pos:
        return "con"
    return "unknown"


def _apply_conflict_detection(rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    if not bool(spec.get("detect_conflicts", False)):
        return rows, 0

    topic_field = str(spec.get("conflict_topic_field") or "topic").strip()
    stance_field = str(spec.get("conflict_stance_field") or "stance").strip()
    text_field = str(spec.get("conflict_text_field") or "claim_text").strip()
    positive_words = [str(x).lower() for x in (spec.get("conflict_positive_words") or ["support", "true", "yes", "approve", "agree"])]
    negative_words = [str(x).lower() for x in (spec.get("conflict_negative_words") or ["oppose", "false", "no", "reject", "disagree"])]

    groups: Dict[str, List[int]] = {}
    row_polarity: List[str] = []
    for idx, row in enumerate(rows):
        topic = str(row.get(topic_field) or "").strip().lower()
        if not topic:
            topic = _infer_topic_key(row.get(text_field), ignore_words=positive_words + negative_words)
        if not topic:
            fallback_src = str(row.get("source_path") or row.get("source_file") or "").strip().lower()
            if fallback_src:
                topic = f"src:{fallback_src}"
        polarity = _detect_polarity(
            row.get(stance_field) if row.get(stance_field) is not None else row.get(text_field),
            positive_words,
            negative_words,
        )
        row_polarity.append(polarity)
        if topic:
            groups.setdefault(topic, []).append(idx)

    conflict_topics = set()
    for topic, indexes in groups.items():
        polarities = {row_polarity[i] for i in indexes}
        if "pro" in polarities and "con" in polarities:
            conflict_topics.add(topic)

    marked = 0
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        topic = str(row.get(topic_field) or "").strip().lower() or _infer_topic_key(
            row.get(text_field), ignore_words=positive_words + negative_words
        )
        if not topic:
            fallback_src = str(row.get("source_path") or row.get("source_file") or "").strip().lower()
            if fallback_src:
                topic = f"src:{fallback_src}"
        next_row = dict(row)
        next_row["conflict_topic"] = topic
        next_row["conflict_polarity"] = row_polarity[idx]
        next_row["conflict_flag"] = bool(topic and topic in conflict_topics)
        if next_row["conflict_flag"]:
            marked += 1
        out.append(next_row)
    return out, marked
