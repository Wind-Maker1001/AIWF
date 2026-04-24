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
    s = re.sub(r"https?://\S+|www\.\S+", " ", s)
    s = re.sub(r"\[[^\]]{1,40}\]", " ", s)
    motion_match = re.search(
        r"\b(?:motion|question|issue|topic)\b.*?\b(?:whether|if)\b\s+(?P<body>.+)$",
        s,
    )
    if motion_match:
        s = motion_match.group("body")
    s = re.split(r"\b(?:because|since|as|due to|given that|but|however|though|while)\b|因为|由于|但是|但|然而", s, maxsplit=1)[0]
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
        "because",
        "since",
        "due",
        "given",
        "but",
        "however",
        "though",
        "while",
        "he",
        "she",
        "his",
        "her",
        "their",
        "our",
        "your",
        "its",
        "speaker",
        "source",
        "claim",
        "citation",
        "quote",
        "support",
        "oppose",
        "agree",
        "disagree",
        "supports",
        "opposes",
    }
    if ignore_words:
        stop.update({str(x).lower() for x in ignore_words})
    topic = [t for t in tokens if t not in stop][:8]
    return " ".join(topic)


def _contains_token(text: str, token: str) -> bool:
    lowered = str(text or "").lower()
    value = str(token or "").lower()
    if not value:
        return False
    if re.fullmatch(r"[a-z0-9 ]+", value):
        return re.search(rf"\b{re.escape(value)}\b", lowered) is not None
    return value in lowered


def _detect_polarity(text: Any, positive_words: List[str], negative_words: List[str]) -> str:
    s = str(text or "").strip().lower()
    if not s:
        return "unknown"
    if s in {"pro", "con", "neutral", "unknown"}:
        return s
    pos = any(_contains_token(s, w) for w in positive_words)
    neg = any(_contains_token(s, w) for w in negative_words)
    if pos and not neg:
        return "pro"
    if neg and not pos:
        return "con"
    if any(_contains_token(s, w) for w in ("neutral", "中立", "mixed", "未表态")):
        return "neutral"
    return "unknown"


def _skip_conflict_row(row: Dict[str, Any]) -> bool:
    speaker_role = str(row.get("speaker_role") or "").strip().lower()
    argument_role = str(row.get("argument_role") or "").strip().lower()
    if speaker_role in {"moderator", "judge"}:
        return True
    if argument_role in {"quote", "evidence", "citation", "moderation", "question", "metadata", "section"}:
        return True
    claim_text = str(row.get("claim_text") or row.get("text") or "").strip()
    return not claim_text


def _row_topic(row: Dict[str, Any], topic_field: str, text_field: str, ignore_words: List[str]) -> str:
    explicit_topic = str(row.get(topic_field) or row.get("debate_topic") or "").strip().lower()
    if explicit_topic:
        normalized_topic = _infer_topic_key(explicit_topic, ignore_words=ignore_words)
        if normalized_topic:
            return normalized_topic
        return explicit_topic
    topic = _infer_topic_key(row.get(text_field) or row.get("claim_text"), ignore_words=ignore_words)
    if topic:
        return topic
    fallback_src = str(row.get("source_path") or row.get("source_file") or "").strip().lower()
    return f"src:{fallback_src}" if fallback_src else ""


def _apply_conflict_detection(rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    if not bool(spec.get("detect_conflicts", False)):
        return rows, 0

    topic_field = str(spec.get("conflict_topic_field") or "debate_topic").strip()
    stance_field = str(spec.get("conflict_stance_field") or "stance").strip()
    text_field = str(spec.get("conflict_text_field") or "claim_text").strip()
    positive_words = [
        str(x).lower()
        for x in (spec.get("conflict_positive_words") or ["support", "true", "yes", "approve", "agree", "支持", "赞成", "正方"])
    ]
    negative_words = [
        str(x).lower()
        for x in (spec.get("conflict_negative_words") or ["oppose", "false", "no", "reject", "disagree", "反对", "不支持", "反方"])
    ]

    groups: Dict[str, List[int]] = {}
    row_polarity: List[str] = []
    row_topics: List[str] = []
    row_skip: List[bool] = []
    ignore_words = positive_words + negative_words

    for idx, row in enumerate(rows):
        skip = _skip_conflict_row(row)
        row_skip.append(skip)
        topic = _row_topic(row, topic_field, text_field, ignore_words)
        row_topics.append(topic)
        polarity_source = row.get(stance_field)
        if polarity_source in {None, ""}:
            polarity_source = row.get("stance")
        if polarity_source in {None, ""}:
            polarity_source = row.get(text_field) if row.get(text_field) is not None else row.get("claim_text")
        polarity = _detect_polarity(polarity_source, positive_words, negative_words)
        row_polarity.append(polarity)
        if topic and not skip and polarity in {"pro", "con"}:
            groups.setdefault(topic, []).append(idx)

    conflict_topics = set()
    for topic, indexes in groups.items():
        polarities = {row_polarity[i] for i in indexes}
        if "pro" in polarities and "con" in polarities:
            conflict_topics.add(topic)

    marked = 0
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        next_row = dict(row)
        normalized_topic = str(row_topics[idx] or next_row.get("debate_topic") or "").strip().lower()
        next_row["debate_topic"] = normalized_topic
        next_row["conflict_topic"] = normalized_topic
        next_row["conflict_polarity"] = row_polarity[idx]
        next_row["conflict_flag"] = bool(
            next_row["conflict_topic"]
            and next_row["conflict_topic"] in conflict_topics
            and not row_skip[idx]
        )
        if next_row["conflict_flag"]:
            marked += 1
        out.append(next_row)
    return out, marked
