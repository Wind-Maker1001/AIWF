from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse


_URL_RE = re.compile(r"https?://\S+|www\.\S+", flags=re.I)
_CITATION_TOKEN_BODY_RE = r"(?:[A-Za-z]?\d[A-Za-z0-9_.-]{0,20}|\d+[A-Za-z][A-Za-z0-9_.-]{0,20})"
_BRACKET_CITATION_RE = re.compile(
    f"(?:\\[{_CITATION_TOKEN_BODY_RE}\\]|\uff3b{_CITATION_TOKEN_BODY_RE}\uff3d|\u3010{_CITATION_TOKEN_BODY_RE}\u3011)"
)
_PAREN_CITATION_RE = re.compile(
    r"[\(\uff08](?:source|citation|according to|ref|reference|来源|引自|出处)[:：]?\s*[^)\uff09]+[\)\uff09]",
    flags=re.I,
)
_QUOTE_RE = re.compile(r"[\"“”'‘’](.{2,400}?)[\"“”'‘’]")
_SOURCE_TITLE_LINE_RE = re.compile(
    r"^\s*(?:[-—–]{1,3}\s*)?(?:[<《「『](?P<bracketed>[^>》」』]{4,240})[>》」』]|(?P<plain>(?:source|citation|according to|来源|出处)\s*[:：]\s*.+))\s*$",
    flags=re.I,
)
_SPEAKER_PREFIX_RE = re.compile(
    r"^\s*(?P<label>"
    r"(?:speaker(?:\s+[A-Za-z0-9_.-]+)?|moderator|host|judge|analyst(?:\s+[A-Za-z0-9_.-]+)?|"
    r"author|witness|reporter|quote|citation|claim|source|"
    r"主持人|评委|裁判|分析师|作者|记者|引文|引用|论点|主张|来源|"
    r"正方(?:[一二三四]辩)?|反方(?:[一二三四]辩)?|一辩|二辩|三辩|四辩|自由辩论|总结陈词))"
    r"\s*[:：]\s*(?P<body>.+)$",
    flags=re.I,
)
_GENERIC_NAME_PREFIX_RE = re.compile(
    r"^\s*(?P<label>(?:[A-Z][A-Za-z0-9_. -]{0,30}|[\u4e00-\u9fff]{2,16}))\s*[:：]\s*(?P<body>.+)$"
)

_SPEAKER_ALIASES = ["speaker", "author", "name", "speaker_name", "发言人", "作者", "说话人"]
_STANCE_ALIASES = ["stance", "position", "side", "立场", "态度"]
_SOURCE_URL_ALIASES = ["source_url", "url", "link", "source_link", "来源链接"]
_SOURCE_TITLE_ALIASES = ["source_title", "title", "source_name", "citation_title", "来源标题"]
_PUBLISHED_AT_ALIASES = ["published_at", "publish_date", "date", "date_published", "发布时间"]
_CONFIDENCE_ALIASES = ["confidence", "score"]
_ARGUMENT_ROLE_ALIASES = ["argument_role", "claim_type", "role", "论证角色"]
_SPEAKER_ROLE_ALIASES = ["speaker_role", "speaker_type", "发言角色"]
_TOPIC_ALIASES = ["debate_topic", "topic", "议题", "主题"]
_CITATION_ALIASES = ["citation_text", "citation", "ref_text", "引用"]
_QUOTE_ALIASES = ["quote_text", "quote", "quoted_text", "引文正文"]
_LANGUAGE_ALIASES = ["language", "lang"]

_RESERVED_NON_SPEAKER_LABELS = {
    "claim",
    "citation",
    "quote",
    "source",
    "bibliography",
    "reference",
    "references",
    "footnote",
    "endnote",
    "论点",
    "主张",
    "引文",
    "引用",
    "来源",
}

_STRUCTURAL_NON_SPEAKER_LABELS = {
    "\u4e00\u53e5\u8bdd\u603b\u7ed3",
    "\u80cc\u666f",
    "\u73b0\u72b6",
    "\u539f\u56e0",
    "\u653f\u7b56",
    "\u5177\u4f53\u673a\u5236",
    "\u673a\u5236",
    "\u8f6c\u578b\u9700\u6c42",
    "\u7ed3\u679c",
    "\u6548\u679c",
    "\u542f\u793a",
    "\u7ed3\u8bba",
    "\u5173\u952e\u7ed3\u8bba",
    "\u7814\u7a76\u65b9\u6cd5",
    "\u65b9\u6cd5",
    "\u8bba\u6587",
    "\u6570\u636e",
    "\u6848\u4f8b",
    "\u534f\u540c\u589e\u6548",
    "\u65b0\u5174\u5e02\u573a\u7684\u673a\u9047",
}

_HEADER_LIKE_LABELS = {
    "account",
    "account no",
    "account number",
    "acct",
    "acct no",
    "acct number",
    "posting dt",
    "posting date",
    "txn date",
    "transaction date",
    "biz date",
    "date",
    "balance",
    "bal",
    "amount",
    "amt",
    "debit",
    "credit",
    "memo",
    "remark",
    "ref",
    "ref no",
    "reference",
    "reference no",
    "customer",
    "customer name",
    "cust name",
    "phone",
    "mobile",
    "mobile no",
    "city",
    "address",
    "topic",
    "sheet",
    "row",
    "column",
    "evidence",
    "id",
    "\u8d26\u53f7",
    "\u8d26\u6237",
    "\u4f59\u989d",
    "\u91d1\u989d",
    "\u65e5\u671f",
    "\u5907\u6ce8",
    "\u5907\u6ce8\u4fe1\u606f",
    "\u5ba2\u6237",
    "\u5ba2\u6237\u540d\u79f0",
    "\u624b\u673a",
    "\u624b\u673a\u53f7",
    "\u7535\u8bdd",
    "\u57ce\u5e02",
    "\u5730\u5740",
    "\u4e3b\u9898",
    "\u8bc1\u636e",
    "\u6d41\u6c34\u53f7",
}

_HEADER_LIKE_LABEL_TOKENS = {
    "account",
    "acct",
    "no",
    "number",
    "posting",
    "posted",
    "txn",
    "transaction",
    "biz",
    "date",
    "balance",
    "bal",
    "amount",
    "amt",
    "debit",
    "credit",
    "memo",
    "remark",
    "ref",
    "reference",
    "customer",
    "cust",
    "name",
    "phone",
    "mobile",
    "city",
    "address",
    "topic",
    "sheet",
    "row",
    "column",
    "evidence",
    "id",
}

_METADATA_LABELS = {
    "time",
    "date",
    "meeting",
    "meeting id",
    "meeting no",
    "meeting number",
    "recording",
    "recording file",
    "recording link",
    "motion",
    "topic",
    "side",
    "opponent",
    "match",
    "match info",
    "competition",
    "competition info",
    "round",
    "script",
    "\u65f6\u95f4",
    "\u65e5\u671f",
    "\u4f1a\u8bae\u53f7",
    "\u5f55\u5236\u6587\u4ef6",
    "\u5f55\u5236\u94fe\u63a5",
    "\u5f55\u5c4f\u94fe\u63a5",
    "\u8fa9\u9898",
    "\u6301\u65b9",
    "\u5bf9\u624b",
    "\u6bd4\u8d5b\u76f8\u5173\u4fe1\u606f",
    "\u6a21\u8fa9",
    "\u4e00\u8fa9\u7a3f",
    "\u4e8c\u8fa9\u7a3f",
    "\u4e09\u8fa9\u7a3f",
    "\u56db\u8fa9\u7a3f",
}

_METADATA_PREFIXES = (
    "match info",
    "competition info",
    "recording file",
    "recording link",
    "meeting id",
    "meeting no",
    "meeting number",
    "mock debate",
    "round",
    "script",
    "\u6bd4\u8d5b\u76f8\u5173\u4fe1\u606f",
    "\u5f55\u5236\u6587\u4ef6",
    "\u5f55\u5236\u94fe\u63a5",
    "\u5f55\u5c4f\u94fe\u63a5",
    "\u817e\u8baf\u4f1a\u8bae",
    "\u6a21\u8fa9",
    "\u4e00\u8fa9\u7a3f",
    "\u4e8c\u8fa9\u7a3f",
    "\u4e09\u8fa9\u7a3f",
    "\u56db\u8fa9\u7a3f",
)

_PRO_TOKENS = [
    "support",
    "agree",
    "approve",
    "affirmative",
    "in favor",
    "positive",
    "pro",
    "支持",
    "赞成",
    "正方",
    "有利",
]
_CON_TOKENS = [
    "oppose",
    "disagree",
    "reject",
    "negative",
    "against",
    "con",
    "anti",
    "反对",
    "不支持",
    "错误",
    "反方",
    "不利",
]
_NEUTRAL_TOKENS = [
    "neutral",
    "mixed",
    "undecided",
    "unclear",
    "中立",
    "未表态",
    "待定",
]

_TOPIC_STOPWORDS = {
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
    "this",
    "that",
    "these",
    "those",
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
    "debate",
    "evidence",
    "argument",
    "analysis",
}
_OCR_NOISE_TOKENS = ("锛", "鈥", "銆", "\uFFFD")


def _normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\u3000", " ")
    return text.strip()


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_alias_value(value: Any) -> str:
    text = _normalize_text(value)
    return text.lower()


def _normalize_key_name(value: Any) -> str:
    text = _normalize_alias_value(value)
    text = re.sub(r"[\s\-/.]+", "_", text)
    text = re.sub(r"[^0-9a-z\u4e00-\u9fff_]+", "", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _normalized_key_variants(value: Any) -> List[str]:
    normalized = _normalize_key_name(value)
    if not normalized:
        return []
    compact = normalized.replace("_", "")
    if compact == normalized:
        return [normalized]
    return [normalized, compact]


def _first_non_empty(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and (not isinstance(value, str) or value.strip() != ""):
            return value
        variants = set(_normalized_key_variants(key))
        if not variants:
            continue
        for row_key, candidate in row.items():
            if candidate is None or (isinstance(candidate, str) and candidate.strip() == ""):
                continue
            row_variants = set(_normalized_key_variants(row_key))
            if row_variants.intersection(variants):
                return candidate
    return None


def _field_aliases(schema: Dict[str, Any], name: str, defaults: List[str]) -> List[str]:
    value = schema.get(name)
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    return defaults


def _normalize_label_token(label: str) -> str:
    text = _normalize_alias_value(label)
    text = re.sub(r"[\s._-]+", " ", text)
    return text.strip()


def _is_non_speaker_label(label: str) -> bool:
    token = _normalize_label_token(label)
    if not token:
        return False
    if _is_metadata_label(label):
        return True
    compact = re.sub(r"\s+", " ", token).strip()
    if compact in _RESERVED_NON_SPEAKER_LABELS or compact in _STRUCTURAL_NON_SPEAKER_LABELS or compact in _HEADER_LIKE_LABELS:
        return True
    ascii_tokens = [part for part in compact.split(" ") if part]
    if ascii_tokens and all(part in _HEADER_LIKE_LABEL_TOKENS for part in ascii_tokens):
        return True
    return False


def _is_metadata_label(label: str) -> bool:
    token = _normalize_label_token(label)
    if not token:
        return False
    compact = re.sub(r"\s+", " ", token).strip()
    if compact in _METADATA_LABELS:
        return True
    return any(compact.startswith(prefix) for prefix in _METADATA_PREFIXES)


def _looks_metadata_text(text: Any, *, label: str = "", body: str = "") -> bool:
    if _is_metadata_label(label):
        return True
    normalized = _normalize_text(body or text)
    if not normalized:
        return False
    cleaned = re.sub(r"[\x00-\x1f\x7f]+", " ", normalized).strip()
    lowered = cleaned.lower()
    if any(lowered.startswith(prefix) for prefix in _METADATA_PREFIXES):
        return True
    if cleaned.startswith(("\u6bd4\u8d5b\u76f8\u5173\u4fe1\u606f", "\u817e\u8baf\u4f1a\u8bae", "#\u817e\u8baf\u4f1a\u8bae")):
        return True
    if re.match(r"^(?:mock debate|round)\s*\d+\s*[:：]", lowered):
        return True
    if re.match(r"^\u6a21\u8fa9\s*\d+\s*[:：]", cleaned):
        return True
    if re.match(r"^[\u4e00\u4e8c\u4e09\u56db1-4]\u8fa9\u7a3f", cleaned):
        return True
    if re.match(r"^\d{1,2}[./-]\d{1,2}\s+\d{1,2}:\d{2}$", cleaned):
        return True
    if re.match(r"^#?(?:tencent\s+)?meeting[:#]?\s*[0-9-]{6,}$", lowered):
        return True
    if re.match(r"^#?\u817e\u8baf\u4f1a\u8bae[:#]?\s*[0-9-]{6,}$", cleaned):
        return True
    if "meeting.tencent.com/" in lowered:
        return True
    return False


def _looks_section_heading_text(text: Any, *, label: str = "", body: str = "") -> bool:
    if _is_metadata_label(label):
        return False
    normalized = _normalize_text(body or text)
    if not normalized:
        return False
    cleaned = re.sub(r"[\x00-\x1f\x7f]+", " ", normalized).strip()
    if not cleaned or "http://" in cleaned.lower() or "https://" in cleaned.lower():
        return False
    if len(cleaned) <= 48 and (
        (cleaned.startswith("【") and cleaned.endswith("】"))
        or (cleaned.startswith("[") and cleaned.endswith("]"))
    ):
        return True
    if len(cleaned) <= 40 and re.match(r"^(?:\*+|•\s*)?[^:：]{1,32}[:：]$", cleaned):
        return True
    if len(cleaned) <= 40 and re.match(r"^(?:\d+(?:\.\d+){0,3}|[一二三四五六七八九十]+)\s*[、.．]?\s*\S.{0,32}$", cleaned):
        return True
    return False


def _contains_token(text: str, token: str) -> bool:
    lowered = str(text or "").lower()
    normalized_token = str(token or "").lower()
    if not normalized_token:
        return False
    if re.fullmatch(r"[a-z0-9 ]+", normalized_token):
        return re.search(rf"\b{re.escape(normalized_token)}\b", lowered) is not None
    return normalized_token in lowered


def _normalize_source_url(value: Any) -> str:
    url = re.sub(r"[\x00-\x1f\x7f]+", "", str(value or "")).rstrip(".,);]")
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    fragment = str(parsed.fragment or "").strip().lower()
    if fragment in {":", "~"} or fragment.startswith(":~:text") or fragment.startswith("~:text"):
        return urlunparse(parsed._replace(fragment=""))
    return url


def _normalize_urls_in_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    return _URL_RE.sub(lambda match: _normalize_source_url(match.group(0)), text)


def _extract_urls(text: str) -> List[str]:
    return [_normalize_source_url(match.group(0)) for match in _URL_RE.finditer(str(text or ""))]


def _extract_raw_urls(text: str) -> List[str]:
    return [match.group(0) for match in _URL_RE.finditer(str(text or ""))]


def _extract_citation_tokens(text: str) -> List[str]:
    matches = [match.group(0) for match in _BRACKET_CITATION_RE.finditer(str(text or ""))]
    matches.extend(match.group(0) for match in _PAREN_CITATION_RE.finditer(str(text or "")))
    out: List[str] = []
    seen: set[str] = set()
    for item in matches:
        normalized = _collapse_whitespace(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _extract_quote_text(text: str) -> str:
    match = _QUOTE_RE.search(str(text or ""))
    if not match:
        return ""
    return _collapse_whitespace(match.group(1))


def _extract_source_title_candidate(text: Any) -> str:
    normalized = _normalize_text(text)
    if not normalized:
        return ""
    candidate = re.sub(r"^(?:[-—–]{1,3}\s*)", "", normalized).strip()
    starts_like_source = candidate.startswith(("<", "《", "「", "『"))
    starts_like_marker = candidate.lower().startswith(("source:", "citation:", "according to"))
    starts_like_marker = starts_like_marker or candidate.startswith(("来源:", "来源：", "出处:", "出处："))
    if not starts_like_source and not starts_like_marker:
        return ""
    if len(candidate) >= 2 and (
        (candidate.startswith("<") and candidate.endswith(">"))
        or (candidate.startswith("《") and candidate.endswith("》"))
        or (candidate.startswith("「") and candidate.endswith("」"))
        or (candidate.startswith("『") and candidate.endswith("』"))
    ):
        candidate = candidate[1:-1].strip()
    title = _collapse_whitespace(candidate)
    title = re.sub(r"^(?:source|citation|according to|来源|出处)\s*[:：]\s*", "", title, flags=re.I)
    return title


def _remove_urls_from_claim(text: str, urls: List[str]) -> str:
    out = str(text or "")
    out = _URL_RE.sub(" ", out)
    for url in urls:
        out = out.replace(url, " ")
    cleaned = _collapse_whitespace(out)
    # Keep the URL-only line readable rather than blanking it out.
    return cleaned if cleaned else (urls[0] if urls else "")


def _strip_inline_citations(text: str) -> str:
    out = _BRACKET_CITATION_RE.sub(" ", str(text or ""))
    out = _PAREN_CITATION_RE.sub(" ", out)
    return _collapse_whitespace(out)


def _strip_ocr_noise(text: str) -> str:
    out = _normalize_text(text)
    out = re.sub(r"[\x00-\x1f\x7f]+", " ", out)
    out = re.sub(r"^\s*(page|p\.)\s*\d+\s*$", " ", out, flags=re.I)
    out = re.sub(r"^\s*第?\s*\d+\s*页\s*$", " ", out)
    out = re.sub(r"([A-Za-z\u4e00-\u9fff])\1{4,}", r"\1", out)
    return _collapse_whitespace(out)


def _speaker_prefix_parts(text: str) -> Tuple[str, str]:
    normalized = _normalize_text(text)
    if not normalized:
        return "", ""
    match = _SPEAKER_PREFIX_RE.match(normalized)
    if match:
        return _collapse_whitespace(match.group("label")), _collapse_whitespace(match.group("body"))
    generic_match = _GENERIC_NAME_PREFIX_RE.match(normalized)
    if not generic_match:
        return "", normalized
    label = _collapse_whitespace(generic_match.group("label"))
    if _is_non_speaker_label(label):
        return label, _collapse_whitespace(generic_match.group("body"))
    return label, _collapse_whitespace(generic_match.group("body"))


def _speaker_role_from_label(label: str) -> str:
    token = _normalize_label_token(label)
    if not token:
        return ""
    if _is_metadata_label(label):
        return "metadata"
    if any(item in token for item in ("moderator", "host", "主持")):
        return "moderator"
    if any(item in token for item in ("judge", "评委", "裁判")):
        return "judge"
    if any(item in token for item in ("reporter", "记者")):
        return "reporter"
    if any(item in token for item in ("analyst", "分析师")):
        return "analyst"
    if any(item in token for item in ("author", "作者")):
        return "author"
    if any(item in token for item in ("witness", "证人")):
        return "witness"
    if any(item in token for item in ("正方", "反方", "辩")):
        return "debater"
    if token in _STRUCTURAL_NON_SPEAKER_LABELS:
        return ""
    if _is_non_speaker_label(label):
        return "source"
    return "speaker"


def _argument_role_from_label(label: str, text: str) -> str:
    combined = f"{label} {text}".strip().lower()
    if not combined:
        return ""
    if any(item in combined for item in ("quote", "引文", "引用")):
        return "quote"
    if any(item in combined for item in ("source", "citation", "according to", "bibliography", "reference", "footnote", "endnote", "来源")):
        return "evidence"
    if any(item in combined for item in ("moderator", "host", "主持")):
        return "moderation"
    if any(item in combined for item in ("rebuttal", "refute", "counter", "反驳", "回应", "驳斥")):
        return "rebuttal"
    if any(item in combined for item in ("summary", "closing", "总结", "结辩")):
        return "summary"
    if any(item in combined for item in ("question", "ask", "提问")):
        return "question"
    return "claim"


def _normalize_stance_value(value: Any, *, label: str = "", speaker: str = "") -> str:
    text = _collapse_whitespace(f"{label} {speaker} {_normalize_text(value)}").lower()
    if not text:
        return ""
    if any(_contains_token(text, token) for token in _NEUTRAL_TOKENS):
        return "neutral"
    pro = any(_contains_token(text, token) for token in _PRO_TOKENS)
    con = any(_contains_token(text, token) for token in _CON_TOKENS)
    if pro and not con:
        return "pro"
    if con and not pro:
        return "con"
    return "unknown"


def _detect_language(text: str) -> str:
    normalized = _normalize_text(text)
    if not normalized:
        return ""
    zh_count = len(re.findall(r"[\u4e00-\u9fff]", normalized))
    en_count = len(re.findall(r"[A-Za-z]", normalized))
    if zh_count and en_count:
        return "mixed"
    if zh_count:
        return "zh"
    if en_count:
        return "en"
    return "unknown"


def _topic_from_text(text: str) -> str:
    lowered = _normalize_alias_value(text)
    lowered = _URL_RE.sub(" ", lowered)
    lowered = _BRACKET_CITATION_RE.sub(" ", lowered)
    lowered = re.sub(r"[^0-9a-z\u4e00-\u9fff\s]+", " ", lowered)
    tokens = [token for token in lowered.split() if token and token not in _TOPIC_STOPWORDS]
    if not tokens:
        return ""
    return " ".join(tokens[:6])


def _source_domain(source_url: str) -> str:
    normalized = _normalize_text(source_url)
    if not normalized:
        return ""
    candidate = normalized if re.match(r"^[a-z]+://", normalized, flags=re.I) else f"https://{normalized}"
    try:
        host = urlparse(candidate).netloc.lower()
    except Exception:
        host = ""
    return host[4:] if host.startswith("www.") else host


def _looks_ocr_noise(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if any(token in normalized for token in _OCR_NOISE_TOKENS):
        return True
    symbol_count = len(re.findall(r"[^0-9A-Za-z\u4e00-\u9fff\s]", normalized))
    if symbol_count / max(1, len(normalized)) > 0.35:
        return True
    return False


def analyze_debate_text_signals(text: Any) -> Dict[str, Any]:
    normalized = _normalize_text(text)
    label, body = _speaker_prefix_parts(normalized)
    citation_tokens = _extract_citation_tokens(normalized)
    urls = _extract_urls(normalized)
    quote_text = _extract_quote_text(normalized)
    source_title_candidate = _extract_source_title_candidate(normalized)
    speaker_role = _speaker_role_from_label(label)
    argument_role = _argument_role_from_label(label, body or normalized)
    metadata_line = _looks_metadata_text(normalized, label=label, body=body or normalized)
    section_line = _looks_section_heading_text(normalized, label=label, body=body or normalized)
    if source_title_candidate and not label:
        argument_role = "evidence"
    if metadata_line:
        speaker_role = "metadata"
        argument_role = "metadata"
    elif section_line:
        speaker_role = "structure"
        argument_role = "section"
    body_text = body or normalized
    claim_text = _strip_ocr_noise(body_text)
    claim_text = _strip_inline_citations(claim_text)
    if urls:
        claim_text = _remove_urls_from_claim(claim_text, urls)
    stance = _normalize_stance_value(body_text, label=label)
    non_quote_text = _collapse_whitespace(_QUOTE_RE.sub(" ", claim_text))
    quote_only = bool(quote_text and len(non_quote_text) <= 12)
    quote_dominant = bool(
        quote_text
        and not source_title_candidate
        and (
            quote_only
            or len(non_quote_text) <= 24
            or (
                speaker_role in {"moderator", "reporter", "author", "source"}
                and len(quote_text) >= 8
            )
        )
    )
    source_ref_signal = bool(
        urls
        or citation_tokens
        or source_title_candidate
        or any(item in normalized.lower() for item in ("source:", "citation:", "according to"))
    )
    if metadata_line or section_line:
        source_ref_signal = False
    quote_signal = bool(quote_text) and not source_title_candidate and not metadata_line and not section_line
    stance_signal = not section_line and (
        stance in {"pro", "con", "neutral"} or bool(label and any(token in label for token in ("正方", "反方", "持方", "side")))
    )
    return {
        "speaker_signal": bool(label and not _is_non_speaker_label(label) and not metadata_line and not section_line),
        "stance_signal": stance_signal,
        "source_ref_signal": source_ref_signal,
        "quote_signal": quote_signal,
        "speaker_label": label,
        "speaker_role": speaker_role,
        "argument_role": "metadata" if metadata_line else ("section" if section_line else ("quote" if quote_dominant else argument_role)),
        "urls": urls,
        "citation_text": " | ".join(citation_tokens),
        "quote_text": "" if source_title_candidate else quote_text,
        "source_title": source_title_candidate,
        "ocr_noise": False if metadata_line or section_line else _looks_ocr_noise(normalized),
    }


def analyze_debate_row_signals(row: Dict[str, Any]) -> Dict[str, Any]:
    text_value = _first_non_empty(row, ["claim_text", "text", "content", "body", "paragraph", "note"])
    signals = analyze_debate_text_signals(text_value)
    explicit_speaker = _first_non_empty(row, _SPEAKER_ALIASES)
    explicit_stance = _first_non_empty(row, _STANCE_ALIASES)
    explicit_source = _first_non_empty(row, _SOURCE_URL_ALIASES + _SOURCE_TITLE_ALIASES + _PUBLISHED_AT_ALIASES)
    return {
        "speaker_signal": bool(explicit_speaker) or bool(signals.get("speaker_signal")),
        "stance_signal": bool(explicit_stance) or bool(signals.get("stance_signal")),
        "source_ref_signal": bool(explicit_source) or bool(signals.get("source_ref_signal")),
        "quote_signal": bool(signals.get("quote_signal")),
        "ocr_noise": bool(signals.get("ocr_noise")),
        "speaker_role": str(signals.get("speaker_role") or ""),
        "argument_role": str(signals.get("argument_role") or ""),
        "citation_text": str(signals.get("citation_text") or ""),
        "quote_text": str(signals.get("quote_text") or ""),
    }


def _speaker_from_signal(label: str, body: str, explicit_speaker: Any) -> str:
    explicit = _normalize_text(explicit_speaker)
    if explicit:
        return explicit
    normalized_label = _normalize_label_token(label)
    if not label or _is_non_speaker_label(label):
        return ""
    if normalized_label.startswith("speaker "):
        return _collapse_whitespace(label.split(" ", 1)[1])
    if normalized_label in {"speaker", "发言人"}:
        name_match = re.match(
            r"^(?P<name>(?:[A-Z][A-Za-z.-]+(?:\s+[A-Z][A-Za-z.-]+){0,2}|[\u4e00-\u9fff]{2,8}))\s+(?:argues|said|says|noted|stated|认为|指出|表示|称)",
            body,
            flags=re.I,
        )
        if name_match:
            return _collapse_whitespace(name_match.group("name"))
        return ""
    return _collapse_whitespace(label)


def _clean_claim_text(text: str, *, urls: List[str]) -> str:
    cleaned = _strip_ocr_noise(text)
    cleaned = _strip_inline_citations(cleaned)
    if urls:
        cleaned = _remove_urls_from_claim(cleaned, urls)
    cleaned = re.sub(r"^(?:claim|source|citation|quote|speaker)\s*[:：]\s*", "", cleaned, flags=re.I)
    return _collapse_whitespace(cleaned)


def _normalized_claim_fingerprint(text: str) -> str:
    normalized = _normalize_alias_value(text)
    normalized = _URL_RE.sub(" ", normalized)
    normalized = _BRACKET_CITATION_RE.sub(" ", normalized)
    normalized = _PAREN_CITATION_RE.sub(" ", normalized)
    normalized = re.sub(r"[^0-9a-z\u4e00-\u9fff\s]+", " ", normalized)
    normalized = _collapse_whitespace(normalized)
    return normalized[:256]


def _canonical_debate_fields(row: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    explicit_claim = _first_non_empty(row, _field_aliases(schema, "claim_text", ["claim_text", "text", "content", "body"]))
    text_value = _normalize_text(explicit_claim)
    signal = analyze_debate_text_signals(text_value)
    label = str(signal.get("speaker_label") or "")
    body = _speaker_prefix_parts(text_value)[1] if text_value else ""
    raw_urls = _extract_raw_urls(text_value)
    urls = [_normalize_source_url(url) for url in raw_urls]

    explicit_speaker = _first_non_empty(row, _field_aliases(schema, "speaker", _SPEAKER_ALIASES))
    speaker = _speaker_from_signal(label, body or text_value, explicit_speaker)
    speaker_role = _normalize_text(_first_non_empty(row, _field_aliases(schema, "speaker_role", _SPEAKER_ROLE_ALIASES))) or str(signal.get("speaker_role") or "")
    argument_role = _normalize_text(_first_non_empty(row, _field_aliases(schema, "argument_role", _ARGUMENT_ROLE_ALIASES))) or str(signal.get("argument_role") or "")

    explicit_source_url = _first_non_empty(row, _field_aliases(schema, "source_url", _SOURCE_URL_ALIASES))
    source_url_seed = _normalize_text(explicit_source_url) or (raw_urls[0] if raw_urls else "")
    source_url = _normalize_source_url(explicit_source_url) or (urls[0] if urls else "")
    source_url_normalized = bool(source_url_seed and source_url and source_url_seed != source_url)
    explicit_source_title = _first_non_empty(row, _field_aliases(schema, "source_title", _SOURCE_TITLE_ALIASES))
    source_title = _normalize_text(explicit_source_title) or str(signal.get("source_title") or "")
    explicit_published_at = _first_non_empty(row, _field_aliases(schema, "published_at", _PUBLISHED_AT_ALIASES))
    published_at = _normalize_text(explicit_published_at)
    explicit_confidence = _first_non_empty(row, _field_aliases(schema, "confidence", _CONFIDENCE_ALIASES))
    confidence = explicit_confidence

    explicit_stance = _first_non_empty(row, _field_aliases(schema, "stance", _STANCE_ALIASES))
    stance = _normalize_text(explicit_stance)
    if stance:
        stance = _normalize_stance_value(stance, label=label, speaker=speaker)
    else:
        stance = _normalize_stance_value(text_value, label=label, speaker=speaker)

    explicit_citation = _first_non_empty(row, _field_aliases(schema, "citation_text", _CITATION_ALIASES))
    citation_text_seed = _normalize_text(explicit_citation) or str(signal.get("citation_text") or "")
    if not citation_text_seed and source_url_seed and source_url_seed == text_value:
        citation_text_seed = source_url_seed
    citation_text = _normalize_urls_in_text(citation_text_seed)
    citation_text_url_normalized = bool(citation_text_seed and citation_text_seed != citation_text)
    explicit_quote = _first_non_empty(row, _field_aliases(schema, "quote_text", _QUOTE_ALIASES))
    quote_text = _normalize_text(explicit_quote) or str(signal.get("quote_text") or "")
    if source_title and not _normalize_text(explicit_quote):
        quote_text = ""
    normalized_label = _normalize_label_token(label)
    if (
        not source_title
        and source_url
        and normalized_label in {"source", "citation", "bibliography", "reference", "references", "footnote", "endnote"}
    ):
        source_title = _clean_claim_text(body or text_value, urls=urls)
    if (source_title or source_url) and not speaker and argument_role == "claim":
        argument_role = "evidence"

    source_domain = _source_domain(source_url)
    claim_text = _clean_claim_text(body or text_value, urls=urls)
    if not claim_text:
        claim_text = _clean_claim_text(text_value, urls=[])
    if source_title and argument_role == "evidence" and not speaker:
        claim_text = source_title
    if not claim_text and source_title:
        claim_text = source_title
    if not claim_text and source_url:
        claim_text = source_url

    explicit_topic = _first_non_empty(row, _field_aliases(schema, "debate_topic", _TOPIC_ALIASES))
    debate_topic = _normalize_text(explicit_topic) or _topic_from_text(claim_text or text_value)
    explicit_language = _first_non_empty(row, _field_aliases(schema, "language", _LANGUAGE_ALIASES))
    language = _normalize_text(explicit_language) or _detect_language(f"{claim_text} {quote_text}")

    return {
        "claim_text": claim_text,
        "speaker": speaker,
        "speaker_role": speaker_role,
        "argument_role": argument_role or "claim",
        "source_title": source_title,
        "source_url": source_url,
        "source_domain": source_domain,
        "published_at": published_at,
        "stance": stance,
        "confidence": confidence,
        "citation_text": citation_text,
        "quote_text": quote_text,
        "debate_topic": debate_topic,
        "language": language,
        "_source_url_normalized": source_url_normalized,
        "_citation_text_url_normalized": citation_text_url_normalized,
    }


def _to_canonical_evidence_row(row: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    debate_fields = _canonical_debate_fields(row, schema)
    source_path = row.get("source_path")
    source_file = row.get("source_file")
    source_type = row.get("source_type")
    chunk_index = row.get("chunk_index")
    page = row.get("page")
    sheet_name = row.get("sheet_name")
    row_index = row.get("row_index")

    normalized_claim = _normalized_claim_fingerprint(debate_fields.get("claim_text") or "")
    speaker_key = _normalize_alias_value(debate_fields.get("speaker"))
    source_key = _normalize_alias_value(
        debate_fields.get("source_domain")
        or debate_fields.get("source_title")
        or debate_fields.get("source_url")
    )
    if normalized_claim and (speaker_key or source_key):
        key_text = "|".join([normalized_claim, speaker_key, source_key])
    else:
        key_text = "|".join(
            [
                str(source_path or ""),
                str(page or ""),
                str(sheet_name or ""),
                str(row_index or chunk_index or ""),
                str(debate_fields.get("claim_text") or ""),
            ]
        )
    evidence_id = hashlib.sha1(key_text.encode("utf-8")).hexdigest()[:16]

    return {
        "evidence_id": evidence_id,
        "claim_text": debate_fields.get("claim_text"),
        "speaker": debate_fields.get("speaker"),
        "speaker_role": debate_fields.get("speaker_role"),
        "argument_role": debate_fields.get("argument_role"),
        "source_title": debate_fields.get("source_title"),
        "source_url": debate_fields.get("source_url"),
        "source_domain": debate_fields.get("source_domain"),
        "published_at": debate_fields.get("published_at"),
        "stance": debate_fields.get("stance"),
        "confidence": debate_fields.get("confidence"),
        "citation_text": debate_fields.get("citation_text"),
        "quote_text": debate_fields.get("quote_text"),
        "debate_topic": debate_fields.get("debate_topic"),
        "language": debate_fields.get("language"),
        "source_file": source_file,
        "source_path": source_path,
        "source_type": source_type,
        "page": page,
        "sheet_name": sheet_name,
        "row_index": row_index,
        "chunk_index": chunk_index,
        "_source_url_normalized": bool(debate_fields.get("_source_url_normalized")),
        "_citation_text_url_normalized": bool(debate_fields.get("_citation_text_url_normalized")),
    }
