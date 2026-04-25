from __future__ import annotations

import importlib
import importlib.util
import hashlib
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import unquote, urlparse, urlunparse
from xml.etree import ElementTree

import requests


_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]+")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", flags=re.I)
_CJK_RADICAL_SUPPLEMENT_MAP = str.maketrans(
    {
        "\u2ea0": "\u6c11",
        "\u2ecb": "\u8f66",
        "\u2ed3": "\u957f",
        "\u2ed4": "\u95e8",
        "\u2edb": "\u98ce",
        "\u2ee2": "\u9a6c",
        "\u2ee9": "\u9ec4",
        "\u2ef0": "\u9f99",
    }
)
_BIBLIO_PREFIX_RE = re.compile(
    r"^\s*(?:(?:references?|bibliography|footnote|endnote|source|citation)\s*[:：])",
    flags=re.I,
)
_CITATION_TOKEN_BODY_RE = r"(?:[A-Za-z]?\d[A-Za-z0-9_.-]{0,20}|\d+[A-Za-z][A-Za-z0-9_.-]{0,20})"
_NUMBERED_CITATION_PREFIX_RE = re.compile(
    f"^\\s*(?:\\[{_CITATION_TOKEN_BODY_RE}\\]|\uff3b{_CITATION_TOKEN_BODY_RE}\uff3d|"
    f"\u3010{_CITATION_TOKEN_BODY_RE}\u3011|\\((?P<paren>\\d+)\\)|(?P<plain>\\d+)[.)])\\s*(?P<body>.+?)\\s*$"
)
_CITATION_TOKEN_RE = re.compile(
    f"(?:\\[{_CITATION_TOKEN_BODY_RE}\\]|\uff3b{_CITATION_TOKEN_BODY_RE}\uff3d|"
    f"\u3010{_CITATION_TOKEN_BODY_RE}\u3011|\\([A-Za-z0-9][A-Za-z0-9_.-]{{0,20}}\\))"
)
_SOURCE_SIGNATURE_RE = re.compile(
    r"^\s*(?:[-—–]{1,3}\s*)?(?:(?P<date>\d{4}(?:[./-]\d{1,2}(?:[./-]\d{1,2})?)?)\s+)?(?P<title>[^:：]{2,160})\s*$"
)
_SOURCE_DASH_PREFIX_RE = re.compile(r"^\s*(?:[-\u2012\u2013\u2014\u2015\u2e3a\u2e3b]{1,3}|[-\u2014]\s*[-\u2014])\s*")
_SOURCE_SIGNATURE_RE = re.compile(
    r"^\s*(?:[-\u2012\u2013\u2014\u2015\u2e3a\u2e3b]{1,3}\s*)?"
    r"(?:(?P<date>\d{4}(?:[./-]\d{1,2}(?:[./-]\d{1,2})?)?)\s+)?"
    r"(?P<title>[^:\uff1a]{2,160})\s*$"
)
_SOURCE_MARKER_RE = re.compile(
    r"^\s*(?:source|citation|according to|来源|出处|参考文献)\s*[:：]\s*(?P<title>.+?)\s*$",
    flags=re.I,
)
_STRUCTURAL_ROLES = {"metadata", "section"}
_NON_CLAIM_ROLES = {"quote", "evidence", "citation", "moderation", "question", "metadata", "section"}
_AZURE_FILE_SUFFIXES = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".docx"}
_GROBID_FILE_SUFFIXES = {".pdf", ".docx"}
_REFERENCE_SECTION_RE = re.compile(
    r"^(?:references?|bibliography|works cited|footnotes?|endnotes?|sources?|citations?|"
    r"\u53c2\u8003\u6587\u732e|\u53c2\u8003\u8d44\u6599|\u5f15\u7528|\u811a\u6ce8|\u5c3e\u6ce8|"
    r"\u6765\u6e90|\u51fa\u5904)\s*[:\uff1a]?$",
    flags=re.I,
)
_SOURCE_ATTRIBUTION_ZH_RE = re.compile(
    r"^\s*(?P<title>[\w\s&.\-:/\u00b7\u3001\u300a\u300b\u201c\u201d\u2018\u2019"
    r"\uff08\uff09()\u4e00-\u9fff]{2,120})(?:\u7684)?"
    r"(?:\u8c03\u67e5|\u7814\u7a76|\u62a5\u544a|\u7edf\u8ba1|\u6570\u636e)"
    r"(?:\u663e\u793a|\u6307\u51fa|\u53d1\u73b0|\u8ba4\u4e3a|\u79f0|\u8868\u660e)"
    r"\s*[:\uff1a,\uff0c]?\s*(?P<body>.*)$"
)
_SOURCE_ATTRIBUTION_EN_RE = re.compile(
    r"^\s*(?:(?:according to|based on)\s+)?"
    r"(?P<title>[A-Z][A-Za-z0-9&.,'’() \-/]{2,120}?)(?:'s)?\s+"
    r"(?:survey|study|report|poll|research|data|analysis)\s+"
    r"(?:shows?|showed|finds?|found|suggests?|indicates?|says?)\b"
    r"\s*[:,-]?\s*(?P<body>.*)$",
    flags=re.I,
)
_MAJOR_SECTION_HEADING_RE = re.compile(
    r"^\s*(?:\u3010[^\u3011]{1,80}\u3011|\[[^\]]{2,80}\]|#{1,6}\s+.+)\s*$"
)
_SECTION_FACT_FRAGMENT_RE = re.compile(
    r"^\s*(?:\d+(?:\.\d+)?\s*%|\d+(?:\.\d+)?\s*(?:\u4e07|\u4ebf|"
    r"million|billion|percent|%))",
    flags=re.I,
)
_SOURCE_LIST_MARKER_ONLY_RE = re.compile(
    r"^\s*(?:[\(\uff08]?(?:\d{1,3}|[\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]{1,4})"
    r"[\)\uff09.、\uff0e\uff1f?]?|[A-Z][.)])\s*$"
)
_SOURCE_LIST_MARKER_PREFIX_RE = re.compile(
    r"^\s*(?:[\(\uff08]?(?:\d{1,3}|[\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]{1,4})"
    r"[\)\uff09.、\uff0e\uff1f?]|[A-Z][.)])\s+(?P<body>.+?)\s*$"
)
_GENERIC_METADATA_TOPICS = {
    "match info",
    "competition info",
    "recording",
    "recording file",
    "recording link",
    "meeting",
    "meeting id",
    "meeting no",
    "meeting number",
    "round",
    "script",
    "\u6bd4\u8d5b\u76f8\u5173\u4fe1\u606f",
    "\u65f6\u95f4",
    "\u65e5\u671f",
    "\u5f55\u5236\u6587\u4ef6",
    "\u5f55\u5236\u94fe\u63a5",
    "\u5f55\u5c4f\u94fe\u63a5",
    "\u4f1a\u8bae\u53f7",
    "\u6a21\u8fa9",
    "\u4e00\u8fa9\u7a3f",
    "\u4e8c\u8fa9\u7a3f",
    "\u4e09\u8fa9\u7a3f",
    "\u56db\u8fa9\u7a3f",
}
_MOTION_TOPIC_RE = re.compile(
    r"\b(?:this house|whether|should|ought|ban|allow|require|policy|incentive|tax|"
    r"more\s+effective|less\s+effective|better|worse|benefit|harm)\b",
    flags=re.I,
)
_ZH_MOTION_TOPIC_RE = re.compile(r"(?:\u5e94\u5f53|\u5e94\u8be5|\u662f\u5426|\u5229\u5927\u4e8e\u5f0a|\u5f0a\u5927\u4e8e\u5229|\u66f4|\u4f18\u4e8e|\u5fc5\u8981|\u53ef\u884c|\u6b63\u5f53)")
_SPEAKER_ROLE_HEADING_PATTERNS = (
    (re.compile(r"(?:^|[\s:：])(?:\u6b63\u65b9|\u53cd\u65b9)?\s*(?:\u4e00|1|first|1st)\s*(?:\u8fa9|\u8fa9\u624b|speaker|speech)", flags=re.I), "first_speaker"),
    (re.compile(r"(?:^|[\s:：])(?:\u6b63\u65b9|\u53cd\u65b9)?\s*(?:\u4e8c|2|second|2nd)\s*(?:\u8fa9|\u8fa9\u624b|speaker|speech)", flags=re.I), "second_speaker"),
    (re.compile(r"(?:^|[\s:：])(?:\u6b63\u65b9|\u53cd\u65b9)?\s*(?:\u4e09|3|third|3rd)\s*(?:\u8fa9|\u8fa9\u624b|speaker|speech)", flags=re.I), "third_speaker"),
    (re.compile(r"(?:^|[\s:：])(?:\u6b63\u65b9|\u53cd\u65b9)?\s*(?:\u56db|4|fourth|4th)\s*(?:\u8fa9|\u8fa9\u624b|speaker|speech)", flags=re.I), "fourth_speaker"),
)
_ARGUMENT_ROLE_HEADING_PATTERNS = (
    (re.compile(r"(?:rebuttal|refute|counter|response|attack\s*/?\s*defen[cs]e|\u53cd\u9a73|\u9a73\u8bba|\u9a73\u65a5|\u56de\u5e94|\u653b\u9632|\u653b\u8fa9)"), "rebuttal"),
    (re.compile(r"(?:summary|closing|conclusion|\u603b\u7ed3|\u7ed3\u8fa9|\u603b\u7ed3\u9648\u8bcd|\u7ed3\u9648)"), "summary"),
    (re.compile(r"(?:question|cross[-\s]?examination|cross[-\s]?exam|ask|\u8d28\u8be2|\u63d0\u95ee|\u76d8\u95ee|\u5bf9\u8fa9)"), "question"),
    (re.compile(r"(?:constructive|opening|case\s+building|\u7acb\u8bba|\u5f00\u7bc7|\u7533\u8bba)"), "constructive"),
)
_CLAIM_TERMINAL_PUNCT_RE = re.compile(r"[。.!?！？；;：:」』”’）》】]$")
_NEW_CLAIM_START_RE = re.compile(
    r"^\s*(?:"
    r"[\(\uff08]?\d{1,3}[\)\uff09.、．]|"
    r"[一二三四五六七八九十]+[、.．]|"
    r"第[一二三四五六七八九十0-9]+[，,、.]|"
    r"[•*]\s+|[-—–]\s+|"
    r"(?:first|second|third|fourth|finally)\b"
    r")",
    flags=re.I,
)
_DEBATE_CANONICAL_FIELDS = (
    "evidence_id",
    "claim_text",
    "speaker",
    "speaker_role",
    "argument_role",
    "source_title",
    "source_url",
    "source_domain",
    "published_at",
    "stance",
    "confidence",
    "citation_text",
    "quote_text",
    "debate_topic",
    "language",
    "source_file",
    "source_path",
    "source_type",
    "page",
    "sheet_name",
    "row_index",
    "chunk_index",
)


def _module_status(name: str) -> tuple[bool, Any]:
    try:
        spec = importlib.util.find_spec(name)
    except ModuleNotFoundError:
        spec = None
    if spec is None:
        return False, None
    try:
        return True, importlib.import_module(name)
    except Exception:
        return False, None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _collapse_ws(value: Any) -> str:
    return re.sub(r"\s+", " ", _normalize_text(value)).strip()


def _looks_source_list_marker_only(value: Any) -> bool:
    return bool(_SOURCE_LIST_MARKER_ONLY_RE.fullmatch(_collapse_ws(value)))


def _source_title_from_list_marker_claim(value: Any) -> str:
    text = _collapse_ws(value)
    match = _SOURCE_LIST_MARKER_PREFIX_RE.match(text)
    if not match:
        return ""
    body = _clean_source_title_text(match.group("body"))
    if not body or len(body) > 120:
        return ""
    if re.search(r"[。！？!?]\s*$", body):
        return ""
    return body


def _normalize_source_url(value: Any) -> str:
    url = _CONTROL_CHAR_RE.sub("", str(value or "")).rstrip(".,);]")
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


def _extract_urls(value: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for match in _URL_RE.finditer(str(value or "")):
        url = _normalize_source_url(match.group(0))
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _strip_urls_from_text(value: Any) -> str:
    return _collapse_ws(_URL_RE.sub(" ", str(value or "")))


def _strip_partial_dates_from_text(value: Any) -> str:
    return _collapse_ws(
        re.sub(
            r"\b\d{4}[./-]\d{1,2}(?:[./-]\d{1,2})?\b",
            " ",
            str(value or ""),
        )
    ).strip(" -—–,，;；")


def _clean_source_title_text(value: Any) -> str:
    title = _strip_urls_from_text(value)
    title = _strip_partial_dates_from_text(title)
    title = _SOURCE_DASH_PREFIX_RE.sub("", title)
    title = re.sub(r"^\s*(?:source|citation|according to|来源|出处|参考文献)\s*[:：]\s*", "", title, flags=re.I)
    return _collapse_ws(title).strip(" -—–,，;；")


def _looks_bare_citation_token(value: Any) -> bool:
    return bool(
        re.fullmatch(
            f"(?:\\[{_CITATION_TOKEN_BODY_RE}\\]|\uff3b{_CITATION_TOKEN_BODY_RE}\uff3d|"
            f"\u3010{_CITATION_TOKEN_BODY_RE}\u3011|\\(\\d+\\)|\\d+[.)])",
            _collapse_ws(value),
        )
    )


def _normalize_citation_token(value: Any) -> str:
    return _collapse_ws(value).lower()


def _citation_tokens_from_text(value: Any) -> List[str]:
    text = _collapse_ws(value)
    if not text:
        return []
    out: List[str] = []
    seen: set[str] = set()
    for part in re.split(r"\s*\|\s*", text):
        for match in _CITATION_TOKEN_RE.finditer(part):
            token = _collapse_ws(match.group(0))
            normalized = _normalize_citation_token(token)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(token)
    return out


def _strip_numbered_citation_prefix(value: Any) -> str:
    text = _collapse_ws(value)
    match = _NUMBERED_CITATION_PREFIX_RE.match(text)
    return _collapse_ws(match.group("body")) if match else text


def _looks_bibliographic_body(value: Any) -> bool:
    text = _collapse_ws(value)
    if not text:
        return False
    if _extract_urls(text):
        return True
    if _SOURCE_MARKER_RE.match(text):
        return True
    has_year = bool(re.search(r"\b(?:19|20)\d{2}\b", text))
    if not has_year:
        return False
    return bool(
        re.search(
            r"\b(?:report|daily|times|journal|review|study|memo|brief|survey|analysis|paper|"
            r"database|yearbook|proceedings|press|news|agency|institute|center|centre|lab|"
            r"university|commission|committee|ministry|office|foundation)\b|"
            r"(?:研究|报告|日报|晚报|时报|新闻网|新华社|人民网|央视|经济日报|青年报|"
            r"参考|文献|期刊|论文|白皮书|蓝皮书|年鉴|统计局|研究院|智库|大学|学院|"
            r"中心|委员会|协会|基金会)",
            text,
            flags=re.I,
        )
    )


def _looks_numbered_citation_entry_text(value: Any) -> bool:
    match = _NUMBERED_CITATION_PREFIX_RE.match(_collapse_ws(value))
    if not match:
        return False
    return _looks_bibliographic_body(match.group("body"))


def _source_domain(url: Any) -> str:
    text = _normalize_text(url)
    if not text:
        return ""
    candidate = text if re.match(r"^[a-z]+://", text, flags=re.I) else f"https://{text}"
    try:
        host = urlparse(candidate).netloc.lower()
    except Exception:
        host = ""
    return host[4:] if host.startswith("www.") else host


def _display_label_from_token(value: str) -> str:
    token = re.sub(r"[_-]+", " ", str(value or "")).strip(" .")
    if not token:
        return ""
    words = []
    for part in token.split():
        words.append(part.upper() if len(part) <= 4 and part.isalpha() else part.capitalize())
    return " ".join(words)


def _looks_machine_generated_url_token(value: Any) -> bool:
    token = re.sub(r"\.[A-Za-z0-9]{1,6}$", "", str(value or "").strip())
    if not token:
        return True
    lowered = token.lower()
    if re.fullmatch(r"[A-Za-z]", token):
        return True
    if re.fullmatch(r"[a-f0-9]{2,4}", lowered) and re.search(r"\d", lowered):
        return True
    if re.fullmatch(r"\d+|[a-f0-9]{8,}", token, flags=re.I):
        return True
    if lowered.startswith(("content_", "artid", "articleid", "docid", "node_", "id_")):
        return True
    if lowered in {"indexch", "page", "pages", "topic", "topics"}:
        return True
    if re.fullmatch(r"[a-z]_?\d{6,}(?:_\d+)?", lowered):
        return True
    if re.fullmatch(r"t\d{6,}(?:_\d+)?", lowered):
        return True
    if re.fullmatch(r"[a-z]{2,12}_\d{4,}(?:_\d+)?", lowered):
        return True
    if re.fullmatch(r"[a-z]{2,12}\d?", lowered) and not re.search(r"[aeiou]", lowered):
        return True
    compact = re.sub(r"[_-]+", "", token)
    if len(compact) >= 12 and re.search(r"[A-Za-z]", compact) and re.search(r"\d", compact):
        alpha_runs = re.findall(r"[A-Za-z]+", compact)
        digit_runs = re.findall(r"\d+", compact)
        if len(alpha_runs) >= 2 or any(len(run) >= 3 for run in digit_runs):
            return True
    if len(compact) >= 16 and re.fullmatch(r"[A-Za-z0-9]+", compact) and not re.search(r"[_-]", token):
        return True
    return False


def _source_title_from_url(url: Any) -> str:
    text = _normalize_text(url)
    if not text:
        return ""
    candidate = text if re.match(r"^[a-z]+://", text, flags=re.I) else f"https://{text}"
    try:
        parsed = urlparse(candidate)
    except Exception:
        return ""
    path_tokens = [
        unquote(part)
        for part in (parsed.path or "").split("/")
        if part and not re.fullmatch(r"(?:index|indexch|art|articles?|content|detail|news|column|pages?|topics?|wap|html?|shtml?|gb|cn|en)", part, flags=re.I)
    ]
    for token in reversed(path_tokens):
        token = re.sub(r"\.[A-Za-z0-9]{1,6}$", "", token)
        if _looks_machine_generated_url_token(token):
            continue
        label = _display_label_from_token(token)
        if label and re.search(r"[A-Za-z\u4e00-\u9fff]", label):
            return label[:120]
    host = _source_domain(text)
    if not host:
        return ""
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host):
        return host
    labels = [
        part
        for part in host.split(".")
        if part and part not in {"www", "m", "wap", "news", "com", "cn", "net", "org", "gov", "edu", "ac", "co"}
    ]
    return " ".join(_display_label_from_token(part) for part in labels[:2]).strip()[:120]


def _source_title_is_url_fallback(title: Any, url: Any) -> bool:
    return bool(_normalize_text(title)) and _normalize_text(title) == _source_title_from_url(url)


def _profile_name(spec: Dict[str, Any]) -> str:
    return str(spec.get("canonical_profile") or "").strip().lower()


def _debate_mode(spec: Dict[str, Any], rows: List[Dict[str, Any]]) -> bool:
    if _profile_name(spec) == "debate_evidence" or bool(spec.get("standardize_evidence", False)):
        return True
    return any("claim_text" in row or "speaker" in row or "stance" in row for row in rows)


def _external_enrichment_mode(spec: Dict[str, Any]) -> str:
    value = str(spec.get("external_enrichment_mode") or "off").strip().lower()
    return value if value in {"off", "private", "public", "auto"} else "off"


def _requested_document_backend(spec: Dict[str, Any]) -> str:
    value = str(spec.get("document_parse_backend") or "auto").strip().lower()
    return value if value in {"auto", "local", "azure_docintelligence"} else "auto"


def _requested_citation_backend(spec: Dict[str, Any]) -> str:
    value = str(spec.get("citation_parse_backend") or "auto").strip().lower()
    return value if value in {"auto", "regex", "grobid"} else "auto"


def _url_metadata_enabled(spec: Dict[str, Any], rows: List[Dict[str, Any]]) -> bool:
    if "url_metadata_enrichment" in spec:
        return bool(spec.get("url_metadata_enrichment"))
    return _profile_name(spec) == "debate_evidence"


def _looks_citation_entry_text(text: Any) -> bool:
    normalized = _collapse_ws(text)
    if not normalized:
        return False
    if _BIBLIO_PREFIX_RE.match(normalized):
        return True
    if _looks_numbered_citation_entry_text(normalized):
        return True
    if _SOURCE_MARKER_RE.match(normalized):
        return True
    if _SOURCE_DASH_PREFIX_RE.match(normalized) and len(normalized) <= 180:
        return True
    if normalized.startswith(("——", "—", "--")) and len(normalized) <= 180:
        return True
    urls = _extract_urls(normalized)
    if urls and normalized in {urls[0], urls[0].rstrip("/")}:
        return True
    if urls and re.search(r"\b(?:report|daily|times|journal|新闻网|日报|网|研究院|报告)\b", normalized, flags=re.I):
        return True
    return False


def _extract_source_attribution_signature(text: Any) -> Dict[str, str]:
    normalized = _collapse_ws(text)
    if not normalized:
        return {}
    for pattern in (_SOURCE_ATTRIBUTION_ZH_RE, _SOURCE_ATTRIBUTION_EN_RE):
        match = pattern.match(normalized)
        if not match:
            continue
        raw_title = _collapse_ws(match.group("title"))
        title = _clean_source_title_text(raw_title)
        title = re.sub(r"(?:\u7684|\s+by)$", "", title, flags=re.I).strip(" :-")
        title_key = _normalize_claim_key(title)
        if not title or len(title) > 140:
            continue
        if title_key in {"the", "this", "a", "an", "survey", "study", "report", "\u8c03\u67e5", "\u7814\u7a76", "\u62a5\u544a"}:
            continue
        return {
            "source_title": title,
            "citation_text": normalized,
            "published_at": _parse_partial_date(normalized),
        }
    return {}


def _normalize_claim_key(text: Any) -> str:
    value = str(text or "").lower()
    value = re.sub(r"https?://\S+|www\.\S+", " ", value)
    value = re.sub(r"\[[^\]]{1,40}\]", " ", value)
    value = re.sub(r"[^0-9a-z\u4e00-\u9fff\s]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _section_topic_from_text(value: Any) -> str:
    text = _collapse_ws(value)
    if not text:
        return ""
    text = re.sub(r"^\s*#{1,6}\s*", "", text)
    text = re.sub(r"^\s*(?:[-*+•]+|\d+(?:\.\d+)*[.)]?|[一二三四五六七八九十]+[、.．])\s*", "", text)
    text = text.strip()
    for left, right in (("【", "】"), ("[", "]"), ("(", ")"), ("（", "）"), ("「", "」"), ("《", "》")):
        if text.startswith(left) and text.endswith(right) and len(text) > len(left) + len(right):
            text = text[len(left) : -len(right)].strip()
            break
    text = text.strip(" \t\r\n:-：")
    if not text:
        return ""
    if _REFERENCE_SECTION_RE.fullmatch(text):
        return ""
    return text


def _metadata_topic_from_row(row: Dict[str, Any]) -> str:
    if _normalize_text(row.get("source_url")):
        return ""
    text = _section_topic_from_text(row.get("claim_text") or row.get("debate_topic"))
    if not text:
        return ""
    lowered = text.lower().strip(" :\uff1a")
    if lowered in _GENERIC_METADATA_TOPICS:
        return ""
    if re.fullmatch(r"\d{1,4}(?:[./-]\d{1,2}){1,2}(?:\s+\d{1,2}:\d{2})?", text):
        return ""
    if re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", text):
        return ""
    if _MOTION_TOPIC_RE.search(text) or _ZH_MOTION_TOPIC_RE.search(text):
        return text
    return ""


def _speaker_role_from_heading_row(row: Dict[str, Any]) -> str:
    text = _collapse_ws(row.get("claim_text") or row.get("debate_topic"))
    if not text:
        return ""
    for pattern, role in _SPEAKER_ROLE_HEADING_PATTERNS:
        if pattern.search(text):
            return role
    return ""


def _argument_role_from_heading_row(row: Dict[str, Any]) -> str:
    text = _collapse_ws(row.get("claim_text") or row.get("debate_topic")).lower()
    if not text:
        return ""
    for pattern, role in _ARGUMENT_ROLE_HEADING_PATTERNS:
        if pattern.search(text):
            return role
    return ""


def _source_type_from_path(source_path: Any, fallback: Any = "") -> str:
    suffix = Path(_normalize_text(source_path)).suffix.lower().lstrip(".")
    if suffix in {"pdf", "docx"}:
        return suffix
    existing = _normalize_text(fallback)
    if existing:
        return existing
    return suffix


def _citation_evidence_id(source_path: Any, citation_text: Any) -> str:
    key = f"{_normalize_text(source_path)}|grobid|{_normalize_claim_key(citation_text)}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _canonical_citation_row_from_grobid(source_path: str, citation_text: str, seed_row: Dict[str, Any]) -> Dict[str, Any]:
    signature = _extract_source_signature(citation_text)
    source_url = _extract_urls(citation_text)
    row = {field: "" for field in _DEBATE_CANONICAL_FIELDS}
    row.update(
        {
            "evidence_id": _citation_evidence_id(source_path, citation_text),
            "claim_text": signature.get("source_title") or _clean_source_title_text(citation_text) or _collapse_ws(citation_text),
            "speaker": "",
            "speaker_role": "source",
            "argument_role": "citation",
            "source_title": signature.get("source_title") or _clean_source_title_text(citation_text),
            "source_url": source_url[0] if source_url else "",
            "source_domain": _source_domain(source_url[0]) if source_url else "",
            "published_at": signature.get("published_at") or _parse_partial_date(citation_text),
            "stance": "unknown",
            "confidence": "",
            "citation_text": _collapse_ws(citation_text),
            "quote_text": "",
            "debate_topic": "",
            "language": "",
            "source_file": seed_row.get("source_file") or Path(source_path).name,
            "source_path": source_path,
            "source_type": _source_type_from_path(source_path, seed_row.get("source_type")),
            "page": "",
            "sheet_name": seed_row.get("sheet_name") or "",
            "row_index": "",
            "chunk_index": "",
        }
    )
    return row


def _azure_paragraph_argument_role(paragraph_role: Any) -> str:
    role = _normalize_text(paragraph_role).lower()
    if role in {"pageheader", "pagefooter", "pagenumber"}:
        return "metadata"
    if role in {"footnote", "footnotecontinued"}:
        return "citation"
    if role in {"sectionheading", "title"}:
        return "section"
    return ""


def _document_paragraph_evidence_id(source_path: Any, engine: str, role: str, content: Any) -> str:
    key = f"{_normalize_text(source_path)}|{engine}|{role}|{_normalize_claim_key(content)}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _canonical_row_from_azure_paragraph(
    source_path: str,
    paragraph_content: str,
    paragraph_role: str,
    seed_row: Dict[str, Any],
) -> Dict[str, Any] | None:
    argument_role = _azure_paragraph_argument_role(paragraph_role)
    content = _collapse_ws(paragraph_content)
    if not argument_role or not content:
        return None
    signature = _extract_source_signature(content) if argument_role == "citation" else {}
    urls = _extract_urls(content)
    source_title = signature.get("source_title") if argument_role == "citation" else ""
    row = {field: "" for field in _DEBATE_CANONICAL_FIELDS}
    row.update(
        {
            "evidence_id": _document_paragraph_evidence_id(source_path, "azure_docintelligence", argument_role, content),
            "claim_text": source_title or _clean_source_title_text(content) or content if argument_role == "citation" else content,
            "speaker": "",
            "speaker_role": "source" if argument_role == "citation" else "metadata",
            "argument_role": argument_role,
            "source_title": source_title,
            "source_url": urls[0] if argument_role == "citation" and urls else "",
            "source_domain": _source_domain(urls[0]) if argument_role == "citation" and urls else "",
            "published_at": signature.get("published_at") if argument_role == "citation" else "",
            "stance": "unknown",
            "confidence": "",
            "citation_text": content if argument_role == "citation" else "",
            "quote_text": "",
            "debate_topic": "",
            "language": "",
            "source_file": seed_row.get("source_file") or Path(source_path).name,
            "source_path": source_path,
            "source_type": _source_type_from_path(source_path, seed_row.get("source_type")),
            "page": "",
            "sheet_name": seed_row.get("sheet_name") or "",
            "row_index": "",
            "chunk_index": "",
        }
    )
    return row


def _parse_partial_date(value: Any) -> str:
    text = _collapse_ws(value)
    if not text:
        return ""
    match = re.search(r"(?P<year>\d{4})(?:[./-](?P<month>\d{1,2}))?(?:[./-](?P<day>\d{1,2}))?", text)
    if not match:
        return ""
    year = int(match.group("year"))
    month = int(match.group("month") or 1)
    day = int(match.group("day") or 1)
    if month < 1 or month > 12 or day < 1 or day > 31:
        return ""
    return f"{year:04d}-{month:02d}-{day:02d}"


def _extract_source_signature(text: Any) -> Dict[str, str]:
    normalized = _collapse_ws(text)
    if not normalized:
        return {}
    marker_match = _SOURCE_MARKER_RE.match(normalized)
    if marker_match:
        raw_title = _collapse_ws(marker_match.group("title"))
        title = _clean_source_title_text(raw_title)
        return {
            "source_title": title,
            "published_at": _parse_partial_date(raw_title),
            "citation_text": normalized,
        }
    if not _looks_citation_entry_text(normalized):
        return {}
    stripped = _strip_numbered_citation_prefix(normalized)
    match = _SOURCE_SIGNATURE_RE.match(stripped)
    if not match:
        title = _clean_source_title_text(stripped) if len(stripped) <= 180 else ""
        published_at = _parse_partial_date(stripped)
        return {"source_title": title, "published_at": published_at, "citation_text": normalized} if title else {}
    raw_title = _collapse_ws(match.group("title"))
    title = _clean_source_title_text(raw_title)
    published_at = _parse_partial_date(match.group("date") or "") or _parse_partial_date(raw_title)
    if title and re.fullmatch(r"https?://\S+|www\.\S+", title, flags=re.I):
        title = ""
    return {
        "source_title": title,
        "published_at": published_at,
        "citation_text": normalized,
    }


def _group_rows_by_source(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], List[int]]:
    groups: Dict[Tuple[str, str, str], List[int]] = {}
    for index, row in enumerate(rows):
        key = (
            _normalize_text(row.get("source_path")),
            _normalize_text(row.get("page")),
            _normalize_text(row.get("sheet_name")),
        )
        groups.setdefault(key, []).append(index)
    return groups


def _row_has_source_ref(row: Dict[str, Any]) -> bool:
    return any(_normalize_text(row.get(field)) for field in ("source_title", "source_url", "citation_text"))


def _row_is_claim_like(row: Dict[str, Any]) -> bool:
    claim_text = _normalize_text(row.get("claim_text"))
    if not claim_text:
        return False
    role = _normalize_text(row.get("argument_role")).lower()
    return role not in _NON_CLAIM_ROLES


def _same_row_location(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    return (
        _normalize_text(left.get("source_path")) == _normalize_text(right.get("source_path"))
        and _normalize_text(left.get("page")) == _normalize_text(right.get("page"))
        and _normalize_text(left.get("sheet_name")) == _normalize_text(right.get("sheet_name"))
    )


def _row_indexes_are_adjacent(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    left_index = _normalize_text(left.get("row_index"))
    right_index = _normalize_text(right.get("row_index"))
    if not left_index or not right_index:
        return True
    try:
        return int(float(right_index)) == int(float(left_index)) + 1
    except Exception:
        return True


def _join_wrapped_claim_text(left: str, right: str) -> str:
    left_text = _normalize_text(left)
    right_text = _normalize_text(right)
    if not left_text:
        return right_text
    if not right_text:
        return left_text
    if left_text.endswith("-"):
        return f"{left_text[:-1]}{right_text}"
    if re.search(r"[\u4e00-\u9fff]$", left_text) and re.match(r"^[\u4e00-\u9fff]", right_text):
        return f"{left_text}{right_text}"
    return f"{left_text} {right_text}"


def _looks_numeric_fact_continuation_fragment(text: Any) -> bool:
    normalized = _collapse_ws(text)
    if not normalized or len(normalized) > 60:
        return False
    if not _SECTION_FACT_FRAGMENT_RE.match(normalized):
        return False
    if re.match(r"^\s*\d+(?:\.\d+)*\s+[\u4e00-\u9fffA-Za-z].*[:\uff1a]$", normalized):
        return False
    return bool(re.search(r"(?:%|\u4e07|\u4ebf|million|billion|percent)", normalized, flags=re.I))


def _looks_wrapped_claim_continuation(previous: Dict[str, Any], current: Dict[str, Any]) -> bool:
    previous_text = _normalize_text(previous.get("claim_text"))
    current_text = _normalize_text(current.get("claim_text"))
    if not previous_text or not current_text:
        return False
    if _CLAIM_TERMINAL_PUNCT_RE.search(previous_text):
        return False
    if _NEW_CLAIM_START_RE.match(current_text):
        return False
    if not _same_row_location(previous, current) or not _row_indexes_are_adjacent(previous, current):
        return False
    current_role = _normalize_text(current.get("argument_role")).lower()
    numeric_fact_fragment = current_role == "section" and _looks_numeric_fact_continuation_fragment(current_text)
    if not _row_is_claim_like(previous) or (not _row_is_claim_like(current) and not numeric_fact_fragment):
        return False
    if _row_has_source_ref(previous) or _row_has_source_ref(current):
        return False
    if _normalize_text(current.get("speaker")):
        return False
    previous_role = _normalize_text(previous.get("argument_role")).lower()
    if current_role not in {"", "claim"} and not numeric_fact_fragment:
        return False
    if previous_role not in {"", "claim", "rebuttal", "summary", "constructive"}:
        return False
    return True


def _merge_wrapped_claim_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    merged = 0
    for row in rows:
        next_row = dict(row)
        if out and _looks_wrapped_claim_continuation(out[-1], next_row):
            out[-1]["claim_text"] = _join_wrapped_claim_text(out[-1].get("claim_text"), next_row.get("claim_text"))
            if not _normalize_text(out[-1].get("language")) and _normalize_text(next_row.get("language")):
                out[-1]["language"] = next_row.get("language")
            merged += 1
            continue
        out.append(next_row)
    return out, merged


def _source_context_citation_text(context: Dict[str, Any]) -> str:
    citation_text = _normalize_text(context.get("citation_text"))
    source_title = _normalize_text(context.get("source_title"))
    source_url = _normalize_text(context.get("source_url"))
    if citation_text and citation_text not in {source_url, source_url.rstrip("/")}:
        return citation_text
    if source_title and source_url:
        return _collapse_ws(f"{source_title} {source_url}")
    return citation_text or source_url or source_title


def _context_can_extend_claim_citations(context: Dict[str, Any]) -> bool:
    role = _normalize_text(context.get("_argument_role")).lower()
    speaker = _normalize_text(context.get("_speaker"))
    speaker_role = _normalize_text(context.get("_speaker_role")).lower()
    return role == "citation" or (not speaker and speaker_role in {"", "source"})


def _append_citation_text(row: Dict[str, Any], citation_text: str) -> bool:
    value = _normalize_text(citation_text)
    if not value:
        return False
    existing = _normalize_text(row.get("citation_text"))
    if value in existing:
        return False
    row["citation_text"] = f"{existing} | {value}" if existing else value
    if existing:
        row["_multi_source_citation_appended"] = True
    return True


def _consume_internal_flag_count(rows: List[Dict[str, Any]], field: str) -> int:
    count = 0
    for row in rows:
        if bool(row.pop(field, False)):
            count += 1
    return count


def _merge_source_context(row: Dict[str, Any], context: Dict[str, Any]) -> bool:
    row_title = _normalize_text(row.get("source_title"))
    context_title = _normalize_text(context.get("source_title"))
    if row_title and context_title and _normalize_claim_key(row_title) != _normalize_claim_key(context_title):
        if (
            _row_is_claim_like(row)
            and _normalize_text(row.get("source_url"))
            and _normalize_text(context.get("source_url"))
            and _context_can_extend_claim_citations(context)
        ):
            return _append_citation_text(row, _source_context_citation_text(context))
        return False
    row_url = _normalize_text(row.get("source_url")).rstrip("/")
    context_url = _normalize_text(context.get("source_url")).rstrip("/")
    if row_url and context_url and row_url.lower() != context_url.lower():
        return (
            _append_citation_text(row, _source_context_citation_text(context))
            if _row_is_claim_like(row) and _context_can_extend_claim_citations(context)
            else False
        )
    changed = False
    for field in ("source_title", "source_url", "source_domain", "published_at", "citation_text"):
        value = _normalize_text(context.get(field))
        if value and not _normalize_text(row.get(field)):
            row[field] = value
            changed = True
    return changed


def _document_group_key(row: Dict[str, Any]) -> Tuple[str, str]:
    return (
        _normalize_text(row.get("source_path")),
        _normalize_text(row.get("sheet_name")),
    )


def _is_document_level_source_row(row: Dict[str, Any]) -> bool:
    role = _normalize_text(row.get("argument_role")).lower()
    return (
        role in {"citation", "evidence"}
        and _row_has_source_ref(row)
        and not _normalize_text(row.get("page"))
        and not _normalize_text(row.get("sheet_name"))
    )


def _backfill_document_level_source_context(rows: List[Dict[str, Any]]) -> int:
    updated = 0
    pending_claims: Dict[Tuple[str, str], List[int]] = {}
    for index, row in enumerate(rows):
        key = _document_group_key(row)
        if not key[0]:
            continue
        role = _normalize_text(row.get("argument_role")).lower()
        if role in _STRUCTURAL_ROLES:
            pending_claims[key] = []
            continue
        if _row_is_claim_like(row):
            if not _row_has_source_ref(row):
                pending_claims.setdefault(key, []).append(index)
            continue
        if not _is_document_level_source_row(row):
            continue
        context = {
            field: row.get(field)
            for field in ("source_title", "source_url", "source_domain", "published_at", "citation_text")
            if _normalize_text(row.get(field))
        }
        context["_argument_role"] = row.get("argument_role")
        context["_speaker"] = row.get("speaker")
        context["_speaker_role"] = row.get("speaker_role")
        if not context:
            continue
        targets = pending_claims.get(key, [])
        for target in targets:
            if _merge_source_context(rows[target], context):
                updated += 1
        pending_claims[key] = []
    return updated


def _backfill_source_context_by_block(rows: List[Dict[str, Any]]) -> int:
    groups = _group_rows_by_source(rows)
    updated = 0
    for indexes in groups.values():
        claim_block: List[int] = []
        source_block: List[int] = []
        context: Dict[str, Any] = {}
        source_block_open = False
        for index in indexes:
            row = rows[index]
            role = _normalize_text(row.get("argument_role")).lower()
            if role in _STRUCTURAL_ROLES:
                claim_block = []
                source_block = []
                context = {}
                source_block_open = False
                continue
            if _row_has_source_ref(row) or role == "citation":
                if role == "citation" and not _normalize_text(row.get("citation_text")):
                    row["citation_text"] = _normalize_text(row.get("claim_text"))
                if role == "citation" and not _normalize_text(row.get("speaker_role")):
                    row["speaker_role"] = "source"
                if not source_block_open:
                    source_block = []
                    context = {}
                source_block.append(index)
                for field in ("source_title", "source_url", "source_domain", "published_at", "citation_text"):
                    value = _normalize_text(row.get(field))
                    if value:
                        context[field] = value
                context["_argument_role"] = row.get("argument_role")
                context["_speaker"] = row.get("speaker")
                context["_speaker_role"] = row.get("speaker_role")
                if _merge_source_context(row, context):
                    updated += 1
                for target in claim_block + source_block[:-1]:
                    if _merge_source_context(rows[target], context):
                        updated += 1
                source_block_open = True
                continue
            if _row_is_claim_like(row):
                if source_block_open:
                    claim_block = []
                    source_block = []
                    context = {}
                claim_block.append(index)
                source_block_open = False
                continue
            source_block_open = False
    updated += _backfill_document_level_source_context(rows)
    return updated


def _citation_token_scope_key(row: Dict[str, Any], *, include_page: bool) -> Tuple[str, str, str]:
    return (
        _normalize_text(row.get("source_path")),
        _normalize_text(row.get("sheet_name")),
        _normalize_text(row.get("page")) if include_page else "",
    )


def _source_context_for_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        field: row.get(field)
        for field in ("source_title", "source_url", "source_domain", "published_at", "citation_text")
        if _normalize_text(row.get(field))
    }


def _page_number(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _same_document_sheet(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    return (
        _normalize_text(left.get("source_path")) == _normalize_text(right.get("source_path"))
        and _normalize_text(left.get("sheet_name")) == _normalize_text(right.get("sheet_name"))
    )


def _adjacent_page_break(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    left_page = _page_number(left.get("page"))
    right_page = _page_number(right.get("page"))
    if left_page is None or right_page is None:
        return False
    return 0 <= right_page - left_page <= 1


def _contexts_compatible(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    left_url = _normalize_text(left.get("source_url")).rstrip("/").lower()
    right_url = _normalize_text(right.get("source_url")).rstrip("/").lower()
    if left_url and right_url and left_url != right_url:
        return False
    left_title = _normalize_text(left.get("source_title"))
    right_title = _normalize_text(right.get("source_title"))
    if left_title and right_title and _normalize_claim_key(left_title) != _normalize_claim_key(right_title):
        return False
    return True


def _index_source_context_by_citation_token(
    rows: List[Dict[str, Any]],
    *,
    include_page: bool,
) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
    contexts: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    ambiguous: set[Tuple[str, str, str, str]] = set()
    for row in rows:
        role = _normalize_text(row.get("argument_role")).lower()
        if role not in {"citation", "evidence"} or not _row_has_source_ref(row):
            continue
        tokens = _citation_tokens_from_text(row.get("citation_text")) or _citation_tokens_from_text(row.get("claim_text"))
        if not tokens:
            continue
        context = _source_context_for_row(row)
        if not context:
            continue
        scope = _citation_token_scope_key(row, include_page=include_page)
        for token in tokens:
            key = (*scope, _normalize_citation_token(token))
            if key in ambiguous:
                continue
            existing = contexts.get(key)
            if existing is not None and not _contexts_compatible(existing, context):
                contexts.pop(key, None)
                ambiguous.add(key)
                continue
            merged = dict(existing or {})
            for field, value in context.items():
                if _normalize_text(value) and not _normalize_text(merged.get(field)):
                    merged[field] = value
            contexts[key] = merged
    return contexts


def _backfill_source_context_by_citation_token(rows: List[Dict[str, Any]]) -> int:
    by_page = _index_source_context_by_citation_token(rows, include_page=True)
    by_document = _index_source_context_by_citation_token(rows, include_page=False)
    updated = 0
    for row in rows:
        if not _row_is_claim_like(row):
            continue
        tokens = _citation_tokens_from_text(row.get("citation_text")) or _citation_tokens_from_text(row.get("claim_text"))
        if not tokens:
            continue
        changed = False
        page_scope = _citation_token_scope_key(row, include_page=True)
        doc_scope = _citation_token_scope_key(row, include_page=False)
        for token in tokens:
            normalized = _normalize_citation_token(token)
            context = by_page.get((*page_scope, normalized)) or by_document.get((*doc_scope, normalized))
            if context and _merge_source_context(row, context):
                changed = True
        if changed:
            updated += 1
    return updated


def _row_has_citation_token(row: Dict[str, Any]) -> bool:
    return bool(_citation_tokens_from_text(row.get("citation_text")) or _citation_tokens_from_text(row.get("claim_text")))


def _backfill_source_context_from_leading_source_cards(rows: List[Dict[str, Any]]) -> int:
    groups = _group_rows_by_source(rows)
    updated = 0
    for indexes in groups.values():
        active_context: Dict[str, Any] = {}
        claims_seen_since_boundary = False
        for index in indexes:
            row = rows[index]
            role = _normalize_text(row.get("argument_role")).lower()
            if role in _STRUCTURAL_ROLES:
                active_context = {}
                claims_seen_since_boundary = False
                continue
            if _row_is_claim_like(row):
                claims_seen_since_boundary = True
                if active_context and not _row_has_source_ref(row):
                    if _merge_source_context(row, active_context):
                        updated += 1
                continue
            if role == "citation" and _row_has_source_ref(row):
                context = _source_context_for_row(row)
                if claims_seen_since_boundary or _row_has_citation_token(row):
                    active_context = {}
                    continue
                active_context = context
                continue
            active_context = {}
    return updated


def _is_major_structural_boundary(row: Dict[str, Any]) -> bool:
    role = _normalize_text(row.get("argument_role")).lower()
    if role == "metadata":
        return True
    text = _collapse_ws(row.get("claim_text"))
    return bool(_REFERENCE_SECTION_RE.match(text) or _MAJOR_SECTION_HEADING_RE.match(text))


def _section_should_update_active_topic(row: Dict[str, Any]) -> bool:
    text = _collapse_ws(row.get("claim_text"))
    if not text:
        return False
    if _REFERENCE_SECTION_RE.match(text):
        return False
    if _extract_source_attribution_signature(text):
        return False
    if _SECTION_FACT_FRAGMENT_RE.match(text):
        return False
    if _MAJOR_SECTION_HEADING_RE.match(text):
        return True
    if len(text) <= 80 and text.endswith((":","：")):
        return True
    if len(text) <= 40 and not re.search(r"[。.!?！？]$", text):
        return True
    return False


def _backfill_source_context_from_structural_attributions(rows: List[Dict[str, Any]]) -> int:
    groups = _group_rows_by_source(rows)
    updated = 0
    for indexes in groups.values():
        active_context: Dict[str, Any] = {}
        for index in indexes:
            row = rows[index]
            role = _normalize_text(row.get("argument_role")).lower()
            if role in _STRUCTURAL_ROLES:
                if _row_has_source_ref(row) and _extract_source_attribution_signature(row.get("claim_text")):
                    active_context = _source_context_for_row(row)
                    continue
                if active_context and not _is_major_structural_boundary(row):
                    if not _row_has_source_ref(row) and _merge_source_context(row, active_context):
                        updated += 1
                    continue
                active_context = {}
                continue
            if _row_is_claim_like(row):
                if active_context and not _row_has_source_ref(row):
                    if _merge_source_context(row, active_context):
                        updated += 1
                continue
            if role in {"citation", "evidence"} and _row_has_source_ref(row):
                active_context = {}
                continue
    return updated


def _source_row_can_backfill_previous_claim(row: Dict[str, Any]) -> bool:
    if not _row_has_source_ref(row):
        return False
    if _normalize_text(row.get("speaker")):
        return False
    role = _normalize_text(row.get("argument_role")).lower()
    if role in {"citation", "evidence"}:
        return True
    text = _collapse_ws(row.get("claim_text"))
    if _extract_source_signature(text):
        return True
    source_url = _normalize_text(row.get("source_url"))
    source_title = _normalize_text(row.get("source_title"))
    citation_text = _normalize_text(row.get("citation_text"))
    if text and text in {source_url, source_title, citation_text}:
        return True
    return bool(source_url and len(text) <= 180 and re.match(r"^\s*(?:[-\u2014\u2e3a]{1,3}|\d{4}\b)", text))


def _backfill_source_context_across_adjacent_page_breaks(rows: List[Dict[str, Any]]) -> int:
    updated = 0
    for index in range(1, len(rows)):
        previous = rows[index - 1]
        row = rows[index]
        if not _row_is_claim_like(previous) or _row_has_source_ref(previous):
            continue
        if not _source_row_can_backfill_previous_claim(row):
            continue
        if not _same_document_sheet(previous, row) or not _adjacent_page_break(previous, row):
            continue
        context = _source_context_for_row(row)
        if context and _merge_source_context(previous, context):
            updated += 1
    return updated


def _propagate_debate_context(rows: List[Dict[str, Any]]) -> Tuple[int, int, int, int, int]:
    base_topic_by_doc: Dict[Tuple[str, str], str] = {}
    active_section_topic_by_doc: Dict[Tuple[str, str], str] = {}
    active_argument_role_by_doc: Dict[Tuple[str, str], str] = {}
    base_stance_by_doc: Dict[Tuple[str, str], str] = {}
    base_speaker_role_by_doc: Dict[Tuple[str, str], str] = {}
    section_updated = 0
    metadata_updated = 0
    stance_updated = 0
    speaker_role_updated = 0
    argument_role_updated = 0
    for row in rows:
        key = _document_group_key(row)
        role = _normalize_text(row.get("argument_role")).lower()
        if role == "metadata":
            metadata_topic = _metadata_topic_from_row(row)
            if metadata_topic:
                base_topic_by_doc[key] = metadata_topic
            metadata_stance = _normalize_text(row.get("stance")).lower()
            if metadata_stance in {"pro", "con", "neutral"}:
                base_stance_by_doc[key] = metadata_stance
            metadata_speaker_role = _speaker_role_from_heading_row(row)
            if metadata_speaker_role:
                base_speaker_role_by_doc[key] = metadata_speaker_role
            metadata_argument_role = _argument_role_from_heading_row(row)
            if metadata_argument_role:
                active_argument_role_by_doc[key] = metadata_argument_role
            active_section_topic_by_doc[key] = ""
            continue
        if role == "section":
            section_text = row.get("claim_text") or row.get("debate_topic")
            if _REFERENCE_SECTION_RE.match(_collapse_ws(section_text)):
                active_section_topic_by_doc[key] = ""
            elif _section_should_update_active_topic(row):
                active_section_topic_by_doc[key] = _section_topic_from_text(section_text)
            section_speaker_role = _speaker_role_from_heading_row(row)
            if section_speaker_role:
                base_speaker_role_by_doc[key] = section_speaker_role
            active_argument_role_by_doc[key] = _argument_role_from_heading_row(row)
            continue
        section_topic = active_section_topic_by_doc.get(key, "")
        metadata_topic = base_topic_by_doc.get(key, "")
        topic = section_topic or metadata_topic
        if not _row_is_claim_like(row):
            continue
        if topic and _normalize_text(row.get("debate_topic")) != topic:
            row["debate_topic"] = topic
            if section_topic:
                section_updated += 1
            else:
                metadata_updated += 1
        metadata_stance = base_stance_by_doc.get(key, "")
        current_stance = _normalize_text(row.get("stance")).lower()
        if metadata_stance and current_stance in {"", "unknown"}:
            row["stance"] = metadata_stance
            stance_updated += 1
        metadata_speaker_role = base_speaker_role_by_doc.get(key, "")
        if metadata_speaker_role and not _normalize_text(row.get("speaker")) and not _normalize_text(row.get("speaker_role")):
            row["speaker_role"] = metadata_speaker_role
            speaker_role_updated += 1
        active_argument_role = active_argument_role_by_doc.get(key, "")
        current_argument_role = _normalize_text(row.get("argument_role")).lower()
        if active_argument_role and current_argument_role in {"", "claim"}:
            row["argument_role"] = active_argument_role
            argument_role_updated += 1
    return section_updated, metadata_updated, stance_updated, speaker_role_updated, argument_role_updated


def _structural_row_dedupe_key(row: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, str]:
    role = _normalize_text(row.get("argument_role")).lower()
    if role not in {"citation", "metadata", "section"}:
        return ("", "", "", "", "", "", "")
    semantic_text = (
        _normalize_claim_key(row.get("citation_text"))
        or _normalize_claim_key(row.get("claim_text"))
        or _normalize_claim_key(row.get("source_title"))
        or _normalize_claim_key(row.get("source_url"))
    )
    if not semantic_text:
        return ("", "", "", "", "", "", "")
    return (
        role,
        _normalize_text(row.get("source_path")),
        _normalize_text(row.get("sheet_name")),
        _normalize_text(row.get("page")),
        _normalize_text(row.get("source_url")).lower(),
        _normalize_claim_key(row.get("source_title")),
        semantic_text,
    )


def _deduplicate_structural_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    seen: set[Tuple[str, str, str, str, str, str, str]] = set()
    out: List[Dict[str, Any]] = []
    removed = 0
    for row in rows:
        key = _structural_row_dedupe_key(row)
        if key[0] and key in seen:
            removed += 1
            continue
        if key[0]:
            seen.add(key)
        out.append(row)
    return out, removed


def _source_location_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _normalize_text(row.get("source_path")),
        _normalize_text(row.get("page")),
        _normalize_text(row.get("sheet_name")),
    )


def _is_url_only_citation_row(row: Dict[str, Any]) -> bool:
    if _normalize_text(row.get("argument_role")).lower() != "citation":
        return False
    source_url = _normalize_text(row.get("source_url"))
    if not source_url:
        return False
    url_values = {source_url, source_url.rstrip("/")}
    claim_text = _normalize_text(row.get("claim_text"))
    citation_text = _normalize_text(row.get("citation_text"))
    if _normalize_text(row.get("source_title")) and claim_text and claim_text not in url_values:
        return False
    return claim_text in url_values or citation_text in url_values


def _collapse_adjacent_citation_url_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    removed = 0
    for row in rows:
        if (
            _is_url_only_citation_row(row)
            and out
            and _normalize_text(out[-1].get("argument_role")).lower() == "citation"
            and _normalize_text(out[-1].get("source_title"))
            and _source_location_key(out[-1]) == _source_location_key(row)
        ):
            previous = out[-1]
            for field in ("source_url", "source_domain", "published_at"):
                if not _normalize_text(previous.get(field)) and _normalize_text(row.get(field)):
                    previous[field] = row.get(field)
            citation_text = _normalize_text(previous.get("citation_text"))
            row_citation = _normalize_text(row.get("citation_text") or row.get("claim_text"))
            if row_citation and row_citation not in citation_text:
                previous["citation_text"] = f"{citation_text} | {row_citation}" if citation_text else row_citation
            removed += 1
            continue
        out.append(row)
    return out, removed


def _grobid_endpoint() -> str:
    return (
        str(os.getenv("AIWF_GROBID_URL") or os.getenv("GROBID_ENDPOINT") or os.getenv("GROBID_URL") or "")
        .strip()
        .rstrip("/")
    )


def _azure_endpoint() -> str:
    return str(
        os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
        or os.getenv("AZURE_DOCINTELLIGENCE_ENDPOINT")
        or ""
    ).strip().rstrip("/")


def _azure_key() -> str:
    return str(
        os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")
        or os.getenv("AZURE_DOCINTELLIGENCE_KEY")
        or ""
    ).strip()


def _azure_available() -> bool:
    installed, _ = _module_status("azure.ai.documentintelligence")
    return installed and bool(_azure_endpoint()) and bool(_azure_key())


def _grobid_available() -> bool:
    installed, _ = _module_status("grobid_client")
    return installed and bool(_grobid_endpoint())


def _has_citation_dense_signals(rows: List[Dict[str, Any]]) -> bool:
    candidate_rows = sum(
        1
        for row in rows
        if _looks_citation_entry_text(row.get("claim_text"))
        or _normalize_text(row.get("citation_text"))
        or (
            _normalize_text(row.get("argument_role")).lower() == "quote"
            and _normalize_text(row.get("claim_text")).startswith("[")
        )
    )
    return candidate_rows >= 2


def _effective_document_backend(spec: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    requested = _requested_document_backend(spec)
    if requested == "local":
        return "local"
    if requested == "azure_docintelligence":
        return "azure_docintelligence"
    mode = _external_enrichment_mode(spec)
    if mode in {"public", "auto"} and _azure_available():
        if any(Path(_normalize_text(row.get("source_path"))).suffix.lower() in _AZURE_FILE_SUFFIXES for row in rows):
            return "azure_docintelligence"
    return "local"


def _effective_citation_backend(spec: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    requested = _requested_citation_backend(spec)
    if requested == "regex":
        return "regex"
    if requested == "grobid":
        return "grobid"
    mode = _external_enrichment_mode(spec)
    if mode in {"private", "auto"} and _grobid_available() and _has_citation_dense_signals(rows):
        return "grobid"
    return "regex"


def _ftfy_fix_text():
    installed, module = _module_status("ftfy")
    if not installed or module is None:
        return None
    return getattr(module, "fix_text", None)


def _repair_cjk_radicals(value: str) -> str:
    repaired = []
    changed = False
    for char in value:
        codepoint = ord(char)
        if 0x2E80 <= codepoint <= 0x2EFF or 0x2F00 <= codepoint <= 0x2FDF:
            normalized = unicodedata.normalize("NFKC", char)
            if normalized != char:
                repaired.append(normalized)
                changed = True
                continue
        translated = char.translate(_CJK_RADICAL_SUPPLEMENT_MAP)
        if translated != char:
            changed = True
        repaired.append(translated)
    return "".join(repaired) if changed else value


def _repair_text_value(value: str, fix_text: Any) -> str:
    fixed = value
    if fix_text is not None:
        try:
            fixed = fix_text(value, uncurl_quotes=False, fix_line_breaks=False)
        except TypeError:
            fixed = fix_text(value)
    fixed = _repair_cjk_radicals(str(fixed or ""))
    return _CONTROL_CHAR_RE.sub(" ", fixed)


def normalize_rows_with_ftfy(
    rows: List[Dict[str, Any]],
    spec: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    fix_text = _ftfy_fix_text()
    trace: Dict[str, Any] = {
        "engine": "ftfy",
        "requested": True,
        "ok": True,
        "ftfy_available": bool(fix_text),
        "builtin_unicode_repair": True,
        "repaired_rows": 0,
        "repaired_cells": 0,
    }
    if fix_text is None:
        trace["warning"] = "ftfy unavailable; using builtin unicode repair"

    repaired_rows = 0
    repaired_cells = 0
    out: List[Dict[str, Any]] = []
    for row in rows:
        next_row = dict(row)
        row_changed = False
        for key, value in row.items():
            if not isinstance(value, str) or not value:
                continue
            fixed = _repair_text_value(value, fix_text)
            if fixed != value:
                next_row[key] = fixed
                row_changed = True
                repaired_cells += 1
        if row_changed:
            repaired_rows += 1
        out.append(next_row)
    trace["repaired_rows"] = repaired_rows
    trace["repaired_cells"] = repaired_cells
    return out, {
        "encoding_rows_repaired": repaired_rows,
        "encoding_cells_repaired": repaired_cells,
        "engine_trace": [trace],
    }


def _fetch_url_metadata_with_trafilatura(url: str, timeout_seconds: float = 8.0) -> Dict[str, str]:
    installed, module = _module_status("trafilatura")
    if not installed or module is None:
        return {"ok": False, "error": "trafilatura unavailable"}
    fetch_url = getattr(module, "fetch_url", None)
    bare_extraction = getattr(module, "bare_extraction", None)
    if fetch_url is None or bare_extraction is None:
        return {"ok": False, "error": "trafilatura metadata API unavailable"}
    try:
        downloaded = fetch_url(url, timeout=timeout_seconds)
        if not downloaded:
            return {"ok": False, "error": "trafilatura fetch returned empty response"}
        extracted = bare_extraction(downloaded, url=url, with_metadata=True)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    if not isinstance(extracted, dict):
        return {"ok": False, "error": "trafilatura extraction returned non-object"}
    return {
        "ok": True,
        "title": _collapse_ws(extracted.get("title")),
        "published_at": _parse_partial_date(extracted.get("date")),
        "source_domain": _collapse_ws(extracted.get("sitename")) or _source_domain(url),
    }


def _parse_tei_citations(tei_xml: str) -> List[str]:
    if not tei_xml:
        return []
    out: List[str] = []
    seen: set[str] = set()
    texts: List[str] = []
    try:
        root = ElementTree.fromstring(tei_xml)
    except Exception:
        root = None
    if root is not None:
        for tag in (".//{*}note", ".//{*}biblStruct", ".//{*}bibl", ".//{*}listBibl"):
            for element in root.findall(tag):
                text = _collapse_ws(" ".join(part.strip() for part in element.itertext() if part and part.strip()))
                if text:
                    texts.append(text)
    if not texts:
        for match in re.finditer(r"<(?:note|biblStruct|bibl|listBibl)[^>]*>(.*?)</(?:note|biblStruct|bibl|listBibl)>", tei_xml, flags=re.I | re.S):
            text = _collapse_ws(re.sub(r"<[^>]+>", " ", match.group(1)))
            if text:
                texts.append(text)
    for text in texts:
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _fetch_grobid_citations(source_path: str, timeout_seconds: float = 15.0) -> Dict[str, Any]:
    endpoint = _grobid_endpoint()
    if not endpoint:
        return {"ok": False, "error": "grobid endpoint not configured"}
    source_file = Path(source_path)
    if not source_file.is_file():
        return {"ok": False, "error": "source file missing"}
    try:
        with source_file.open("rb") as handle:
            response = requests.post(
                f"{endpoint}/api/processFulltextDocument",
                files={"input": (source_file.name, handle)},
                data={
                    "includeRawCitations": "1",
                    "consolidateCitations": "0",
                    "consolidateHeader": "0",
                },
                timeout=timeout_seconds,
            )
        response.raise_for_status()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    citations = _parse_tei_citations(response.text)
    return {"ok": True, "citations": citations}


def _poll_azure_analyze(operation_location: str, headers: Dict[str, str], timeout_seconds: float = 20.0) -> Dict[str, Any]:
    try:
        response = requests.get(operation_location, headers=headers, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    status = str(payload.get("status") or "").strip().lower()
    if status != "succeeded":
        return {"ok": False, "error": f"azure analyze status={status or 'unknown'}"}
    return {"ok": True, "payload": payload}


def _fetch_azure_layout(source_path: str, timeout_seconds: float = 20.0) -> Dict[str, Any]:
    endpoint = _azure_endpoint()
    key = _azure_key()
    if not endpoint or not key:
        return {"ok": False, "error": "azure document intelligence credentials not configured"}
    source_file = Path(source_path)
    if not source_file.is_file():
        return {"ok": False, "error": "source file missing"}
    suffix = source_file.suffix.lower()
    if suffix not in _AZURE_FILE_SUFFIXES:
        return {"ok": False, "error": "unsupported file type for azure layout"}
    content_type = "application/pdf" if suffix == ".pdf" else "application/octet-stream"
    url = f"{endpoint}/documentintelligence/documentModels/prebuilt-layout:analyze?api-version=2024-11-30"
    headers = {"Ocp-Apim-Subscription-Key": key, "Content-Type": content_type}
    try:
        response = requests.post(url, headers=headers, data=source_file.read_bytes(), timeout=timeout_seconds)
        response.raise_for_status()
        operation_location = str(response.headers.get("operation-location") or "")
        if not operation_location:
            return {"ok": False, "error": "azure analyze missing operation-location"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    return _poll_azure_analyze(operation_location, {"Ocp-Apim-Subscription-Key": key}, timeout_seconds=timeout_seconds)


def _apply_local_source_and_citation_rules(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    source_title_enriched_rows = 0
    citation_candidate_rows = 0
    citation_success_rows = 0
    source_url_normalized_rows = 0
    citation_text_url_normalized_rows = 0
    source_marker_claim_replaced_rows = 0
    for row in rows:
        source_url_already_normalized = bool(row.pop("_source_url_normalized", False))
        citation_text_already_normalized = bool(row.pop("_citation_text_url_normalized", False))
        if source_url_already_normalized:
            source_url_normalized_rows += 1
        if citation_text_already_normalized:
            citation_text_url_normalized_rows += 1
        role = _normalize_text(row.get("argument_role")).lower()
        claim_text = _collapse_ws(row.get("claim_text"))
        if not claim_text:
            continue
        if role in _STRUCTURAL_ROLES:
            attribution = _extract_source_attribution_signature(claim_text)
            if attribution:
                if attribution.get("source_title") and not _normalize_text(row.get("source_title")):
                    row["source_title"] = attribution["source_title"]
                    source_title_enriched_rows += 1
                if attribution.get("published_at") and not _normalize_text(row.get("published_at")):
                    row["published_at"] = attribution["published_at"]
                if attribution.get("citation_text") and not _normalize_text(row.get("citation_text")):
                    row["citation_text"] = attribution["citation_text"]
                if not _normalize_text(row.get("speaker_role")):
                    row["speaker_role"] = "source"
            continue
        source_url = _normalize_text(row.get("source_url"))
        if source_url:
            normalized_source_url = _normalize_source_url(source_url)
            if normalized_source_url != source_url:
                row["source_url"] = normalized_source_url
                source_url = normalized_source_url
                if not source_url_already_normalized:
                    source_url_normalized_rows += 1
        source_title = _normalize_text(row.get("source_title"))
        citation_text = _normalize_text(row.get("citation_text"))
        if citation_text:
            normalized_citation_text = _normalize_urls_in_text(citation_text)
            if normalized_citation_text != citation_text:
                row["citation_text"] = normalized_citation_text
                citation_text = normalized_citation_text
                if not citation_text_already_normalized:
                    citation_text_url_normalized_rows += 1
        speaker = _normalize_text(row.get("speaker"))
        if source_url and not _normalize_text(row.get("source_domain")):
            row["source_domain"] = _source_domain(source_url)
        signature = _extract_source_signature(claim_text)
        if (
            not signature
            and not speaker
            and role in {"citation", "evidence"}
            and source_url
            and (citation_text or source_title)
            and claim_text != source_url
        ):
            source_signature_text = citation_text or source_title or claim_text
            if _looks_bare_citation_token(source_signature_text):
                source_signature_text = claim_text
            signature = {
                "source_title": _clean_source_title_text(source_signature_text),
                "published_at": _parse_partial_date(source_signature_text),
                "citation_text": citation_text or claim_text,
            }
        if signature:
            existing_source_title = _normalize_text(row.get("source_title"))
            should_replace_source_title = (
                not existing_source_title
                or existing_source_title == claim_text
                or _looks_bare_citation_token(existing_source_title)
                or bool(_extract_urls(existing_source_title))
                or _source_title_is_url_fallback(existing_source_title, source_url)
            )
            if signature.get("source_title") and should_replace_source_title:
                row["source_title"] = signature["source_title"]
                source_title_enriched_rows += 1
            if signature.get("published_at") and not _normalize_text(row.get("published_at")):
                row["published_at"] = signature["published_at"]
            if signature.get("citation_text") and not _normalize_text(row.get("citation_text")):
                row["citation_text"] = signature["citation_text"]
            if not _normalize_text(row.get("speaker_role")):
                row["speaker_role"] = "source"
        attribution = _extract_source_attribution_signature(claim_text)
        if attribution and not _normalize_text(row.get("source_title")):
            row["source_title"] = attribution["source_title"]
            source_title_enriched_rows += 1
            if attribution.get("published_at") and not _normalize_text(row.get("published_at")):
                row["published_at"] = attribution["published_at"]
        source_title = _normalize_text(row.get("source_title"))
        source_title_from_url_fallback = False
        if source_url and not source_title:
            url_title = _source_title_from_url(source_url)
            if url_title:
                row["source_title"] = url_title
                source_title = url_title
                source_title_from_url_fallback = True
                source_title_enriched_rows += 1
        marker_source_title = ""
        if (
            source_url
            and not speaker
            and role in {"claim", "evidence", "citation"}
        ):
            marker_source_title = _source_title_from_list_marker_claim(claim_text)
            if marker_source_title:
                marker_claim_text = claim_text
                if marker_source_title != source_title:
                    row["source_title"] = marker_source_title
                    source_title = marker_source_title
                    if not source_title_from_url_fallback:
                        source_title_enriched_rows += 1
                row["claim_text"] = marker_source_title
                claim_text = marker_source_title
                row["argument_role"] = "citation"
                role = "citation"
                if not _normalize_text(row.get("citation_text")):
                    row["citation_text"] = _collapse_ws(f"{marker_source_title} {source_url}".strip())
                    citation_text = _normalize_text(row.get("citation_text"))
                if not _normalize_text(row.get("speaker_role")):
                    row["speaker_role"] = "source"
                topic_key = _normalize_claim_key(row.get("debate_topic"))
                if (
                    _looks_source_list_marker_only(row.get("debate_topic"))
                    or topic_key in {_normalize_claim_key(marker_source_title), _normalize_claim_key(marker_claim_text)}
                ):
                    row["debate_topic"] = ""
                if _normalize_text(row.get("language")).lower() == "unknown" and re.search(r"[A-Za-z]", marker_source_title):
                    row["language"] = "en"
                source_marker_claim_replaced_rows += 1
        if (
            not marker_source_title
            and
            _looks_source_list_marker_only(claim_text)
            and not speaker
            and role in {"claim", "evidence", "citation"}
            and (source_title or source_url)
        ):
            replacement_claim = source_title or source_url
            row["claim_text"] = replacement_claim
            claim_text = replacement_claim
            row["argument_role"] = "citation"
            role = "citation"
            if not _normalize_text(row.get("citation_text")):
                row["citation_text"] = source_url or source_title
                citation_text = _normalize_text(row.get("citation_text"))
            if not _normalize_text(row.get("speaker_role")):
                row["speaker_role"] = "source"
            if _looks_source_list_marker_only(row.get("debate_topic")):
                row["debate_topic"] = ""
            if _normalize_text(row.get("language")).lower() == "unknown" and re.search(r"[A-Za-z]", replacement_claim):
                row["language"] = "en"
            source_marker_claim_replaced_rows += 1
        citation_candidate = (
            _looks_citation_entry_text(claim_text)
            or bool(signature)
            or (
                not speaker
                and role in {"claim", "evidence", "citation"}
                and (
                    bool(citation_text)
                    or claim_text == source_url
                    or claim_text == source_title
                )
            )
        )
        if citation_candidate:
            citation_candidate_rows += 1
        if citation_candidate and role not in _STRUCTURAL_ROLES and role != "quote":
            row["argument_role"] = "citation"
            if not _normalize_text(row.get("citation_text")):
                row["citation_text"] = claim_text
            if not _normalize_text(row.get("speaker_role")):
                row["speaker_role"] = "source"
            if (
                _normalize_text(row.get("citation_text"))
                or _normalize_text(row.get("source_title"))
                or _normalize_text(row.get("source_url"))
            ):
                citation_success_rows += 1
        elif (source_url or _normalize_text(row.get("source_title"))) and not _normalize_text(row.get("speaker")) and role == "claim":
            row["argument_role"] = "evidence"
    return {
        "source_title_enriched_rows": source_title_enriched_rows,
        "citation_candidate_rows": citation_candidate_rows,
        "citation_success_rows": citation_success_rows,
        "source_url_normalized_rows": source_url_normalized_rows,
        "citation_text_url_normalized_rows": citation_text_url_normalized_rows,
        "source_marker_claim_replaced_rows": source_marker_claim_replaced_rows,
    }


def _apply_grobid_citation_backend(rows: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    success_rows = 0
    traces: List[Dict[str, Any]] = []
    grouped_sources: Dict[str, List[int]] = {}
    for index, row in enumerate(rows):
        source_path = _normalize_text(row.get("source_path"))
        if not source_path or Path(source_path).suffix.lower() not in _GROBID_FILE_SUFFIXES:
            continue
        grouped_sources.setdefault(source_path, []).append(index)
    for source_path, indexes in grouped_sources.items():
        trace: Dict[str, Any] = {
            "engine": "grobid",
            "source_path": source_path,
            "requested": True,
            "ok": False,
            "fallback": "regex",
        }
        result = _fetch_grobid_citations(source_path)
        if not bool(result.get("ok")):
            trace["warning"] = str(result.get("error") or "grobid request failed")
            traces.append(trace)
            continue
        citations = [str(item) for item in (result.get("citations") or []) if _collapse_ws(item)]
        if not citations:
            trace["warning"] = "grobid returned no citations"
            traces.append(trace)
            continue
        matched = 0
        appended = 0
        normalized_citations = [(_normalize_claim_key(text), text) for text in citations if _normalize_claim_key(text)]
        existing_keys = {
            key
            for row_index in indexes
            for key in (
                _normalize_claim_key(rows[row_index].get("claim_text")),
                _normalize_claim_key(rows[row_index].get("citation_text")),
            )
            if key
        }
        matched_keys: set[str] = set()
        for index in indexes:
            row = rows[index]
            claim_key = _normalize_claim_key(row.get("claim_text"))
            if not claim_key:
                continue
            matched_citation = next(
                (
                    text
                    for normalized, text in normalized_citations
                    if claim_key and (claim_key in normalized or normalized in claim_key)
                ),
                "",
            )
            if not matched_citation:
                continue
            matched_key = _normalize_claim_key(matched_citation)
            if matched_key:
                matched_keys.add(matched_key)
            row["argument_role"] = "citation"
            row["citation_text"] = matched_citation
            signature = _extract_source_signature(matched_citation)
            if signature.get("source_title") and not _normalize_text(row.get("source_title")):
                row["source_title"] = signature["source_title"]
            if signature.get("published_at") and not _normalize_text(row.get("published_at")):
                row["published_at"] = signature["published_at"]
            if not _normalize_text(row.get("speaker_role")):
                row["speaker_role"] = "source"
            matched += 1
        seed_row = rows[indexes[0]] if indexes else {}
        for normalized, citation_text in normalized_citations:
            if not normalized or normalized in matched_keys or normalized in existing_keys:
                continue
            rows.append(_canonical_citation_row_from_grobid(source_path, citation_text, seed_row))
            existing_keys.add(normalized)
            appended += 1
        trace["ok"] = True
        trace["candidate_rows"] = len(normalized_citations)
        trace["matched_rows"] = matched
        trace["appended_rows"] = appended
        traces.append(trace)
        success_rows += matched + appended
    return success_rows, traces


def _apply_azure_document_backend(rows: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    success_rows = 0
    traces: List[Dict[str, Any]] = []
    grouped_sources: Dict[str, List[int]] = {}
    for index, row in enumerate(rows):
        source_path = _normalize_text(row.get("source_path"))
        if not source_path or Path(source_path).suffix.lower() not in _AZURE_FILE_SUFFIXES:
            continue
        grouped_sources.setdefault(source_path, []).append(index)
    for source_path, indexes in grouped_sources.items():
        trace: Dict[str, Any] = {
            "engine": "azure_docintelligence",
            "source_path": source_path,
            "requested": True,
            "ok": False,
            "fallback": "local",
        }
        result = _fetch_azure_layout(source_path)
        if not bool(result.get("ok")):
            trace["warning"] = str(result.get("error") or "azure layout request failed")
            traces.append(trace)
            continue
        payload = result.get("payload") if isinstance(result.get("payload"), dict) else {}
        analyze_result = payload.get("analyzeResult") if isinstance(payload.get("analyzeResult"), dict) else {}
        paragraphs = analyze_result.get("paragraphs") if isinstance(analyze_result.get("paragraphs"), list) else []
        if not paragraphs:
            trace["warning"] = "azure returned no paragraphs"
            traces.append(trace)
            continue
        paragraph_map: Dict[str, str] = {}
        paragraph_items: List[Tuple[str, str, str]] = []
        for paragraph in paragraphs:
            if not isinstance(paragraph, dict):
                continue
            content = _collapse_ws(paragraph.get("content"))
            role = _normalize_text(paragraph.get("role")).lower()
            key = _normalize_claim_key(content)
            if content and key:
                paragraph_map[key] = role
                if _azure_paragraph_argument_role(role):
                    paragraph_items.append((key, content, role))
        matched = 0
        appended = 0
        matched_keys: set[str] = set()
        existing_keys = {
            key
            for index in indexes
            for key in (
                _normalize_claim_key(rows[index].get("claim_text")),
                _normalize_claim_key(rows[index].get("citation_text")),
            )
            if key
        }
        for index in indexes:
            row = rows[index]
            claim_key = _normalize_claim_key(row.get("claim_text"))
            if not claim_key or claim_key not in paragraph_map:
                continue
            paragraph_role = paragraph_map[claim_key]
            argument_role = _azure_paragraph_argument_role(paragraph_role)
            if argument_role == "metadata":
                row["argument_role"] = "metadata"
                row["speaker_role"] = "metadata"
                matched += 1
                matched_keys.add(claim_key)
            elif argument_role == "citation":
                row["argument_role"] = "citation"
                if not _normalize_text(row.get("citation_text")):
                    row["citation_text"] = _normalize_text(row.get("claim_text"))
                if not _normalize_text(row.get("speaker_role")):
                    row["speaker_role"] = "source"
                matched += 1
                matched_keys.add(claim_key)
            elif argument_role == "section":
                row["argument_role"] = "section"
                if not _normalize_text(row.get("speaker_role")):
                    row["speaker_role"] = "metadata"
                matched += 1
                matched_keys.add(claim_key)
        seed_row = rows[indexes[0]] if indexes else {}
        for key, content, role in paragraph_items:
            if key in matched_keys or key in existing_keys:
                continue
            appended_row = _canonical_row_from_azure_paragraph(source_path, content, role, seed_row)
            if appended_row is None:
                continue
            rows.append(appended_row)
            existing_keys.add(key)
            appended += 1
        if matched + appended <= 0:
            trace["warning"] = "azure returned no matching layout roles"
            traces.append(trace)
            continue
        trace["ok"] = True
        trace["candidate_rows"] = len(paragraph_items)
        trace["matched_rows"] = matched
        trace["appended_rows"] = appended
        traces.append(trace)
        success_rows += matched + appended
    return success_rows, traces


def _apply_url_metadata_enrichment(rows: List[Dict[str, Any]], enabled: bool, external_mode: str) -> Dict[str, Any]:
    trace: Dict[str, Any] = {
        "engine": "trafilatura",
        "requested": enabled,
        "ok": False,
        "candidate_rows": 0,
        "enriched_rows": 0,
    }
    if not enabled:
        trace["warning"] = "url metadata enrichment disabled"
        return {"candidate_rows": 0, "enriched_rows": 0, "engine_trace": [trace]}
    if external_mode == "off":
        trace["warning"] = "external enrichment mode is off"
        return {"candidate_rows": 0, "enriched_rows": 0, "engine_trace": [trace]}

    url_groups: Dict[str, List[int]] = {}
    for index, row in enumerate(rows):
        url = _normalize_text(row.get("source_url"))
        if not url:
            continue
        if _normalize_text(row.get("source_title")) and _normalize_text(row.get("published_at")):
            continue
        url_groups.setdefault(url, []).append(index)
    trace["candidate_rows"] = sum(len(indexes) for indexes in url_groups.values())
    if not url_groups:
        trace["ok"] = True
        return {"candidate_rows": 0, "enriched_rows": 0, "engine_trace": [trace]}

    enriched_rows = 0
    installed, _ = _module_status("trafilatura")
    if not installed:
        trace["warning"] = "trafilatura unavailable"
        return {"candidate_rows": trace["candidate_rows"], "enriched_rows": 0, "engine_trace": [trace]}

    for url, indexes in url_groups.items():
        metadata = _fetch_url_metadata_with_trafilatura(url)
        if not bool(metadata.get("ok")):
            continue
        for index in indexes:
            row = rows[index]
            row_changed = False
            existing_title = _normalize_text(row.get("source_title"))
            if metadata.get("title") and (
                not existing_title or _source_title_is_url_fallback(existing_title, row.get("source_url"))
            ):
                row["source_title"] = metadata["title"]
                row_changed = True
            if metadata.get("published_at") and not _normalize_text(row.get("published_at")):
                row["published_at"] = metadata["published_at"]
                row_changed = True
            if metadata.get("source_domain") and not _normalize_text(row.get("source_domain")):
                row["source_domain"] = metadata["source_domain"]
                row_changed = True
            if row_changed:
                enriched_rows += 1
    trace["ok"] = True
    trace["enriched_rows"] = enriched_rows
    return {
        "candidate_rows": trace["candidate_rows"],
        "enriched_rows": enriched_rows,
        "engine_trace": [trace],
    }


def enrich_standardized_evidence_rows(
    rows: List[Dict[str, Any]],
    spec: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not rows or not _debate_mode(spec, rows):
        return rows, {
            "source_context_backfilled_rows": 0,
            "source_title_enriched_rows": 0,
            "citation_candidate_rows": 0,
            "citation_backend_success_rows": 0,
            "url_metadata_candidate_rows": 0,
            "url_metadata_enriched_rows": 0,
            "structural_duplicate_rows_removed": 0,
            "block_source_context_backfilled_rows": 0,
            "structural_source_context_backfilled_rows": 0,
            "adjacent_page_source_context_backfilled_rows": 0,
            "section_topic_rows_propagated": 0,
            "metadata_topic_rows_propagated": 0,
            "metadata_stance_rows_propagated": 0,
            "metadata_speaker_role_rows_propagated": 0,
            "heading_argument_role_rows_propagated": 0,
            "citation_token_source_backfilled_rows": 0,
            "leading_source_context_backfilled_rows": 0,
            "adjacent_citation_url_rows_collapsed": 0,
            "wrapped_claim_rows_merged": 0,
            "source_url_normalized_rows": 0,
            "citation_text_url_normalized_rows": 0,
            "source_marker_claim_replaced_rows": 0,
            "multi_source_citation_appended_rows": 0,
            "engine_trace": [],
        }

    out = [dict(row) for row in rows]
    external_mode = _external_enrichment_mode(spec)
    requested_doc_backend = _requested_document_backend(spec)
    effective_doc_backend = _effective_document_backend(spec, out)
    requested_citation_backend = _requested_citation_backend(spec)
    effective_citation_backend = _effective_citation_backend(spec, out)
    url_metadata_enabled = _url_metadata_enabled(spec, out)

    engine_trace: List[Dict[str, Any]] = [
        {
            "engine": "local_debate_enrichment",
            "requested_document_backend": requested_doc_backend,
            "effective_document_backend": effective_doc_backend,
            "requested_citation_backend": requested_citation_backend,
            "effective_citation_backend": effective_citation_backend,
            "external_enrichment_mode": external_mode,
            "url_metadata_enrichment": url_metadata_enabled,
            "ok": True,
        }
    ]

    document_success_rows = 0
    if effective_doc_backend == "azure_docintelligence":
        matched_rows, traces = _apply_azure_document_backend(out)
        document_success_rows += matched_rows
        engine_trace.extend(traces)
    else:
        engine_trace.append({"engine": "local_document_parse", "requested": requested_doc_backend != "auto", "ok": True})

    local_stats = _apply_local_source_and_citation_rules(out)
    engine_trace.append(
        {
            "engine": "regex_citation_parser",
            "requested": requested_citation_backend == "regex",
            "ok": True,
            "candidate_rows": local_stats["citation_candidate_rows"],
            "matched_rows": local_stats["citation_success_rows"],
        }
    )
    source_url_normalized_rows = int(local_stats["source_url_normalized_rows"])
    citation_text_url_normalized_rows = int(local_stats["citation_text_url_normalized_rows"])
    source_marker_claim_replaced_rows = int(local_stats["source_marker_claim_replaced_rows"])
    if source_url_normalized_rows or citation_text_url_normalized_rows:
        engine_trace.append(
            {
                "engine": "local_url_normalizer",
                "ok": True,
                "matched_rows": source_url_normalized_rows + citation_text_url_normalized_rows,
                "source_url_rows": source_url_normalized_rows,
                "citation_text_rows": citation_text_url_normalized_rows,
            }
        )
    if source_marker_claim_replaced_rows:
        engine_trace.append(
            {
                "engine": "local_source_marker_normalizer",
                "ok": True,
                "matched_rows": source_marker_claim_replaced_rows,
                "source_marker_rows": source_marker_claim_replaced_rows,
            }
        )

    citation_backend_success_rows = 0
    if effective_citation_backend == "grobid":
        matched_rows, traces = _apply_grobid_citation_backend(out)
        citation_backend_success_rows += matched_rows
        engine_trace.extend(traces)

    url_stats = _apply_url_metadata_enrichment(out, url_metadata_enabled, external_mode)
    engine_trace.extend(url_stats["engine_trace"])

    out, structural_duplicate_rows_removed = _deduplicate_structural_rows(out)
    out, wrapped_claim_rows_merged = _merge_wrapped_claim_rows(out)
    (
        section_topic_rows_propagated,
        metadata_topic_rows_propagated,
        metadata_stance_rows_propagated,
        metadata_speaker_role_rows_propagated,
        heading_argument_role_rows_propagated,
    ) = _propagate_debate_context(out)
    block_source_context_backfilled_rows = _backfill_source_context_by_block(out)
    source_context_backfilled_rows = block_source_context_backfilled_rows
    out, adjacent_citation_url_rows_collapsed = _collapse_adjacent_citation_url_rows(out)
    structural_source_context_backfilled_rows = _backfill_source_context_from_structural_attributions(out)
    source_context_backfilled_rows += structural_source_context_backfilled_rows
    adjacent_page_source_context_backfilled_rows = _backfill_source_context_across_adjacent_page_breaks(out)
    source_context_backfilled_rows += adjacent_page_source_context_backfilled_rows
    leading_source_context_backfilled_rows = _backfill_source_context_from_leading_source_cards(out)
    source_context_backfilled_rows += leading_source_context_backfilled_rows
    citation_token_source_backfilled_rows = _backfill_source_context_by_citation_token(out)
    source_context_backfilled_rows += citation_token_source_backfilled_rows
    multi_source_citation_appended_rows = _consume_internal_flag_count(out, "_multi_source_citation_appended")
    engine_trace.append(
        {
            "engine": "local_source_context_backfill",
            "ok": True,
            "matched_rows": source_context_backfilled_rows,
            "block_rows": block_source_context_backfilled_rows,
            "structural_source_rows": structural_source_context_backfilled_rows,
            "adjacent_page_rows": adjacent_page_source_context_backfilled_rows,
            "leading_source_rows": leading_source_context_backfilled_rows,
            "citation_token_rows": citation_token_source_backfilled_rows,
            "multi_source_citation_rows": multi_source_citation_appended_rows,
        }
    )
    citation_candidate_rows = max(
        int(local_stats["citation_candidate_rows"]),
        int(citation_backend_success_rows + document_success_rows),
    )
    citation_success_rows = min(
        citation_candidate_rows,
        max(
            int(local_stats["citation_success_rows"]),
            int(citation_backend_success_rows + document_success_rows),
        ),
    )

    return out, {
        "source_context_backfilled_rows": source_context_backfilled_rows,
        "block_source_context_backfilled_rows": block_source_context_backfilled_rows,
        "structural_source_context_backfilled_rows": structural_source_context_backfilled_rows,
        "adjacent_page_source_context_backfilled_rows": adjacent_page_source_context_backfilled_rows,
        "source_title_enriched_rows": int(local_stats["source_title_enriched_rows"]),
        "citation_candidate_rows": citation_candidate_rows,
        "citation_backend_success_rows": citation_success_rows,
        "url_metadata_candidate_rows": int(url_stats["candidate_rows"]),
        "url_metadata_enriched_rows": int(url_stats["enriched_rows"]),
        "source_url_normalized_rows": source_url_normalized_rows,
        "citation_text_url_normalized_rows": citation_text_url_normalized_rows,
        "source_marker_claim_replaced_rows": source_marker_claim_replaced_rows,
        "multi_source_citation_appended_rows": multi_source_citation_appended_rows,
        "structural_duplicate_rows_removed": structural_duplicate_rows_removed,
        "section_topic_rows_propagated": section_topic_rows_propagated,
        "metadata_topic_rows_propagated": metadata_topic_rows_propagated,
        "metadata_stance_rows_propagated": metadata_stance_rows_propagated,
        "metadata_speaker_role_rows_propagated": metadata_speaker_role_rows_propagated,
        "heading_argument_role_rows_propagated": heading_argument_role_rows_propagated,
        "citation_token_source_backfilled_rows": citation_token_source_backfilled_rows,
        "leading_source_context_backfilled_rows": leading_source_context_backfilled_rows,
        "adjacent_citation_url_rows_collapsed": adjacent_citation_url_rows_collapsed,
        "wrapped_claim_rows_merged": wrapped_claim_rows_merged,
        "engine_trace": engine_trace,
    }
