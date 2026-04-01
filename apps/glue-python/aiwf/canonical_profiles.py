from __future__ import annotations

import copy
from typing import Any


DEFAULT_HEADER_ALIASES: dict[str, list[str]] = {
    "id": ["id", "record_id", "row_id", "identifier", "编号", "序号", "单号", "缂栧彿", "搴忓彿", "鍗曞彿"],
    "amount": [
        "amount",
        "amt",
        "total_amount",
        "金额",
        "总金额",
        "发生额",
        "本期金额",
        "收款金额",
        "付款金额",
        "金额（万元）",
        "閲戦",
        "鎬婚噾棰",
        "鍙戠敓棰",
        "鏈湡閲戦",
        "鏀舵閲戦",
        "浠樻閲戦",
        "閲戦锛堜竾鍏冿級",
    ],
    "currency": ["currency", "ccy", "币种", "货币", "结算币种", "甯佺", "璐у竵", "缁撶畻甯佺"],
    "biz_date": ["biz_date", "date", "transaction_date", "业务日期", "发生日期", "交易日期", "记账日期", "入账日期", "涓氬姟鏃ユ湡", "鍙戠敓鏃ユ湡", "浜ゆ槗鏃ユ湡", "璁拌处鏃ユ湡", "鍏ヨ处鏃ユ湡"],
    "published_at": ["published_at", "publish_date", "published_date", "发布日期", "发布时间", "发布时点", "鍙戝竷鏃ユ湡", "鍙戝竷鏃堕棿", "鍙戝竷鏃剁偣"],
    "customer_name": ["customer_name", "name", "customer", "客户", "客户名称", "姓名", "瀹㈡埛", "瀹㈡埛鍚嶇О", "濮撳悕"],
    "phone": ["phone", "mobile", "tel", "telephone", "手机", "电话", "联系电话", "鎵嬫満", "鐢佃瘽", "鑱旂郴鐢佃瘽"],
    "claim_text": ["claim_text", "text", "content", "正文", "内容", "文本", "观点", "论点", "主张", "姝ｆ枃", "鍐呭", "鏂囨湰", "瑙傜偣", "璁虹偣", "涓诲紶"],
    "source_url": ["source_url", "url", "link", "链接", "网址", "来源链接", "来源网址", "原文链接", "閾炬帴", "缃戝潃", "鏉ユ簮閾炬帴", "鏉ユ簮缃戝潃", "鍘熸枃閾炬帴"],
    "source_title": ["source_title", "title", "标题", "来源标题", "文章标题", "文档标题", "鏍囬", "鏉ユ簮鏍囬", "鏂囩珷鏍囬", "鏂囨。鏍囬"],
    "speaker": ["speaker", "author", "name", "作者", "发言人", "说话人", "发布者", "浣滆€", "鍙戣█浜", "璇磋瘽浜", "鍙戝竷鑰"],
    "stance": ["stance", "立场", "态度", "绔嬪満", "鎬佸害"],
    "confidence": ["confidence", "置信度", "可信度", "缃俊搴", "鍙俊搴"],
}


PROFILE_REGISTRY: dict[str, dict[str, Any]] = {
    "finance_statement": {
        "required_fields": ["id", "amount"],
        "string_fields": ["currency"],
        "numeric_fields": ["id", "amount"],
        "date_fields": ["biz_date", "published_at"],
        "unique_keys": ["id"],
        "defaults": {"currency": "CNY"},
        "header_aliases": {
            "id": ["编号", "序号", "单号", "缂栧彿", "搴忓彿", "鍗曞彿"],
            "amount": ["金额", "总金额", "发生额", "本期金额", "收款金额", "付款金额", "金额（万元）", "閲戦", "鎬婚噾棰", "鍙戠敓棰", "鏈湡閲戦", "鏀舵閲戰", "浠樻閲戰"],
            "currency": ["币种", "货币", "结算币种", "甯佺", "璐у竵", "缁撶畻甯佺"],
            "biz_date": ["业务日期", "发生日期", "交易日期", "记账日期", "入账日期", "涓氬姟鏃ユ湡", "鍙戠敓鏃ユ湡", "浜ゆ槗鏃ユ湡", "璁拌处鏃ユ湡", "鍏ヨ处鏃ユ湡"],
            "published_at": ["发布日期", "发布时间", "鍙戝竷鏃ユ湡", "鍙戝竷鏃堕棿"],
        },
    },
    "customer_contact": {
        "required_fields": ["customer_name", "phone"],
        "string_fields": ["customer_name", "city", "phone"],
        "numeric_fields": [],
        "date_fields": [],
        "unique_keys": ["phone"],
        "defaults": {},
        "header_aliases": {
            "customer_name": ["客户", "客户名称", "姓名", "瀹㈡埛", "瀹㈡埛鍚嶇О", "濮撳悕"],
            "phone": ["手机", "电话", "联系电话", "鎵嬫滿", "鐢佃瘽", "鑱旂郴鐢佃瘽"],
        },
    },
    "debate_evidence": {
        "required_fields": ["claim_text"],
        "string_fields": ["claim_text", "speaker", "source_url", "source_title", "stance"],
        "numeric_fields": ["confidence"],
        "date_fields": ["published_at"],
        "unique_keys": ["source_path", "chunk_index", "claim_text"],
        "defaults": {},
        "header_aliases": {
            "claim_text": ["正文", "内容", "文本", "观点", "论点", "主张", "姝ｆ枃", "鍐呭", "鏂囨湰", "瑙傜偣", "璁虹偣", "涓诲紶"],
            "source_title": ["标题", "来源标题", "文章标题", "文档标题", "鏍囬", "鏉ユ簮鏍囬", "鏂囩珷鏍囬", "鏂囨。鏍囬"],
            "source_url": ["链接", "网址", "来源链接", "来源网址", "原文链接", "閾炬帴", "缃戝潃", "鏉ユ簮閾炬帴", "鏉ユ簮缃戝潃", "鍘熸枃閾炬帴"],
            "published_at": ["发布日期", "发布时间", "鍙戝竷鏃ユ湡", "鍙戝竷鏃堕棿"],
            "speaker": ["作者", "发言人", "说话人", "发布者", "浣滆€", "鍙戣█浜", "璇磋瘽浜", "鍙戝竷鑰"],
            "stance": ["立场", "态度", "绔嬪滿", "鎬佸害"],
        },
    },
}


def resolve_profile_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in PROFILE_REGISTRY:
        return text
    return ""


def get_profile_registry() -> dict[str, dict[str, Any]]:
    return copy.deepcopy(PROFILE_REGISTRY)


def get_profile_spec(profile_name: str) -> dict[str, Any]:
    normalized = resolve_profile_name(profile_name)
    if not normalized:
        return {}
    return copy.deepcopy(PROFILE_REGISTRY.get(normalized, {}))


def get_profile_header_aliases(profile_name: str) -> dict[str, list[str]]:
    profile = get_profile_spec(profile_name)
    aliases = profile.get("header_aliases")
    return copy.deepcopy(aliases) if isinstance(aliases, dict) else {}


def get_default_header_aliases() -> dict[str, list[str]]:
    return copy.deepcopy(DEFAULT_HEADER_ALIASES)
