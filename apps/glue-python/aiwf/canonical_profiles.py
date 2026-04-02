from __future__ import annotations

import copy
from typing import Any


DEFAULT_HEADER_ALIASES: dict[str, list[str]] = {
    "id": ["id", "record_id", "row_id", "identifier", "编号", "序号", "单号"],
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
        "金额(万元)",
    ],
    "currency": ["currency", "ccy", "币种", "货币", "结算币种"],
    "biz_date": [
        "biz_date",
        "date",
        "transaction_date",
        "业务日期",
        "发生日期",
        "交易日期",
        "记账日期",
        "入账日期",
    ],
    "published_at": [
        "published_at",
        "publish_date",
        "published_date",
        "pub dt",
        "pub_dt",
        "发布日期",
        "发布时间",
        "发表时间",
    ],
    "account_no": ["account_no", "account", "账号", "账户", "账户号", "卡号"],
    "txn_date": [
        "txn_date",
        "transaction_date",
        "posting_date",
        "交易日期",
        "记账日期",
        "入账日期",
        "日期",
    ],
    "debit_amount": ["debit_amount", "debit", "out_amount", "借方金额", "支出", "付款金额"],
    "credit_amount": ["credit_amount", "credit", "in_amount", "贷方金额", "收入", "收款金额"],
    "balance": ["balance", "余额", "账户余额"],
    "counterparty_name": ["counterparty_name", "counterparty", "cp", "对方户名", "对手方", "交易对手"],
    "remark": ["remark", "memo", "摘要", "附言", "备注", "用途"],
    "ref_no": ["ref_no", "reference_no", "ref no", "流水号", "交易流水号", "凭证号"],
    "txn_type": ["txn_type", "transaction_type", "交易类型", "业务类型", "方向"],
    "customer_name": [
        "customer_name",
        "customer",
        "name",
        "客户",
        "客户名称",
        "姓名",
        "联系人",
        "联系人姓名",
    ],
    "phone": [
        "phone",
        "mobile",
        "tel",
        "telephone",
        "手机号",
        "手机",
        "电话号码",
        "联系电话",
        "座机",
    ],
    "city": ["city", "city_name", "city name", "城市", "所在城市"],
    "claim_text": ["claim_text", "text", "content", "claim", "正文", "内容", "文本", "观点", "论点", "主张"],
    "source_url": ["source_url", "url", "link", "src url", "链接", "网址", "来源链接", "来源网址", "原文链接"],
    "source_title": ["source_title", "title", "src title", "标题", "来源标题", "文章标题", "文档标题"],
    "speaker": ["speaker", "author", "speaker name", "作者", "发言人", "说话人", "发布者"],
    "stance": ["stance", "立场", "态度"],
    "confidence": ["confidence", "可信度", "置信度"],
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
            "id": ["编号", "序号", "单号"],
            "amount": ["金额", "总金额", "发生额", "本期金额", "收款金额", "付款金额", "金额（万元）", "金额(万元)"],
            "currency": ["币种", "货币", "结算币种"],
            "biz_date": ["业务日期", "发生日期", "交易日期", "记账日期", "入账日期"],
            "published_at": ["发布日期", "发布时间", "发表时间"],
        },
    },
    "bank_statement": {
        "required_fields": ["account_no", "txn_date"],
        "string_fields": ["account_no", "currency", "counterparty_name", "remark", "ref_no", "txn_type"],
        "numeric_fields": ["debit_amount", "credit_amount", "amount", "balance"],
        "date_fields": ["txn_date"],
        "unique_keys": ["account_no", "txn_date", "ref_no", "amount"],
        "defaults": {"currency": "CNY"},
        "header_aliases": {
            "account_no": ["账号", "账户", "账户号", "卡号"],
            "txn_date": ["交易日期", "记账日期", "入账日期", "日期"],
            "debit_amount": ["借方金额", "支出", "付款金额"],
            "credit_amount": ["贷方金额", "收入", "收款金额"],
            "balance": ["余额", "账户余额"],
            "counterparty_name": ["对方户名", "对手方", "交易对手"],
            "remark": ["摘要", "附言", "备注", "用途"],
            "ref_no": ["流水号", "交易流水号", "凭证号"],
            "txn_type": ["交易类型", "业务类型", "方向"],
            "currency": ["币种", "货币", "结算币种"],
        },
    },
    "customer_contact": {
        "required_fields": ["customer_name", "phone"],
        "string_fields": ["customer_name", "phone", "city"],
        "numeric_fields": [],
        "date_fields": [],
        "unique_keys": ["phone"],
        "defaults": {},
        "header_aliases": {
            "customer_name": ["客户", "客户名称", "姓名", "联系人", "联系人姓名"],
            "phone": ["手机", "手机号", "电话号码", "联系电话", "座机"],
            "city": ["城市", "所在城市"],
        },
    },
    "customer_ledger": {
        "required_fields": ["customer_name", "phone", "amount", "biz_date"],
        "string_fields": ["customer_name", "phone", "city"],
        "numeric_fields": ["amount"],
        "date_fields": ["biz_date"],
        "unique_keys": ["phone", "biz_date", "amount"],
        "defaults": {},
        "header_aliases": {
            "customer_name": ["客户", "客户名称", "姓名", "联系人", "联系人姓名"],
            "phone": ["手机", "手机号", "电话号码", "联系电话", "座机"],
            "city": ["城市", "所在城市"],
            "amount": ["金额", "总金额", "发生额", "本期金额", "收款金额", "付款金额", "金额（万元）", "金额(万元)"],
            "biz_date": ["业务日期", "发生日期", "交易日期", "记账日期", "入账日期"],
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
            "claim_text": ["正文", "内容", "文本", "观点", "论点", "主张"],
            "source_title": ["标题", "来源标题", "文章标题", "文档标题"],
            "source_url": ["链接", "网址", "来源链接", "来源网址", "原文链接"],
            "published_at": ["发布日期", "发布时间", "发表时间"],
            "speaker": ["作者", "发言人", "说话人", "发布者"],
            "stance": ["立场", "态度"],
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
