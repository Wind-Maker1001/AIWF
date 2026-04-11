from __future__ import annotations

from typing import Any, Dict, List, Optional


class CleaningGuardrailError(RuntimeError):
    def __init__(
        self,
        *,
        error_code: str,
        message: str,
        reason_codes: Optional[List[str]] = None,
        requested_profile: str = "",
        recommended_profile: str = "",
        profile_confidence: float = 0.0,
        required_field_coverage: float = 0.0,
        template_id: str = "",
        template_expected_profile: str = "",
        blank_output_expected: Optional[bool] = None,
        zero_output_unexpected: Optional[bool] = None,
        blocking_reason_codes: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.error_code = str(error_code or "cleaning_guardrail_blocked")
        self.reason_codes = [str(item).strip() for item in (reason_codes or []) if str(item).strip()]
        self.requested_profile = str(requested_profile or "")
        self.recommended_profile = str(recommended_profile or "")
        self.profile_confidence = float(profile_confidence or 0.0)
        self.required_field_coverage = float(required_field_coverage or 0.0)
        self.template_id = str(template_id or "")
        self.template_expected_profile = str(template_expected_profile or "")
        self.blank_output_expected = blank_output_expected
        self.zero_output_unexpected = zero_output_unexpected
        self.blocking_reason_codes = [
            str(item).strip()
            for item in (blocking_reason_codes or self.reason_codes)
            if str(item).strip()
        ]
        self.details = dict(details or {})

    def to_response(self, *, job_id: str = "", flow: str = "cleaning") -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "ok": False,
            "error": str(self),
            "error_code": self.error_code,
            "reason_codes": list(self.reason_codes),
            "job_id": str(job_id or ""),
            "flow": str(flow or "cleaning"),
            "requested_profile": self.requested_profile,
            "recommended_profile": self.recommended_profile,
            "profile_confidence": self.profile_confidence,
            "required_field_coverage": self.required_field_coverage,
            "template_id": self.template_id,
            "template_expected_profile": self.template_expected_profile,
            "blocking_reason_codes": list(self.blocking_reason_codes),
        }
        if self.blank_output_expected is not None:
            payload["blank_output_expected"] = bool(self.blank_output_expected)
        if self.zero_output_unexpected is not None:
            payload["zero_output_unexpected"] = bool(self.zero_output_unexpected)
        if self.details:
            payload["details"] = dict(self.details)
        return payload


def guardrail_template_id(params: Dict[str, Any]) -> str:
    template = params.get("_resolved_cleaning_template") if isinstance(params.get("_resolved_cleaning_template"), dict) else {}
    return str(template.get("id") or params.get("cleaning_template") or "").strip().lower()


def guardrail_template_expected_profile(params: Dict[str, Any]) -> str:
    return str(params.get("template_expected_profile") or params.get("canonical_profile") or "").strip().lower()
