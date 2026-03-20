from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from aiwf.registry_domains import normalize_registry_domain, summarize_registry_domains
from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.runtime_state import get_runtime_state
from aiwf.registry_utils import infer_caller_module
from aiwf.ingest_file_readers import (
    load_docx_input as _load_docx_input_impl,
    load_image_input as _load_image_input_impl,
    load_pdf_input as _load_pdf_input_impl,
    load_txt_input as _load_txt_input_impl,
    load_xlsx_input as _load_xlsx_input_impl,
    read_docx as _read_docx_impl,
    read_image as _read_image_impl,
    read_pdf as _read_pdf_impl,
    read_text_with_fallback as _read_text_with_fallback_impl,
    read_txt as _read_txt_impl,
    read_xlsx as _read_xlsx_impl,
    split_text_to_rows as _split_text_to_rows_impl,
)
from aiwf.ingest_ocr import (
    resolve_tesseract_cmd as _resolve_tesseract_cmd,
    ocr_preprocess_mode as _ocr_preprocess_mode,
    ocr_try_modes as _ocr_try_modes,
    preprocess_image_for_ocr as _ingest_preprocess_image_for_ocr,
    ocr_text_score as _ingest_ocr_text_score,
)


InputReaderFn = Callable[[str, Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]]


@dataclass(frozen=True)
class InputReaderRegistration:
    input_format: str
    extensions: Tuple[str, ...]
    loader: InputReaderFn
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
    source_module: str


def _ext(path: str) -> str:
    return Path(path).suffix.lower()


def _normalize_extension(value: str) -> str:
    ext = str(value or "").strip().lower()
    if not ext:
        return ""
    if ext.startswith("."):
        return ext
    if "/" not in ext and "\\" not in ext and "." not in ext:
        return f".{ext}"
    return _ext(ext)


def _normalize_input_format(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("input format must be non-empty")
    return normalized


def _extension_conflicts(
    normalized_format: str,
    normalized_extensions: Tuple[str, ...],
) -> Dict[str, Tuple[InputReaderRegistration, set[str]]]:
    state = get_runtime_state()
    conflicts: Dict[str, Tuple[InputReaderRegistration, set[str]]] = {}
    for ext in normalized_extensions:
        current = state.input_readers_by_extension.get(ext)
        if current is None or current.input_format == normalized_format:
            continue
        registration, captured = conflicts.get(current.input_format, (current, set()))
        captured.add(ext)
        conflicts[current.input_format] = (registration, captured)
    return conflicts


def _ensure_builtin_input_readers() -> None:
    state = get_runtime_state()
    if state.builtins_inputs_registered or state.input_bootstrap_in_progress:
        return
    state.input_bootstrap_in_progress = True
    try:
        from aiwf.domains.ingest import register_builtin_input_domains

        register_builtin_input_domains(_register_input_reader)
        state.builtins_inputs_registered = True
    finally:
        state.input_bootstrap_in_progress = False


def _register_input_reader(
    input_format: str,
    extensions: Iterable[str],
    loader: InputReaderFn,
    *,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> InputReaderRegistration:
    state = get_runtime_state()
    normalized_format = _normalize_input_format(input_format)
    normalized_extensions = tuple(
        sorted({ext for ext in (_normalize_extension(value) for value in extensions) if ext})
    )
    if not normalized_extensions:
        raise ValueError("register_input_reader requires at least one extension")
    if not callable(loader):
        raise TypeError("input reader loader must be callable")
    normalized_domain, normalized_domain_metadata = normalize_registry_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
    existing = state.input_readers_by_format.get(normalized_format)
    if existing is not None:
        if policy == "error":
            record_registry_event(
                registry="input_reader",
                name=normalized_format,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"input reader {normalized_format} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="input_reader",
                name=normalized_format,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="input_reader",
            name=normalized_format,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
        unregister_input_reader(normalized_format)

    ext_conflicts = _extension_conflicts(normalized_format, normalized_extensions)
    if ext_conflicts:
        first_conflict = next(iter(ext_conflicts.values()))[0]
        if policy == "error":
            for conflicting_registration, captured_extensions in ext_conflicts.values():
                for ext in sorted(captured_extensions):
                    record_registry_event(
                        registry="input_reader_extension",
                        name=ext,
                        action="error",
                        policy=policy,
                        existing_source_module=conflicting_registration.source_module,
                        new_source_module=source,
                        detail=(
                            f"extension {ext} already registered to "
                            f"{conflicting_registration.input_format}"
                        ),
                    )
            raise RuntimeError(
                "input reader extensions already registered: "
                + ", ".join(
                    f"{ext}->{registration.input_format}"
                    for registration, captured_extensions in ext_conflicts.values()
                    for ext in sorted(captured_extensions)
                )
            )
        if policy == "keep":
            for conflicting_registration, captured_extensions in ext_conflicts.values():
                for ext in sorted(captured_extensions):
                    record_registry_event(
                        registry="input_reader_extension",
                        name=ext,
                        action="keep",
                        policy=policy,
                        existing_source_module=conflicting_registration.source_module,
                        new_source_module=source,
                        detail=(
                            f"kept existing extension owner "
                            f"{conflicting_registration.input_format}"
                        ),
                    )
            return first_conflict

        action = "replace_with_warning" if policy == "warn" else "replace"
        for conflicting_registration, captured_extensions in ext_conflicts.values():
            remaining_extensions = tuple(
                ext for ext in conflicting_registration.extensions if ext not in captured_extensions
            )
            if remaining_extensions:
                state.input_readers_by_format[conflicting_registration.input_format] = replace(
                    conflicting_registration,
                    extensions=remaining_extensions,
                )
            else:
                state.input_readers_by_format.pop(conflicting_registration.input_format, None)
            for ext in sorted(captured_extensions):
                record_registry_event(
                    registry="input_reader_extension",
                    name=ext,
                    action=action,
                    policy=policy,
                    existing_source_module=conflicting_registration.source_module,
                    new_source_module=source,
                    detail=(
                        f"extension {ext} moved from {conflicting_registration.input_format} "
                        f"to {normalized_format}"
                    ),
                )
                state.input_readers_by_extension.pop(ext, None)

    registration = InputReaderRegistration(
        input_format=normalized_format,
        extensions=normalized_extensions,
        loader=loader,
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.input_readers_by_format[normalized_format] = registration
    for ext in normalized_extensions:
        state.input_readers_by_extension[ext] = registration
    return registration


def register_input_reader(
    input_format: str,
    extensions: Iterable[str],
    loader: InputReaderFn,
    *,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> InputReaderRegistration:
    _ensure_builtin_input_readers()
    effective_source = source_module or infer_caller_module()
    return _register_input_reader(
        input_format,
        extensions,
        loader,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
    )


def unregister_input_reader(input_format: str) -> Optional[InputReaderRegistration]:
    _ensure_builtin_input_readers()
    state = get_runtime_state()
    normalized_format = _normalize_input_format(input_format)
    registration = state.input_readers_by_format.pop(normalized_format, None)
    if registration is None:
        return None
    for ext, current in list(state.input_readers_by_extension.items()):
        if current.input_format == normalized_format:
            del state.input_readers_by_extension[ext]
    return registration


def get_input_reader(path: str) -> InputReaderRegistration:
    _ensure_builtin_input_readers()
    ext = _ext(path)
    registration = get_runtime_state().input_readers_by_extension.get(ext)
    if registration is None:
        raise RuntimeError(f"unsupported input file type: {path}")
    return registration


def list_input_formats() -> List[str]:
    _ensure_builtin_input_readers()
    return sorted(get_runtime_state().input_readers_by_format.keys())


def list_input_reader_details() -> List[Dict[str, Any]]:
    _ensure_builtin_input_readers()
    state = get_runtime_state()
    return [
        {
            "input_format": registration.input_format,
            "extensions": list(registration.extensions),
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in sorted(state.input_readers_by_format.values(), key=lambda item: item.input_format)
    ]


def list_input_reader_domains() -> List[Dict[str, Any]]:
    _ensure_builtin_input_readers()
    state = get_runtime_state()
    return summarize_registry_domains(
        sorted(state.input_readers_by_format.values(), key=lambda item: item.input_format),
        item_name_attr="input_format",
        list_key="input_formats",
    )


def _split_text_to_rows(text: str, path: str, source_type: str, by_line: bool = False) -> List[Dict[str, Any]]:
    return _split_text_to_rows_impl(text, path, source_type, by_line=by_line)


def _preprocess_image_for_ocr(image: Any, mode: str) -> Any:
    return _ingest_preprocess_image_for_ocr(image, mode)


def _ocr_text_score(text: str) -> int:
    return _ingest_ocr_text_score(text)


def _ocr_extract_text(pytesseract: Any, image: Any, lang: str, config: str, modes: List[str]) -> str:
    best = ""
    best_score = -1
    successful_modes = 0
    last_err: Optional[Exception] = None
    for mode in modes:
        try:
            processed = _preprocess_image_for_ocr(image, mode)
            text = pytesseract.image_to_string(processed, lang=lang, config=config)
        except Exception as exc:
            last_err = exc
            continue
        successful_modes += 1
        score = _ocr_text_score(text)
        if score > best_score:
            best = text
            best_score = score
    if successful_modes == 0 and last_err is not None:
        raise RuntimeError(f"OCR failed for all preprocess modes: {last_err}") from last_err
    return best


def _read_text_with_fallback(path: str) -> str:
    return _read_text_with_fallback_impl(path)


def read_txt(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    return _read_txt_impl(path, by_line=by_line)


def read_docx(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    return _read_docx_impl(path, by_line=by_line)


def read_pdf(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    return _read_pdf_impl(path, by_line=by_line)


def read_image(
    path: str,
    *,
    by_line: bool = False,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return _read_image_impl(
        path,
        by_line=by_line,
        ocr_lang=ocr_lang,
        ocr_config=ocr_config,
        ocr_preprocess=ocr_preprocess,
        resolve_tesseract_cmd=_resolve_tesseract_cmd,
        ocr_try_modes=_ocr_try_modes,
        ocr_extract_text=_ocr_extract_text,
    )


def read_xlsx(path: str, *, include_all_sheets: bool = False) -> List[Dict[str, Any]]:
    return _read_xlsx_impl(path, include_all_sheets=include_all_sheets)


def _load_txt_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _load_txt_input_impl(path, options)


def _load_docx_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _load_docx_input_impl(path, options)


def _load_pdf_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _load_pdf_input_impl(path, options)


def _load_image_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _load_image_input_impl(
        path,
        options,
        read_image_fn=lambda file_path, **kwargs: read_image(file_path, **kwargs),
    )


def _load_xlsx_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _load_xlsx_input_impl(path, options)


def load_rows_from_file(
    path: str,
    *,
    text_by_line: bool = False,
    ocr_enabled: bool = True,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
    xlsx_all_sheets: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    _ensure_builtin_input_readers()
    registration = get_input_reader(path)
    return registration.loader(
        path,
        {
            "text_by_line": text_by_line,
            "ocr_enabled": ocr_enabled,
            "ocr_lang": ocr_lang,
            "ocr_config": ocr_config,
            "ocr_preprocess": ocr_preprocess,
            "xlsx_all_sheets": xlsx_all_sheets,
        },
    )


def load_rows_from_files(
    paths: List[str],
    *,
    text_by_line: bool = False,
    ocr_enabled: bool = True,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
    xlsx_all_sheets: bool = False,
    max_retries: int = 0,
    on_file_error: str = "skip",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    _ensure_builtin_input_readers()
    all_rows: List[Dict[str, Any]] = []
    formats: List[str] = []
    skipped_files: List[Dict[str, Any]] = []
    failed_files: List[Dict[str, Any]] = []
    for p in paths:
        last_err: Optional[str] = None
        loaded = False
        for _ in range(max_retries + 1):
            try:
                rows, meta = load_rows_from_file(
                    p,
                    text_by_line=text_by_line,
                    ocr_enabled=ocr_enabled,
                    ocr_lang=ocr_lang,
                    ocr_config=ocr_config,
                    ocr_preprocess=ocr_preprocess,
                    xlsx_all_sheets=xlsx_all_sheets,
                )
                fmt = str(meta.get("input_format"))
                formats.append(fmt)
                if meta.get("skipped"):
                    skipped_files.append({"path": p, "reason": str(meta.get("reason") or "skipped")})
                else:
                    all_rows.extend(rows)
                loaded = True
                break
            except Exception as e:
                last_err = str(e)
        if not loaded:
            failed_files.append({"path": p, "error": last_err or "unknown error"})
            if on_file_error == "raise":
                raise RuntimeError(f"failed to load file {p}: {last_err}")
    return all_rows, {
        "input_format": ",".join(formats),
        "file_count": len(paths),
        "skipped_files": skipped_files,
        "failed_files": failed_files,
    }
