from __future__ import annotations

from aiwf import ingest as ingest_registry


INPUT_DOMAIN = {
    "name": "ingest",
    "label": "Ingest",
    "backend": "python",
    "builtin": True,
}


def register_builtin_input_domains(register_input_reader) -> None:
    register_input_reader(
        "txt",
        [".txt"],
        ingest_registry._load_txt_input,
        domain="ingest",
        domain_metadata=INPUT_DOMAIN,
    )
    register_input_reader(
        "docx",
        [".docx"],
        ingest_registry._load_docx_input,
        domain="ingest",
        domain_metadata=INPUT_DOMAIN,
    )
    register_input_reader(
        "pdf",
        [".pdf"],
        ingest_registry._load_pdf_input,
        domain="ingest",
        domain_metadata=INPUT_DOMAIN,
    )
    register_input_reader(
        "image",
        [".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"],
        ingest_registry._load_image_input,
        domain="ingest",
        domain_metadata=INPUT_DOMAIN,
    )
    register_input_reader(
        "xlsx",
        [".xlsx", ".xlsm"],
        ingest_registry._load_xlsx_input,
        domain="ingest",
        domain_metadata=INPUT_DOMAIN,
    )
