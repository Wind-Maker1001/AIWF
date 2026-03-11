from __future__ import annotations

from aiwf.flows import office_artifacts as registry


ARTIFACT_DOMAIN = {
    "name": "cleaning-office",
    "label": "Cleaning Office Artifacts",
    "backend": "python",
    "builtin": True,
}


def register_builtin_office_artifacts(register_office_artifact) -> None:
    register_office_artifact(
        "xlsx_fin",
        artifact_id="xlsx_fin_001",
        kind="xlsx",
        filename="fin.xlsx",
        path_key="xlsx_path",
        sha_key="sha_xlsx",
        writer=registry._write_xlsx_artifact,
        domain="cleaning-office",
        domain_metadata=ARTIFACT_DOMAIN,
    )
    register_office_artifact(
        "docx_audit",
        artifact_id="docx_audit_001",
        kind="docx",
        filename="audit.docx",
        path_key="docx_path",
        sha_key="sha_docx",
        writer=registry._write_docx_artifact,
        domain="cleaning-office",
        domain_metadata=ARTIFACT_DOMAIN,
    )
    register_office_artifact(
        "pptx_deck",
        artifact_id="pptx_deck_001",
        kind="pptx",
        filename="deck.pptx",
        path_key="pptx_path",
        sha_key="sha_pptx",
        writer=registry._write_pptx_artifact,
        domain="cleaning-office",
        domain_metadata=ARTIFACT_DOMAIN,
    )
