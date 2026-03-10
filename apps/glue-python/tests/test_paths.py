import os
import unittest
from unittest.mock import patch

from aiwf import paths
from aiwf.flows.cleaning_flow_helpers import prepare_job_layout


def make_job_context(job_root: str) -> dict[str, str]:
    job_root = os.path.normpath(job_root)
    return {
        "job_root": job_root,
        "stage_dir": os.path.join(job_root, "stage-x"),
        "artifacts_dir": os.path.join(job_root, "artifacts-x"),
        "evidence_dir": os.path.join(job_root, "evidence-x"),
    }


class PathResolutionTests(unittest.TestCase):
    def test_resolve_jobs_root_prefers_explicit_jobs_root(self):
        with patch.dict(os.environ, {"AIWF_JOBS_ROOT": r"D:\custom\jobs"}, clear=True):
            self.assertEqual(paths.resolve_jobs_root(), os.path.normpath(r"D:\custom\jobs"))
            self.assertEqual(paths.resolve_bus_root(), os.path.normpath(r"D:\custom"))

    def test_resolve_jobs_root_falls_back_to_bus_root(self):
        with patch.dict(os.environ, {"AIWF_BUS": r"D:\custom\bus"}, clear=True):
            self.assertEqual(paths.resolve_bus_root(), os.path.normpath(r"D:\custom\bus"))
            self.assertEqual(paths.resolve_jobs_root(), os.path.normpath(r"D:\custom\bus\jobs"))

    def test_prepare_job_layout_uses_resolved_jobs_root(self):
        with patch.dict(os.environ, {"AIWF_JOBS_ROOT": r"D:\custom\jobs"}, clear=True):
            layout = prepare_job_layout("job-1", {}, ensure_dirs=lambda *args: None)
            expected_root = os.path.normpath(r"D:\custom\jobs\job-1")
            self.assertEqual(layout["job_root"], expected_root)
            expected_uri = os.path.join(expected_root, "")
            self.assertEqual(layout["input_uri"], expected_uri)
            self.assertEqual(layout["output_uri"], expected_uri)

    def test_prepare_job_layout_uses_explicit_job_context(self):
        with patch.dict(os.environ, {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False):
            layout = prepare_job_layout(
                "job-ctx",
                {"job_context": make_job_context(r"D:\right\job")},
                ensure_dirs=lambda *args: None,
            )
        self.assertEqual(layout["job_root"], os.path.normpath(r"D:\right\job"))
        self.assertEqual(layout["stage_dir"], os.path.normpath(r"D:\right\job\stage-x"))
        self.assertEqual(layout["artifacts_dir"], os.path.normpath(r"D:\right\job\artifacts-x"))
        self.assertEqual(layout["evidence_dir"], os.path.normpath(r"D:\right\job\evidence-x"))

    def test_prepare_job_layout_rejects_legacy_path_params(self):
        with patch.dict(os.environ, {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False):
            with self.assertRaisesRegex(ValueError, "legacy flow path params are no longer supported"):
                prepare_job_layout(
                    "job-legacy",
                    {"job_root": r"D:\legacy\job"},
                    ensure_dirs=lambda *args: None,
                )

    def test_prepare_job_layout_accepts_explicit_job_context(self):
        with patch.dict(os.environ, {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False):
            layout = prepare_job_layout(
                "job-strict",
                {"job_context": make_job_context(r"D:\strict\job")},
                ensure_dirs=lambda *args: None,
            )
        self.assertEqual(layout["job_root"], os.path.normpath(r"D:\strict\job"))
        self.assertEqual(layout["stage_dir"], os.path.normpath(r"D:\strict\job\stage-x"))
        self.assertEqual(layout["artifacts_dir"], os.path.normpath(r"D:\strict\job\artifacts-x"))
        self.assertEqual(layout["evidence_dir"], os.path.normpath(r"D:\strict\job\evidence-x"))

    def test_resolve_job_root_rejects_traversal_job_id(self):
        with patch.dict(os.environ, {"AIWF_JOBS_ROOT": r"D:\custom\jobs"}, clear=True):
            with self.assertRaises(ValueError):
                paths.resolve_job_root(r"..\escape")

    def test_resolve_job_root_rejects_external_absolute_override_by_default(self):
        with patch.dict(os.environ, {"AIWF_JOBS_ROOT": r"D:\custom\jobs"}, clear=True):
            with self.assertRaises(ValueError):
                paths.resolve_job_root("job-1", override=r"D:\elsewhere\job-1")

    def test_resolve_job_root_allows_external_absolute_override_with_opt_in(self):
        with patch.dict(
            os.environ,
            {
                "AIWF_JOBS_ROOT": r"D:\custom\jobs",
                "AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true",
            },
            clear=True,
        ):
            self.assertEqual(
                paths.resolve_job_root("job-1", override=r"D:\elsewhere\job-1"),
                os.path.normpath(r"D:\elsewhere\job-1"),
            )


if __name__ == "__main__":
    unittest.main()
