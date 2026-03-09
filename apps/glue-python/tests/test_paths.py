import os
import unittest
from unittest.mock import patch

from aiwf import paths
from aiwf.flows.cleaning_flow_helpers import prepare_job_layout


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
