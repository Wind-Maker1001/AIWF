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


if __name__ == "__main__":
    unittest.main()
