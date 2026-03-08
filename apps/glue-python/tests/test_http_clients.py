import unittest
from unittest.mock import patch

from aiwf.base_client import BaseClient
from aiwf import rust_client


class HttpClientTests(unittest.TestCase):
    def test_base_client_returns_ok_for_empty_success_body(self):
        client = BaseClient("http://base")

        class Resp:
            content = b""
            text = ""

            def raise_for_status(self):
                return None

        with patch("aiwf.base_client.requests.post", return_value=Resp()):
            result = client.step_start("job-1", "step-1", "actor", {"x": 1})

        self.assertEqual(result, {"ok": True})

    def test_base_client_raises_clear_error_for_invalid_json(self):
        client = BaseClient("http://base")

        class Resp:
            content = b"not-json"
            text = "not-json"

            def raise_for_status(self):
                return None

            def json(self):
                raise ValueError("bad json")

        with patch("aiwf.base_client.requests.post", return_value=Resp()):
            with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
                client.step_done("job-1", "step-1", "actor", {"x": 1})

    def test_rust_client_returns_ok_for_empty_success_body(self):
        class Resp:
            status_code = 200
            content = b""
            text = ""

        with patch("requests.post", return_value=Resp()):
            result = rust_client.post_json("/health", {})

        self.assertEqual(result, {"ok": True})

    def test_rust_client_raises_clear_error_for_invalid_json(self):
        class Resp:
            status_code = 200
            content = b"oops"
            text = "oops"

            def json(self):
                raise ValueError("bad json")

        with patch("requests.get", return_value=Resp()):
            with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
                rust_client.get_json("/health")


if __name__ == "__main__":
    unittest.main()
