import json
import tempfile
import unittest
from pathlib import Path

from aiwf import preprocess


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _normalize_rows(rows, fields):
    slim = [{field: row.get(field) for field in fields} for row in rows]
    return sorted(slim, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))


class PreprocessGoldRegressionTests(unittest.TestCase):
    def test_debate_gold_scenarios(self):
        root = Path(__file__).resolve().parents[3] / "lake" / "datasets" / "preprocess_debate_gold"
        manifest = _load_json(root / "manifest.json")
        scenarios = manifest.get("scenarios") if isinstance(manifest.get("scenarios"), list) else []
        self.assertGreaterEqual(len(scenarios), 4)

        for item in scenarios:
            scenario_dir = root / str(item.get("dir") or item.get("id") or "")
            scenario = _load_json(scenario_dir / "scenario.json")
            expected_rows = _load_jsonl(scenario_dir / "expected_rows.jsonl")
            input_files = [str((scenario_dir / rel).resolve()) for rel in scenario.get("input_files", [])]
            compare_fields = [str(field) for field in scenario.get("expected_row_fields", []) if str(field).strip()]
            expected_metrics = scenario.get("expected_metrics") if isinstance(scenario.get("expected_metrics"), dict) else {}
            expected_summary = scenario.get("expected_summary") if isinstance(scenario.get("expected_summary"), dict) else {}
            expected_raw_signal_summary = (
                scenario.get("expected_raw_signal_summary")
                if isinstance(scenario.get("expected_raw_signal_summary"), dict)
                else {}
            )
            expected_warnings = [str(item) for item in (scenario.get("expected_warnings") or []) if str(item).strip()]
            assert_uniform_schema = bool(scenario.get("assert_uniform_schema", False))

            with self.subTest(scenario=str(scenario.get("id") or scenario_dir.name)):
                with tempfile.TemporaryDirectory() as tmp:
                    output_path = Path(tmp) / "out.jsonl"
                    spec = dict(scenario.get("preprocess_spec") or {})
                    spec["input_files"] = input_files
                    result = preprocess.preprocess_file(input_files[0], str(output_path), spec)
                    rows = preprocess._read_jsonl(str(output_path))

                    self.assertEqual(_normalize_rows(rows, compare_fields), _normalize_rows(expected_rows, compare_fields))
                    self.assertTrue(all(str(row.get("claim_text") or "").strip() for row in rows))
                    self.assertTrue(all(str(row.get("source_path") or "").strip() for row in rows))
                    if assert_uniform_schema and rows:
                        schema_keys = set(rows[0].keys())
                        self.assertTrue(all(set(row.keys()) == schema_keys for row in rows))

                    with open(result["quality_report_path"], "r", encoding="utf-8") as handle:
                        report = json.load(handle)

                    self.assertEqual(report["required_field_missing"]["claim_text"], 0)
                    self.assertEqual(report["required_field_missing"]["source_path"], 0)
                    self.assertEqual(report["metrics"]["conditional_required_failures"], [])
                    self.assertEqual(report.get("warnings") or [], expected_warnings)

                    for key, expected in expected_raw_signal_summary.items():
                        self.assertEqual(report["raw_signal_summary"].get(key), expected)
                    for key, expected in expected_summary.items():
                        self.assertEqual(report["summary"].get(key), expected)
                    signal_hits = report.get("raw_signal_hit_summary") or {}
                    self.assertIn("hit_labels", signal_hits)
                    self.assertIn("recommendation_reason", signal_hits)
                    self.assertEqual(signal_hits.get("input_row_count"), report["raw_signal_summary"].get("input_row_count"))

                    for key, expected in expected_metrics.items():
                        actual = report["metrics"].get(key)
                        if isinstance(expected, float):
                            self.assertAlmostEqual(float(actual), expected, places=6)
                        else:
                            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
