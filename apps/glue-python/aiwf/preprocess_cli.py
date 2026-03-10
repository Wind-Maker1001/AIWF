from __future__ import annotations

import argparse
import json
import os
from typing import Any, Callable, Dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AIWF raw-to-cooked preprocessing")
    parser.add_argument("--input", required=True, help="input path (csv/json/jsonl)")
    parser.add_argument("--output", required=True, help="output path (csv/json/jsonl)")
    parser.add_argument("--config", required=False, help="JSON/YAML config path for preprocess spec")
    return parser.parse_args()


def run_cli(
    args: argparse.Namespace,
    *,
    validate_preprocess_spec: Callable[[Dict[str, Any]], Dict[str, Any]],
    preprocess_file: Callable[[str, str, Dict[str, Any]], Dict[str, Any]],
) -> int:
    spec: Dict[str, Any] = {}
    if args.config:
        with open(args.config, "r", encoding="utf-8-sig") as file:
            text = file.read()
        ext = os.path.splitext(args.config)[1].lower()
        if ext in {".yaml", ".yml"}:
            try:
                import yaml  # type: ignore
            except Exception as exc:
                print(json.dumps({"ok": False, "errors": [f"yaml support requires pyyaml: {exc}"]}, ensure_ascii=False))
                return 2
            loaded = yaml.safe_load(text)
        else:
            loaded = json.loads(text)
        if isinstance(loaded, dict):
            spec = loaded.get("preprocess") if isinstance(loaded.get("preprocess"), dict) else loaded
        else:
            print(json.dumps({"ok": False, "errors": ["config must be an object"]}, ensure_ascii=False))
            return 2

    validation = validate_preprocess_spec(spec)
    if not validation["ok"]:
        print(json.dumps(validation, ensure_ascii=False))
        return 2

    result = preprocess_file(args.input, args.output, spec)
    print(json.dumps({"ok": True, "result": result, "warnings": validation.get("warnings", [])}, ensure_ascii=False))
    return 0
