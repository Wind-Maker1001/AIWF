from __future__ import annotations

import pathlib
import re
import sys


TEMPLATE = """from __future__ import annotations

from typing import Any, Dict
import requests


def _url(base: str, path: str) -> str:
    return str(base).rstrip("/") + (path if path.startswith("/") else "/" + path)


def get_json(path: str, base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    r = requests.get(_url(base_url, path), timeout=timeout)
    r.raise_for_status()
    return r.json()


def post_json(path: str, payload: Dict[str, Any], base_url: str = "http://127.0.0.1:18082", timeout: float = 30.0) -> Dict[str, Any]:
    r = requests.post(_url(base_url, path), json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

"""


def py_name(path: str, method: str) -> str:
    p = path.strip("/").replace("{", "").replace("}", "")
    p = re.sub(r"[^a-zA-Z0-9_/]+", "_", p)
    p = p.replace("/", "_")
    return f"{method.lower()}_{p}".replace("__", "_")


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: generate_rust_client.py <openapi_yaml> <out_py>")
        return 2
    openapi = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
    out = pathlib.Path(sys.argv[2])
    paths: list[tuple[str, str]] = []
    current_path = ""
    for line in openapi.splitlines():
        if re.match(r"^  /", line):
            current_path = line.strip().rstrip(":")
        elif re.match(r"^    (get|post):", line):
            method = line.strip().rstrip(":")
            if current_path:
                paths.append((current_path, method))
    buf = [TEMPLATE]
    for p, m in paths:
        fn = py_name(p, m)
        if m == "get":
            body = f"""def {fn}(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return get_json("{p}", base_url=base_url, timeout=timeout)

"""
        else:
            body = f"""def {fn}(payload: Dict[str, Any], base_url: str = "http://127.0.0.1:18082", timeout: float = 30.0) -> Dict[str, Any]:
    return post_json("{p}", payload, base_url=base_url, timeout=timeout)

"""
        buf.append(body)
    out.write_text("".join(buf), encoding="utf-8")
    print(f"generated: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
