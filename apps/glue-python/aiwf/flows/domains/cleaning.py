from __future__ import annotations

from typing import Any, Callable


FLOW_DOMAIN = {
    "name": "cleaning",
    "label": "Cleaning",
    "backend": "python",
    "builtin": True,
    "description": "Built-in data cleaning and artifact generation flows.",
}


def register_builtin_flows(register_flow: Callable[..., Any]) -> None:
    register_flow(
        "cleaning",
        module_path="aiwf.flows.cleaning",
        attr_name="run_cleaning",
        domain=FLOW_DOMAIN["name"],
        domain_metadata=FLOW_DOMAIN,
    )
