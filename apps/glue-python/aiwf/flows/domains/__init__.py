from __future__ import annotations

from importlib import import_module
from typing import Any, Callable


_BUILTIN_FLOW_DOMAIN_MODULES = (
    "aiwf.flows.domains.cleaning",
)


def register_builtin_flow_domains(register_flow: Callable[..., Any]) -> None:
    for module_path in _BUILTIN_FLOW_DOMAIN_MODULES:
        module = import_module(module_path)
        registrar = getattr(module, "register_builtin_flows", None)
        if registrar is None or not callable(registrar):
            raise RuntimeError(f"flow domain module missing register_builtin_flows: {module_path}")
        registrar(register_flow)
