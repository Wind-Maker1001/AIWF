param(
  [string]$RepoRoot = "",
  [string[]]$RequireGovernanceRoutes = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "python not found in PATH"
}

$requiredRoutesJson = ConvertTo-Json -InputObject @($RequireGovernanceRoutes) -Compress

$pythonScript = @'
import json
import os
import re
import sys
from pathlib import Path


def unique_sorted(values):
    return sorted({str(value).strip() for value in values if str(value).strip()})

def capability_to_constant(capability):
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(capability or "").strip()).strip("_")
    return normalized.upper()

def parse_desktop_generated(path):
    text = path.read_text(encoding="utf-8")
    schema_match = re.search(r'const\s+GOVERNANCE_CAPABILITY_SCHEMA_VERSION\s*=\s*"([^"]+)"', text)
    authority_match = re.search(r'const\s+GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY\s*=\s*"([^"]+)"', text)
    items_match = re.search(r'const\s+GOVERNANCE_CAPABILITY_ITEMS\s*=\s*Object\.freeze\((\[.*?\])\);\s*const\s+GOVERNANCE_CAPABILITIES', text, re.S)
    if not schema_match or not authority_match or not items_match:
        raise ValueError("desktop governance capability generated module parse failed")
    return {
        "schema_version": schema_match.group(1),
        "source_authority": authority_match.group(1),
        "items": json.loads(items_match.group(1)),
    }

def parse_winui_generated(path):
    text = path.read_text(encoding="utf-8")
    schema_match = re.search(r'public const string SchemaVersion = "([^"]+)";', text)
    authority_match = re.search(r'public const string SourceAuthority = "([^"]+)";', text)
    if not schema_match or not authority_match:
        raise ValueError("winui governance capability generated module parse failed")
    capabilities = {}
    route_prefixes = {}
    for match in re.finditer(r'public const string ([A-Z0-9_]+) = "([^"]+)";', text):
        name, value = match.groups()
        if name in {"SchemaVersion", "SourceAuthority"}:
            continue
        if name.endswith("_ROUTE_PREFIX"):
            route_prefixes[name[: -len("_ROUTE_PREFIX")]] = value
        else:
            capabilities[name] = value
    return {
        "schema_version": schema_match.group(1),
        "source_authority": authority_match.group(1),
        "capabilities": capabilities,
        "route_prefixes": route_prefixes,
    }


def fail(payload):
    print(json.dumps(payload, ensure_ascii=False))
    sys.exit(1)


repo_root = Path(sys.argv[1])
sys.path.insert(0, str(repo_root / "apps" / "glue-python"))

from app import app
from aiwf.governance_surface import (
    GOVERNANCE_CONTROL_PLANE_ROLE,
    GOVERNANCE_CONTROL_PLANE_STATUS,
    GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
    GOVERNANCE_SURFACE_META_ROUTE,
    GOVERNANCE_SURFACE_SCHEMA_VERSION,
    JOB_LIFECYCLE_CONTROL_PLANE_OWNER,
    REQUIRED_GOVERNANCE_SURFACE_FIELDS,
    build_governance_capability_map,
    list_governance_surface_entries,
    validate_governance_surface_entries,
)


entries = list_governance_surface_entries()
capability_map = build_governance_capability_map()
required_routes = json.loads(os.environ.get("AIWF_REQUIRE_GOVERNANCE_ROUTES_JSON", "[]"))
manifest_path = repo_root / "contracts" / "governance" / "governance_capabilities.v1.json"
desktop_generated_path = repo_root / "apps" / "dify-desktop" / "workflow_governance_capabilities.generated.js"
winui_generated_path = repo_root / "apps" / "dify-native-winui" / "src" / "WinUI3Bootstrap" / "Runtime" / "GovernanceCapabilities.Generated.cs"
manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
manifest_items = [
    {
        "constant": str(item.get("constant") or "").strip(),
        "capability": str(item.get("capability") or "").strip(),
        "route_prefix": str(item.get("route_prefix") or "").strip(),
        "owned_route_prefixes": unique_sorted(item.get("owned_route_prefixes") or []),
    }
    for item in (manifest_payload.get("items") if isinstance(manifest_payload.get("items"), list) else [])
]
desktop_generated = parse_desktop_generated(desktop_generated_path)
winui_generated = parse_winui_generated(winui_generated_path)

issues = list(validate_governance_surface_entries(entries))
governance_routes = unique_sorted(
    route.path
    for route in app.routes
    if isinstance(getattr(route, "path", None), str) and route.path.startswith("/governance/")
)
owned_route_claims = {}
invalid_control_plane_roles = []
invalid_state_owners = []
invalid_lifecycle_owners = []
lifecycle_mutation_allowed = []
missing_required_fields = []
invalid_owned_route_prefixes = []
manifest_constant_drift = []
manifest_capability_drift = []
manifest_route_prefix_drift = []
manifest_owned_route_prefix_drift = []
manifest_source_authority_drift = []
desktop_generated_capability_drift = []
desktop_generated_route_prefix_drift = []
winui_generated_capability_drift = []
winui_generated_route_prefix_drift = []
generated_source_authority_drift = []

for entry in entries:
    capability = str(entry.get("capability") or "").strip()
    for field in REQUIRED_GOVERNANCE_SURFACE_FIELDS:
      value = entry.get(field)
      if field == "owned_route_prefixes":
          if not isinstance(value, list) or not value:
              missing_required_fields.append(f"{capability}:{field}")
          continue
      if value is None or (isinstance(value, str) and not value.strip()):
          missing_required_fields.append(f"{capability}:{field}")
    if str(entry.get("control_plane_role") or "").strip() != GOVERNANCE_CONTROL_PLANE_ROLE:
        invalid_control_plane_roles.append(capability)
    if str(entry.get("state_owner") or "").strip() != GOVERNANCE_STATE_CONTROL_PLANE_OWNER:
        invalid_state_owners.append(capability)
    if str(entry.get("job_lifecycle_control_plane_owner") or "").strip() != JOB_LIFECYCLE_CONTROL_PLANE_OWNER:
        invalid_lifecycle_owners.append(capability)
    if bool(entry.get("lifecycle_mutation_allowed")):
        lifecycle_mutation_allowed.append(capability)
    for prefix in entry.get("owned_route_prefixes") or []:
        normalized = str(prefix or "").strip()
        if not normalized.startswith("/governance/"):
            invalid_owned_route_prefixes.append(f"{capability}:{normalized}")
            continue
        owned_route_claims.setdefault(normalized, []).append(capability)

duplicate_owned_route_prefixes = unique_sorted(
    prefix for prefix, capabilities in owned_route_claims.items() if len(set(capabilities)) > 1
)
for prefix in duplicate_owned_route_prefixes:
    issues.append(f"owned route prefix claimed by multiple capabilities: {prefix}")
if missing_required_fields:
    issues.append("governance surface required field drift: " + ", ".join(unique_sorted(missing_required_fields)))
if invalid_control_plane_roles:
    issues.append("governance surface control_plane_role drift: " + ", ".join(unique_sorted(invalid_control_plane_roles)))
if invalid_state_owners:
    issues.append("governance surface state_owner drift: " + ", ".join(unique_sorted(invalid_state_owners)))
if invalid_lifecycle_owners:
    issues.append("governance surface job_lifecycle_control_plane_owner drift: " + ", ".join(unique_sorted(invalid_lifecycle_owners)))
if lifecycle_mutation_allowed:
    issues.append("governance surfaces must keep lifecycle_mutation_allowed=false: " + ", ".join(unique_sorted(lifecycle_mutation_allowed)))
if invalid_owned_route_prefixes:
    issues.append("governance surface invalid owned route prefixes: " + ", ".join(unique_sorted(invalid_owned_route_prefixes)))

covered_routes = []
uncovered_governance_routes = []
for route in governance_routes:
    if route == GOVERNANCE_SURFACE_META_ROUTE:
        covered_routes.append(route)
        continue
    if any(route.startswith(prefix) for prefix in owned_route_claims):
        covered_routes.append(route)
    else:
        uncovered_governance_routes.append(route)
if uncovered_governance_routes:
    issues.append("uncovered governance routes: " + ", ".join(unique_sorted(uncovered_governance_routes)))

missing_required_routes = [str(route).strip() for route in required_routes if str(route).strip() and str(route).strip() not in governance_routes]
if missing_required_routes:
    issues.append("required governance routes missing: " + ", ".join(unique_sorted(missing_required_routes)))

meta_route_present = GOVERNANCE_SURFACE_META_ROUTE in governance_routes
if not meta_route_present:
    issues.append(f"governance meta route missing: {GOVERNANCE_SURFACE_META_ROUTE}")

surface_capabilities = unique_sorted(entry.get("capability") for entry in entries)
capability_keys = unique_sorted(
    key
    for key, value in capability_map.items()
    if isinstance(value, dict) and "owner" in value and "route_prefix" in value
)
capability_map_drift = unique_sorted(
    list(set(surface_capabilities) - set(capability_keys))
    + list(set(capability_keys) - set(surface_capabilities))
)
if capability_map_drift:
    issues.append("governance capability map drift: " + ", ".join(capability_map_drift))

surface_by_capability = {
    str(entry.get("capability") or "").strip(): entry
    for entry in entries
    if str(entry.get("capability") or "").strip()
}
manifest_by_capability = {
    item["capability"]: item
    for item in manifest_items
    if item.get("capability")
}
expected_constants = {
    capability: capability_to_constant(capability)
    for capability in surface_by_capability.keys()
}

manifest_constant_drift = unique_sorted(
    capability
    for capability, expected_constant in expected_constants.items()
    if capability not in manifest_by_capability
    or manifest_by_capability[capability].get("constant") != expected_constant
)
manifest_capability_drift = unique_sorted(
    list(set(surface_by_capability.keys()) - set(manifest_by_capability.keys()))
    + list(set(manifest_by_capability.keys()) - set(surface_by_capability.keys()))
)
manifest_route_prefix_drift = unique_sorted(
    capability
    for capability, surface in surface_by_capability.items()
    if capability in manifest_by_capability
    and str(surface.get("route_prefix") or "").strip() != manifest_by_capability[capability].get("route_prefix")
)
manifest_owned_route_prefix_drift = unique_sorted(
    capability
    for capability, surface in surface_by_capability.items()
    if capability in manifest_by_capability
    and unique_sorted(surface.get("owned_route_prefixes") or []) != unique_sorted(manifest_by_capability[capability].get("owned_route_prefixes") or [])
)
if str(manifest_payload.get("source_authority") or "").strip() != "apps/glue-python/aiwf/governance_surface.py":
    manifest_source_authority_drift.append(str(manifest_payload.get("source_authority") or "").strip() or "<missing>")

desktop_items_by_constant = {
    str(item.get("constant") or "").strip(): item
    for item in desktop_generated.get("items", [])
    if str(item.get("constant") or "").strip()
}
for capability, expected_constant in expected_constants.items():
    manifest_item = manifest_by_capability.get(capability)
    desktop_item = desktop_items_by_constant.get(expected_constant)
    winui_capability = winui_generated.get("capabilities", {}).get(expected_constant)
    winui_route_prefix = winui_generated.get("route_prefixes", {}).get(expected_constant)
    if desktop_item is None or desktop_item.get("capability") != capability:
        desktop_generated_capability_drift.append(capability)
    if manifest_item is not None and desktop_item is not None:
        if desktop_item.get("route_prefix") != manifest_item.get("route_prefix"):
            desktop_generated_route_prefix_drift.append(capability)
    if winui_capability != capability:
        winui_generated_capability_drift.append(capability)
    if manifest_item is not None and winui_route_prefix != manifest_item.get("route_prefix"):
        winui_generated_route_prefix_drift.append(capability)

extra_desktop_constants = unique_sorted(
    constant
    for constant in desktop_items_by_constant.keys()
    if constant not in expected_constants.values()
)
extra_winui_constants = unique_sorted(
    constant
    for constant in winui_generated.get("capabilities", {}).keys()
    if constant not in expected_constants.values()
)
desktop_generated_capability_drift = unique_sorted(desktop_generated_capability_drift + extra_desktop_constants)
winui_generated_capability_drift = unique_sorted(winui_generated_capability_drift + extra_winui_constants)

if desktop_generated.get("schema_version") != str(manifest_payload.get("schema_version") or "").strip():
    generated_source_authority_drift.append("desktop schema version drift")
if winui_generated.get("schema_version") != str(manifest_payload.get("schema_version") or "").strip():
    generated_source_authority_drift.append("winui schema version drift")
if desktop_generated.get("source_authority") != str(manifest_payload.get("source_authority") or "").strip():
    generated_source_authority_drift.append("desktop source authority drift")
if winui_generated.get("source_authority") != str(manifest_payload.get("source_authority") or "").strip():
    generated_source_authority_drift.append("winui source authority drift")

if manifest_constant_drift:
    issues.append("governance capability manifest constant drift: " + ", ".join(manifest_constant_drift))
if manifest_capability_drift:
    issues.append("governance capability manifest capability drift: " + ", ".join(manifest_capability_drift))
if manifest_route_prefix_drift:
    issues.append("governance capability manifest route prefix drift: " + ", ".join(manifest_route_prefix_drift))
if manifest_owned_route_prefix_drift:
    issues.append("governance capability manifest owned route prefix drift: " + ", ".join(manifest_owned_route_prefix_drift))
if manifest_source_authority_drift:
    issues.append("governance capability manifest source authority drift: " + ", ".join(manifest_source_authority_drift))
if desktop_generated_capability_drift:
    issues.append("desktop governance capability generated drift: " + ", ".join(desktop_generated_capability_drift))
if desktop_generated_route_prefix_drift:
    issues.append("desktop governance capability route prefix drift: " + ", ".join(desktop_generated_route_prefix_drift))
if winui_generated_capability_drift:
    issues.append("winui governance capability generated drift: " + ", ".join(winui_generated_capability_drift))
if winui_generated_route_prefix_drift:
    issues.append("winui governance capability route prefix drift: " + ", ".join(winui_generated_route_prefix_drift))
if generated_source_authority_drift:
    issues.append("governance capability generated source authority drift: " + ", ".join(generated_source_authority_drift))

payload = {
    "status": "failed" if issues else "passed",
    "schemaVersion": GOVERNANCE_SURFACE_SCHEMA_VERSION,
    "controlPlaneStatus": GOVERNANCE_CONTROL_PLANE_STATUS,
    "controlPlaneRole": GOVERNANCE_CONTROL_PLANE_ROLE,
    "governanceStateControlPlaneOwner": GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
    "jobLifecycleControlPlaneOwner": JOB_LIFECYCLE_CONTROL_PLANE_OWNER,
    "metaRoute": GOVERNANCE_SURFACE_META_ROUTE,
    "manifestPath": str(manifest_path),
    "desktopGeneratedPath": str(desktop_generated_path),
    "winUiGeneratedPath": str(winui_generated_path),
    "surfaceCount": len(entries),
    "governanceRouteCount": len(governance_routes),
    "coveredGovernanceRouteCount": len(covered_routes),
    "drift": {
        "missingRequiredFields": unique_sorted(missing_required_fields),
        "invalidControlPlaneRoles": unique_sorted(invalid_control_plane_roles),
        "invalidStateOwners": unique_sorted(invalid_state_owners),
        "invalidLifecycleOwners": unique_sorted(invalid_lifecycle_owners),
        "lifecycleMutationAllowed": unique_sorted(lifecycle_mutation_allowed),
        "invalidOwnedRoutePrefixes": unique_sorted(invalid_owned_route_prefixes),
        "duplicateOwnedRoutePrefixes": duplicate_owned_route_prefixes,
        "uncoveredGovernanceRoutes": unique_sorted(uncovered_governance_routes),
        "missingRequiredRoutes": unique_sorted(missing_required_routes),
        "capabilityMapDrift": capability_map_drift,
        "manifestConstantDrift": manifest_constant_drift,
        "manifestCapabilityDrift": manifest_capability_drift,
        "manifestRoutePrefixDrift": manifest_route_prefix_drift,
        "manifestOwnedRoutePrefixDrift": manifest_owned_route_prefix_drift,
        "manifestSourceAuthorityDrift": manifest_source_authority_drift,
        "desktopGeneratedCapabilityDrift": desktop_generated_capability_drift,
        "desktopGeneratedRoutePrefixDrift": desktop_generated_route_prefix_drift,
        "winUiGeneratedCapabilityDrift": winui_generated_capability_drift,
        "winUiGeneratedRoutePrefixDrift": winui_generated_route_prefix_drift,
        "generatedSourceAuthorityDrift": generated_source_authority_drift,
    },
    "issues": unique_sorted(issues),
}

if payload["status"] != "passed":
    fail(payload)

print(json.dumps(payload, ensure_ascii=False))
'@

$env:AIWF_REQUIRE_GOVERNANCE_ROUTES_JSON = $requiredRoutesJson
try {
  $pythonScript | python - $RepoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "governance control plane boundary checks failed"
  }
}
finally {
  Remove-Item Env:AIWF_REQUIRE_GOVERNANCE_ROUTES_JSON -ErrorAction SilentlyContinue
}

Ok "governance control plane boundary check passed"
