param(
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$checks = @(
  @{
    Path = Join-Path $RepoRoot "README.md"
    Patterns = @(
      "primary native WinUI desktop frontend",
      "secondary Electron compatibility shell",
      "run_aiwf_frontend.ps1",
      "run_dify_native_winui.ps1",
      "release_electron_compatibility.ps1",
      "## Compatibility Paths"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\quickstart.md"
    Patterns = @(
      "quickstart_native_winui.md",
      "frontend_convergence_decision_20260320.md",
      "offline_delivery_native_winui.md",
      "electron_compatibility_retirement_plan_20260321.md",
      "personal_sideload_certificate_20260321.md",
      "## Compatibility"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\dify_desktop_app.md"
    Patterns = @(
      "secondary Electron compatibility frontend",
      "WinUI is the primary frontend",
      "release_electron_compatibility.ps1",
      "electron_compatibility_retirement_plan_20260321.md",
      "WorkflowAdmin"
    )
  },
  @{
    Path = Join-Path $RepoRoot "apps\dify-native-winui\README.md"
    Patterns = @(
      "primary desktop frontend",
      "secondary compatibility shell",
      "GenerateAppInstaller"
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\run_aiwf_frontend.ps1"
    Patterns = @(
      'string]$Frontend = "WinUI"',
      "publish_native_winui.ps1",
      "package_native_winui_bundle.ps1",
      "publishing primary frontend app: WinUI",
      "packaging primary frontend installer bundle: WinUI",
      "launching primary frontend: WinUI",
      '[switch]$WorkflowAdmin',
      "SkipEnsureGlueBridge",
      '[string]$ReleaseChannel = "dev"'
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\run_dify_native_winui.ps1"
    Patterns = @(
      "ensure_local_governance_bridge.ps1",
      "AIWF_MANUAL_REVIEW_PROVIDER",
      "AIWF_QUALITY_RULE_SET_PROVIDER",
      "AIWF_WORKFLOW_APP_REGISTRY_PROVIDER",
      "AIWF_WORKFLOW_VERSION_PROVIDER",
      "AIWF_WORKFLOW_RUN_AUDIT_PROVIDER",
      "AIWF_RUN_BASELINE_PROVIDER",
      "backend-owned manual review, quality rule, app registry, workflow version, run audit, and run baseline providers"
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\run_dify_desktop.ps1"
    Patterns = @(
      "ensure_local_governance_bridge.ps1",
      "AIWF_MANUAL_REVIEW_PROVIDER",
      "AIWF_QUALITY_RULE_SET_PROVIDER",
      "AIWF_WORKFLOW_APP_REGISTRY_PROVIDER",
      "AIWF_WORKFLOW_VERSION_PROVIDER",
      "AIWF_WORKFLOW_RUN_AUDIT_PROVIDER",
      "AIWF_RUN_BASELINE_PROVIDER",
      "backend-owned manual review, quality rule, app registry, workflow version, run audit, and run baseline providers"
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\ci_check.ps1"
    Patterns = @(
      'ValidateSet("Default","Quick","Full","Compatibility")',
      "frontend_primary_verification_latest.json",
      "frontend_compatibility_verification_latest.json",
      "SkipDesktopPackageTests",
      "Use -CiProfile Compatibility",
      "running native winui primary frontend smoke check",
      "running Electron compatibility packaged startup check",
      "skip Electron compatibility packaged startup checks"
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\dispatch_full_integration_self_hosted.ps1"
    Patterns = @(
      'ValidateSet("Default","Quick","Full","Compatibility")'
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\verify_branch_ci.ps1"
    Patterns = @(
      'ValidateSet("Default","Quick","Full","Compatibility")'
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\offline_delivery_native_winui.md"
    Patterns = @(
      "package_native_winui_bundle.ps1",
      "release_frontend_productize.ps1",
      "Install_AIWF_Native_WinUI",
      "package_native_winui_msix.ps1",
      "GenerateAppInstaller",
      "ensure_personal_sideload_certificate.ps1",
      "PersonalSideloadCert"
    )
  },
  @{
    Path = Join-Path $RepoRoot "apps\dify-native-winui\README.md"
    Patterns = @(
      "publish_native_winui.ps1",
      "release_frontend_productize.ps1"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\personal_sideload_certificate_20260321.md"
    Patterns = @(
      "ensure_personal_sideload_certificate.ps1",
      "check_personal_sideload_certificate.ps1",
      "aiwf_personal_sideload.metadata.json"
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\release_frontend_productize.ps1"
    Patterns = @(
      'string]$Frontend = "WinUI"',
      "release_electron_compatibility.ps1",
      "Electron release path is compatibility-only. WinUI is the primary frontend."
    )
  },
  @{
    Path = Join-Path $RepoRoot "ops\scripts\release_electron_compatibility.ps1"
    Patterns = @(
      "Electron compatibility release path invoked",
      "release_productize.ps1"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\electron_compatibility_retirement_plan_20260321.md"
    Patterns = @(
      "2026-03-21",
      "2026-04-05",
      "2026-04-19",
      "2026-05-19",
      "2026-06-18",
      "secondary compatibility frontend",
      "--workflow-admin"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\electron_capability_inventory_20260321.md"
    Patterns = @(
      "compat-hidden",
      "--workflow-admin",
      "?legacyAdmin=1",
      "technically heavy semantics",
      "frequent manual adjustment",
      "keep in frontend; this is human-curated workflow authoring"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\quickstart_native_winui.md"
    Patterns = @(
      "glue-python governance bridge",
      "manual review, quality rule sets, workflow app registry, workflow version storage, workflow run audit, and run baseline",
      "SkipEnsureGlueBridge"
    )
  },
  @{
    Path = Join-Path $RepoRoot "docs\verification.md"
    Patterns = @(
      "native WinUI primary frontend smoke outside CI unless you explicitly skip it",
      "Electron compatibility packaged-startup checks",
      "compatibility-only",
      "ci_check.ps1 -CiProfile Compatibility",
      "frontend_primary_verification_latest.json",
      "frontend_compatibility_verification_latest.json",
      "architecture_scorecard_latest.json",
      "architecture_scorecard_latest.md",
      "architecture_scorecard_release_ready_latest.json",
      "architecture_scorecard_release_ready_latest.md"
    )
  },
  @{
    Path = Join-Path $RepoRoot "apps\dify-desktop\main_window_support.js"
    Patterns = @(
      "createWorkflowWindow({ legacyAdmin: true })",
      "openWorkflowAdmin",
      "--workflow-admin",
      "legacyAdmin"
    )
  },
  @{
    Path = Join-Path $RepoRoot "apps\dify-desktop\renderer\workflow.html"
    Patterns = @(
      "?legacyAdmin=1",
      "btnTemplatePackInstall",
      "btnTimelineRefresh",
      "btnCompareRuns",
      "compat-admin-only"
    )
  }
)

foreach ($check in $checks) {
  if (-not (Test-Path $check.Path)) {
    throw "frontend convergence check file missing: $($check.Path)"
  }
  $content = Get-Content $check.Path -Raw
  foreach ($pattern in $check.Patterns) {
    if ($content -notmatch [regex]::Escape($pattern)) {
      throw "frontend convergence drift: '$pattern' missing in $($check.Path)"
    }
  }
}

Ok "frontend convergence check passed"
