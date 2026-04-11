param(
  [Parameter(Mandatory = $true)][string]$Version,
  [string]$Root = "",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Release",
  [ValidateSet("PersonalSideload", "ManagedTrusted")]
  [string]$ReleaseAudience = "PersonalSideload",
  [ValidateSet("PersonalSideloadCert", "PreviewSelfSigned", "ProvidedPfx", "StoreThumbprint")]
  [string]$SigningMode = "PersonalSideloadCert",
  [string]$OutDir = "",
  [string]$PublishedDir = "",
  [switch]$SkipPublish,
  [switch]$SkipFrontendConvergenceGate,
  [switch]$SkipWorkflowContractSyncGate,
  [switch]$SkipGovernanceControlPlaneBoundaryGate,
  [switch]$SkipOperatorCatalogSyncGate,
  [switch]$SkipGovernanceStoreSchemaVersionsGate,
  [switch]$SkipLocalWorkflowStoreSchemaVersionsGate,
  [switch]$SkipTemplatePackContractSyncGate,
  [switch]$SkipLocalTemplateStorageContractSyncGate,
  [switch]$SkipOfflineTemplateCatalogSyncGate,
  [switch]$SkipNativeWinuiSmokeGate,
  [string]$PublisherSubject = "CN=AIWF Dev",
  [string]$PackageIdentity = "AIWF.Native.WinUI",
  [string]$DisplayName = "AIWF Native WinUI",
  [string]$PublisherDisplayName = "AIWF",
  [string]$Description = "AIWF primary desktop frontend (MSIX preview)",
  [string]$MaxVersionTested = "10.0.26300.0",
  [string]$PfxPath = "",
  [string]$PfxPassword = "",
  [string]$CertificatePath = "",
  [string]$SigningThumbprint = "",
  [string]$TimestampUrl = "",
  [switch]$GenerateAppInstaller,
  [string]$AppInstallerUriBase = "",
  [int]$HoursBetweenUpdateChecks = 4,
  [switch]$KeepGeneratedCertificate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
. (Join-Path $PSScriptRoot "governance_capability_export_support.ps1")

function Extract-LastPathLine([object]$Output) {
  $text = [string]::Join("`n", @($Output))
  $matches = $text -split "`r?`n" | Where-Object { $_ -match '^[A-Za-z]:\\' }
  $last = $matches | Select-Object -Last 1
  if ($null -eq $last) { return "" }
  return [string]$last
}

function Resolve-SdkTool([string]$ToolName) {
  $paths = @(
    "C:\Program Files (x86)\Windows Kits\10\bin\10.0.26100.0\x64\$ToolName",
    "C:\Program Files (x86)\Windows Kits\10\App Certification Kit\$ToolName"
  )
  foreach ($path in $paths) {
    if (Test-Path $path) { return $path }
  }
  throw "$ToolName not found in Windows SDK"
}

function ConvertTo-PackageVersion([string]$VersionText) {
  $parts = [regex]::Matches($VersionText, '\d+') | ForEach-Object { [int]$_.Value }
  if ($parts.Count -ge 3) {
    return "{0}.{1}.{2}.0" -f $parts[0], $parts[1], $parts[2]
  }
  if ($parts.Count -eq 2) {
    return "{0}.{1}.0.0" -f $parts[0], $parts[1]
  }
  if ($parts.Count -eq 1) {
    return "{0}.0.0.0" -f $parts[0]
  }
  return "1.0.0.0"
}

function Get-CertificateSubject([string]$PfxPath, [string]$PfxPassword, [string]$CerPath) {
  if (-not [string]::IsNullOrWhiteSpace($CerPath) -and (Test-Path $CerPath)) {
    $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 $CerPath
    return [string]$cert.Subject
  }
  if (-not [string]::IsNullOrWhiteSpace($PfxPath) -and (Test-Path $PfxPath) -and (-not [string]::IsNullOrWhiteSpace($PfxPassword))) {
    $secure = ConvertTo-SecureString $PfxPassword -AsPlainText -Force
    $pfx = Get-PfxData -FilePath $PfxPath -Password $secure
    if ($pfx -and $pfx.EndEntityCertificates.Count -gt 0) {
      return [string]$pfx.EndEntityCertificates[0].Subject
    }
  }
  return ""
}

function Get-StoreCertificate([string]$Thumbprint) {
  if ([string]::IsNullOrWhiteSpace($Thumbprint)) { return $null }
  $normalized = ($Thumbprint -replace '\s', '').ToUpperInvariant()
  Get-ChildItem Cert:\CurrentUser\My | Where-Object { ($_.Thumbprint -replace '\s', '').ToUpperInvariant() -eq $normalized } | Select-Object -First 1
}

function New-IconPng([string]$Path, [int]$Width, [int]$Height, [string]$Text) {
  Add-Type -AssemblyName System.Drawing
  $bitmap = New-Object System.Drawing.Bitmap $Width, $Height
  $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
  $graphics.Clear([System.Drawing.Color]::FromArgb(255, 11, 79, 108))
  $fontSize = [math]::Max(14, [int]([math]::Min($Width, $Height) / 4))
  $font = New-Object System.Drawing.Font("Segoe UI", $fontSize, [System.Drawing.FontStyle]::Bold, [System.Drawing.GraphicsUnit]::Pixel)
  $brush = [System.Drawing.Brushes]::White
  $stringFormat = New-Object System.Drawing.StringFormat
  $stringFormat.Alignment = [System.Drawing.StringAlignment]::Center
  $stringFormat.LineAlignment = [System.Drawing.StringAlignment]::Center
  $graphics.DrawString($Text, $font, $brush, (New-Object System.Drawing.RectangleF 0, 0, $Width, $Height), $stringFormat)
  $bitmap.Save($Path, [System.Drawing.Imaging.ImageFormat]::Png)
  $graphics.Dispose()
  $font.Dispose()
  $bitmap.Dispose()
}

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if ($GenerateAppInstaller -and [string]::IsNullOrWhiteSpace($AppInstallerUriBase)) {
  throw "AppInstallerUriBase is required when GenerateAppInstaller is set"
}

$frontendGate = Join-Path $PSScriptRoot "check_frontend_convergence.ps1"
$workflowGate = Join-Path $PSScriptRoot "check_workflow_contract_sync.ps1"
$governanceCapabilityExportScript = Join-Path $PSScriptRoot "export_governance_capabilities.ps1"
$governanceControlPlaneBoundaryGate = Join-Path $PSScriptRoot "check_governance_control_plane_boundary.ps1"
$operatorGate = Join-Path $PSScriptRoot "check_operator_catalog_sync.ps1"
$governanceStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_governance_store_schema_versions.ps1"
$localWorkflowStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_local_workflow_store_schema_versions.ps1"
$templatePackContractSyncGate = Join-Path $PSScriptRoot "check_template_pack_contract_sync.ps1"
$localTemplateStorageContractSyncGate = Join-Path $PSScriptRoot "check_local_template_storage_contract_sync.ps1"
$offlineTemplateCatalogSyncGate = Join-Path $PSScriptRoot "check_offline_template_catalog_sync.ps1"
$winuiSmoke = Join-Path $PSScriptRoot "check_native_winui_smoke.ps1"
$ensurePersonalSideloadCert = Join-Path $PSScriptRoot "ensure_personal_sideload_certificate.ps1"
$publishScript = Join-Path $PSScriptRoot "publish_native_winui.ps1"
$makeAppx = Resolve-SdkTool "makeappx.exe"
$signTool = Resolve-SdkTool "signtool.exe"

if (-not $SkipFrontendConvergenceGate) {
  Info "running frontend convergence msix gate"
  powershell -ExecutionPolicy Bypass -File $frontendGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by frontend convergence gate" }
  Ok "frontend convergence msix gate passed"
}
if (-not $SkipWorkflowContractSyncGate) {
  Info "running workflow contract msix gate"
  powershell -ExecutionPolicy Bypass -File $workflowGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by workflow contract gate" }
  Ok "workflow contract msix gate passed"
}
$governanceCapabilityExportResult = Invoke-GovernanceCapabilityExportStep -ScriptPath $governanceCapabilityExportScript -FailureScope "msix"
if (-not $governanceCapabilityExportResult.ok) { throw [string]$governanceCapabilityExportResult.failure_message }
if (-not $SkipGovernanceControlPlaneBoundaryGate) {
  Info "running governance control plane boundary msix gate"
  powershell -ExecutionPolicy Bypass -File $governanceControlPlaneBoundaryGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by governance control plane boundary gate" }
  Ok "governance control plane boundary msix gate passed"
}
if (-not $SkipOperatorCatalogSyncGate) {
  Info "running operator catalog msix gate"
  powershell -ExecutionPolicy Bypass -File $operatorGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by operator catalog gate" }
  Ok "operator catalog msix gate passed"
}
if (-not $SkipGovernanceStoreSchemaVersionsGate) {
  Info "running governance store schema version msix gate"
  powershell -ExecutionPolicy Bypass -File $governanceStoreSchemaVersionsGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by governance store schema version gate" }
  Ok "governance store schema version msix gate passed"
}
if (-not $SkipLocalWorkflowStoreSchemaVersionsGate) {
  Info "running local workflow store schema version msix gate"
  powershell -ExecutionPolicy Bypass -File $localWorkflowStoreSchemaVersionsGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by local workflow store schema version gate" }
  Ok "local workflow store schema version msix gate passed"
}
if (-not $SkipTemplatePackContractSyncGate) {
  Info "running template pack contract msix gate"
  powershell -ExecutionPolicy Bypass -File $templatePackContractSyncGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by template pack contract gate" }
  Ok "template pack contract msix gate passed"
}
if (-not $SkipLocalTemplateStorageContractSyncGate) {
  Info "running local template storage contract msix gate"
  powershell -ExecutionPolicy Bypass -File $localTemplateStorageContractSyncGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by local template storage contract gate" }
  Ok "local template storage contract msix gate passed"
}
if (-not $SkipOfflineTemplateCatalogSyncGate) {
  Info "running offline template catalog msix gate"
  powershell -ExecutionPolicy Bypass -File $offlineTemplateCatalogSyncGate
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by offline template catalog gate" }
  Ok "offline template catalog msix gate passed"
}

if ([string]::IsNullOrWhiteSpace($PublishedDir)) {
  if ($SkipPublish) {
    throw "PublishedDir is required when SkipPublish is set"
  }
  $publishOutput = powershell -ExecutionPolicy Bypass -File $publishScript -Root $Root -Configuration $Configuration -Version $Version
  if ($LASTEXITCODE -ne 0) { throw "native winui publish step failed" }
  $PublishedDir = (Extract-LastPathLine $publishOutput).Trim()
}

if (-not (Test-Path $PublishedDir)) {
  throw "published dir not found: $PublishedDir"
}

$publishedExe = Join-Path $PublishedDir "WinUI3Bootstrap.exe"
if (-not (Test-Path $publishedExe)) {
  throw "published native winui executable not found: $publishedExe"
}

if (-not $SkipNativeWinuiSmokeGate) {
  Info "running native winui msix smoke gate"
  powershell -ExecutionPolicy Bypass -File $winuiSmoke -Root $Root -Configuration $Configuration -SkipBuild -ExePath $publishedExe
  if ($LASTEXITCODE -ne 0) { throw "msix blocked by native winui smoke gate" }
  Ok "native winui msix smoke gate passed"
}

if (-not $OutDir) {
  $OutDir = Join-Path $Root ("release\native_winui_msix_{0}" -f $Version)
}

$stageRoot = Join-Path $OutDir "stage"
$assetsRoot = Join-Path $stageRoot "Assets"
$installScriptPath = Join-Path $OutDir "Install_AIWF_Native_WinUI_MSIX.ps1"
$installCmdPath = Join-Path $OutDir "Install_AIWF_Native_WinUI_MSIX.cmd"
$packageVersion = ConvertTo-PackageVersion $Version
$msixPath = Join-Path $OutDir ("AIWF_Native_WinUI_{0}.msix" -f $Version)
$appInstallerPath = Join-Path $OutDir ("AIWF_Native_WinUI_{0}.appinstaller" -f $Version)
$generatedCerPath = Join-Path $OutDir "AIWF_Native_WinUI_Dev.cer"
$generatedPfxPath = Join-Path $OutDir "AIWF_Native_WinUI_Dev.pfx"
$manifestPath = Join-Path $stageRoot "AppxManifest.xml"
$notesPath = Join-Path $OutDir "RELEASE_NOTES.md"
$shaPath = Join-Path $OutDir "SHA256SUMS.txt"
$metadataPath = Join-Path $OutDir "manifest.json"
$trustedCert = $null
$generatedCertThumbprint = ""
$effectiveSigningThumbprint = ""
$importedProvidedPfxThumbprint = ""
$effectivePfxPath = ""
$effectivePfxPassword = ""
$effectiveCertificatePath = ""

if (Test-Path $OutDir) {
  Remove-Item $OutDir -Recurse -Force
}
New-Item -ItemType Directory -Path $assetsRoot -Force | Out-Null

Info "copying publish output into msix staging"
Copy-Item (Join-Path $PublishedDir "*") $stageRoot -Recurse -Force

New-IconPng -Path (Join-Path $assetsRoot "Square44x44Logo.png") -Width 44 -Height 44 -Text "AIWF"
New-IconPng -Path (Join-Path $assetsRoot "Square150x150Logo.png") -Width 150 -Height 150 -Text "AIWF"
New-IconPng -Path (Join-Path $assetsRoot "Wide310x150Logo.png") -Width 310 -Height 150 -Text "AIWF"
New-IconPng -Path (Join-Path $assetsRoot "LargeTile.png") -Width 310 -Height 310 -Text "AIWF"
New-IconPng -Path (Join-Path $assetsRoot "SplashScreen.png") -Width 620 -Height 300 -Text "AIWF"
Copy-Item (Join-Path $assetsRoot "Square150x150Logo.png") (Join-Path $assetsRoot "StoreLogo.png") -Force

if ($SigningMode -eq "PersonalSideloadCert") {
  $personalCertOutput = powershell -ExecutionPolicy Bypass -File $ensurePersonalSideloadCert
  if ($LASTEXITCODE -ne 0) { throw "personal sideload certificate ensure step failed" }
  $personalCert = ($personalCertOutput | Select-Object -Last 1 | ConvertFrom-Json)
  $effectiveSigningThumbprint = [string]$personalCert.thumbprint
  $effectiveCertificatePath = [string]$personalCert.cer_path
  $effectivePfxPath = [string]$personalCert.pfx_path
  if (Test-Path ([string]$personalCert.password_path)) {
    $effectivePfxPassword = (Get-Content ([string]$personalCert.password_path) -Raw -Encoding UTF8).Trim()
  }
  if ([string]::IsNullOrWhiteSpace($PublisherSubject) -or $PublisherSubject -eq "CN=AIWF Dev") {
    $PublisherSubject = [string]$personalCert.subject
  }
  $trustedCert = Import-Certificate -FilePath $effectiveCertificatePath -CertStoreLocation "Cert:\CurrentUser\TrustedPeople"
} elseif ($SigningMode -eq "PreviewSelfSigned") {
  Info "creating self-signed code-signing certificate for local msix packaging"
  $plainPassword = [Guid]::NewGuid().ToString("N")
  $securePassword = ConvertTo-SecureString $plainPassword -AsPlainText -Force
  $cert = New-SelfSignedCertificate -Type CodeSigningCert -Subject $PublisherSubject -CertStoreLocation "Cert:\CurrentUser\My" -KeyExportPolicy Exportable -KeySpec Signature -NotAfter (Get-Date).AddYears(2)
  $generatedCertThumbprint = $cert.Thumbprint
  Export-PfxCertificate -Cert ("Cert:\CurrentUser\My\" + $cert.Thumbprint) -FilePath $generatedPfxPath -Password $securePassword | Out-Null
  Export-Certificate -Cert ("Cert:\CurrentUser\My\" + $cert.Thumbprint) -FilePath $generatedCerPath | Out-Null
  $effectiveSigningThumbprint = $cert.Thumbprint
  $effectivePfxPath = $generatedPfxPath
  $effectivePfxPassword = $plainPassword
  $effectiveCertificatePath = $generatedCerPath
  $trustedCert = Import-Certificate -FilePath $effectiveCertificatePath -CertStoreLocation "Cert:\CurrentUser\TrustedPeople"
} else {
  if ($SigningMode -eq "ProvidedPfx") {
    if ([string]::IsNullOrWhiteSpace($PfxPath) -or -not (Test-Path $PfxPath)) {
      throw "PfxPath is required and must exist when SigningMode=ProvidedPfx"
    }
    $effectivePfxPath = (Resolve-Path $PfxPath).Path
    $effectivePfxPassword = if ([string]::IsNullOrWhiteSpace($PfxPassword)) { [string]$env:AIWF_MSIX_PFX_PASSWORD } else { $PfxPassword }
    if ([string]::IsNullOrWhiteSpace($effectivePfxPassword)) {
      throw "PfxPassword is required when SigningMode=ProvidedPfx (or set AIWF_MSIX_PFX_PASSWORD)"
    }
    $secureProvidedPassword = ConvertTo-SecureString $effectivePfxPassword -AsPlainText -Force
    $importedProvided = Import-PfxCertificate -FilePath $effectivePfxPath -CertStoreLocation "Cert:\CurrentUser\My" -Password $secureProvidedPassword -Exportable
    if (-not $importedProvided -or -not $importedProvided.Thumbprint) {
      throw "failed to import provided pfx into CurrentUser\\My for signing"
    }
    $importedProvidedPfxThumbprint = $importedProvided.Thumbprint
    $effectiveSigningThumbprint = $importedProvidedPfxThumbprint
    $providedSubject = Get-CertificateSubject -PfxPath $effectivePfxPath -PfxPassword $effectivePfxPassword -CerPath $CertificatePath
    if ([string]::IsNullOrWhiteSpace($PublisherSubject) -or $PublisherSubject -eq "CN=AIWF Dev") {
      if ([string]::IsNullOrWhiteSpace($providedSubject)) {
        throw "could not infer PublisherSubject from provided certificate"
      }
      $PublisherSubject = $providedSubject
    }
    if (-not [string]::IsNullOrWhiteSpace($CertificatePath)) {
      if (-not (Test-Path $CertificatePath)) {
        throw "CertificatePath not found: $CertificatePath"
      }
      $effectiveCertificatePath = (Resolve-Path $CertificatePath).Path
      $trustedCert = Import-Certificate -FilePath $effectiveCertificatePath -CertStoreLocation "Cert:\CurrentUser\TrustedPeople"
    } else {
      $generatedCertPath = Join-Path $OutDir "AIWF_Native_WinUI_Signing.cer"
      $secureExportPassword = ConvertTo-SecureString $effectivePfxPassword -AsPlainText -Force
      $pfxData = Get-PfxData -FilePath $effectivePfxPath -Password $secureExportPassword
      if ($pfxData -and $pfxData.EndEntityCertificates.Count -gt 0) {
        Export-Certificate -Cert $pfxData.EndEntityCertificates[0] -FilePath $generatedCertPath | Out-Null
        $effectiveCertificatePath = $generatedCertPath
        $trustedCert = Import-Certificate -FilePath $effectiveCertificatePath -CertStoreLocation "Cert:\CurrentUser\TrustedPeople"
      }
    }
  } else {
    $storeCert = Get-StoreCertificate $SigningThumbprint
    if (-not $storeCert) {
      throw "SigningThumbprint not found in Cert:\\CurrentUser\\My"
    }
    $effectiveSigningThumbprint = $storeCert.Thumbprint
    if ([string]::IsNullOrWhiteSpace($PublisherSubject) -or $PublisherSubject -eq "CN=AIWF Dev") {
      $PublisherSubject = [string]$storeCert.Subject
    }
    if (-not [string]::IsNullOrWhiteSpace($CertificatePath)) {
      if (-not (Test-Path $CertificatePath)) {
        throw "CertificatePath not found: $CertificatePath"
      }
      $effectiveCertificatePath = (Resolve-Path $CertificatePath).Path
    } else {
      $generatedCertPath = Join-Path $OutDir "AIWF_Native_WinUI_Signing.cer"
      Export-Certificate -Cert $storeCert -FilePath $generatedCertPath | Out-Null
      $effectiveCertificatePath = $generatedCertPath
    }
    $trustedCert = Import-Certificate -FilePath $effectiveCertificatePath -CertStoreLocation "Cert:\CurrentUser\TrustedPeople"
  }
}

$manifestXml = @"
<?xml version="1.0" encoding="utf-8"?>
<Package
  xmlns="http://schemas.microsoft.com/appx/manifest/foundation/windows10"
  xmlns:uap="http://schemas.microsoft.com/appx/manifest/uap/windows10"
  xmlns:desktop="http://schemas.microsoft.com/appx/manifest/desktop/windows10"
  xmlns:rescap="http://schemas.microsoft.com/appx/manifest/foundation/windows10/restrictedcapabilities"
  IgnorableNamespaces="uap desktop rescap">
  <Identity Name="$PackageIdentity" Publisher="$PublisherSubject" Version="$packageVersion" ProcessorArchitecture="x64" />
  <Properties>
    <DisplayName>$DisplayName</DisplayName>
    <PublisherDisplayName>$PublisherDisplayName</PublisherDisplayName>
    <Description>$Description</Description>
    <Logo>Assets\StoreLogo.png</Logo>
  </Properties>
  <Resources>
    <Resource Language="en-US" />
  </Resources>
  <Dependencies>
    <TargetDeviceFamily Name="Windows.Desktop" MinVersion="10.0.19041.0" MaxVersionTested="$MaxVersionTested" />
  </Dependencies>
  <Applications>
    <Application Id="AIWFNativeWinUI" Executable="WinUI3Bootstrap.exe" EntryPoint="Windows.FullTrustApplication">
      <uap:VisualElements
        DisplayName="$DisplayName"
        Description="$Description"
        BackgroundColor="transparent"
        Square150x150Logo="Assets\Square150x150Logo.png"
        Square44x44Logo="Assets\Square44x44Logo.png">
        <uap:DefaultTile Wide310x150Logo="Assets\Wide310x150Logo.png" Square310x310Logo="Assets\LargeTile.png" />
        <uap:SplashScreen Image="Assets\SplashScreen.png" />
      </uap:VisualElements>
      <Extensions>
        <desktop:Extension Category="windows.fullTrustProcess" Executable="WinUI3Bootstrap.exe" />
      </Extensions>
    </Application>
  </Applications>
  <Capabilities>
    <Capability Name="internetClient" />
    <rescap:Capability Name="runFullTrust" />
  </Capabilities>
</Package>
"@
$manifestXml | Set-Content $manifestPath -Encoding UTF8

Info "packing msix"
& $makeAppx pack /o /d $stageRoot /p $msixPath | Out-Host
if ($LASTEXITCODE -ne 0) { throw "makeappx pack failed" }

Info "signing msix"
$signArgs = @("sign", "/fd", "SHA256")
if (-not [string]::IsNullOrWhiteSpace($effectiveSigningThumbprint)) {
  $signArgs += @("/sha1", $effectiveSigningThumbprint, "/s", "My")
} else {
  $signArgs += @("/f", $effectivePfxPath, "/p", $effectivePfxPassword)
}
if (-not [string]::IsNullOrWhiteSpace($TimestampUrl)) {
  $signArgs += @("/tr", $TimestampUrl, "/td", "SHA256")
}
$signArgs += $msixPath
& $signTool @signArgs | Out-Host
if ($LASTEXITCODE -ne 0) { throw "signtool sign failed" }

Info "verifying msix signature"
$verifyOutput = cmd /c "`"$signTool`" verify /pa `"$msixPath`" 2>&1"
$verifyCode = $LASTEXITCODE
if ($verifyCode -ne 0) {
  $verifyText = [string]::Join("`n", @($verifyOutput))
  if ($verifyText -match "not trusted by the trust provider") {
    Warn "msix signature exists but the self-signed certificate is not trusted by the local root store yet; installation script will import the exported .cer before Add-AppxPackage"
  } else {
    $verifyOutput | Out-Host
    throw "signtool verify failed"
  }
} else {
  $verifyOutput | Out-Host
}

$installScript = @"
param(
  [string]`$PackagePath = "",
  [string]`$CertificatePath = ""
)

Set-StrictMode -Version Latest
`$ErrorActionPreference = "Stop"

if (-not `$PackagePath) {
  `$PackagePath = Join-Path (Split-Path -Parent `$MyInvocation.MyCommand.Path) "$(Split-Path $msixPath -Leaf)"
}
if (-not `$CertificatePath) {
  `$CertificatePath = Join-Path (Split-Path -Parent `$MyInvocation.MyCommand.Path) "$(if ($effectiveCertificatePath) { Split-Path $effectiveCertificatePath -Leaf } else { "" })"
}
if (`$CertificatePath -and (Test-Path `$CertificatePath)) {
  Import-Certificate -FilePath `$CertificatePath -CertStoreLocation Cert:\CurrentUser\TrustedPeople | Out-Null
}
Add-AppxPackage -Path `$PackagePath
Write-Host "[ OK ] native winui msix installed" -ForegroundColor Green
"@
$installScript | Set-Content $installScriptPath -Encoding UTF8

$installCmd = @(
  "@echo off",
  "powershell -ExecutionPolicy Bypass -File ""%~dp0Install_AIWF_Native_WinUI_MSIX.ps1"" %*"
)
$installCmd | Set-Content $installCmdPath -Encoding ASCII

$notes = @(
  "# Release Notes",
  "",
  "- Version: $Version",
  "- PackageVersion: $packageVersion",
  "- Format: MSIX",
  "- ReleaseAudience: $ReleaseAudience",
  "- Publisher: $PublisherSubject",
  "- SigningMode: $SigningMode",
  "",
  $(if ($ReleaseAudience -eq "PersonalSideload") { "This MSIX is intended for personal/friend sideload distribution." } else { "This MSIX is intended for a managed trusted distribution channel." }),
  $(if ($SigningMode -eq "PreviewSelfSigned") { "This is a self-signed local MSIX preview for the primary WinUI frontend." } else { "This MSIX uses a trusted signing path for the primary WinUI frontend." })
)
$notes | Set-Content $notesPath -Encoding UTF8

$metadata = [ordered]@{
  product = "AIWF Native WinUI"
  version = $Version
  package_version = $packageVersion
  format = "msix"
  release_audience = $ReleaseAudience
  signing_mode = $SigningMode
  msix = (Split-Path $msixPath -Leaf)
  certificate = $(if ($effectiveCertificatePath) { Split-Path $effectiveCertificatePath -Leaf } else { "" })
  install_script = (Split-Path $installScriptPath -Leaf)
  appinstaller = $(if ($GenerateAppInstaller) { Split-Path $appInstallerPath -Leaf } else { "" })
  generated_at = (Get-Date).ToString("s")
}
($metadata | ConvertTo-Json -Depth 4) | Set-Content $metadataPath -Encoding UTF8

if ($GenerateAppInstaller) {
  $trimmedBase = $AppInstallerUriBase.TrimEnd('/')
  $appInstallerLeaf = Split-Path $appInstallerPath -Leaf
  $msixLeaf = Split-Path $msixPath -Leaf
  $mainPackageUri = "$trimmedBase/$msixLeaf"
  $appInstallerUri = "$trimmedBase/$appInstallerLeaf"
  $appInstallerXml = @"
<?xml version="1.0" encoding="utf-8"?>
<AppInstaller Uri="$appInstallerUri" Version="$packageVersion" xmlns="http://schemas.microsoft.com/appx/appinstaller/2018">
  <MainPackage Name="$PackageIdentity" Publisher="$PublisherSubject" Version="$packageVersion" Uri="$mainPackageUri" />
  <UpdateSettings>
    <OnLaunch HoursBetweenUpdateChecks="$HoursBetweenUpdateChecks" ShowPrompt="true" UpdateBlocksActivation="false" />
    <AutomaticBackgroundTask />
  </UpdateSettings>
</AppInstaller>
"@
  $appInstallerXml | Set-Content $appInstallerPath -Encoding UTF8
}

$shaLines = @()
foreach ($path in @($msixPath, $effectiveCertificatePath, $installScriptPath, $installCmdPath, $notesPath, $metadataPath, $(if ($GenerateAppInstaller) { $appInstallerPath } else { $null }))) {
  if ([string]::IsNullOrWhiteSpace($path) -or -not (Test-Path $path)) { continue }
  $hash = (Get-FileHash $path -Algorithm SHA256).Hash
  $shaLines += ("{0}  {1}" -f $hash, (Split-Path $path -Leaf))
}
$shaLines | Set-Content $shaPath -Encoding ASCII

if (-not $KeepGeneratedCertificate) {
  if ($trustedCert -and $trustedCert.Thumbprint) {
    Remove-Item ("Cert:\CurrentUser\TrustedPeople\" + $trustedCert.Thumbprint) -Force -ErrorAction SilentlyContinue
  }
  if ($importedProvidedPfxThumbprint) {
    Remove-Item ("Cert:\CurrentUser\My\" + $importedProvidedPfxThumbprint) -Force -ErrorAction SilentlyContinue
  }
  if ($generatedCertThumbprint) {
    Remove-Item ("Cert:\CurrentUser\My\" + $generatedCertThumbprint) -Force -ErrorAction SilentlyContinue
  }
}

Ok ("native winui msix ready: " + $msixPath)
Write-Output $msixPath
