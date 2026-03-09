param(
  [string]$Root = "",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Release",
  [int]$TimeoutSeconds = 20,
  [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

function Resolve-NativeWinUiExePath([string]$Root, [string]$Configuration) {
  $candidates = @(
    (Join-Path $Root ("apps\dify-native-winui\src\WinUI3Bootstrap\bin\x64\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64\WinUI3Bootstrap.exe")),
    (Join-Path $Root ("apps\dify-native-winui\src\WinUI3Bootstrap\bin\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64\WinUI3Bootstrap.exe"))
  )

  return $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
}

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$projectPath = Join-Path $Root "apps\dify-native-winui\src\WinUI3Bootstrap\WinUI3Bootstrap.csproj"
if (-not (Test-Path $projectPath)) {
  throw "native winui project not found: $projectPath"
}

try {
  dotnet --info | Out-Null
}
catch {
  throw "dotnet SDK not found. Install .NET SDK 8+ and Windows App SDK prerequisites first."
}

if (-not $SkipBuild) {
  Info "building native winui project"
  dotnet build $projectPath -c $Configuration -p:Platform=x64
  if ($LASTEXITCODE -ne 0) {
    throw "native winui build failed"
  }
  Ok "native winui build passed"
}

$exePath = Resolve-NativeWinUiExePath -Root $Root -Configuration $Configuration
if (-not $exePath) {
  throw "native winui executable not found: $exePath"
}

Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class NativeMouse
{
    [DllImport("user32.dll", SetLastError = true)]
    public static extern bool SetCursorPos(int x, int y);

    [DllImport("user32.dll", SetLastError = true)]
    public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);
}
"@

function Wait-Until([string]$Label, [int]$TimeoutSec, [scriptblock]$Probe) {
  $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSec)
  while ([DateTime]::UtcNow -lt $deadline) {
    $value = & $Probe
    if ($null -ne $value) {
      return $value
    }

    Start-Sleep -Milliseconds 200
  }

  throw "timeout waiting for $Label"
}

function Get-MainWindow([int]$ProcessId) {
  $root = [System.Windows.Automation.AutomationElement]::RootElement
  $processCondition = New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::ProcessIdProperty,
    $ProcessId)
  $windowCondition = New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
    [System.Windows.Automation.ControlType]::Window)
  $condition = New-Object System.Windows.Automation.AndCondition($processCondition, $windowCondition)
  return $root.FindFirst([System.Windows.Automation.TreeScope]::Children, $condition)
}

function Find-ElementByAutomationId($RootElement, [string]$AutomationId) {
  if ($null -eq $RootElement) {
    return $null
  }

  $automationIdCondition = New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
    $AutomationId)
  return $RootElement.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $automationIdCondition)
}

function Wait-VisibleElement($RootElement, [string]$AutomationId, [int]$TimeoutSec) {
  return Wait-Until "element '$AutomationId'" $TimeoutSec {
    $element = Find-ElementByAutomationId $RootElement $AutomationId
    if ($null -eq $element) {
      return $null
    }

    if ($element.Current.IsOffscreen) {
      return $null
    }

    return $element
  }
}

function Invoke-Element($Element, [string]$Label) {
  $patternObject = $null
  if (-not $Element.TryGetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern, [ref]$patternObject)) {
    throw "element '$Label' does not support InvokePattern"
  }

  try {
    ([System.Windows.Automation.InvokePattern]$patternObject).Invoke()
  }
  catch {
    try {
      Start-Sleep -Milliseconds 300
      ([System.Windows.Automation.InvokePattern]$patternObject).Invoke()
    }
    catch {
      try {
        $Element.SetFocus()
        Start-Sleep -Milliseconds 150
        [System.Windows.Forms.SendKeys]::SendWait("{ENTER}")
      }
      catch {
        $rect = $Element.Current.BoundingRectangle
        $centerX = [int][Math]::Round($rect.Left + ($rect.Width / 2.0))
        $centerY = [int][Math]::Round($rect.Top + ($rect.Height / 2.0))
        [NativeMouse]::SetCursorPos($centerX, $centerY) | Out-Null
        Start-Sleep -Milliseconds 120
        [NativeMouse]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)
        [NativeMouse]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
      }
    }
  }
}

function Set-ElementValue($Element, [string]$Value, [string]$Label) {
  $patternObject = $null
  if (-not $Element.TryGetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern, [ref]$patternObject)) {
    throw "element '$Label' does not support ValuePattern"
  }

  ([System.Windows.Automation.ValuePattern]$patternObject).SetValue($Value)
}

function Get-ElementValue($Element, [string]$Label) {
  $patternObject = $null
  if (-not $Element.TryGetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern, [ref]$patternObject)) {
    throw "element '$Label' does not support ValuePattern"
  }

  return ([System.Windows.Automation.ValuePattern]$patternObject).Current.Value
}

$env:AIWF_NATIVE_UIA_SMOKE = "1"
$proc = Start-Process -FilePath $exePath -PassThru
try {
  Info ("launched native winui app, pid=" + $proc.Id)
  $window = Wait-Until "main window" $TimeoutSeconds { Get-MainWindow -ProcessId $proc.Id }
  Info ("attached to window: " + $window.Current.Name)

  $workspaceNav = Wait-VisibleElement $window "WorkspaceNavButton" $TimeoutSeconds
  $canvasNav = Wait-VisibleElement $window "CanvasNavButton" $TimeoutSeconds
  $resultNav = Wait-VisibleElement $window "ResultNavButton" $TimeoutSeconds

  $bridgeUrlTextBox = Wait-VisibleElement $window "BridgeUrlTextBox" $TimeoutSeconds
  $ownerTextBox = Wait-VisibleElement $window "OwnerTextBox" $TimeoutSeconds
  $canvasStatePath = Join-Path ([Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)) "AIWF\\canvas-workflow.json"
  $canvasStateBackupPath = $canvasStatePath + ".uia-smoke-backup"
  if (Test-Path $canvasStateBackupPath) {
    Remove-Item $canvasStateBackupPath -Force -ErrorAction SilentlyContinue
  }

  if (Test-Path $canvasStatePath) {
    Move-Item -Force $canvasStatePath $canvasStateBackupPath
  }

  Set-ElementValue $ownerTextBox "uia-smoke-owner" "OwnerTextBox"
  Ok "workspace controls are visible and editable"

  Invoke-Element $canvasNav "CanvasNavButton"
  $addNodeButton = Wait-VisibleElement $window "AddNodeButton" $TimeoutSeconds
  $canvasFitButton = Wait-VisibleElement $window "CanvasFitButton" $TimeoutSeconds
  $newCanvasButton = Wait-VisibleElement $window "NewCanvasButton" $TimeoutSeconds
  $saveCanvasButton = Wait-VisibleElement $window "SaveCanvasButton" $TimeoutSeconds

  Ok "canvas section is visible"
  $canvasSelectionInfo = Wait-VisibleElement $window "CanvasSelectionInfoTextBlock" $TimeoutSeconds
  $null = Wait-Until "canvas snapshot file" $TimeoutSeconds {
    if (-not (Test-Path $canvasStatePath)) {
      return $null
    }

    return Get-Item $canvasStatePath
  }
  Ok "canvas commands produced a snapshot file"

  Invoke-Element $resultNav "ResultNavButton"
  $artifactsListView = Wait-VisibleElement $window "ArtifactsListView" $TimeoutSeconds
  $runResultTextBlock = Wait-VisibleElement $window "RunResultTextBlock" $TimeoutSeconds
  $jobIdTextBlock = Wait-VisibleElement $window "JobIdTextBlock" $TimeoutSeconds
  $retryInfoTextBlock = Wait-VisibleElement $window "RetryInfoTextBlock" $TimeoutSeconds
  $runModeTextBlock = Wait-VisibleElement $window "RunModeTextBlock" $TimeoutSeconds
  $durationTextBlock = Wait-VisibleElement $window "DurationTextBlock" $TimeoutSeconds
  $okMetricTextBlock = Wait-VisibleElement $window "OkMetricTextBlock" $TimeoutSeconds
  $modeMetricTextBlock = Wait-VisibleElement $window "ModeMetricTextBlock" $TimeoutSeconds
  $durationMetricTextBlock = Wait-VisibleElement $window "DurationMetricTextBlock" $TimeoutSeconds
  Ok "results section is visible"

  Invoke-Element $workspaceNav "WorkspaceNavButton"
  $bridgeUrlTextBoxAgain = Wait-VisibleElement $window "BridgeUrlTextBox" $TimeoutSeconds
  $ownerTextBoxAgain = Wait-VisibleElement $window "OwnerTextBox" $TimeoutSeconds
  if ((Get-ElementValue $ownerTextBoxAgain "OwnerTextBox") -ne "uia-smoke-owner") {
    throw "owner textbox value was not updated by UIA smoke interaction"
  }

  Ok "workspace section is visible after round-trip navigation"
  Ok "native winui UIA smoke passed"
}
finally {
  $canvasStatePath = Join-Path ([Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)) "AIWF\\canvas-workflow.json"
  $canvasStateBackupPath = $canvasStatePath + ".uia-smoke-backup"
  if (Test-Path $canvasStateBackupPath) {
    Move-Item -Force $canvasStateBackupPath $canvasStatePath
  }

  if ($null -ne $proc -and -not $proc.HasExited) {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
  }
  Remove-Item Env:AIWF_NATIVE_UIA_SMOKE -ErrorAction SilentlyContinue
}
