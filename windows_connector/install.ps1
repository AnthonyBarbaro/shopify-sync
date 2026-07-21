param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot "connector.env"),
    [string]$TaskName = "Shopify POS Inventory Connector",
    [switch]$StartNow
)

$ErrorActionPreference = "Stop"
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this installer from PowerShell as Administrator."
}

$config = (Resolve-Path -LiteralPath $ConfigPath).Path
$connector = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "connector.py")).Path
$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$reader = Join-Path $projectRoot "jbarbaro_db\dbf_pos_sync.py"
if (-not (Test-Path -LiteralPath $reader)) {
    throw "Missing POS reader: $reader"
}

$launcher = Get-Command py.exe -ErrorAction SilentlyContinue
if ($launcher) {
    & $launcher.Source -3 -m venv (Join-Path $PSScriptRoot ".venv")
}
else {
    $python = Get-Command python.exe -ErrorAction Stop
    & $python.Source -m venv (Join-Path $PSScriptRoot ".venv")
}

$venvPython = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot ".venv\Scripts\python.exe")).Path
& $venvPython -m pip install --disable-pip-version-check "requests>=2.32,<3"
if ($LASTEXITCODE -ne 0) {
    throw "Could not install the connector dependency."
}

& $venvPython $connector --config $config --once --dry-run
if ($LASTEXITCODE -ne 0) {
    throw "Connector validation failed. Review connector.env and the console output."
}

$arguments = '"{0}" --config "{1}"' -f $connector, $config
$action = New-ScheduledTaskAction -Execute $venvPython -Argument $arguments -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger -AtStartup
$trigger.Delay = "PT1M"
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries
$taskPrincipal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $taskPrincipal `
    -Description "Reads POS DBF inventory every three minutes and reconciles it with Shopify." `
    -Force | Out-Null

if ($StartNow) {
    Start-ScheduledTask -TaskName $TaskName
}

Write-Host "Installed scheduled task: $TaskName"
Write-Host "Config: $config"
Write-Host "Log: C:\ProgramData\ShopifyPosConnector\connector.log (unless CONNECTOR_DATA_DIR was changed)"
if (-not $StartNow) {
    Write-Host "The connector will start one minute after the next boot. Use Start-ScheduledTask to start it now."
}
