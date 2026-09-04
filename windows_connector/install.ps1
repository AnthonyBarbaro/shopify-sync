param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot "connector.env"),
    [string]$TaskName = "Shopify POS Inventory Connector",
    [string]$PythonPath = "",
    [switch]$StartNow
)

$ErrorActionPreference = "Stop"
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this installer as Administrator."
}

$config = (Resolve-Path -LiteralPath $ConfigPath).Path
$connector = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "connector.py")).Path
$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$reader = Join-Path $projectRoot "jbarbaro_db\dbf_pos_sync.py"
if (-not (Test-Path -LiteralPath $reader -PathType Leaf)) {
    throw "Missing POS reader: $reader"
}

if ($PythonPath) {
    $python = (Resolve-Path -LiteralPath $PythonPath).Path
}
else {
    $launcher = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($launcher) {
        $python = (& $launcher.Source -3 -c "import sys; print(sys.executable)" | Select-Object -Last 1).Trim()
    }
    else {
        $pythonCommand = Get-Command python.exe -ErrorAction Stop
        $python = $pythonCommand.Source
    }
}

$venvPath = Join-Path $PSScriptRoot ".venv"
& $python -m venv $venvPath
if ($LASTEXITCODE -ne 0) {
    throw "Could not create the connector Python environment."
}
$venvPython = (Resolve-Path -LiteralPath (Join-Path $venvPath "Scripts\python.exe")).Path
$requirements = Join-Path $PSScriptRoot "requirements.txt"
if (Test-Path -LiteralPath $requirements -PathType Leaf) {
    & $venvPython -m pip install --disable-pip-version-check --requirement $requirements
}
else {
    & $venvPython -m pip install --disable-pip-version-check "requests>=2.32,<3"
}
if ($LASTEXITCODE -ne 0) {
    throw "Could not install the connector dependency. Check this computer's internet or proxy settings."
}

& $venvPython $connector --config $config --validate
if ($LASTEXITCODE -ne 0) {
    throw "Connector validation failed. Review the setup message and $config."
}

Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$arguments = '"{0}" --config "{1}"' -f $connector, $config
$action = New-ScheduledTaskAction -Execute $venvPython -Argument $arguments -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger -AtStartup
$trigger.Delay = "PT1M"
$taskSettings = New-ScheduledTaskSettingsSet `
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
    -Settings $taskSettings `
    -Principal $taskPrincipal `
    -Description "Synchronizes the store POS, Shopify inventory, prices, and order inbox." `
    -Force | Out-Null

if ($StartNow) {
    Start-ScheduledTask -TaskName $TaskName
}

Write-Host "Prepared scheduled task: $TaskName"
Write-Host "Config: $config"
Write-Host "Log: C:\ProgramData\ShopifyPosConnector\connector.log"
