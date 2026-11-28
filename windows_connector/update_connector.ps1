param(
    [string]$ProjectRoot = "C:\ShopifySync",
    [string]$TaskName = "Shopify POS Inventory Connector",
    [string]$ConfigPath,
    [switch]$RefreshUpdater
)

$ErrorActionPreference = "Stop"
$ProjectRoot = [IO.Path]::GetFullPath($ProjectRoot)
$runtimeDirectory = Join-Path $ProjectRoot "windows_connector"
if (-not $ConfigPath) {
    $ConfigPath = Join-Path $runtimeDirectory "connector.env"
}
$repositoryBase = "https://raw.githubusercontent.com/AnthonyBarbaro/shopify-sync/main"
$python = Join-Path $runtimeDirectory ".venv/Scripts/python.exe"

function Get-ConnectorSettings {
    $settings = @{}
    if (Test-Path -LiteralPath $ConfigPath -PathType Leaf) {
        foreach ($line in Get-Content -LiteralPath $ConfigPath) {
            if ($line.Trim() -match '^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$') {
                $key = $Matches[1]
                $value = $Matches[2].Trim()
                if ($value.Length -ge 2 -and $value[0] -eq $value[-1] -and $value[0] -in @('"', "'")) {
                    $value = $value.Substring(1, $value.Length - 2)
                }
                if (-not $settings.ContainsKey($key)) {
                    $settings[$key] = $value
                }
            }
        }
    }
    foreach ($name in @("SHOPIFY_SYNC_BASE_URL", "SHOPIFY_SYNC_API_KEY", "SHOPIFY_SYNC_API_SECRET", "CONNECTOR_DATA_DIR")) {
        $value = [Environment]::GetEnvironmentVariable($name)
        if ($null -ne $value) {
            $settings[$name] = $value
        }
    }
    return $settings
}

function Send-UpdaterReport {
    param(
        [string]$Status,
        [string]$Stage,
        [string]$File,
        [System.Management.Automation.ErrorRecord]$Failure,
        [int]$FilesUpdated = 0
    )

    $runId = [Guid]::NewGuid().ToString()
    $report = @{
        run_id = $runId
        occurred_at = [DateTime]::UtcNow.ToString("o")
        status = $Status
        stage = $Stage
    }
    if ($File) { $report.file = $File }
    if ($Status -eq "succeeded") { $report.files_updated = $FilesUpdated }
    if ($Failure) {
        if ($Failure.Exception.Response -and $Failure.Exception.Response.StatusCode) {
            $report.error_code = "HTTP-$([int]$Failure.Exception.Response.StatusCode)"
        }
        else {
            $report.error_code = $Failure.Exception.GetType().Name
        }
    }
    $payload = $report | ConvertTo-Json -Compress

    $settings = @{}
    try {
        $settings = Get-ConnectorSettings
    }
    catch {
        Write-Warning "Could not read connector settings for updater reporting."
    }
    $dataDirectory = $settings["CONNECTOR_DATA_DIR"]
    if (-not $dataDirectory) { $dataDirectory = Join-Path $runtimeDirectory "data" }
    $reportPath = $null
    try {
        $reports = Join-Path $dataDirectory "updater-reports"
        New-Item -ItemType Directory -Path $reports -Force | Out-Null
        $reportPath = Join-Path $reports "$runId.json"
        [IO.File]::WriteAllText("$reportPath.tmp", $payload)
        Move-Item -LiteralPath "$reportPath.tmp" -Destination $reportPath
    }
    catch {
        Write-Warning "Could not save a local copy of the updater report."
    }

    $url = [string]$settings["SHOPIFY_SYNC_BASE_URL"]
    $key = [string]$settings["SHOPIFY_SYNC_API_KEY"]
    $secret = [string]$settings["SHOPIFY_SYNC_API_SECRET"]
    try {
        if (-not $url -or -not $key -or -not $secret) {
            throw "Connector reporting credentials are unavailable."
        }
        $response = Invoke-RestMethod -UseBasicParsing -Method Post `
            -Uri "$($url.TrimEnd('/'))/sync/connector/update-report" `
            -Headers @{"X-API-Key" = $key; "X-API-Secret" = $secret} `
            -ContentType "application/json" -Body $payload -TimeoutSec 10 -MaximumRedirection 0
        if ($response.accepted -ne $true) {
            throw "The dashboard did not accept the updater report."
        }
        if ($reportPath) { Remove-Item -LiteralPath $reportPath -Force -ErrorAction SilentlyContinue }
    }
    catch {
        Write-Warning "Could not deliver the updater report to the Shopify dashboard."
    }
}

function Assert-PowerShellFile {
    param([string]$Path)
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$parseErrors) | Out-Null
    if ($parseErrors.Count) {
        throw "PowerShell validation failed for ${Path}: $($parseErrors | Out-String)"
    }
}

if ($RefreshUpdater) {
    $destination = Join-Path $runtimeDirectory "update_connector.ps1"
    $download = "$destination.$([Guid]::NewGuid().ToString()).download"
    try {
        Write-Host "Downloading the latest Windows updater..."
        Invoke-WebRequest -UseBasicParsing "$repositoryBase/windows_connector/update_connector.ps1" -OutFile $download -TimeoutSec 60
        Assert-PowerShellFile -Path $download
        Move-Item -LiteralPath $download -Destination $destination -Force
    }
    catch {
        Send-UpdaterReport -Status failed -Stage refresh_updater -File "update_connector.ps1" -Failure $_
        throw
    }
    finally {
        Remove-Item -LiteralPath $download -Force -ErrorAction SilentlyContinue
    }
    & $destination -ProjectRoot $ProjectRoot -TaskName $TaskName -ConfigPath $ConfigPath
    return
}

$files = @(
    "windows_connector/connector.py",
    "windows_connector/order_dbf.py",
    "windows_connector/sales_board.py",
    "jbarbaro_db/dbf_pos_sync.py",
    "windows_connector/write_pos_quantity.ps1",
    "windows_connector/install.ps1",
    "windows_connector/uninstall.ps1",
    "windows_connector/connector.env.example",
    "windows_connector/README.md",
    "windows_connector/SHOPIFY_ORDER_DB_SCHEMA.md"
)

$downloads = @()
$installed = @()
$taskStopped = $false
$taskWasEnabled = $false
$updateLock = $null
$stage = "preflight"
$file = $null
$updated = $false

try {
    $updateLock = [IO.File]::Open((Join-Path $runtimeDirectory "update.lock"), "OpenOrCreate", "ReadWrite", "None")
    if (-not (Test-Path -LiteralPath $python)) {
        throw "Missing connector Python environment. Run windows_connector\install.ps1 first."
    }

    try {
        $stage = "download"
        Write-Host "Downloading Windows connector runtime files..."
        foreach ($relativePath in $files) {
            $destination = Join-Path $ProjectRoot $relativePath
            $file = Split-Path -Leaf $destination
            $download = "$destination.download"
            $downloads += [pscustomobject]@{
                Destination = $destination
                Download = $download
                Backup = "$destination.backup"
                HadOriginal = Test-Path -LiteralPath $destination
            }
            New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
            Invoke-WebRequest -UseBasicParsing "$repositoryBase/$relativePath" -OutFile $download -TimeoutSec 60
        }

        $stage = "validate_download"
        Write-Host "Validating downloaded Python and PowerShell files..."
        foreach ($item in $downloads) {
            $file = Split-Path -Leaf $item.Destination
            if ($item.Destination.EndsWith(".py")) {
                & $python -m py_compile $item.Download
                if ($LASTEXITCODE -ne 0) {
                    throw "Python validation failed for $($item.Destination)"
                }
            }
            elseif ($item.Destination.EndsWith(".ps1")) {
                Assert-PowerShellFile -Path $item.Download
            }
        }
        $file = $null

        $stage = "stop"
        Write-Host "Stopping Shopify connector..."
        $existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        $taskWasEnabled = $existingTask.Settings.Enabled
        Disable-ScheduledTask -TaskName $TaskName | Out-Null
        $taskStopped = $true
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction Stop | Out-Null
        $stopDeadline = [DateTime]::UtcNow.AddSeconds(15)
        while ((Get-ScheduledTask -TaskName $TaskName).State -eq "Running") {
            if ([DateTime]::UtcNow -ge $stopDeadline) {
                throw "The scheduled connector task did not stop."
            }
            Start-Sleep -Milliseconds 250
        }

        $stage = "install"
        Write-Host "Backing up and installing updates..."
        foreach ($item in $downloads) {
            $file = Split-Path -Leaf $item.Destination
            if ($item.HadOriginal) {
                Copy-Item -LiteralPath $item.Destination -Destination $item.Backup -Force
            }
            $installed += $item
            Move-Item -LiteralPath $item.Download -Destination $item.Destination -Force
        }

        $stage = "validate_install"
        $file = "connector.py"
        Write-Host "Validating the installed connector imports..."
        & $python (Join-Path $runtimeDirectory "connector.py") --help | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "The updated connector failed its import validation."
        }
        $file = $null

        $stage = "configure_task"
        $startupTrigger = New-ScheduledTaskTrigger -AtStartup
        $startupTrigger.Delay = "PT1M"
        $recoveryTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
            -RepetitionInterval (New-TimeSpan -Minutes 1)
        $taskSettings = New-ScheduledTaskSettingsSet `
            -MultipleInstances IgnoreNew `
            -RestartCount 999 `
            -RestartInterval (New-TimeSpan -Minutes 1) `
            -ExecutionTimeLimit ([TimeSpan]::Zero) `
            -StartWhenAvailable `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries
        $taskSettings.Enabled = $false
        Set-ScheduledTask -TaskName $TaskName -Trigger @($startupTrigger, $recoveryTrigger) `
            -Settings $taskSettings | Out-Null
        $updated = $true
    }
    catch {
        foreach ($item in $installed) {
            try {
                if ($item.HadOriginal) {
                    Copy-Item -LiteralPath $item.Backup -Destination $item.Destination -Force
                }
                elseif (Test-Path -LiteralPath $item.Destination) {
                    Remove-Item -LiteralPath $item.Destination -Force
                }
            }
            catch {
                Write-Warning "Could not restore $($item.Destination) from the previous installation."
            }
        }
        throw
    }
    finally {
        foreach ($item in $downloads) {
            Remove-Item -LiteralPath $item.Download -Force -ErrorAction SilentlyContinue
        }
        if ($taskStopped -and $taskWasEnabled) {
            if ($updated) { $stage = "start" }
            try {
                Write-Host "Starting Shopify connector..."
                Enable-ScheduledTask -TaskName $TaskName | Out-Null
                Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop
            }
            catch {
                if ($updated) { throw }
                Write-Warning "The Windows connector could not be restarted after the update failed."
            }
        }
    }
}
catch {
    Send-UpdaterReport -Status failed -Stage $stage -File $file -Failure $_
    throw
}
finally {
    if ($updateLock) { $updateLock.Dispose() }
}

Send-UpdaterReport -Status succeeded -Stage complete -FilesUpdated $files.Count
Write-Host "Updated runtime files: $($files.Count)"
Write-Host "Preserved: connector.env, connector state, logs, Shopify order DBFs, POS DBFs, and the legacy order SQLite database"
Write-Host "Update completed successfully."
