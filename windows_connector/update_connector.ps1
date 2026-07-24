param(
    [string]$ProjectRoot = "C:\ShopifySync",
    [string]$TaskName = "Shopify POS Inventory Connector"
)

$ErrorActionPreference = "Stop"
$repositoryBase = "https://raw.githubusercontent.com/AnthonyBarbaro/shopify-sync/main"
$python = Join-Path $ProjectRoot "windows_connector\.venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Missing connector Python environment: $python. Run windows_connector\install.ps1 first."
}

$files = @(
    "windows_connector/connector.py",
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

try {
    Write-Host "Downloading Windows connector runtime files..."
    foreach ($relativePath in $files) {
        $windowsRelativePath = $relativePath.Replace("/", "\")
        $destination = Join-Path $ProjectRoot $windowsRelativePath
        $download = "$destination.download"
        $directory = Split-Path -Parent $destination
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
        Invoke-WebRequest "$repositoryBase/$relativePath" -OutFile $download
        $downloads += [pscustomobject]@{
            Destination = $destination
            Download = $download
            Backup = "$destination.backup"
        }
    }

    Write-Host "Validating downloaded Python and PowerShell files..."
    foreach ($item in $downloads) {
        if ($item.Destination.EndsWith(".py")) {
            & $python -m py_compile $item.Download
            if ($LASTEXITCODE -ne 0) {
                throw "Python validation failed for $($item.Destination)"
            }
        }
        elseif ($item.Destination.EndsWith(".ps1")) {
            $tokens = $null
            $parseErrors = $null
            [System.Management.Automation.Language.Parser]::ParseFile(
                $item.Download,
                [ref]$tokens,
                [ref]$parseErrors
            ) | Out-Null
            if ($parseErrors.Count) {
                throw "PowerShell validation failed for $($item.Destination): $($parseErrors | Out-String)"
            }
        }
    }

    Write-Host "Stopping Shopify connector..."
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    $taskStopped = $true

    Write-Host "Backing up and installing updates..."
    foreach ($item in $downloads) {
        if (Test-Path -LiteralPath $item.Destination) {
            Copy-Item -LiteralPath $item.Destination -Destination $item.Backup -Force
        }
        Move-Item -LiteralPath $item.Download -Destination $item.Destination -Force
        $installed += $item
    }

    Write-Host "Validating the installed connector imports..."
    & $python (Join-Path $ProjectRoot "windows_connector\connector.py") --help | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "The updated connector failed its import validation."
    }
}
catch {
    Write-Warning "Update failed: $($_.Exception.Message)"
    foreach ($item in $installed) {
        if (Test-Path -LiteralPath $item.Backup) {
            Copy-Item -LiteralPath $item.Backup -Destination $item.Destination -Force
        }
    }
    throw
}
finally {
    foreach ($item in $downloads) {
        Remove-Item -LiteralPath $item.Download -Force -ErrorAction SilentlyContinue
    }
    if ($taskStopped) {
        Write-Host "Starting Shopify connector..."
        Start-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    }
}

Write-Host "Updated runtime files: $($files.Count)"
Write-Host "Preserved: connector.env, connector state, logs, POS DBFs, and shopify-orders.db"
