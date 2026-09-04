$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Get-RequiredEnvironmentValue {
    param([string]$Name)
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "The installer is incomplete ($Name is missing). Download a fresh installer from Shopify."
    }
    return $value.Trim()
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Stop-ConnectorTask {
    param(
        [string]$TaskName,
        [int]$TimeoutSeconds = 10
    )
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $task) {
        return
    }
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        if (-not $task -or $task.State -notin @("Running", "Queued")) {
            return
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "The existing Cash Register Express connector task did not stop within $TimeoutSeconds seconds."
}

function Get-ScheduledTaskConfigPath {
    param([object]$Task)
    if (-not $Task) {
        return $null
    }
    foreach ($action in @($Task.Actions)) {
        $arguments = [string]$action.Arguments
        $match = [regex]::Match(
            $arguments,
            '(?i)(?:^|\s)--config(?:\s+|=)(?:"([^"]+)"|(\S+))'
        )
        if (-not $match.Success) {
            continue
        }
        $candidate = if ($match.Groups[1].Success) {
            $match.Groups[1].Value
        }
        else {
            $match.Groups[2].Value
        }
        $candidate = [Environment]::ExpandEnvironmentVariables($candidate)
        if (-not [IO.Path]::IsPathRooted($candidate) -and [string]$action.WorkingDirectory) {
            $candidate = Join-Path ([string]$action.WorkingDirectory) $candidate
        }
        if (Test-Path -LiteralPath $candidate -PathType Leaf) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    return $null
}

function Get-IntegerSetting {
    param(
        [hashtable]$Config,
        [string]$Name,
        [int]$Default,
        [int]$Minimum,
        [int]$Maximum
    )
    if (-not $Config.ContainsKey($Name)) {
        return $Default
    }
    $parsed = 0
    if ([int]::TryParse([string]$Config[$Name], [ref]$parsed) -and $parsed -ge $Minimum -and $parsed -le $Maximum) {
        return $parsed
    }
    Write-Warning "The existing $Name value is invalid; using the safe default ($Default)."
    return $Default
}

function Get-BooleanSetting {
    param(
        [hashtable]$Config,
        [string]$Name,
        [bool]$Default
    )
    if (-not $Config.ContainsKey($Name)) {
        return $Default.ToString().ToLowerInvariant()
    }
    switch (([string]$Config[$Name]).Trim().ToLowerInvariant()) {
        { $_ -in @("1", "true", "yes", "on") } { return "true" }
        { $_ -in @("0", "false", "no", "off") } { return "false" }
        default {
            $fallback = $Default.ToString().ToLowerInvariant()
            Write-Warning "The existing $Name value is invalid; using the safe default ($fallback)."
            return $fallback
        }
    }
}

function Get-ChoiceSetting {
    param(
        [hashtable]$Config,
        [string]$Name,
        [string]$Default,
        [string[]]$AllowedValues
    )
    if (-not $Config.ContainsKey($Name)) {
        return $Default
    }
    $value = ([string]$Config[$Name]).Trim().ToLowerInvariant()
    if ($value -in $AllowedValues) {
        return $value
    }
    Write-Warning "The existing $Name value is invalid; using the safe default ($Default)."
    return $Default
}

function Get-SafeCustomPathSetting {
    param(
        [hashtable]$Config,
        [string]$Name
    )
    $value = ([string]$Config[$Name]).Trim()
    if (-not $value) {
        return $null
    }
    if ($value.Contains("`r") -or $value.Contains("`n") -or $value.StartsWith("\\") -or $value -notmatch '^[A-Za-z]:\\') {
        Write-Warning "The existing $Name path is not a fully qualified local path and will not be preserved."
        return $null
    }
    try {
        return [IO.Path]::GetFullPath($value)
    }
    catch {
        Write-Warning "The existing $Name path is invalid and will not be preserved."
        return $null
    }
}

function Get-PreservedOperationalSettings {
    param(
        [hashtable]$Config,
        [bool]$ShopVerified
    )
    $settings = @{
        "SYNC_INTERVAL_SECONDS" = Get-IntegerSetting $Config "SYNC_INTERVAL_SECONDS" 180 30 86400
        "NIGHTLY_FULL_SYNC_HOUR" = Get-IntegerSetting $Config "NIGHTLY_FULL_SYNC_HOUR" 0 0 23
        "CONNECTOR_BATCH_SIZE" = Get-IntegerSetting $Config "CONNECTOR_BATCH_SIZE" 25 1 100
        "CONNECTOR_WORKERS" = Get-IntegerSetting $Config "CONNECTOR_WORKERS" 2 1 4
        "CONNECTOR_TIMEOUT_SECONDS" = Get-IntegerSetting $Config "CONNECTOR_TIMEOUT_SECONDS" 300 30 1800
        "INITIAL_CATALOG_UPLOAD" = Get-BooleanSetting $Config "INITIAL_CATALOG_UPLOAD" $true
        "PRICE_SYNC_ENABLED" = Get-BooleanSetting $Config "PRICE_SYNC_ENABLED" $true
        "ORDER_SYNC_ENABLED" = Get-BooleanSetting $Config "ORDER_SYNC_ENABLED" $true
        "ORDER_DB_RETENTION_ROWS" = Get-IntegerSetting $Config "ORDER_DB_RETENTION_ROWS" 250 100 500
        "CONNECTOR_LOG_MAX_BYTES" = Get-IntegerSetting $Config "CONNECTOR_LOG_MAX_BYTES" 5242880 100000 50000000
        "CONNECTOR_LOG_BACKUPS" = Get-IntegerSetting $Config "CONNECTOR_LOG_BACKUPS" 3 1 10
        "POS_WRITEBACK_MODE" = "dry-run"
    }
    if ($ShopVerified) {
        $settings["POS_WRITEBACK_MODE"] = Get-ChoiceSetting `
            $Config `
            "POS_WRITEBACK_MODE" `
            "dry-run" `
            @("disabled", "dry-run", "vfp-oledb")
    }
    if ($ShopVerified) {
        foreach ($name in @(
            "SHOPIFY_ORDER_DB_PATH",
            "SHOPIFY_ORDER_HEADER_DBF_PATH",
            "SHOPIFY_ORDER_DETAIL_DBF_PATH"
        )) {
            $path = Get-SafeCustomPathSetting $Config $name
            if ($path) {
                $settings[$name] = $path
            }
        }
    }
    return $settings
}

function Assert-CompatibleDataDirectory {
    param(
        [hashtable]$Config,
        [string]$ExpectedDataRoot
    )
    $configured = ([string]$Config["CONNECTOR_DATA_DIR"]).Trim()
    if (-not $configured) {
        return
    }
    $expanded = [Environment]::ExpandEnvironmentVariables($configured)
    try {
        $configuredPath = [IO.Path]::GetFullPath($expanded).TrimEnd("\")
        $expectedPath = [IO.Path]::GetFullPath($ExpectedDataRoot).TrimEnd("\")
    }
    catch {
        throw "The existing CONNECTOR_DATA_DIR path is invalid. No existing settings were changed."
    }
    if (
        $configuredPath.StartsWith("\\") -or
        -not [String]::Equals($configuredPath, $expectedPath, [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "The existing Cash Register Express connector uses the custom data folder '$configured'. Contact support to migrate its inventory state before installing. No existing settings were changed."
    }
}

function Read-ConnectorEnvironment {
    param([string]$Path)
    $values = @{}
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return $values
    }
    foreach ($line in [IO.File]::ReadAllLines($Path)) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) {
            continue
        }
        $separator = $trimmed.IndexOf("=")
        $key = $trimmed.Substring(0, $separator).Trim()
        $value = $trimmed.Substring($separator + 1).Trim().Trim('"').Trim("'")
        if ($key) {
            $values[$key] = $value
        }
    }
    return $values
}

function Test-ConnectorStateHasHistory {
    param([object]$State)
    foreach ($name in @(
        "catalog_products",
        "quantities",
        "event_cursors",
        "sku_bases",
        "pending_catalog_products",
        "pending_matrix_structure_repairs",
        "matrix_structure_probe_skus",
        "matrix_structure_probe_not_before",
        "blocked_inventory_skus",
        "price_snapshot"
    )) {
        $property = $State.PSObject.Properties[$name]
        if ($null -eq $property -or $null -eq $property.Value) {
            continue
        }
        $value = $property.Value
        if ($value -is [System.Collections.IDictionary] -and $value.Count -gt 0) {
            return $true
        }
        if ($value -is [System.Array] -and $value.Length -gt 0) {
            return $true
        }
        if (
            $value -is [PSCustomObject] -and
            @($value.PSObject.Properties).Count -gt 0
        ) {
            return $true
        }
    }
    foreach ($name in @(
        "catalog_complete",
        "catalog_structure_schema_version",
        "inventory_location_id",
        "inventory_reconcile_schema_version",
        "last_cycle_epoch",
        "last_full_reconcile_date",
        "matrix_option_schema_version",
        "numeric_sku_digit_width",
        "numeric_sku_high_water"
    )) {
        $property = $State.PSObject.Properties[$name]
        if ($null -eq $property -or $null -eq $property.Value) {
            continue
        }
        $value = $property.Value
        if (($value -is [bool] -and $value) -or ($value -isnot [bool] -and ([string]$value).Trim() -notin @("", "0"))) {
            return $true
        }
    }
    return $false
}

function Get-ConnectorStateSummary {
    param([string]$DataRoot)
    $statePath = Join-Path $DataRoot "state.json"
    $candidates = @($statePath, [IO.Path]::ChangeExtension($statePath, ".bak"))
    $stateFileFound = $false
    foreach ($candidate in $candidates) {
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            continue
        }
        $stateFileFound = $true
        try {
            $state = Get-Content -LiteralPath $candidate -Raw | ConvertFrom-Json
            if ($null -eq $state -or [int]$state.version -ne 1) {
                continue
            }
            return [PSCustomObject]@{
                Path = $candidate
                Shop = ([string]$state.shop_domain).Trim().ToLowerInvariant()
                InstallationId = ([string]$state.installation_id).Trim()
                HasHistory = Test-ConnectorStateHasHistory $state
            }
        }
        catch {
            continue
        }
    }
    if ($stateFileFound) {
        throw "The existing connector state could not be read safely. No existing settings were changed."
    }
    return $null
}

function Test-PosDirectory {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path) -or $Path.StartsWith("\\")) {
        return $false
    }
    try {
        $resolved = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
        return Test-Path -LiteralPath (Join-Path $resolved "Item.dbf") -PathType Leaf
    }
    catch {
        return $false
    }
}

function Select-PosDirectory {
    param([hashtable]$ExistingConfig)
    $existingPath = [string]$ExistingConfig["POS_DBF_DIR"]
    if (Test-PosDirectory $existingPath) {
        return (Resolve-Path -LiteralPath $existingPath).Path
    }

    $candidates = @()
    try {
        $fixedDrives = Get-CimInstance Win32_LogicalDisk -Filter "DriveType=3" | Select-Object -ExpandProperty DeviceID
    }
    catch {
        $fixedDrives = @($env:SystemDrive)
    }
    foreach ($drive in $fixedDrives) {
        $candidate = Join-Path "$drive\" "ashpsdat"
        if (Test-PosDirectory $candidate) {
            $candidates += (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    $candidates = @($candidates | Sort-Object -Unique)
    if ($candidates.Count -eq 1) {
        return $candidates[0]
    }

    $message = if ($candidates.Count -gt 1) {
        "More than one POS data folder was found. Select the live ashpsdat folder."
    }
    else {
        "Select the Cash Register Express ashpsdat folder. It must contain Item.dbf."
    }
    $shell = New-Object -ComObject Shell.Application
    $selection = $shell.BrowseForFolder(0, $message, 0, 0)
    if ($null -eq $selection) {
        throw "No POS data folder was selected. Setup was canceled without changing Inventory Sync POS Bridge."
    }
    $selectedPath = [string]$selection.Self.Path
    if (-not (Test-PosDirectory $selectedPath)) {
        throw "The selected folder is not a valid POS data folder because Item.dbf was not found."
    }
    return (Resolve-Path -LiteralPath $selectedPath).Path
}

function Get-WorkingPython {
    $commands = @("py.exe", "python.exe")
    foreach ($commandName in $commands) {
        $command = Get-Command $commandName -ErrorAction SilentlyContinue
        if (-not $command) {
            continue
        }
        try {
            if ($commandName -eq "py.exe") {
                $executable = (& $command.Source -3 -c "import sys; print(sys.executable)" 2>$null | Select-Object -Last 1).Trim()
            }
            else {
                $executable = (& $command.Source -c "import sys; print(sys.executable)" 2>$null | Select-Object -Last 1).Trim()
            }
            if ($LASTEXITCODE -eq 0 -and (Test-Path -LiteralPath $executable -PathType Leaf)) {
                $versionOk = & $executable -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)"
                if ($LASTEXITCODE -eq 0) {
                    return (Resolve-Path -LiteralPath $executable).Path
                }
            }
        }
        catch {
            continue
        }
    }
    return $null
}

function Install-PythonRuntime {
    param([string]$TemporaryRoot)
    $pythonVersion = "3.13.15"
    $installerPath = Join-Path $TemporaryRoot "python-$pythonVersion-amd64.exe"
    $downloadUrl = "https://www.python.org/ftp/python/$pythonVersion/python-$pythonVersion-amd64.exe"
    Write-Host "Python is not installed. Downloading the official Python runtime..."
    Invoke-WebRequest -UseBasicParsing -Uri $downloadUrl -OutFile $installerPath
    $signature = Get-AuthenticodeSignature -LiteralPath $installerPath
    if ($signature.Status -ne "Valid" -or $signature.SignerCertificate.Subject -notmatch "Python Software Foundation") {
        throw "The downloaded Python installer did not have a valid Python Software Foundation signature."
    }
    $process = Start-Process -FilePath $installerPath -ArgumentList @(
        "/quiet",
        "InstallAllUsers=1",
        "PrependPath=0",
        "Include_launcher=1",
        "Include_test=0",
        "Shortcuts=0"
    ) -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        throw "Python installation failed with exit code $($process.ExitCode)."
    }
    $candidate = Join-Path $env:ProgramFiles "Python313\python.exe"
    if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        throw "Python finished installing, but its executable could not be found."
    }
    return (Resolve-Path -LiteralPath $candidate).Path
}

function New-DeviceSecret {
    $bytes = New-Object byte[] 32
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    $encoded = [Convert]::ToBase64String($bytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
    return "sec_$encoded"
}

function Write-ConnectorConfig {
    param(
        [string]$Path,
        [string]$Shop,
        [string]$InstallationId,
        [string]$PosDirectory,
        [string]$BaseUrl,
        [string]$ApiKey,
        [string]$ApiSecret,
        [string]$StartupNonce,
        [hashtable]$OperationalSettings,
        [bool]$AllowLegacyStateAdoption = $false
    )
    foreach ($value in @($Shop, $InstallationId, $PosDirectory, $BaseUrl, $ApiKey, $ApiSecret, $StartupNonce)) {
        if ($value.Contains("`r") -or $value.Contains("`n")) {
            throw "A connector setting contained an invalid line break."
        }
    }
    $contentLines = @(
        "# Generated by the Inventory Sync POS Bridge installer.",
        "CONNECTOR_SHOP_DOMAIN=$Shop",
        "CONNECTOR_INSTALLATION_ID=$InstallationId",
        "POS_DBF_DIR=$PosDirectory",
        "SHOPIFY_SYNC_BASE_URL=$BaseUrl",
        "SHOPIFY_SYNC_API_KEY=$ApiKey",
        "SHOPIFY_SYNC_API_SECRET=$ApiSecret",
        "CONNECTOR_STARTUP_NONCE=$StartupNonce"
    )
    if ($AllowLegacyStateAdoption) {
        $contentLines += "CONNECTOR_ALLOW_LEGACY_STATE_ADOPTION=true"
    }
    $contentLines += @(
        "",
        "SYNC_INTERVAL_SECONDS=$($OperationalSettings['SYNC_INTERVAL_SECONDS'])",
        "NIGHTLY_FULL_SYNC_HOUR=$($OperationalSettings['NIGHTLY_FULL_SYNC_HOUR'])",
        "CONNECTOR_BATCH_SIZE=$($OperationalSettings['CONNECTOR_BATCH_SIZE'])",
        "CONNECTOR_WORKERS=$($OperationalSettings['CONNECTOR_WORKERS'])",
        "CONNECTOR_TIMEOUT_SECONDS=$($OperationalSettings['CONNECTOR_TIMEOUT_SECONDS'])",
        "INITIAL_CATALOG_UPLOAD=$($OperationalSettings['INITIAL_CATALOG_UPLOAD'])",
        "PRICE_SYNC_ENABLED=$($OperationalSettings['PRICE_SYNC_ENABLED'])",
        "POS_WRITEBACK_MODE=$($OperationalSettings['POS_WRITEBACK_MODE'])",
        "ORDER_SYNC_ENABLED=$($OperationalSettings['ORDER_SYNC_ENABLED'])",
        "ORDER_DB_RETENTION_ROWS=$($OperationalSettings['ORDER_DB_RETENTION_ROWS'])"
    )
    foreach ($name in @(
        "SHOPIFY_ORDER_DB_PATH",
        "SHOPIFY_ORDER_HEADER_DBF_PATH",
        "SHOPIFY_ORDER_DETAIL_DBF_PATH"
    )) {
        if ($OperationalSettings.ContainsKey($name)) {
            $contentLines += "$name=$($OperationalSettings[$name])"
        }
    }
    $contentLines += @(
        "CONNECTOR_DATA_DIR=$env:ProgramData\ShopifyPosConnector",
        "CONNECTOR_LOG_MAX_BYTES=$($OperationalSettings['CONNECTOR_LOG_MAX_BYTES'])",
        "CONNECTOR_LOG_BACKUPS=$($OperationalSettings['CONNECTOR_LOG_BACKUPS'])",
        ""
    )
    $content = $contentLines -join "`r`n"
    $encoding = New-Object Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($Path, $content, $encoding)

    $acl = New-Object Security.AccessControl.FileSecurity
    $acl.SetAccessRuleProtection($true, $false)
    $fullControl = [Security.AccessControl.FileSystemRights]::FullControl
    $allow = [Security.AccessControl.AccessControlType]::Allow
    foreach ($sidValue in @("S-1-5-18", "S-1-5-32-544")) {
        $sid = New-Object Security.Principal.SecurityIdentifier($sidValue)
        $rule = New-Object Security.AccessControl.FileSystemAccessRule($sid, $fullControl, $allow)
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

$baseUrl = Get-RequiredEnvironmentValue "SHOPIFY_CONNECTOR_BASE_URL"
$expectedShop = (Get-RequiredEnvironmentValue "SHOPIFY_CONNECTOR_EXPECTED_SHOP").ToLowerInvariant()
$runtimeUrl = Get-RequiredEnvironmentValue "SHOPIFY_CONNECTOR_RUNTIME_URL"
$runtimeSha256 = (Get-RequiredEnvironmentValue "SHOPIFY_CONNECTOR_RUNTIME_SHA256").ToUpperInvariant()
$enrollmentToken = Get-RequiredEnvironmentValue "SHOPIFY_CONNECTOR_ENROLLMENT_TOKEN"

if (-not $baseUrl.StartsWith("https://", [StringComparison]::OrdinalIgnoreCase)) {
    throw "Inventory Sync POS Bridge can only be installed from an HTTPS server."
}

if (-not (Test-IsAdministrator)) {
    Write-Host "Waiting for Administrator approval..."
    $powershell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
    $arguments = "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    try {
        $elevated = Start-Process -FilePath $powershell -ArgumentList $arguments -Verb RunAs -Wait -PassThru
    }
    catch {
        Write-Host "Administrator approval was canceled. No Inventory Sync POS Bridge changes were made." -ForegroundColor Red
        exit 1
    }
    exit $elevated.ExitCode
}
[Environment]::SetEnvironmentVariable("SHOPIFY_CONNECTOR_ENROLLMENT_TOKEN", $null)

$dataRoot = Join-Path $env:ProgramData "ShopifyPosConnector"
$runtimeRoot = Join-Path $dataRoot "runtime"
$configPath = Join-Path $dataRoot "connector.env"
$startupReadyPath = Join-Path $dataRoot "startup.ready.json"
$installLog = Join-Path $dataRoot "install.log"
$taskName = "Shopify POS Inventory Connector"
$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ("ShopifyPosConnector-" + [Guid]::NewGuid().ToString("N"))
$mutex = New-Object Threading.Mutex($false, "Global\ShopifyPosConnectorSetup")
$mutexHeld = $false
$transcriptStarted = $false
$configBackup = $null
$configTouched = $false
$newDevicePending = $false
$deviceActivated = $false
$activationMayHaveCommitted = $false
$oldTaskWasPresent = $false
$oldTaskWasRunning = $false
$oldTaskXml = $null
$installationMutationStarted = $false
$runtimeMovedAside = $false
$runtimeReplaced = $false
$previousRuntime = Join-Path $dataRoot "runtime.previous"

try {
    $mutexHeld = $mutex.WaitOne(0)
    if (-not $mutexHeld) {
        throw "Another Inventory Sync POS Bridge setup is already running."
    }
    New-Item -ItemType Directory -Path $dataRoot -Force | Out-Null
    Start-Transcript -Path $installLog -Append | Out-Null
    $transcriptStarted = $true
    Write-Host "Starting Inventory Sync POS Bridge setup for $expectedShop..."

    $existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    $existingConfigPath = $configPath
    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf) -and $existingTask) {
        $existingConfigPath = Get-ScheduledTaskConfigPath $existingTask
        if (-not $existingConfigPath) {
            throw "An existing Cash Register Express connector task was found, but its configuration file could not be located. No existing settings were changed."
        }
        Write-Host "Found the existing Cash Register Express connector configuration at $existingConfigPath"
    }

    $existingConfig = Read-ConnectorEnvironment $existingConfigPath
    $existingShop = [string]$existingConfig["CONNECTOR_SHOP_DOMAIN"]
    if ($existingShop -and $existingShop.ToLowerInvariant() -ne $expectedShop) {
        throw "This computer is already paired with $existingShop. Its inventory state cannot be reused for $expectedShop."
    }

    $existingConfigVerified = $false
    $existingCredentialStatus = ""
    $existingConfigVerificationError = ""
    $legacyStatus = $null
    if ($existingConfig.Count -gt 0) {
        $legacyBaseUrl = ([string]$existingConfig["SHOPIFY_SYNC_BASE_URL"]).TrimEnd("/")
        $legacyApiKey = [string]$existingConfig["SHOPIFY_SYNC_API_KEY"]
        $legacyApiSecret = [string]$existingConfig["SHOPIFY_SYNC_API_SECRET"]
        if (-not $legacyBaseUrl -or -not $legacyApiKey -or -not $legacyApiSecret) {
            $existingConfigVerificationError = "Required credentials are missing."
        }
        else {
            $legacyUri = $null
            if (
                -not [Uri]::TryCreate($legacyBaseUrl, [UriKind]::Absolute, [ref]$legacyUri) -or
                -not (
                    $legacyUri.Scheme -eq "https" -or
                    ($legacyUri.Scheme -eq "http" -and $legacyUri.IsLoopback)
                )
            ) {
                $existingConfigVerificationError = "Its server URL is not HTTPS or local loopback HTTP."
            }
            else {
                $legacyHeaders = @{
                    "X-API-Key" = $legacyApiKey
                    "X-API-Secret" = $legacyApiSecret
                }
                try {
                    $legacyStatus = Invoke-RestMethod -Method Get -Uri "$legacyBaseUrl/api/connector/verify" -Headers $legacyHeaders
                }
                catch {
                    $existingConfigVerificationError = "the server did not accept or could not check the credentials"
                }
            }
        }
        if (-not $legacyStatus -and -not $existingConfigVerificationError) {
            $existingConfigVerificationError = "the server returned no credential status"
        }
        if ($legacyStatus) {
            $verifiedShop = ([string]$legacyStatus.shop).Trim().ToLowerInvariant()
            if (-not $verifiedShop) {
                $existingConfigVerificationError = "The server did not return a shop identity."
            }
            elseif ($verifiedShop -ne $expectedShop) {
                throw "The existing connector belongs to $($legacyStatus.shop), not $expectedShop."
            }
            else {
                $existingShop = $verifiedShop
                $existingCredentialStatus = ([string]$legacyStatus.device_status).Trim().ToLowerInvariant()
                if ($legacyApiKey.StartsWith("dev_")) {
                    $remoteInstallationId = ([string]$legacyStatus.installation_id).Trim()
                    $configuredInstallationId = ([string]$existingConfig["CONNECTOR_INSTALLATION_ID"]).Trim()
                    if (-not $remoteInstallationId) {
                        $existingConfigVerificationError = "The server did not return a Windows installation identity."
                    }
                    elseif ($configuredInstallationId -and $configuredInstallationId -ne $remoteInstallationId) {
                        throw "The existing connector credentials belong to a different Windows installation."
                    }
                    elseif ($existingCredentialStatus -notin @("active", "pending")) {
                        $existingConfigVerificationError = "The managed connector is not active or pending."
                    }
                    else {
                        $existingConfig["CONNECTOR_INSTALLATION_ID"] = $remoteInstallationId
                        $existingConfigVerified = $true
                    }
                }
                elseif ($existingCredentialStatus -eq "legacy") {
                    $existingConfigVerified = $true
                }
                else {
                    $existingConfigVerificationError = "The connector credential type was not recognized."
                }
            }
        }
    }
    elseif (Test-Path -LiteralPath $existingConfigPath -PathType Leaf) {
        throw "The existing Cash Register Express connector configuration is empty. No existing settings were changed."
    }

    if ($existingConfig.Count -gt 0) {
        Assert-CompatibleDataDirectory $existingConfig $dataRoot
    }

    $stateSummary = Get-ConnectorStateSummary $dataRoot
    $adoptLegacyState = $false
    if ($stateSummary) {
        if ($stateSummary.Shop -and $stateSummary.Shop -ne $expectedShop) {
            throw "The saved inventory state belongs to $($stateSummary.Shop), not $expectedShop. No existing settings were changed."
        }
        $configuredInstallationId = ([string]$existingConfig["CONNECTOR_INSTALLATION_ID"]).Trim()
        if (
            $stateSummary.InstallationId -and
            $configuredInstallationId -and
            $stateSummary.InstallationId -ne $configuredInstallationId
        ) {
            throw "The saved inventory state belongs to a different Windows installation. No existing settings were changed."
        }
        if ($stateSummary.InstallationId -and -not $configuredInstallationId) {
            $existingConfig["CONNECTOR_INSTALLATION_ID"] = $stateSummary.InstallationId
        }
        if (-not $stateSummary.Shop -and $stateSummary.HasHistory) {
            if (-not $existingConfigVerified) {
                throw "Saved inventory history has no shop identity and no existing connector configuration could verify it. Contact support before installing for $expectedShop."
            }
            $adoptLegacyState = $true
        }
    }

    if ($existingConfig.Count -gt 0 -and -not $existingConfigVerified) {
        if (-not $stateSummary -or -not $stateSummary.HasHistory -or $stateSummary.Shop -eq $expectedShop) {
            Write-Warning "The previous connector credentials may be expired, revoked, or unavailable ($existingConfigVerificationError). Setup will use a fresh shop-bound pairing and safe write-back mode."
        }
    }

    $operationalSettings = Get-PreservedOperationalSettings $existingConfig $existingConfigVerified
    $customWritebackScript = ([string]$existingConfig["POS_WRITEBACK_SCRIPT"]).Trim()
    if (
        $existingConfigVerified -and
        $operationalSettings["POS_WRITEBACK_MODE"] -eq "vfp-oledb" -and
        $customWritebackScript
    ) {
        throw "The existing connector uses a custom POS write-back script. Contact support to verify that script before migrating an enabled write-back installation. No existing settings were changed."
    }

    $posConfig = if ($existingConfigVerified -or ($stateSummary -and $stateSummary.Shop -eq $expectedShop)) {
        $existingConfig
    }
    else {
        @{}
    }
    $posDirectory = Select-PosDirectory $posConfig
    Write-Host "POS data found at $posDirectory"

    New-Item -ItemType Directory -Path $temporaryRoot -Force | Out-Null
    $runtimeArchive = Join-Path $temporaryRoot "runtime.zip"
    $runtimeExtracted = Join-Path $temporaryRoot "runtime"
    Write-Host "Downloading the verified Inventory Sync POS Bridge software..."
    Invoke-WebRequest -UseBasicParsing -Uri $runtimeUrl -OutFile $runtimeArchive
    $downloadedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $runtimeArchive).Hash.ToUpperInvariant()
    if ($downloadedHash -ne $runtimeSha256) {
        throw "The Inventory Sync POS Bridge software did not pass its security check. Download a fresh installer from Shopify."
    }
    Expand-Archive -LiteralPath $runtimeArchive -DestinationPath $runtimeExtracted -Force
    foreach ($requiredFile in @(
        "windows_connector\connector.py",
        "windows_connector\install.ps1",
        "jbarbaro_db\dbf_pos_sync.py"
    )) {
        if (-not (Test-Path -LiteralPath (Join-Path $runtimeExtracted $requiredFile) -PathType Leaf)) {
            throw "The Inventory Sync POS Bridge software is incomplete ($requiredFile is missing)."
        }
    }

    $python = Get-WorkingPython
    if (-not $python) {
        $python = Install-PythonRuntime $temporaryRoot
    }
    Write-Host "Using Python at $python"

    $existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($existingTask) {
        $oldTaskWasPresent = $true
        $oldTaskWasRunning = $existingTask.State -eq "Running"
        $oldTaskXml = Export-ScheduledTask -TaskName $taskName
    }
    $installationMutationStarted = $true
    if ($existingTask) {
        Stop-ConnectorTask -TaskName $taskName
    }

    if (Test-Path -LiteralPath $previousRuntime) {
        Remove-Item -LiteralPath $previousRuntime -Recurse -Force
    }
    if (Test-Path -LiteralPath $runtimeRoot) {
        Move-Item -LiteralPath $runtimeRoot -Destination $previousRuntime
        $runtimeMovedAside = $true
    }
    try {
        Move-Item -LiteralPath $runtimeExtracted -Destination $runtimeRoot
        $runtimeReplaced = $true
    }
    catch {
        if ((Test-Path -LiteralPath $previousRuntime) -and -not (Test-Path -LiteralPath $runtimeRoot)) {
            Move-Item -LiteralPath $previousRuntime -Destination $runtimeRoot
        }
        throw
    }

    $installationId = [string]$existingConfig["CONNECTOR_INSTALLATION_ID"]
    $apiKey = [string]$existingConfig["SHOPIFY_SYNC_API_KEY"]
    $apiSecret = [string]$existingConfig["SHOPIFY_SYNC_API_SECRET"]
    $startupNonce = (New-DeviceSecret).Substring(4)
    $reuseActiveDevice = (
        $existingConfigVerified -and
        $existingShop -eq $expectedShop -and
        $installationId -and
        $apiKey.StartsWith("dev_") -and
        $apiSecret -and
        $existingCredentialStatus -eq "active"
    )
    $reusePendingDevice = (
        $existingConfigVerified -and
        $existingShop -eq $expectedShop -and
        $installationId -and
        $apiKey.StartsWith("dev_") -and
        $apiSecret -and
        $existingCredentialStatus -eq "pending"
    )
    if ($reusePendingDevice) {
        $newDevicePending = $true
    }

    if (-not $reuseActiveDevice -and -not $reusePendingDevice) {
        if (-not $installationId) {
            $installationId = "win_" + [Guid]::NewGuid().ToString("N")
        }
        $apiSecret = New-DeviceSecret
        $enrollmentBody = @{
            enrollment_token = $enrollmentToken
            installation_id = $installationId
            device_secret = $apiSecret
            connector_version = "1.2.0"
        } | ConvertTo-Json -Compress
        Write-Host "Securely pairing this computer with Shopify..."
        $enrollment = $null
        for ($attempt = 1; $attempt -le 3 -and $null -eq $enrollment; $attempt++) {
            try {
                $enrollment = Invoke-RestMethod `
                    -Method Post `
                    -Uri "$baseUrl/connector/v1/enroll" `
                    -ContentType "application/json" `
                    -Body $enrollmentBody
            }
            catch {
                if ($attempt -eq 3) {
                    throw
                }
                Start-Sleep -Seconds 2
            }
        }
        if (([string]$enrollment.shop).ToLowerInvariant() -ne $expectedShop) {
            throw "The server paired the installer with an unexpected Shopify shop."
        }
        $apiKey = [string]$enrollment.api_key
        $newDevicePending = $true
    }

    if (Test-Path -LiteralPath $configPath) {
        $configBackup = Join-Path $temporaryRoot "connector.env.before-setup"
        Copy-Item -LiteralPath $configPath -Destination $configBackup -Force
    }
    $configTouched = $true
    Write-ConnectorConfig `
        -Path $configPath `
        -Shop $expectedShop `
        -InstallationId $installationId `
        -PosDirectory $posDirectory `
        -BaseUrl $baseUrl `
        -ApiKey $apiKey `
        -ApiSecret $apiSecret `
        -StartupNonce $startupNonce `
        -OperationalSettings $operationalSettings `
        -AllowLegacyStateAdoption $adoptLegacyState

    $installer = Join-Path $runtimeRoot "windows_connector\install.ps1"
    & $installer -ConfigPath $configPath -PythonPath $python -TaskName $taskName
    if ($LASTEXITCODE -ne 0) {
        throw "The Windows scheduled task could not be prepared."
    }
    if ($adoptLegacyState) {
        Write-ConnectorConfig `
            -Path $configPath `
            -Shop $expectedShop `
            -InstallationId $installationId `
            -PosDirectory $posDirectory `
            -BaseUrl $baseUrl `
            -ApiKey $apiKey `
            -ApiSecret $apiSecret `
            -StartupNonce $startupNonce `
            -OperationalSettings $operationalSettings
    }

    # A pending device can authenticate only with the setup endpoint. The
    # connector writes this marker only after SYSTEM has read the POS folder,
    # bound the saved state, and authenticated with the expected server.
    Remove-Item -LiteralPath $startupReadyPath -Force -ErrorAction SilentlyContinue
    Start-ScheduledTask -TaskName $taskName
    $startupDeadline = [DateTime]::UtcNow.AddSeconds(60)
    $startupReady = $false
    do {
        Start-Sleep -Milliseconds 500
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
        if ($task.State -notin @("Running", "Ready", "Queued")) {
            $taskInfo = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction Stop
            throw "The Cash Register Express connector stopped during startup (state $($task.State), task result $($taskInfo.LastTaskResult))."
        }
        if ($task.State -eq "Running" -and (Test-Path -LiteralPath $startupReadyPath -PathType Leaf)) {
            try {
                $startupStatus = Get-Content -LiteralPath $startupReadyPath -Raw | ConvertFrom-Json
                $startupReady = (
                    ([string]$startupStatus.shop).ToLowerInvariant() -eq $expectedShop -and
                    [string]$startupStatus.installation_id -eq $installationId -and
                    [string]$startupStatus.device_status -in @("pending", "active") -and
                    [string]$startupStatus.startup_nonce -eq $startupNonce
                )
            }
            catch {
                $startupReady = $false
            }
        }
    } while (-not $startupReady -and [DateTime]::UtcNow -lt $startupDeadline)
    if (-not $startupReady) {
        $taskInfo = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction Stop
        throw "The Cash Register Express connector could not validate the POS folder and server within 60 seconds (task result $($taskInfo.LastTaskResult))."
    }
    Remove-Item -LiteralPath $startupReadyPath -Force -ErrorAction SilentlyContinue

    if ($newDevicePending) {
        $activationHeaders = @{ "X-API-Key" = $apiKey; "X-API-Secret" = $apiSecret }
        $activation = $null
        $activationError = $null
        for ($attempt = 1; $attempt -le 3 -and $null -eq $activation; $attempt++) {
            try {
                $activationResponse = Invoke-RestMethod `
                    -Method Post `
                    -Uri "$baseUrl/connector/v1/activate" `
                    -Headers $activationHeaders
                if ($null -eq $activationResponse) {
                    $activationMayHaveCommitted = $true
                }
                else {
                    $activation = $activationResponse
                }
            }
            catch {
                $activationError = $_
                $statusCode = 0
                if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
                    $statusCode = [int]$_.Exception.Response.StatusCode
                }
                if ($statusCode -eq 0 -or $statusCode -eq 408 -or $statusCode -eq 429 -or $statusCode -ge 500) {
                    $activationMayHaveCommitted = $true
                }
            }
            if ($null -eq $activation -and $attempt -lt 3) {
                Start-Sleep -Seconds 2
            }
        }
        if ($null -eq $activation) {
            $activationStatus = $null
            try {
                $activationStatus = Invoke-RestMethod `
                    -Method Get `
                    -Uri "$baseUrl/api/connector/verify" `
                    -Headers $activationHeaders
            }
            catch {
                $activationStatus = $null
            }
            if (
                $activationStatus -and
                ([string]$activationStatus.shop).ToLowerInvariant() -eq $expectedShop -and
                [string]$activationStatus.installation_id -eq $installationId -and
                [string]$activationStatus.device_status -eq "active"
            ) {
                $activation = [PSCustomObject]@{ status = "active" }
            }
            elseif ($activationMayHaveCommitted) {
                throw "The server may have activated this computer, but its response could not be confirmed. The verified new installation will be retained instead of restoring credentials that may have been revoked."
            }
            elseif ($activationError) {
                throw "The pending connector pairing was rejected or expired. Download a fresh installer from Shopify; if the problem repeats, contact support."
            }
            else {
                throw "The new Cash Register Express connector could not be activated."
            }
        }
        if ([string]$activation.status -ne "active") {
            throw "The new Cash Register Express connector could not be activated."
        }
        $deviceActivated = $true
    }

    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
    if ($task.State -ne "Running") {
        Start-ScheduledTask -TaskName $taskName
        Start-Sleep -Seconds 3
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
        if ($task.State -ne "Running") {
            throw "The Cash Register Express connector stopped after activation (state $($task.State))."
        }
    }
    if (Test-Path -LiteralPath $previousRuntime) {
        Remove-Item -LiteralPath $previousRuntime -Recurse -Force -ErrorAction SilentlyContinue
    }
    Write-Host ""
    Write-Host "Shop:       $expectedShop"
    Write-Host "POS folder: $posDirectory"
    Write-Host "Task:       $($task.State)"
    Write-Host "Mode:       POS write-back is $($operationalSettings['POS_WRITEBACK_MODE'])"
    Write-Host "Log:        $dataRoot\connector.log"
}
catch {
    Write-Host "Setup failed: $($_.Exception.Message)" -ForegroundColor Red
    if ($installationMutationStarted -and -not $deviceActivated -and -not $activationMayHaveCommitted) {
        try {
            Stop-ConnectorTask -TaskName $taskName
        }
        catch {
            Write-Warning "The failed Cash Register Express connector task could not be stopped cleanly: $($_.Exception.Message)"
        }
        Remove-Item -LiteralPath $startupReadyPath -Force -ErrorAction SilentlyContinue
        if ($configTouched) {
            try {
                if ($configBackup -and (Test-Path -LiteralPath $configBackup)) {
                    Copy-Item -LiteralPath $configBackup -Destination $configPath -Force
                }
                elseif (Test-Path -LiteralPath $configPath) {
                    Remove-Item -LiteralPath $configPath -Force
                }
            }
            catch {
                Write-Warning "The previous connector config could not be restored automatically: $($_.Exception.Message)"
            }
        }
        if ($runtimeReplaced -or $runtimeMovedAside) {
            try {
                $failedRuntime = Join-Path $temporaryRoot "runtime.failed"
                if ($runtimeReplaced -and (Test-Path -LiteralPath $runtimeRoot)) {
                    Move-Item -LiteralPath $runtimeRoot -Destination $failedRuntime -Force
                }
                if (Test-Path -LiteralPath $previousRuntime) {
                    Move-Item -LiteralPath $previousRuntime -Destination $runtimeRoot -Force
                }
            }
            catch {
                Write-Warning "The previous connector runtime could not be restored automatically: $($_.Exception.Message)"
            }
        }
        try {
            if ($oldTaskWasPresent -and $oldTaskXml) {
                Register-ScheduledTask -TaskName $taskName -Xml $oldTaskXml -Force | Out-Null
                if ($oldTaskWasRunning) {
                    Start-ScheduledTask -TaskName $taskName
                }
            }
            elseif (-not $oldTaskWasPresent) {
                Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
            }
        }
        catch {
            Write-Warning "The previous connector task could not be restored automatically: $($_.Exception.Message)"
        }
    }
    elseif ($deviceActivated) {
        # Activation is the hand-off point. The new device owns the shop, so
        # retain its verified runtime, config, and already-running task.
        Write-Warning "Inventory Sync POS Bridge was activated and remains installed; review $installLog for the final setup error."
    }
    elseif ($activationMayHaveCommitted) {
        # A timed-out activation request may have committed on the server. Do
        # not restore the old credentials because activation revokes them.
        Write-Warning "Activation could not be confirmed. The verified new installation was retained to avoid restoring credentials that may have been revoked. Review $installLog and run a fresh installer if the bridge remains offline."
    }
    exit 1
}
finally {
    [Environment]::SetEnvironmentVariable("SHOPIFY_CONNECTOR_ENROLLMENT_TOKEN", $null)
    if ($transcriptStarted) {
        Stop-Transcript | Out-Null
    }
    if (Test-Path -LiteralPath $temporaryRoot) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    if ($mutexHeld) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
