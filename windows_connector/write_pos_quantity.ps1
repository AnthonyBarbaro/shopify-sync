param(
    [Parameter(Mandatory = $true)]
    [string]$DbfDirectory,

    [Parameter(Mandatory = $true)]
    [string]$AdjustmentsJson
)

$ErrorActionPreference = "Stop"

function Add-Parameter {
    param(
        [System.Data.OleDb.OleDbCommand]$Command,
        [object]$Value,
        [System.Data.OleDb.OleDbType]$Type = [System.Data.OleDb.OleDbType]::VarChar
    )
    $parameter = $Command.Parameters.Add("?", $Type)
    $parameter.Value = if ($Type -eq [System.Data.OleDb.OleDbType]::Decimal) { [decimal]$Value } else { [string]$Value }
}

function Get-Quantity {
    param(
        [System.Data.OleDb.OleDbConnection]$Connection,
        [System.Data.OleDb.OleDbTransaction]$Transaction,
        [string]$Table,
        [string]$Sku,
        [string]$Cell
    )
    $where = if ($Cell) { "SKU = ? AND CELL = ?" } else { "SKU = ?" }
    $command = $Connection.CreateCommand()
    $command.Transaction = $Transaction
    $command.CommandText = "SELECT QTY FROM [$Table] WHERE $where"
    Add-Parameter -Command $command -Value $Sku
    if ($Cell) {
        Add-Parameter -Command $command -Value $Cell
    }
    $value = $command.ExecuteScalar()
    if ($null -eq $value -or $value -is [System.DBNull]) {
        throw "No POS quantity row found in $Table for SKU '$Sku'$(if ($Cell) { " cell '$Cell'" })"
    }
    return [decimal]$value
}

function Add-QuantityDelta {
    param(
        [System.Data.OleDb.OleDbConnection]$Connection,
        [System.Data.OleDb.OleDbTransaction]$Transaction,
        [string]$Table,
        [string]$Sku,
        [string]$Cell,
        [int]$Delta,
        [Nullable[decimal]]$ExpectedQuantity
    )
    $where = if ($Cell) { "SKU = ? AND CELL = ?" } else { "SKU = ?" }
    if ($null -ne $ExpectedQuantity) {
        $where += " AND QTY = ?"
    }
    $command = $Connection.CreateCommand()
    $command.Transaction = $Transaction
    $command.CommandText = "UPDATE [$Table] SET QTY = QTY + ? WHERE $where"
    Add-Parameter -Command $command -Value $Delta -Type ([System.Data.OleDb.OleDbType]::Decimal)
    Add-Parameter -Command $command -Value $Sku
    if ($Cell) {
        Add-Parameter -Command $command -Value $Cell
    }
    if ($null -ne $ExpectedQuantity) {
        Add-Parameter -Command $command -Value $ExpectedQuantity.Value -Type ([System.Data.OleDb.OleDbType]::Decimal)
    }
    return $command.ExecuteNonQuery()
}

if (-not (Test-Path -LiteralPath $DbfDirectory -PathType Container)) {
    throw "DBF directory does not exist: $DbfDirectory"
}

$adjustments = @($AdjustmentsJson | ConvertFrom-Json)
$connectionString = "Provider=VFPOLEDB.1;Data Source=$DbfDirectory;Collating Sequence=machine;"
$connection = New-Object System.Data.OleDb.OleDbConnection($connectionString)
$results = New-Object System.Collections.Generic.List[object]

try {
    $connection.Open()
    foreach ($adjustment in $adjustments) {
        $sku = [string]$adjustment.sku
        $delta = [int]$adjustment.delta
        $expected = [decimal]$adjustment.expected_quantity
        $transaction = $connection.BeginTransaction()
        try {
            $baseSku = $sku
            $cell = $null
            if ($sku -match '^(?<base>.+)\.\s(?<row>\d+)\s(?<column>\d+)$') {
                $baseSku = $Matches.base
                $cell = "$($Matches.row) $($Matches.column)"
            }

            if ($cell) {
                $current = Get-Quantity -Connection $connection -Transaction $transaction -Table "Itemmqty" -Sku $baseSku -Cell $cell
                $target = $expected + $delta
                if ($current -eq $target) {
                    $transaction.Rollback()
                    $results.Add([pscustomobject]@{ sku = $sku; success = $true; already_applied = $true })
                    continue
                }
                if ($current -ne $expected) {
                    throw "POS quantity changed concurrently for '$sku': expected $expected, found $current"
                }
                $updated = Add-QuantityDelta -Connection $connection -Transaction $transaction -Table "Itemmqty" -Sku $baseSku -Cell $cell -Delta $delta -ExpectedQuantity $expected
                if ($updated -ne 1) {
                    throw "Expected one Itemmqty row for '$sku', updated $updated"
                }
                $itemUpdated = Add-QuantityDelta -Connection $connection -Transaction $transaction -Table "Item" -Sku $baseSku -Cell $null -Delta $delta -ExpectedQuantity $null
                if ($itemUpdated -ne 1) {
                    throw "Expected one Item row for matrix base SKU '$baseSku', updated $itemUpdated"
                }
            }
            else {
                $current = Get-Quantity -Connection $connection -Transaction $transaction -Table "Item" -Sku $baseSku -Cell $null
                $target = $expected + $delta
                if ($current -eq $target) {
                    $transaction.Rollback()
                    $results.Add([pscustomobject]@{ sku = $sku; success = $true; already_applied = $true })
                    continue
                }
                if ($current -ne $expected) {
                    throw "POS quantity changed concurrently for '$sku': expected $expected, found $current"
                }
                $updated = Add-QuantityDelta -Connection $connection -Transaction $transaction -Table "Item" -Sku $baseSku -Cell $null -Delta $delta -ExpectedQuantity $expected
                if ($updated -ne 1) {
                    throw "Expected one Item row for '$sku', updated $updated"
                }
            }

            $transaction.Commit()
            $results.Add([pscustomobject]@{ sku = $sku; success = $true; already_applied = $false })
        }
        catch {
            try { $transaction.Rollback() } catch { }
            $results.Add([pscustomobject]@{ sku = $sku; success = $false; message = $_.Exception.Message })
        }
        finally {
            $transaction.Dispose()
        }
    }
}
finally {
    $connection.Close()
    $connection.Dispose()
}

@($results) | ConvertTo-Json -Compress
