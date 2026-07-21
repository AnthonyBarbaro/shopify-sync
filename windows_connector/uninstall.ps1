param(
    [string]$TaskName = "Shopify POS Inventory Connector"
)

$ErrorActionPreference = "Stop"
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($task) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task: $TaskName"
}
else {
    Write-Host "Scheduled task was not installed: $TaskName"
}

Write-Host "Local connector state and logs were left in place so inventory baselines are not lost."
