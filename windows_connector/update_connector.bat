@echo off
setlocal

rem Request Administrator access when the file is opened normally.
fltmc >nul 2>&1
if errorlevel 1 (
    powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

cd /d C:\ShopifySync
if errorlevel 1 goto FAILED

echo Downloading the latest Windows updater...
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; Invoke-WebRequest 'https://raw.githubusercontent.com/AnthonyBarbaro/shopify-sync/main/windows_connector/update_connector.ps1' -OutFile 'C:\ShopifySync\windows_connector\update_connector.ps1.download'; $tokens=$null; $errors=$null; [System.Management.Automation.Language.Parser]::ParseFile('C:\ShopifySync\windows_connector\update_connector.ps1.download',[ref]$tokens,[ref]$errors) | Out-Null; if ($errors.Count) { throw ($errors | Out-String) }; Move-Item -Force 'C:\ShopifySync\windows_connector\update_connector.ps1.download' 'C:\ShopifySync\windows_connector\update_connector.ps1'"
if errorlevel 1 goto FAILED

powershell -NoProfile -ExecutionPolicy Bypass -File C:\ShopifySync\windows_connector\update_connector.ps1
if errorlevel 1 goto FAILED

echo.
echo Update completed successfully.
echo.
powershell -NoProfile -Command "Get-Content 'C:\ProgramData\ShopifyPosConnector\connector.log' -Tail 25 -ErrorAction SilentlyContinue"
pause
exit /b 0

:FAILED
echo.
echo UPDATE FAILED. Existing connector settings and POS data were preserved.
del /Q C:\ShopifySync\windows_connector\update_connector.ps1.download 2>nul
powershell -NoProfile -Command "Start-ScheduledTask -TaskName 'Shopify POS Inventory Connector' -ErrorAction SilentlyContinue"
pause
exit /b 1
