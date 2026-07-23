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

echo Stopping Shopify connector...
powershell -NoProfile -Command "Stop-ScheduledTask -TaskName 'Shopify POS Inventory Connector' -ErrorAction SilentlyContinue"

echo Downloading updated connector...
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; Invoke-WebRequest 'https://raw.githubusercontent.com/AnthonyBarbaro/shopify-sync/main/windows_connector/connector.py' -OutFile 'C:\ShopifySync\windows_connector\connector.py.download'"
if errorlevel 1 goto FAILED

echo Checking downloaded file...
windows_connector\.venv\Scripts\python.exe -m py_compile windows_connector\connector.py.download
if errorlevel 1 goto FAILED

echo Backing up current connector...
copy /Y windows_connector\connector.py windows_connector\connector.py.backup >nul
if errorlevel 1 goto FAILED

echo Installing update...
move /Y windows_connector\connector.py.download windows_connector\connector.py >nul
if errorlevel 1 goto FAILED

echo Starting Shopify connector...
powershell -NoProfile -Command "Start-ScheduledTask -TaskName 'Shopify POS Inventory Connector'"
if errorlevel 1 goto FAILED

echo.
echo Update completed successfully.
echo.
powershell -NoProfile -Command "Get-Content 'C:\ProgramData\ShopifyPosConnector\connector.log' -Tail 20 -ErrorAction SilentlyContinue"
pause
exit /b 0

:FAILED
echo.
echo UPDATE FAILED. The existing connector was not replaced.
del /Q windows_connector\connector.py.download 2>nul
powershell -NoProfile -Command "Start-ScheduledTask -TaskName 'Shopify POS Inventory Connector' -ErrorAction SilentlyContinue"
pause
exit /b 1
