@echo off
setlocal

rem Run this launcher as Administrator or as an elevated scheduled task.
set "UPDATER_ROOT=C:\ShopifySync"
if exist "%~dp0update_connector.ps1" set "UPDATER_ROOT=%~dp0.."

powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%UPDATER_ROOT%\windows_connector\update_connector.ps1" -ProjectRoot "%UPDATER_ROOT%" -RefreshUpdater
exit /b %ERRORLEVEL%
