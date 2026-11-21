@echo off
rem One-time setup: starts the Schwab feed now and at every Windows logon. No admin rights needed.
cd /d "%~dp0"
set "PYW="
for /f "delims=" %%P in ('where pythonw.exe 2^>nul') do if not defined PYW set "PYW=%%P"
if not defined PYW (
  echo Could not find pythonw.exe. Reinstall Python from python.org with "Add python.exe to PATH" ticked.
  pause
  exit /b 1
)
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
> "%STARTUP%\schwab_0dte_feed.cmd" echo @start "" "%PYW%" "%~dp0zdte_feed.pyw"
start "" "%PYW%" "%~dp0zdte_feed.pyw"
echo.
echo Done. The Schwab feed is running and will start each time you log in to Windows.
echo Using: %PYW%
pause
