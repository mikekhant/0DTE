@echo off
rem Stops the running Schwab feed (it exits within a few seconds).
type nul > "%~dp0stop.request"
echo Stop requested. To stop it starting at logon, delete schwab_0dte_feed.cmd from:
echo %APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
pause
