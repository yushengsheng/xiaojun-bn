@echo off
setlocal

cd /d "%~dp0"

if exist "%~dp0launch_hidden.vbs" (
    start "" wscript.exe //nologo "%~dp0launch_hidden.vbs"
    exit /b 0
)

echo [ERROR] Missing launcher: "%~dp0launch_hidden.vbs"
echo Run the console diagnostic launcher if needed.
pause
exit /b 1
