@echo off
setlocal

cd /d "%~dp0"
set "TARGET="
for %%F in ("*.py") do (
    if not defined TARGET set "TARGET=%%~nxF"
)

if not defined TARGET (
    echo [ERROR] No .py file found in:
    echo %CD%
    pause
    exit /b 1
)

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 "%TARGET%"
) else (
    where python >nul 2>nul
    if %errorlevel%==0 (
        python "%TARGET%"
    ) else (
        echo [ERROR] Python 3 was not found.
        echo Install Python 3, then run this launcher again.
        pause
        exit /b 1
    )
)

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Program failed to start.
    echo If needed, install dependency:
    echo   py -3 -m pip install requests
    pause
    exit /b %errorlevel%
)

endlocal
