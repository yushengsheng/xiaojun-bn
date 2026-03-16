@echo off
setlocal

cd /d "%~dp0"
set "TARGET="

for /f "delims=" %%F in ('dir /b /a:-d "*bn.py" 2^>nul') do (
    if not defined TARGET set "TARGET=%%F"
)

if not defined TARGET (
    echo [ERROR] Main script not found:
    echo %CD%\
    echo.
    echo Available .py files:
    dir /b "*.py"
    pause
    exit /b 1
)

where py >nul 2>nul
if %errorlevel%==0 (
    set "PYTHON_CMD=py -3"
) else (
    where python >nul 2>nul
    if %errorlevel%==0 (
        set "PYTHON_CMD=python"
    ) else (
        echo [ERROR] Python 3 was not found.
        echo Install Python 3, then run this launcher again.
        pause
        exit /b 1
    )
)

%PYTHON_CMD% -c "import importlib.util, sys; missing=[]; mapping={'requests':'requests','eth_account':'eth-account','eth_utils':'eth-utils'}; [missing.append(pkg) for mod,pkg in mapping.items() if importlib.util.find_spec(mod) is None]; sys.exit(0 if not missing else 1)"
if %errorlevel% neq 0 (
    echo [INFO] Installing runtime dependencies...
    %PYTHON_CMD% -m pip install --user requests eth-account eth-utils
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed.
        echo Try:
        echo   %PYTHON_CMD% -m pip install --user requests eth-account eth-utils
        pause
        exit /b %errorlevel%
    )
)

%PYTHON_CMD% "%TARGET%"

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Program failed to start.
    echo If needed, install dependencies:
    echo   py -3 -m pip install requests eth-account eth-utils
    pause
    exit /b %errorlevel%
)

endlocal
exit /b 0
