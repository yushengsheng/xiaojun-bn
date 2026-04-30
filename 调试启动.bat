@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

cd /d "%~dp0"
set "TARGET=%~dp0小军bn.py"

if not exist "%TARGET%" (
    echo [ERROR] Main script not found:
    echo %TARGET%
    echo.
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

%PYTHON_CMD% -c "import importlib.util, sys; missing=[]; mapping={'requests':'requests','socks':'PySocks','eth_account':'eth-account','eth_utils':'eth-utils'}; [missing.append(pkg) for mod,pkg in mapping.items() if importlib.util.find_spec(mod) is None]; modern_ok=importlib.util.find_spec('cryptography') is not None or importlib.util.find_spec('Crypto') is not None; sys.exit(0 if (not missing and modern_ok) else 1)"
if %errorlevel% neq 0 (
    echo [INFO] Installing runtime dependencies...
    %PYTHON_CMD% -m pip install --user -r requirements.txt
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed.
        echo Try:
        echo   %PYTHON_CMD% -m pip install --user -r requirements.txt
        pause
        exit /b %errorlevel%
    )
)

%PYTHON_CMD% "%TARGET%"

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Program failed to start.
    echo If needed, install dependencies:
    echo   py -3 -m pip install -r requirements.txt
    pause
    exit /b %errorlevel%
)

endlocal
exit /b 0
