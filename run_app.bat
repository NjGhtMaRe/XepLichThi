@echo off
chcp 65001 > nul
title XepLichThi - Exam Scheduler

echo ============================================
echo    XEP LICH THI - EXAM SCHEDULER
echo ============================================
echo.

:: Check for Python 3.11
echo [1/3] Checking Python 3.11...

py -3.11 --version >nul 2>&1
if %errorlevel% == 0 (
    echo     Python 3.11 found!
    py -3.11 --version
    goto :install_deps
)

:: Python 3.11 not found - try to install via py launcher
echo     Python 3.11 not found. Installing...
echo.

py install 3.11 2>nul
if %errorlevel% == 0 (
    echo     Python 3.11 installed successfully!
    goto :install_deps
)

:: py install failed, show manual instructions
echo.
echo ========================================
echo  Python 3.11 not found!
echo ========================================
echo.
echo Please install Python 3.11.9 from:
echo   https://www.python.org/downloads/release/python-3119/
echo.
echo Or run: py install 3.11
echo.
pause
exit /b 1

:install_deps
echo.
echo [2/3] Installing/Checking dependencies...
py -3.11 -m pip install --upgrade pip --quiet 2>nul
py -3.11 -m pip install -r requirements.txt --quiet

if %errorlevel% neq 0 (
    echo.
    echo ========================================
    echo  ERROR: Failed to install dependencies!
    echo ========================================
    echo.
    pause
    exit /b 1
)
echo     Dependencies OK!

:: Start the application
echo.
echo [3/3] Starting web server...
echo.
echo ============================================
echo  Server: http://127.0.0.1:5000
echo  Press Ctrl+C to stop
echo ============================================
echo.

:: Open browser
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://127.0.0.1:5000"

:: Run Flask app with Python 3.11
py -3.11 app.py

echo.
echo Server stopped.
pause
