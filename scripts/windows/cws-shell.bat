@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "REPO_ROOT=%%~fI"
set "PYTHONPATH=%REPO_ROOT%\src;%PYTHONPATH%"
pushd "%REPO_ROOT%" >nul 2>&1
py -3.12 -m cws shell
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul 2>&1
exit /b %EXIT_CODE%
