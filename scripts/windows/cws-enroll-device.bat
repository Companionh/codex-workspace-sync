@echo off
setlocal
if not defined CWS_ENROLL_KEEP_OPEN (
  set "CWS_ENROLL_KEEP_OPEN=1"
  cmd /k ""%~f0" __run"
  exit /b
)
if /i "%~1"=="__run" shift
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "REPO_ROOT=%%~fI"
set "PYTHONPATH=%REPO_ROOT%\src;%PYTHONPATH%"
set "LOG_DIR=%LOCALAPPDATA%\CodexWorkspaceSync\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
set "CWS_ENROLL_LOG=%LOG_DIR%\cws-enroll-device-transcript.log"
pushd "%REPO_ROOT%" >nul 2>&1
echo Transcript log: %CWS_ENROLL_LOG%
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Start-Transcript -Path $env:CWS_ENROLL_LOG -Force | Out-Null; " ^
  "try { py -3.12 -m cws enroll-device; $code = $LASTEXITCODE } " ^
  "catch { Write-Error $_; $code = 1 } " ^
  "finally { Stop-Transcript | Out-Null }; " ^
  "Write-Host ''; Write-Host ('Transcript log saved to ' + $env:CWS_ENROLL_LOG); exit $code"
set "EXIT_CODE=%ERRORLEVEL%"
echo.
if not "%EXIT_CODE%"=="0" (
  echo Enrollment command exited with code %EXIT_CODE%.
) else (
  echo Enrollment command finished successfully.
)
popd >nul 2>&1
endlocal
exit /b %EXIT_CODE%
