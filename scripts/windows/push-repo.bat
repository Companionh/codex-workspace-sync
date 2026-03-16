@echo off
setlocal EnableExtensions
set "GIT_TERMINAL_PROMPT=0"
set "GCM_INTERACTIVE=never"
set "GCM_PRESERVE_CREDS=0"

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "REPO_ROOT=%%~fI"
set "LOCAL_CONFIG=%SCRIPT_DIR%push-config.local.cmd"
set "DEFAULT_PUBLISH_CHECKOUT=%REPO_ROOT%\backups\push_tmp_repo"
set "EXPORT_TOOL=%REPO_ROOT%\tools\export_github_tree.py"

if exist "%LOCAL_CONFIG%" call "%LOCAL_CONFIG%"

if not defined GIT_EXE set "GIT_EXE=C:\Program Files\Git\cmd\git.exe"
if not defined PYTHON_EXE set "PYTHON_EXE=python"
if not defined PUBLISH_CHECKOUT set "PUBLISH_CHECKOUT=%DEFAULT_PUBLISH_CHECKOUT%"
if not defined GITHUB_BRANCH set "GITHUB_BRANCH=main"

"%GIT_EXE%" --version >nul 2>nul
if errorlevel 1 (
  echo Git is not installed or not configured correctly.
  echo Set GIT_EXE in push-config.local.cmd or install Git for Windows.
  exit /b 1
)

"%PYTHON_EXE%" --version >nul 2>nul
if errorlevel 1 (
  echo Python is not installed or not configured correctly.
  echo Set PYTHON_EXE in push-config.local.cmd if needed.
  exit /b 1
)

if not exist "%EXPORT_TOOL%" (
  echo Missing export tool: "%EXPORT_TOOL%"
  exit /b 1
)

pushd "%REPO_ROOT%" >nul

if not defined GITHUB_REPO_URL (
  for /f "delims=" %%I in ('"%GIT_EXE%" remote get-url origin 2^>nul') do set "GITHUB_REPO_URL=%%I"
)

if not defined GITHUB_REPO_URL set /p GITHUB_REPO_URL=GitHub repo URL:
if not defined GITHUB_USERNAME set /p GITHUB_USERNAME=GitHub username:
if not defined GITHUB_PAT set /p GITHUB_PAT=GitHub fine-grained token:

for /f "delims=" %%I in ('powershell -NoProfile -Command "[Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(''%GITHUB_USERNAME%:%GITHUB_PAT%''))"') do set "AUTH_B64=%%I"

if not defined AUTH_B64 (
  echo Failed to prepare GitHub authorization header.
  popd >nul
  exit /b 1
)

if not exist "%PUBLISH_CHECKOUT%\.git" (
  echo Local publish checkout missing. Cloning into:
  echo   %PUBLISH_CHECKOUT%
  "%GIT_EXE%" -c credential.helper= -c credential.interactive=never -c "http.https://github.com/.extraheader=AUTHORIZATION: basic %AUTH_B64%" clone "%GITHUB_REPO_URL%" "%PUBLISH_CHECKOUT%"
  if errorlevel 1 (
    echo git clone failed.
    popd >nul
    exit /b 1
  )
)

pushd "%PUBLISH_CHECKOUT%" >nul
"%GIT_EXE%" remote set-url origin "%GITHUB_REPO_URL%" >nul 2>nul
"%GIT_EXE%" -c credential.helper= -c credential.interactive=never -c "http.https://github.com/.extraheader=AUTHORIZATION: basic %AUTH_B64%" fetch origin "%GITHUB_BRANCH%" >nul 2>nul
if errorlevel 1 (
  "%GIT_EXE%" checkout -B "%GITHUB_BRANCH%"
) else (
  "%GIT_EXE%" checkout -B "%GITHUB_BRANCH%" FETCH_HEAD
)
if errorlevel 1 (
  echo git checkout failed in publish checkout.
  popd >nul
  popd >nul
  exit /b 1
)
popd >nul

echo Exporting curated project tree into publish checkout...
"%PYTHON_EXE%" "%EXPORT_TOOL%" --dest "%PUBLISH_CHECKOUT%"
if errorlevel 1 (
  echo Export failed.
  popd >nul
  exit /b 1
)

pushd "%PUBLISH_CHECKOUT%" >nul
echo.
echo Current git status:
"%GIT_EXE%" status --short
if errorlevel 1 (
  echo git status failed.
  popd >nul
  popd >nul
  exit /b 1
)

"%GIT_EXE%" add -A
if errorlevel 1 (
  echo git add failed.
  popd >nul
  popd >nul
  exit /b 1
)

"%GIT_EXE%" diff --cached --quiet
if not errorlevel 1 (
  echo.
  echo No staged changes to commit.
  popd >nul
  popd >nul
  exit /b 0
)

set "COMMIT_MSG=%~1"
if "%COMMIT_MSG%"=="" if defined COMMIT_MESSAGE set "COMMIT_MSG=%COMMIT_MESSAGE%"
if "%COMMIT_MSG%"=="" (
  echo.
  set /p COMMIT_MSG=Enter commit message - blank cancels: 
)

if "%COMMIT_MSG%"=="" (
  echo Commit cancelled.
  popd >nul
  popd >nul
  exit /b 0
)

echo.
echo Committing...
"%GIT_EXE%" commit -m "%COMMIT_MSG%"
if errorlevel 1 (
  echo git commit failed.
  popd >nul
  popd >nul
  exit /b 1
)

echo.
echo Pushing to origin %GITHUB_BRANCH%...
"%GIT_EXE%" -c credential.helper= -c credential.interactive=never -c "http.https://github.com/.extraheader=AUTHORIZATION: basic %AUTH_B64%" push origin "%GITHUB_BRANCH%"
if errorlevel 1 (
  echo git push failed.
  popd >nul
  popd >nul
  exit /b 1
)

echo.
echo Push successful. Latest commit:
"%GIT_EXE%" log --oneline -1

popd >nul
popd >nul
endlocal
exit /b 0
