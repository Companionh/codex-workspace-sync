@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "REPO_ROOT=%%~fI"
set "LOCAL_CONFIG=%SCRIPT_DIR%push-config.local.cmd"
set "DEFAULT_REMOTE=origin"

if exist "%LOCAL_CONFIG%" call "%LOCAL_CONFIG%"

if not defined GIT_EXE set "GIT_EXE=C:\Program Files\Git\cmd\git.exe"
if not exist "%GIT_EXE%" (
  echo Git executable not found at "%GIT_EXE%".
  echo Set GIT_EXE in push-config.local.cmd or install Git for Windows.
  exit /b 1
)

pushd "%REPO_ROOT%" >nul

if not defined GITHUB_REMOTE set "GITHUB_REMOTE=%DEFAULT_REMOTE%"

if not defined GITHUB_REPO_URL (
  for /f "delims=" %%I in ('"%GIT_EXE%" remote get-url "%GITHUB_REMOTE%" 2^>nul') do set "GITHUB_REPO_URL=%%I"
)

if not defined GITHUB_REPO_URL (
  set /p GITHUB_REPO_URL=GitHub repo URL:
)

if not defined GITHUB_USERNAME (
  set /p GITHUB_USERNAME=GitHub username:
)

if not defined GITHUB_PAT (
  set /p GITHUB_PAT=GitHub fine-grained token:
)

if not defined GITHUB_BRANCH (
  for /f "delims=" %%I in ('"%GIT_EXE%" branch --show-current') do set "GITHUB_BRANCH=%%I"
)

if not defined GITHUB_BRANCH (
  set "GITHUB_BRANCH=main"
)

for /f "delims=" %%I in ('"%GIT_EXE%" status --porcelain') do (
  set "HAS_CHANGES=1"
  goto :status_checked
)
:status_checked

if defined HAS_CHANGES (
  if not defined COMMIT_MESSAGE (
    set /p COMMIT_MESSAGE=Commit message:
  )
  if not defined COMMIT_MESSAGE (
    echo Commit message is required when there are uncommitted changes.
    popd >nul
    exit /b 1
  )
  "%GIT_EXE%" add -A
  if errorlevel 1 (
    echo Failed to stage changes.
    popd >nul
    exit /b 1
  )
  "%GIT_EXE%" commit -m "%COMMIT_MESSAGE%"
  if errorlevel 1 (
    echo Commit failed.
    popd >nul
    exit /b 1
  )
)

for /f "delims=" %%I in ('powershell -NoProfile -Command "[Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(''%GITHUB_USERNAME%:%GITHUB_PAT%''))"') do set "AUTH_B64=%%I"

if not defined AUTH_B64 (
  echo Failed to prepare GitHub authorization header.
  popd >nul
  exit /b 1
)

echo Pushing "%GITHUB_BRANCH%" to "%GITHUB_REPO_URL%"...
"%GIT_EXE%" -c "http.https://github.com/.extraheader=AUTHORIZATION: basic %AUTH_B64%" push "%GITHUB_REPO_URL%" "%GITHUB_BRANCH%"
set "PUSH_EXIT=%ERRORLEVEL%"

if %PUSH_EXIT% neq 0 (
  echo Push failed. Check that the token has repo write access.
  popd >nul
  exit /b %PUSH_EXIT%
)

echo Push completed successfully.
popd >nul
exit /b 0
