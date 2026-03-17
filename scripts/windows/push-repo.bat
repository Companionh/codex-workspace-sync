@echo off
setlocal EnableExtensions
set "CWS_PAUSE_ON_ERROR=1"
set "CWS_PAUSE_ON_SUCCESS=1"

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
if not defined GITHUB_REMOTE set "GITHUB_REMOTE=origin"
if not defined SYNC_WORKING_BRANCH_AFTER_PUSH set "SYNC_WORKING_BRANCH_AFTER_PUSH=true"
if not defined WORKING_BRANCH_BACKUP_PREFIX set "WORKING_BRANCH_BACKUP_PREFIX=backup/post_publish_"

"%GIT_EXE%" --version >nul 2>nul
if errorlevel 1 (
  echo Git is not installed or not configured correctly.
  echo Set GIT_EXE in push-config.local.cmd or install Git for Windows.
  goto :error_exit
)

"%PYTHON_EXE%" --version >nul 2>nul
if errorlevel 1 (
  echo Python is not installed or not configured correctly.
  echo Set PYTHON_EXE in push-config.local.cmd if needed.
  goto :error_exit
)

if not exist "%EXPORT_TOOL%" (
  echo Missing export tool: "%EXPORT_TOOL%"
  goto :error_exit
)

pushd "%REPO_ROOT%" >nul

if not defined GITHUB_REPO_URL (
  for /f "delims=" %%I in ('"%GIT_EXE%" remote get-url origin 2^>nul') do set "GITHUB_REPO_URL=%%I"
)

if not defined GITHUB_REPO_URL set /p GITHUB_REPO_URL=GitHub repo URL:
set "PUBLISH_REPO_URL=%GITHUB_REPO_URL%"
if /I not "%GITHUB_REPO_URL%"=="%GITHUB_REPO_URL:https://github.com/=git@github.com:%" (
  set "PUBLISH_REPO_URL=%GITHUB_REPO_URL:https://github.com/=git@github.com:%"
)

if not defined GIT_USER_NAME (
  for /f "delims=" %%I in ('"%GIT_EXE%" -C "%REPO_ROOT%" config --get user.name 2^>nul') do set "GIT_USER_NAME=%%I"
)
if not defined GIT_USER_EMAIL (
  for /f "delims=" %%I in ('"%GIT_EXE%" -C "%REPO_ROOT%" config --get user.email 2^>nul') do set "GIT_USER_EMAIL=%%I"
)
if not defined GIT_USER_NAME (
  for /f "delims=" %%I in ('"%GIT_EXE%" config --global --get user.name 2^>nul') do set "GIT_USER_NAME=%%I"
)
if not defined GIT_USER_EMAIL (
  for /f "delims=" %%I in ('"%GIT_EXE%" config --global --get user.email 2^>nul') do set "GIT_USER_EMAIL=%%I"
)

if not exist "%PUBLISH_CHECKOUT%\.git" (
  echo Local publish checkout missing. Cloning into:
  echo   %PUBLISH_CHECKOUT%
  "%GIT_EXE%" clone "%PUBLISH_REPO_URL%" "%PUBLISH_CHECKOUT%"
  if errorlevel 1 (
    echo git clone failed.
    popd >nul
    goto :error_exit
  )
)

pushd "%PUBLISH_CHECKOUT%" >nul
"%GIT_EXE%" remote set-url origin "%PUBLISH_REPO_URL%" >nul 2>nul
"%GIT_EXE%" remote set-url --push origin "%PUBLISH_REPO_URL%" >nul 2>nul
"%GIT_EXE%" fetch origin "%GITHUB_BRANCH%" >nul 2>nul
if errorlevel 1 (
  "%GIT_EXE%" checkout -B "%GITHUB_BRANCH%"
) else (
  "%GIT_EXE%" checkout -B "%GITHUB_BRANCH%" FETCH_HEAD
)
if errorlevel 1 (
  echo git checkout failed in publish checkout.
  popd >nul
  popd >nul
  goto :error_exit
)
if defined GIT_USER_NAME "%GIT_EXE%" config user.name "%GIT_USER_NAME%"
if defined GIT_USER_EMAIL "%GIT_EXE%" config user.email "%GIT_USER_EMAIL%"
popd >nul

echo Exporting curated project tree into publish checkout...
"%PYTHON_EXE%" "%EXPORT_TOOL%" --dest "%PUBLISH_CHECKOUT%"
if errorlevel 1 (
  echo Export failed.
  popd >nul
  goto :error_exit
)

pushd "%PUBLISH_CHECKOUT%" >nul
echo.
echo Current git status:
"%GIT_EXE%" status --short
if errorlevel 1 (
  echo git status failed.
  popd >nul
  popd >nul
  goto :error_exit
)

"%GIT_EXE%" add -A
if errorlevel 1 (
  echo git add failed.
  popd >nul
  popd >nul
  goto :error_exit
)

"%GIT_EXE%" diff --cached --quiet
if not errorlevel 1 (
  for /f "tokens=1,2" %%I in ('"%GIT_EXE%" rev-list --left-right --count HEAD...origin/%GITHUB_BRANCH% 2^>nul') do (
    set "AHEAD_COUNT=%%I"
    set "BEHIND_COUNT=%%J"
  )
  if not defined AHEAD_COUNT set "AHEAD_COUNT=0"
  if not defined BEHIND_COUNT set "BEHIND_COUNT=0"
  echo.
  if not "%AHEAD_COUNT%"=="0" (
    echo No new staged changes, but the publish checkout is ahead of origin/%GITHUB_BRANCH% by %AHEAD_COUNT% commit^(s^).
    echo Pushing existing unpublished commit^(s^)...
    "%GIT_EXE%" push origin "%GITHUB_BRANCH%"
    if errorlevel 1 (
      echo git push failed.
      popd >nul
      popd >nul
      goto :error_exit
    )
    "%GIT_EXE%" update-ref "refs/remotes/%GITHUB_REMOTE%/%GITHUB_BRANCH%" HEAD >nul 2>nul
    call :sync_working_checkout
    echo.
    echo Push successful. Latest commit:
    "%GIT_EXE%" log --oneline -1
    popd >nul
    popd >nul
    goto :success_exit
  )
  echo No staged changes to commit and no unpublished commits to push.
  popd >nul
  popd >nul
  goto :success_exit
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
  goto :success_exit
)

echo.
echo Committing...
if not defined GIT_USER_NAME (
  echo Git user.name is not configured for the publish checkout.
  echo Set it in push-config.local.cmd as GIT_USER_NAME, or run:
  echo   git config --global user.name "Your Name"
  popd >nul
  popd >nul
  goto :error_exit
)
if not defined GIT_USER_EMAIL (
  echo Git user.email is not configured for the publish checkout.
  echo Set it in push-config.local.cmd as GIT_USER_EMAIL, or run:
  echo   git config --global user.email "you@example.com"
  popd >nul
  popd >nul
  goto :error_exit
)
"%GIT_EXE%" commit -m "%COMMIT_MSG%"
if errorlevel 1 (
  echo git commit failed.
  popd >nul
  popd >nul
  goto :error_exit
)

echo.
echo Pushing to origin %GITHUB_BRANCH%...
"%GIT_EXE%" push origin "%GITHUB_BRANCH%"
if errorlevel 1 (
  echo git push failed.
  popd >nul
  popd >nul
  goto :error_exit
)
"%GIT_EXE%" update-ref "refs/remotes/%GITHUB_REMOTE%/%GITHUB_BRANCH%" HEAD >nul 2>nul
call :sync_working_checkout

echo.
echo Push successful. Latest commit:
"%GIT_EXE%" log --oneline -1

popd >nul
popd >nul
goto :success_exit

:sync_working_checkout
if /I not "%SYNC_WORKING_BRANCH_AFTER_PUSH%"=="true" goto :eof

for /f "delims=" %%I in ('"%GIT_EXE%" -C "%REPO_ROOT%" branch --show-current 2^>nul') do set "WORKING_BRANCH=%%I"
if not defined WORKING_BRANCH goto :eof
if /I not "%WORKING_BRANCH%"=="%GITHUB_BRANCH%" (
  echo.
  echo Skipping working checkout sync because the current branch is %WORKING_BRANCH%, not %GITHUB_BRANCH%.
  goto :eof
)

"%GIT_EXE%" -C "%REPO_ROOT%" fetch "%PUBLISH_CHECKOUT%" "%GITHUB_BRANCH%" >nul 2>nul
if errorlevel 1 (
  echo.
  echo Warning: failed to fetch the published branch back into the working checkout.
  goto :eof
)

for /f "delims=" %%I in ('"%GIT_EXE%" -C "%REPO_ROOT%" rev-parse HEAD 2^>nul') do set "WORKING_HEAD=%%I"
for /f "delims=" %%I in ('"%GIT_EXE%" -C "%REPO_ROOT%" rev-parse FETCH_HEAD 2^>nul') do set "PUBLISHED_HEAD=%%I"
if not defined WORKING_HEAD goto :eof
if not defined PUBLISHED_HEAD goto :eof
if /I "%WORKING_HEAD%"=="%PUBLISHED_HEAD%" (
  "%GIT_EXE%" -C "%REPO_ROOT%" update-ref "refs/remotes/%GITHUB_REMOTE%/%GITHUB_BRANCH%" FETCH_HEAD >nul 2>nul
  goto :eof
)

for /f "delims=" %%I in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString(\"yyyyMMdd_HHmmss\")"') do set "SYNC_BACKUP_STAMP=%%I"
if not defined SYNC_BACKUP_STAMP set "SYNC_BACKUP_STAMP=%RANDOM%%RANDOM%"
set "SYNC_BACKUP_BRANCH=%WORKING_BRANCH_BACKUP_PREFIX%%SYNC_BACKUP_STAMP%"

echo.
echo Creating working-branch safety backup: %SYNC_BACKUP_BRANCH%
"%GIT_EXE%" -C "%REPO_ROOT%" branch "%SYNC_BACKUP_BRANCH%" HEAD >nul 2>nul
if errorlevel 1 (
  echo Warning: failed to create a working-branch backup. Leaving the working checkout unchanged.
  goto :eof
)

echo Syncing the working checkout to the published branch...
"%GIT_EXE%" -C "%REPO_ROOT%" reset --hard FETCH_HEAD >nul 2>nul
if errorlevel 1 (
  echo Warning: failed to realign the working checkout after publish.
  goto :eof
)
"%GIT_EXE%" -C "%REPO_ROOT%" update-ref "refs/remotes/%GITHUB_REMOTE%/%GITHUB_BRANCH%" FETCH_HEAD >nul 2>nul
echo Working checkout is now aligned to the published commit.
goto :eof

:error_exit
if "%CWS_PAUSE_ON_ERROR%"=="1" pause
endlocal
exit /b 1

:success_exit
if "%CWS_PAUSE_ON_SUCCESS%"=="1" pause
endlocal
exit /b 0
