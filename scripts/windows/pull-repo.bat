@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "CWS_PAUSE_ON_ERROR=1"
set "CWS_PAUSE_ON_SUCCESS=1"

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "REPO_ROOT=%%~fI"
set "LOCAL_CONFIG=%SCRIPT_DIR%push-config.local.cmd"

if exist "%LOCAL_CONFIG%" call "%LOCAL_CONFIG%"

if not defined GIT_EXE set "GIT_EXE=C:\Program Files\Git\cmd\git.exe"
if not defined PYTHON_EXE set "PYTHON_EXE=python"
if not defined GITHUB_REMOTE set "GITHUB_REMOTE=origin"
if not defined AUTOSTASH set "AUTOSTASH=true"
if not defined REFRESH_INSTALL set "REFRESH_INSTALL=true"
if not defined RUN_COMPILE_CHECK set "RUN_COMPILE_CHECK=true"
if not defined REBASE_ON_DIVERGENCE set "REBASE_ON_DIVERGENCE=prompt"

"%GIT_EXE%" --version >nul 2>nul
if errorlevel 1 (
  echo Git is not installed or not configured correctly.
  echo Set GIT_EXE in push-config.local.cmd or install Git for Windows.
  goto :error_exit
)

if /I "%REFRESH_INSTALL%"=="true" (
  "%PYTHON_EXE%" --version >nul 2>nul
  if errorlevel 1 (
    echo Python is not installed or not configured correctly.
    echo Set PYTHON_EXE in push-config.local.cmd if needed.
    goto :error_exit
  )
)

pushd "%REPO_ROOT%" >nul

if not defined GITHUB_BRANCH (
  for /f "delims=" %%I in ('"%GIT_EXE%" branch --show-current 2^>nul') do set "GITHUB_BRANCH=%%I"
)
if not defined GITHUB_BRANCH set "GITHUB_BRANCH=main"

for /f "delims=" %%I in ('"%GIT_EXE%" status --porcelain') do (
  set "HAS_LOCAL_CHANGES=1"
  goto :status_checked
)
:status_checked

if defined HAS_LOCAL_CHANGES (
  if /I "%AUTOSTASH%"=="true" (
    echo Local checkout changes detected. Autostashing before update.
    "%GIT_EXE%" stash push --include-untracked -m "codex-workspace-sync windows pre-update autostash"
    if errorlevel 1 (
      echo git stash failed.
      popd >nul
      goto :error_exit
    )
    set "STASHED_LOCAL_CHANGES=1"
  ) else (
    echo Local checkout changes detected and AUTOSTASH=false.
    echo Commit, stash, or discard the local changes first.
    popd >nul
    goto :error_exit
  )
)

echo Fetching %GITHUB_REMOTE%/%GITHUB_BRANCH%...
"%GIT_EXE%" fetch "%GITHUB_REMOTE%" "%GITHUB_BRANCH%"
if errorlevel 1 (
  echo git fetch failed.
  popd >nul
  goto :error_exit
)

for /f "tokens=1,2" %%I in ('"%GIT_EXE%" rev-list --left-right --count HEAD...FETCH_HEAD') do (
  set "AHEAD_COUNT=%%I"
  set "BEHIND_COUNT=%%J"
)
if not defined AHEAD_COUNT set "AHEAD_COUNT=0"
if not defined BEHIND_COUNT set "BEHIND_COUNT=0"

if "%AHEAD_COUNT%"=="0" if "%BEHIND_COUNT%"=="0" (
  echo Local repo is already up to date.
  goto :post_update
)

if "%AHEAD_COUNT%"=="0" (
  echo Applying fast-forward update...
  "%GIT_EXE%" merge --ff-only FETCH_HEAD
  if errorlevel 1 (
    echo git merge --ff-only failed.
    popd >nul
    goto :error_exit
  )
  goto :post_update
)

if "%BEHIND_COUNT%"=="0" (
  echo Local branch is ahead of %GITHUB_REMOTE%/%GITHUB_BRANCH% by %AHEAD_COUNT% commit^(s^).
  echo Nothing to pull from the remote.
  goto :post_update
)

echo Local and remote branches have diverged.
echo Local branch is ahead by %AHEAD_COUNT% commit^(s^) and behind by %BEHIND_COUNT% commit^(s^).

set "DO_REBASE="
if /I "%REBASE_ON_DIVERGENCE%"=="true" set "DO_REBASE=1"
if /I "%REBASE_ON_DIVERGENCE%"=="prompt" (
  set /p REBASE_CHOICE=Rebase local commits onto %GITHUB_REMOTE%/%GITHUB_BRANCH% now? [y/N]: 
  if /I "!REBASE_CHOICE!"=="y" set "DO_REBASE=1"
  if /I "!REBASE_CHOICE!"=="yes" set "DO_REBASE=1"
)

if not defined DO_REBASE (
  echo Pull aborted to avoid rewriting local history unexpectedly.
  echo Run push-repo.bat first, or rerun pull-repo.bat and choose rebase when prompted.
  popd >nul
  goto :error_exit
)

echo Rebasing local commits onto FETCH_HEAD...
"%GIT_EXE%" rebase FETCH_HEAD
if errorlevel 1 (
  echo git rebase failed.
  echo Resolve conflicts, then run:
  echo   git rebase --continue
  echo or abort it with:
  echo   git rebase --abort
  popd >nul
  goto :error_exit
)

:post_update
if /I "%REFRESH_INSTALL%"=="true" (
  echo Refreshing editable Python install...
  "%PYTHON_EXE%" -m pip install -e "%REPO_ROOT%"
  if errorlevel 1 (
    echo pip install failed.
    popd >nul
    goto :error_exit
  )
)

if /I "%RUN_COMPILE_CHECK%"=="true" (
  echo Running syntax checks...
  "%PYTHON_EXE%" -m compileall "%REPO_ROOT%\src" "%REPO_ROOT%\tests" "%REPO_ROOT%\tools"
  if errorlevel 1 (
    echo compileall failed.
    popd >nul
    goto :error_exit
  )
)

if defined STASHED_LOCAL_CHANGES (
  echo Restoring stashed local changes...
  "%GIT_EXE%" stash pop --index
  if errorlevel 1 (
    echo Update completed, but reapplying local changes caused conflicts.
    echo The stash was kept. Resolve it manually with: git stash list
    popd >nul
    goto :error_exit
  )
)

echo.
echo Pull completed successfully.
"%GIT_EXE%" log --oneline -1

popd >nul
goto :success_exit

:error_exit
if "%CWS_PAUSE_ON_ERROR%"=="1" pause
endlocal
exit /b 1

:success_exit
if "%CWS_PAUSE_ON_SUCCESS%"=="1" pause
endlocal
exit /b 0
