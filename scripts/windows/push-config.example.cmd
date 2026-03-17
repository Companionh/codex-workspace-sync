@echo off
rem Copy this file to push-config.local.cmd and fill in the values below.
rem This local file is ignored by git.
rem SSH or an existing Git credential setup should already handle authentication.

set "GIT_EXE=C:\Program Files\Git\cmd\git.exe"
set "PYTHON_EXE=python"
set "GITHUB_REPO_URL=git@github.com:Companionh/codex-workspace-sync.git"
set "GITHUB_REMOTE=origin"
set "GITHUB_BRANCH=main"
set "PUBLISH_CHECKOUT="
rem Optional: override the commit identity used in the temp publish checkout.
rem set "GIT_USER_NAME=Companionh"
rem set "GIT_USER_EMAIL=companionh@users.noreply.github.com"
rem Optional: settings used by pull-repo.bat.
rem set "AUTOSTASH=true"
rem set "REFRESH_INSTALL=true"
rem set "RUN_COMPILE_CHECK=true"
rem set "REBASE_ON_DIVERGENCE=prompt"
rem set "RESET_ON_DIVERGENCE=prompt"
rem Optional: after a successful publish, sync the working branch to the published mirror.
rem set "SYNC_WORKING_BRANCH_AFTER_PUSH=true"
rem set "WORKING_BRANCH_BACKUP_PREFIX=backup/post_publish_"
rem Optional: window behavior for the Windows batch helpers.
rem set "CWS_PAUSE_ON_ERROR=1"
rem set "CWS_PAUSE_ON_SUCCESS=1"
rem Optional: if the repo has local changes and you want a fixed commit message.
rem set "COMMIT_MESSAGE=Update codex-workspace-sync"
