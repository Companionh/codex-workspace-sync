@echo off
rem Copy this file to push-config.local.cmd and fill in the values below.
rem That local file is ignored by git so your token does not get committed.

set "GIT_EXE=C:\Program Files\Git\cmd\git.exe"
set "GITHUB_REMOTE=origin"
set "GITHUB_REPO_URL=https://github.com/Companionh/codex-workspace-sync.git"
set "GITHUB_USERNAME=Companionh"
set "GITHUB_BRANCH=main"
set "GITHUB_PAT=replace_me"
rem Optional: if the repo has local changes and you want a fixed commit message.
rem set "COMMIT_MESSAGE=Update codex-workspace-sync"
