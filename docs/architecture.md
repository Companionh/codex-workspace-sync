# Architecture

## Overview

`codex-workspace-sync` has two runtime halves:

- a Windows client that watches Codex session artifacts and managed Markdown
- a self-hosted sync hub that stores canonical state for all superprojects

The repo itself is published with a sanitized export flow:

- Windows exports a curated tree into a dedicated temp checkout before commit and push
- the server app checkout fast-forwards from GitHub and leaves live state under `/opt/codex-workspace-sync/state` outside the code repo

The sync hub owns:

- device registration
- the global active lease
- shared skills catalog
- superproject manifests
- raw Codex bundles
- normalized checkpoints
- mismatch history
- server backups before overrides

## Sync model

The client creates atomic checkpoints that combine:

- managed Markdown deltas
- raw Codex artifacts
- normalized thread hashes
- summary metadata

If a turn is still in progress, the client may stage scratch state, but only completed checkpoints are canonical.

## Safety model

- missing heartbeats for 60 seconds expire the global lease
- devices must align with the server before starting live sync
- new and missing files are summarized in one prompt rather than per-file prompts
- destructive deletes are quarantined
- overrides require sync to be off and create server backups first
