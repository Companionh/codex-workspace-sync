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
- lease state, globally or per-superproject depending on client configuration
- shared skills catalog
- superproject manifests
- raw Codex bundles
- normalized checkpoints
- cached thread display metadata
- mismatch history
- server backups before overrides

## Sync model

The client separates three kinds of sync state:

- shared runtime state for the Codex installation on a device
- tracked per-thread payloads
- managed Markdown docs and superproject metadata

The client creates atomic checkpoints that combine the right subset of:

- managed Markdown deltas
- raw Codex artifacts for one tracked thread
- shared Codex runtime artifacts
- normalized thread hashes
- summary metadata

Update flow is metadata-first:

1. fetch manifest, thread summaries, shared runtime revision, and shared skill revision
2. compare local docs and revision markers
3. prompt the operator about docs, shared runtime, shared skills, and threads separately
4. bulk-fetch only the selected payloads

If a turn is still in progress, the client may stage scratch state, but only completed checkpoints are canonical.

## Safety model

- missing heartbeats for 2 minutes expire the global lease
- devices must pass `doctor` and align with the server before starting live sync
- new and missing files are summarized in one prompt rather than per-file prompts
- destructive deletes are quarantined
- overrides require sync to be off and create server backups first
- queue retry state is persisted locally so transient failures do not silently drop checkpoints
