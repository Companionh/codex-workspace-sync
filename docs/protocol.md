# Protocol Notes

## Authentication

- first-time device setup uses SSH-backed enrollment
- the server mints a device secret and stores only a hash
- normal sync uses HTTPS with device ID and device secret headers

## Lease flow

1. Client requests the global active lease.
2. Server grants it if no live device owns it.
3. If another live device owns it, the requester must abort or steal the lease.
4. Heartbeats refresh `last_heartbeat_at`.
5. If heartbeats stop for 60 seconds, the lease expires automatically.

## Alignment flow

Before `turn-on-sync`, the client compares local state to the server.

If mismatches exist and the client has not explicitly aligned with `update-from-server` or `override-current-state`, live sync is blocked and the user must choose one of those actions.

## Mismatch flow

Thread mismatch resolution is lineage-based:

- compare the last common normalized turn hash
- choose server or local lineage
- publish the decision to the server
- apply that choice on every device during the next update cycle

