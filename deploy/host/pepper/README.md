# Pepper relay deployment

Immutable-release deployment for the shared `buzz.lymarinc.com` relay.

This document exists because a release that passed source review still took the
whole on-host agent fleet off the relay, crash-looped 97 times without the
installer noticing, and could not be rolled back. Each section below records
what went wrong and what now prevents it.

## Layout

```
/opt/buzz-relay/
  releases/<revision>/     immutable: relay binary, launcher, web/dist, manifest
  current -> releases/<revision>
  rollback/
    previous-release              revision displaced by the current one
    previous-buzz-relay.service   unit that shipped with that revision
    legacy-buzz-relay.service     pre-immutable unit, if there was one
    relay.env                     runtime env displaced by the current release
    metadata                      revisions + which snapshots exist (0600)
/etc/systemd/system/buzz-relay.service
/etc/buzz-relay/relay.env         root-owned, 0600, never logged or echoed
```

The runtime env file is the only input from outside a release. It is copied
byte-for-byte and its values never reach argv, the journal, or `metadata`.

## Install

```bash
deploy/host/pepper/buzz-relay-release.sh \
    --apply --source /path/to/built/checkout --revision <sha> --restart
```

`--restart` triggers verification (below). Add `--no-verify` only for a
non-production dry run — never to get past a failing soak.

## The NIP-42 transport alias (required on this host)

This relay is reachable two ways:

- publicly as `wss://buzz.lymarinc.com` through the TLS ingress;
- on-host as `ws://buzz.lymarinc.com` through `buzz-local-loopback`, which is
  how every `buzz-acp-fleet@*` and `buzz-acp-kimi` connector reaches it.

NIP-42 binds an AUTH event to the relay by its URL, so a client that connects
over the plaintext hop signs `ws://…`. A relay that accepts only its canonical
`wss://…` identity rejects every one of those handshakes. That is exactly what
happened: 431 `relay url mismatch` rejections in the first 17 minutes after
deployment, against zero in the preceding 459,037 journal lines, with each
connector stuck in a ~50s reconnect loop and receiving no mentions.

The unit therefore sets:

```ini
Environment=BUZZ_RELAY_URL_ALIAS_SCHEMES=ws
```

**This is a scheme list, not a URL list, and that distinction is the security
property.** At verification time each scheme is composed onto the connection's
own resolved community host, never onto anything from configuration or from the
client. So an AUTH event signed for community A is still rejected at community
B under every configured scheme — the cross-community replay guarantee is
unchanged. Only `ws` and `wss` are accepted; anything else fails startup rather
than being ignored, because a silently dropped typo would look configured while
the fleet stayed locked out.

Removing this line does not harden the relay. It re-breaks the fleet.

If the fleet is ever reconfigured to sign `wss://` (for example by terminating
TLS on the loopback), drop the alias in the same change — not before.

## Verification and automatic rollback

`--restart` runs a soak, because the previous installer's only check was
`systemctl is-active` sampled immediately after `systemctl restart`, while a
doomed process was still alive. It passed on a release that was already
crash-looping, and it recorded that deployment as successful.

The soak (default 45s, `--soak-seconds`) requires all of:

| Check | Why |
|---|---|
| unit stays `active` for the whole window | `is-active` at t=0 proves nothing |
| systemd restart counter does not advance | crash-loop detection |
| `/_liveness`, `/_readiness`, `/_status` all answer | the real endpoints; `/healthz` does not exist on this relay |
| NIP-42 relay-tag rejections stay at or below `--max-auth-mismatch` (default 3) | a relay that is up but refusing every client is still a failed deploy |

Only counts are read from the journal — never message bodies, never values.

On any failure the installer rolls back automatically and exits **70**. The
rollback restart runs with verification disabled so a second failure cannot
recurse; if that rollback also fails, the relay needs manual attention and the
installer says so.

## Rollback

```bash
deploy/host/pepper/buzz-relay-release.sh --apply --rollback --restart
```

Rollback restores the previous **release, unit, and runtime env** together.
Restoring only the release pointer would leave the current unit's settings —
pool size, handler ceiling, alias schemes — in force against older bytes, which
is a combination that was never tested.

**A rollback that cannot change anything fails with exit 66 instead of
reporting success.** The deployed installer recorded its own immutable unit as
the "legacy" unit, so `--rollback` reinstalled identical bytes, exited 0, and
printed `rolled back relay release=legacy-unit` while changing nothing. Two
guards now prevent that: a release-shaped unit is never recorded as the legacy
unit in the first place, and rollback refuses when its target is byte-identical
to what is installed.

The first immutable install on a host has no previous release. Its rollback
target is the legacy unit, and if none was captured, rollback refuses — which
is the honest answer, not a silent no-op.

## Logging

The launcher clamps `RUST_LOG` to `buzz_relay=info` when it is unset or
requests debug/trace. Production was found emitting a per-request DEBUG trace
line — 1,675 of 2,458 lines in a sampled window — which is retention pressure
on a shared host and evicted the startup records a later audit needed.

For a live investigation, set `BUZZ_ALLOW_DEBUG_LOGGING=1` in the runtime env
file; the clamp then steps aside. Any explicit non-debug level is passed
through untouched.

## Capacity

`BUZZ_DB_POOL_SIZE=50` with `BUZZ_MAX_CONCURRENT_HANDLERS=45` keeps
`handlers + 5 reserved <= pool`, so background acquires (health, audit, admin)
always have a connection and a REQ cannot lose an acquire race into
`error: database error`. The relay clamps to the same number it computes
itself, so the unit and the code cannot silently disagree; a pool at or below
the reservation fails startup rather than collapsing to a one-handler ceiling.

Under a live burst past that ceiling the relay sheds load as
`rate-limited: too many concurrent requests` and never as a database error.

## Tests

```bash
deploy/host/pepper/tests/test-buzz-relay-release.sh
shellcheck -x deploy/host/pepper/*.sh deploy/host/pepper/tests/*.sh
```

The fixture covers the release pointer, rollback of release + unit + env, the
no-op-rollback refusal, legacy-unit capture, install ordering, launcher refusal
without a release, the logging clamp, and that rollback metadata records
revisions without leaking env values.
