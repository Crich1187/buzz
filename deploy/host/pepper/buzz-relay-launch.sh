#!/usr/bin/env bash
# The only runtime input outside a release is the root-owned, mode-0600 env
# file installed by buzz-relay-release.sh.  It is sourced without logging so
# credentials never enter argv or the journal.
set -euo pipefail

release_root=${BUZZ_RELAY_RELEASE_ROOT:-/opt/buzz-relay/current}
env_file=${BUZZ_RELAY_ENV_FILE:?BUZZ_RELAY_ENV_FILE must name the runtime env file}

# root-jk1sw Major 3: refuse explicitly when the immutable release is absent.
# Without this the final `exec` fails with a bare 126/127 and the operator sees
# a crash loop rather than "this unit was installed without its release".
if [[ ! -d "$release_root" ]]; then
    printf 'buzz-relay launch refused: no immutable release at %s\n' "$release_root" >&2
    exit 78
fi
if [[ ! -x "$release_root/target/release/buzz-relay" ]]; then
    printf 'buzz-relay launch refused: release at %s has no executable relay binary\n' "$release_root" >&2
    exit 78
fi

if [[ ! -r "$env_file" ]]; then
    printf 'buzz-relay launch refused: runtime env file is unavailable\n' >&2
    exit 78
fi

set -a
# shellcheck disable=SC1090
# Deployment selects the root-owned runtime file.
source "$env_file"
set +a

export BUZZ_WEB_DIR=${BUZZ_WEB_DIR:-"$release_root/web/dist"}

# root-jk1sw Gate4 Minor 3: production must not log at DEBUG by default.
#
# The deployed relay emitted a per-request DEBUG trace line — 1,675 of 2,458
# journal lines in a sampled window. That is retention pressure on a shared
# host, and it is what evicted the pre-deploy startup records that a later
# audit needed. The runtime env file is operator-owned and may legitimately
# carry a debug level for a live investigation, so this clamps rather than
# overrides: set BUZZ_ALLOW_DEBUG_LOGGING=1 to keep a debug/trace level.
if [[ "${BUZZ_ALLOW_DEBUG_LOGGING:-0}" != "1" ]]; then
    case "${RUST_LOG:-}" in
        *debug*|*trace*|*DEBUG*|*TRACE*)
            printf 'buzz-relay: clamping RUST_LOG to info (set BUZZ_ALLOW_DEBUG_LOGGING=1 to keep debug)\n' >&2
            export RUST_LOG=buzz_relay=info
            ;;
        '')
            export RUST_LOG=buzz_relay=info
            ;;
    esac
fi

exec "$release_root/target/release/buzz-relay"
