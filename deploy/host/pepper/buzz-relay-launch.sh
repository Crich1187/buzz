#!/usr/bin/env bash
# The only runtime input outside a release is the root-owned, mode-0600 env
# file installed by buzz-relay-release.sh.  It is sourced without logging so
# credentials never enter argv or the journal.
set -euo pipefail

release_root=/opt/buzz-relay/current
env_file=${BUZZ_RELAY_ENV_FILE:?BUZZ_RELAY_ENV_FILE must name the runtime env file}

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
exec "$release_root/target/release/buzz-relay"
