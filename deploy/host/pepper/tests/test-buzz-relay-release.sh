#!/usr/bin/env bash
# Regression: a Pepper relay release must run immutable release bytes and be
# reversible without touching the shared developer checkout or secret values.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
installer="$repo_root/deploy/host/pepper/buzz-relay-release.sh"
unit="$repo_root/deploy/host/pepper/buzz-relay.service"
launcher="$repo_root/deploy/host/pepper/buzz-relay-launch.sh"

test -x "$installer"
test -f "$unit"
test -x "$launcher"

grep -Fq 'WorkingDirectory=/opt/buzz-relay/current' "$unit"
grep -Fq 'ExecStart=/opt/buzz-relay/current/deploy/host/pepper/buzz-relay-launch.sh' "$unit"
grep -Fq 'Environment=BUZZ_DB_POOL_SIZE=50' "$unit"
grep -Fq 'Environment=BUZZ_MAX_CONCURRENT_HANDLERS=64' "$unit"
grep -Fq 'Environment=BUZZ_DRAIN_JITTER_MS=5000' "$unit"
if grep -Fq '/root/buzz' "$unit"; then exit 1; fi
if grep -Fq 'just relay' "$unit"; then exit 1; fi
grep -Fq 'BUZZ_RELAY_ENV_FILE' "$launcher"
grep -Fq 'target/release/buzz-relay' "$launcher"

tmp=$(mktemp -d)
cleanup() {
    status=$?
    rm -rf "$tmp"
    exit "$status"
}
trap cleanup EXIT
source_a="$tmp/source-a"
source_b="$tmp/source-b"
root="$tmp/release-root"
mkdir -p "$source_a/target/release" "$source_b/target/release"
mkdir -p "$source_a/web/dist" "$source_b/web/dist"
printf '#!/usr/bin/env bash\nexit 0\n' >"$source_a/target/release/buzz-relay"
printf '#!/usr/bin/env bash\nexit 0\n' >"$source_b/target/release/buzz-relay"
chmod 0755 "$source_a/target/release/buzz-relay" "$source_b/target/release/buzz-relay"
printf 'alpha\n' >"$source_a/REVISION"
printf 'bravo\n' >"$source_b/REVISION"
printf 'EXAMPLE=one\n' >"$source_a/runtime.env"
printf 'EXAMPLE=two\n' >"$source_b/runtime.env"
printf 'fixture-a\n' >"$source_a/web/dist/index.html"
printf 'fixture-b\n' >"$source_b/web/dist/index.html"
printf '[Service]\nExecStart=/legacy/relay\n' >"$tmp/buzz-relay.service"

"$installer" --apply --no-systemd --root "$root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/runtime.env" --unit-dest "$tmp/buzz-relay.service"
test "$(readlink "$root/current")" = "releases/alpha"
test -f "$tmp/buzz-relay.service"
grep -Fqx 'revision=alpha' "$root/releases/alpha/manifest"
cmp -s "$root/rollback/legacy-buzz-relay.service" <(printf '[Service]\nExecStart=/legacy/relay\n')
"$installer" --apply --no-systemd --root "$root" --source "$source_b" --revision bravo \
    --env-source "$source_b/runtime.env" --env-dest "$tmp/runtime.env" --unit-dest "$tmp/buzz-relay.service"
test "$(readlink "$root/current")" = "releases/bravo"
previous=$(<"$root/rollback/previous-release")
test "$previous" = "alpha"
"$installer" --apply --no-systemd --root "$root" --rollback --env-dest "$tmp/runtime.env"
test "$(readlink "$root/current")" = "releases/alpha"
cmp -s "$tmp/runtime.env" "$source_a/runtime.env"

printf '[Service]\nExecStart=/legacy/relay\n' >"$tmp/legacy.service"
"$installer" --apply --no-systemd --root "$tmp/legacy-root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/legacy.env" --unit-dest "$tmp/legacy.service"
"$installer" --apply --no-systemd --root "$tmp/legacy-root" --rollback --unit-dest "$tmp/legacy.service"
cmp -s "$tmp/legacy.service" <(printf '[Service]\nExecStart=/legacy/relay\n')

printf 'PASS: release pointer and rollback contract\n'
