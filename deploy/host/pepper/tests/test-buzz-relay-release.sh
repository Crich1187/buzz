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
grep -Fq 'Environment=BUZZ_MAX_CONCURRENT_HANDLERS=45' "$unit"
# root-jk1sw Major 2: the previously shipped 64-over-pool-50 combination is
# a regression, not a tuning choice — refuse it explicitly.
if grep -Fq 'Environment=BUZZ_MAX_CONCURRENT_HANDLERS=64' "$unit"; then exit 1; fi
# The unit's handler ceiling must stay strictly below the pool it draws from.
unit_pool=$(sed -n 's/^Environment=BUZZ_DB_POOL_SIZE=//p' "$unit")
unit_handlers=$(sed -n 's/^Environment=BUZZ_MAX_CONCURRENT_HANDLERS=//p' "$unit")
test "$unit_handlers" -lt "$unit_pool"
test "$unit_handlers" -eq "$((unit_pool - 5))"
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

# ---------------------------------------------------------------------------
# root-jk1sw Major 3 — the unit must never be a viable half-deployment.
#
# The prior candidate left unit bytes at /etc/systemd/system with no release
# tree, so a future daemon-reload + start would have ExecStart'd a missing path.
# Source-side that is closed two ways: the unit refuses to start without the
# release (Condition/Assert), and the launcher refuses explicitly rather than
# failing on exec.
# ---------------------------------------------------------------------------
grep -Fq 'ConditionPathExists=/opt/buzz-relay/current/target/release/buzz-relay' "$unit"
grep -Fq 'AssertPathExists=/opt/buzz-relay/current/deploy/host/pepper/buzz-relay-launch.sh' "$unit"

# Launcher refuses when the release root is absent entirely.
missing_root="$tmp/no-such-release"
printf 'EXAMPLE=one\n' >"$tmp/launch.env"
set +e
BUZZ_RELAY_RELEASE_ROOT="$missing_root" BUZZ_RELAY_ENV_FILE="$tmp/launch.env" \
    "$launcher" >"$tmp/launch-missing.out" 2>"$tmp/launch-missing.err"
launch_missing_rc=$?
set -e
test "$launch_missing_rc" -eq 78
test ! -s "$tmp/launch-missing.out"
grep -Fq 'no immutable release at' "$tmp/launch-missing.err"

# Launcher refuses when the release exists but carries no relay binary.
partial_root="$tmp/partial-release"
mkdir -p "$partial_root/target/release"
set +e
BUZZ_RELAY_RELEASE_ROOT="$partial_root" BUZZ_RELAY_ENV_FILE="$tmp/launch.env" \
    "$launcher" >"$tmp/launch-partial.out" 2>"$tmp/launch-partial.err"
launch_partial_rc=$?
set -e
test "$launch_partial_rc" -eq 78
test ! -s "$tmp/launch-partial.out"
grep -Fq 'no executable relay binary' "$tmp/launch-partial.err"

# Ordering: --apply installs the unit only once `current` resolves to a
# complete release, so the unit and its release land together or not at all.
order_root="$tmp/order-root"
order_unit="$tmp/order-unit.service"
"$installer" --apply --no-systemd --root "$order_root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/order.env" --unit-dest "$order_unit"
test -L "$order_root/current"
test -x "$order_root/current/target/release/buzz-relay"
test -x "$order_root/current/deploy/host/pepper/buzz-relay-launch.sh"
test -f "$order_unit"

# A refused release must not leave a unit behind: an incomplete source is
# rejected before any unit byte is written.
bad_source="$tmp/bad-source"
bad_unit="$tmp/bad-unit.service"
mkdir -p "$bad_source/web/dist"
set +e
"$installer" --apply --no-systemd --root "$tmp/bad-root" --source "$bad_source" --revision charlie \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/bad.env" --unit-dest "$bad_unit" \
    >"$tmp/bad.out" 2>"$tmp/bad.err"
bad_rc=$?
set -e
test "$bad_rc" -ne 0
test ! -e "$bad_unit"
test ! -e "$tmp/bad-root/current"

printf 'PASS: release pointer and rollback contract\n'
printf 'PASS: unit/launcher fail closed without an immutable release\n'
