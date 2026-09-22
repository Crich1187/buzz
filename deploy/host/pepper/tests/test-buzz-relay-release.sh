#!/usr/bin/env bash
# Regression: a Pepper relay release must run immutable release bytes and be
# reversible without touching the shared developer checkout or secret values.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
installer="$repo_root/deploy/host/pepper/buzz-relay-release.sh"
unit="$repo_root/deploy/host/pepper/buzz-relay.service"
launcher="$repo_root/deploy/host/pepper/buzz-relay-launch.sh"
preflight="$repo_root/deploy/host/pepper/buzz-relay-s3-preflight.py"

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
    [[ -n "${fake_pid:-}" ]] && kill "$fake_pid" 2>/dev/null
    rm -rf "$tmp"
    exit "$status"
}
trap cleanup EXIT
# root-jljfy: every --apply runs the S3 auth preflight. The fixture has no
# object store, so the harness substitutes recording stubs; the real preflight
# is exercised against a local fake object store further down.
fake_pid=
preflight_ok="$tmp/preflight-ok.sh"
preflight_fail="$tmp/preflight-fail.sh"
preflight_log="$tmp/preflight.log"
# The stubs must record their own "$*" at run time, so single quotes are deliberate.
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$*" >>"%s"\nexit 0\n' "$preflight_log" >"$preflight_ok"
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$*" >>"%s"\nexit 1\n' "$preflight_log" >"$preflight_fail"
chmod 0755 "$preflight_ok" "$preflight_fail"
export BUZZ_RELAY_S3_PREFLIGHT="$preflight_ok"
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

# ---------------------------------------------------------------------------
# root-jk1sw Gate4 Blocker — the unit must declare the internal transport alias.
#
# The deployed relay accepted only its canonical wss:// identity while the
# on-host fleet signs ws:// through buzz-local-loopback, so every fleet NIP-42
# handshake failed. The alias is what makes the two transports one relay.
# ---------------------------------------------------------------------------
grep -Fq 'Environment=BUZZ_RELAY_URL_ALIAS_SCHEMES=ws' "$unit"
# It must stay a *scheme* list. A URL here would let a different host be named
# and would reopen cross-community AUTH replay.
if grep -E '^Environment=BUZZ_RELAY_URL_ALIAS_SCHEMES=.*://' "$unit" >/dev/null; then exit 1; fi

# ---------------------------------------------------------------------------
# root-jk1sw Gate4 Minor 3 — production must not log at DEBUG by default.
# ---------------------------------------------------------------------------
grep -Fq 'BUZZ_ALLOW_DEBUG_LOGGING' "$launcher"
log_root="$tmp/log-release"
mkdir -p "$log_root/target/release" "$log_root/deploy/host/pepper"
# A stub "relay" that reports the RUST_LOG it was launched with. The inner
# expansion must reach the stub verbatim, so single quotes are deliberate.
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\nprintf "RUST_LOG=%%s\\n" "${RUST_LOG:-<unset>}"\n' \
    >"$log_root/target/release/buzz-relay"
chmod 0755 "$log_root/target/release/buzz-relay"
printf 'EXAMPLE=one\n' >"$tmp/log.env"

# Unset upstream -> clamped to info.
out=$(RUST_LOG='' BUZZ_RELAY_RELEASE_ROOT="$log_root" BUZZ_RELAY_ENV_FILE="$tmp/log.env" \
    "$launcher" 2>/dev/null)
test "$out" = "RUST_LOG=buzz_relay=info"

# A debug level from the operator-owned env file -> clamped to info.
out=$(RUST_LOG=buzz_relay=debug BUZZ_RELAY_RELEASE_ROOT="$log_root" \
    BUZZ_RELAY_ENV_FILE="$tmp/log.env" "$launcher" 2>/dev/null)
test "$out" = "RUST_LOG=buzz_relay=info"

# ...unless the operator explicitly opts in for an investigation.
out=$(RUST_LOG=buzz_relay=debug BUZZ_ALLOW_DEBUG_LOGGING=1 \
    BUZZ_RELAY_RELEASE_ROOT="$log_root" BUZZ_RELAY_ENV_FILE="$tmp/log.env" \
    "$launcher" 2>/dev/null)
test "$out" = "RUST_LOG=buzz_relay=debug"

# A non-debug explicit level is passed through untouched.
out=$(RUST_LOG=buzz_relay=warn BUZZ_RELAY_RELEASE_ROOT="$log_root" \
    BUZZ_RELAY_ENV_FILE="$tmp/log.env" "$launcher" 2>/dev/null)
test "$out" = "RUST_LOG=buzz_relay=warn"

# ---------------------------------------------------------------------------
# root-jk1sw Gate4 Major 1 — rollback must be real, and a no-op must FAIL.
#
# Reproduction of the deployed defect: the immutable unit is already installed
# at $unit_dest when the first --apply runs, so the installer recorded its own
# unit as the "legacy" unit. `--rollback` then reinstalled the identical unit,
# exited 0, and printed "rolled back relay release=legacy-unit" while changing
# nothing at all.
# ---------------------------------------------------------------------------
noop_root="$tmp/noop-root"
noop_unit="$tmp/noop-unit.service"
# Pre-seed $unit_dest with a release-shaped unit, exactly as production had it.
sed "s#/opt/buzz-relay/current#$noop_root/current#g" "$unit" >"$noop_unit"
"$installer" --apply --no-systemd --root "$noop_root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/noop.env" --unit-dest "$noop_unit"
# The release-shaped unit must NOT have been recorded as the legacy unit.
test ! -e "$noop_root/rollback/legacy-buzz-relay.service"
# With nothing to roll back to, rollback must refuse loudly.
set +e
"$installer" --apply --no-systemd --root "$noop_root" --rollback \
    --unit-dest "$noop_unit" --env-dest "$tmp/noop.env" \
    >"$tmp/noop.out" 2>"$tmp/noop.err"
noop_rc=$?
set -e
test "$noop_rc" -ne 0
if grep -Fq 'rolled back' "$tmp/noop.out"; then exit 1; fi
grep -Fq 'rollback refused' "$tmp/noop.err"

# The state production is actually in right now: a PREVIOUS installer already
# recorded a release-shaped unit as the legacy unit, so the file exists and is
# byte-identical to what is installed. The `is_release_unit` guard above stops
# this being created going forward, but it cannot un-create the one on disk —
# so rollback itself must refuse rather than reinstall the same bytes and
# report success. This is the assertion that binds `refuse_noop_rollback`.
seeded_root="$tmp/seeded-root"
seeded_unit="$tmp/seeded-unit.service"
sed "s#/opt/buzz-relay/current#$seeded_root/current#g" "$unit" >"$seeded_unit"
"$installer" --apply --no-systemd --root "$seeded_root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/seeded.env" --unit-dest "$seeded_unit"
# Reproduce the bad artifact the deployed installer left behind.
install -m 0644 "$seeded_unit" "$seeded_root/rollback/legacy-buzz-relay.service"
# Remove the release pointer so the legacy branch is the one taken.
rm -f "$seeded_root/rollback/previous-release"
set +e
"$installer" --apply --no-systemd --root "$seeded_root" --rollback \
    --unit-dest "$seeded_unit" --env-dest "$tmp/seeded.env" \
    >"$tmp/seeded.out" 2>"$tmp/seeded.err"
seeded_rc=$?
set -e
test "$seeded_rc" -ne 0
if grep -Fq 'rolled back' "$tmp/seeded.out"; then exit 1; fi
grep -Fq 'nothing to roll back to' "$tmp/seeded.err"

# Sabotage control: if the legacy unit genuinely differs, rollback proceeds.
diff_root="$tmp/diff-root"
diff_unit="$tmp/diff-unit.service"
printf '[Service]\nExecStart=/legacy/relay\n' >"$diff_unit"
"$installer" --apply --no-systemd --root "$diff_root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/diff.env" --unit-dest "$diff_unit"
test -e "$diff_root/rollback/legacy-buzz-relay.service"
"$installer" --apply --no-systemd --root "$diff_root" --rollback \
    --unit-dest "$diff_unit" --env-dest "$tmp/diff.env"
cmp -s "$diff_unit" <(printf '[Service]\nExecStart=/legacy/relay\n')

# ---------------------------------------------------------------------------
# root-jk1sw Gate4 Major 1 — the outgoing unit and env are preserved across
# release-to-release rollback, not just the release pointer.
# ---------------------------------------------------------------------------
unit_root="$tmp/unit-root"
unit_dest_a="$tmp/unit-roll.service"
printf '[Service]\nExecStart=/legacy/relay\n' >"$unit_dest_a"
"$installer" --apply --no-systemd --root "$unit_root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/unit-roll.env" --unit-dest "$unit_dest_a"
# Mark the unit that ships with release alpha so the rollback is observable.
printf '[Service]\nExecStart=/alpha/relay\nEnvironment=MARKER=alpha\n' >"$unit_dest_a"
"$installer" --apply --no-systemd --root "$unit_root" --source "$source_b" --revision bravo \
    --env-source "$source_b/runtime.env" --env-dest "$tmp/unit-roll.env" --unit-dest "$unit_dest_a"
test -f "$unit_root/rollback/previous-buzz-relay.service"
grep -Fq 'MARKER=alpha' "$unit_root/rollback/previous-buzz-relay.service"
"$installer" --apply --no-systemd --root "$unit_root" --rollback \
    --unit-dest "$unit_dest_a" --env-dest "$tmp/unit-roll.env"
test "$(readlink "$unit_root/current")" = "releases/alpha"
# The unit that shipped with alpha is back — not bravo's unit.
grep -Fq 'MARKER=alpha' "$unit_dest_a"
# ...and alpha's runtime env came back with it.
cmp -s "$tmp/unit-roll.env" "$source_a/runtime.env"

# ---------------------------------------------------------------------------
# Rollback metadata is recorded and is value-safe: it names revisions and
# whether snapshots exist, never any runtime env value.
# ---------------------------------------------------------------------------
meta="$unit_root/rollback/metadata"
test -f "$meta"
test "$(stat -c '%a' "$meta")" = "600"
grep -Eq '^installed_revision=(alpha|bravo)$' "$meta"
grep -Eq '^env_backup=(yes|no)$' "$meta"
grep -Eq '^previous_unit_backup=(yes|no)$' "$meta"
# No runtime env value may appear in the metadata. `EXAMPLE=one|two` are the
# fixture's env values; their presence would mean the metadata leaks config.
if grep -Fq 'EXAMPLE=' "$meta"; then exit 1; fi

# ---------------------------------------------------------------------------
# root-jljfy — the release refuses to swap when the runtime env cannot
# authenticate to the object store.
#
# 2026-09-20: the relay's S3 secret had silently drifted from the MinIO
# container's; the startup git object-store conformance probe then crash-looped
# both the candidate and the rollback target (HTTP 403 SignatureDoesNotMatch).
# Checking auth *before* the swap makes that a refused release instead.
# ---------------------------------------------------------------------------
test -x "$preflight"
pf_root="$tmp/preflight-root"
pf_env="$tmp/pf.env"
pf_unit="$tmp/pf.service"

# 1) A failing preflight refuses the release before any byte is written.
set +e
BUZZ_RELAY_S3_PREFLIGHT="$preflight_fail" \
    "$installer" --apply --no-systemd --root "$pf_root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$pf_env" --unit-dest "$pf_unit" \
    >"$tmp/pf.out" 2>"$tmp/pf.err"
pf_rc=$?
set -e
test "$pf_rc" -eq 66
grep -Fq 'S3 auth preflight failed' "$tmp/pf.err"
test ! -e "$pf_root/current"
test ! -e "$pf_root/releases/alpha"
test ! -e "$pf_env"
test ! -e "$pf_unit"

# 2) The preflight is handed exactly the env source this release would
#    install, and without systemd it must not try to introspect a unit.
grep -Fq -- "--env-file $source_a/runtime.env" "$preflight_log"
if grep -Fq -- '--unit' "$preflight_log"; then exit 1; fi

# 3) --skip-s3-preflight bypasses it (dry runs only) and says so.
BUZZ_RELAY_S3_PREFLIGHT="$preflight_fail" \
    "$installer" --apply --no-systemd --skip-s3-preflight --root "$pf_root" --source "$source_a" \
    --revision alpha --env-source "$source_a/runtime.env" --env-dest "$pf_env" --unit-dest "$pf_unit" \
    2>"$tmp/pf-skip.err"
test "$(readlink "$pf_root/current")" = "releases/alpha"
grep -Fq 'S3 auth preflight skipped' "$tmp/pf-skip.err"

# 4) Rollback never runs the preflight: a broken object store must not block
#    reverting to the previous release.
"$installer" --apply --no-systemd --root "$pf_root" --source "$source_b" --revision bravo \
    --env-source "$source_b/runtime.env" --env-dest "$pf_env" --unit-dest "$pf_unit"
: >"$preflight_log"
BUZZ_RELAY_S3_PREFLIGHT="$preflight_fail" \
    "$installer" --apply --no-systemd --root "$pf_root" --rollback --env-dest "$pf_env" --unit-dest "$pf_unit"
test "$(readlink "$pf_root/current")" = "releases/alpha"
test ! -s "$preflight_log"

# 5) A missing preflight command is a refusal, not a silent pass.
set +e
BUZZ_RELAY_S3_PREFLIGHT="$tmp/no-such-preflight" \
    "$installer" --apply --no-systemd --root "$tmp/pf-missing-root" --source "$source_a" --revision alpha \
    --env-source "$source_a/runtime.env" --env-dest "$tmp/pf-missing.env" --unit-dest "$tmp/pf-missing.service" \
    >/dev/null 2>"$tmp/pf-missing.err"
pf_missing_rc=$?
set -e
test "$pf_missing_rc" -eq 66
grep -Fq 'S3 preflight command is missing' "$tmp/pf-missing.err"
test ! -e "$tmp/pf-missing-root/current"

# 6) The real preflight against a local fake object store: it signs SigV4,
#    reports verdicts by status/error code only, and never prints a secret.
python3 -m py_compile "$preflight"
fake_port_file="$tmp/fake-s3.port"
python3 - "$fake_port_file" <<'PY' &
import re, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
port_file = sys.argv[1]
GOOD = "fixture-access-key"
EMPTY_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
AUTH_RE = re.compile(r"^AWS4-HMAC-SHA256 Credential=([^/]+)/\d{8}/us-east-1/s3/aws4_request, "
                     r"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=[0-9a-f]{64}$")
BODY_403 = (b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>SignatureDoesNotMatch</Code>'
            b'<Message>The request signature we calculated does not match</Message></Error>')
BODY_200 = b'<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><KeyCount>0</KeyCount></ListBucketResult>'
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_GET(self):
        # Shape-checks the SigV4 authorization (credential scope, signed
        # headers, 64-hex signature, empty-payload hash); it does not recompute
        # the HMAC — the live MinIO 200/403 run covers signature correctness.
        auth = self.headers.get("Authorization", "")
        match = AUTH_RE.match(auth)
        ok = (match is not None and match.group(1) == GOOD
              and self.headers.get("x-amz-content-sha256") == EMPTY_SHA
              and bool(self.headers.get("x-amz-date"))
              and self.path == "/fixture/?list-type=2&max-keys=1")
        body = BODY_200 if ok else BODY_403
        self.send_response(200 if ok else 403)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
server = HTTPServer(("127.0.0.1", 0), Handler)
with open(port_file, "w") as handle:
    handle.write(str(server.server_port))
server.serve_forever()
PY
fake_pid=$!
for _ in $(seq 1 50); do [[ -s "$fake_port_file" ]] && break; sleep 0.1; done
test -s "$fake_port_file"
fake_port=$(<"$fake_port_file")
printf 'BUZZ_S3_ENDPOINT=http://127.0.0.1:%s\nBUZZ_S3_BUCKET=fixture\nBUZZ_S3_REGION=us-east-1\nBUZZ_S3_ADDRESSING_STYLE=path\n' \
    "$fake_port" >"$tmp/s3-base.env"
printf 'BUZZ_S3_ACCESS_KEY="fixture-access-key"\nBUZZ_S3_SECRET_KEY="fixture-secret-value"\n' >"$tmp/s3-good.env"
printf 'BUZZ_S3_ACCESS_KEY=stale-access-key\nBUZZ_S3_SECRET_KEY=stale-secret-value\n' >"$tmp/s3-stale.env"

# Good credentials -> authenticated, exit 0.
"$preflight" --env-file "$tmp/s3-base.env" --env-file "$tmp/s3-good.env" >"$tmp/s3-good.out" 2>"$tmp/s3-good.err"
grep -Fq 's3 preflight ok' "$tmp/s3-good.out"
grep -Fq 'http=200' "$tmp/s3-good.out"

# Stale credentials -> rejected with the S3 error code, exit 1, no value echoed.
set +e
"$preflight" --env-file "$tmp/s3-base.env" --env-file "$tmp/s3-stale.env" >"$tmp/s3-stale.out" 2>"$tmp/s3-stale.err"
s3_stale_rc=$?
set -e
test "$s3_stale_rc" -eq 1
grep -Fq 'http=403' "$tmp/s3-stale.err"
grep -Fq 'code=SignatureDoesNotMatch' "$tmp/s3-stale.err"
if grep -Fq 'stale-secret-value' "$tmp/s3-stale.out" "$tmp/s3-stale.err"; then exit 1; fi
if grep -Fq 'stale-access-key' "$tmp/s3-stale.out" "$tmp/s3-stale.err"; then exit 1; fi

# Later env files override earlier ones, exactly like `set -a; source` at launch.
set +e
"$preflight" --env-file "$tmp/s3-base.env" --env-file "$tmp/s3-good.env" --env-file "$tmp/s3-stale.env" \
    >/dev/null 2>&1
s3_order_rc=$?
set -e
test "$s3_order_rc" -eq 1

# Unit EnvironmentFile= entries are honoured (via `systemctl cat`), optional
# ones may be absent, and they sit BELOW the runtime env file: on pepper the
# host drop-in supplies the keys and relay.env the rest.
shim="$tmp/shim"
mkdir -p "$shim"
printf '[Service]\nEnvironmentFile=-%s\nEnvironmentFile=%s\n' "$tmp/does-not-exist.env" "$tmp/s3-good.env" \
    >"$tmp/fake-unit.txt"
# The shim must see its own "$1" at run time, so single quotes are deliberate.
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\n[[ "$1" = cat ]] && cat "%s"\n' "$tmp/fake-unit.txt" >"$shim/systemctl"
chmod 0755 "$shim/systemctl"
PATH="$shim:$PATH" "$preflight" --unit fake.service --env-file "$tmp/s3-base.env" >"$tmp/s3-unit.out" 2>&1
grep -Fq 's3 preflight ok' "$tmp/s3-unit.out"

# Missing configuration is a configuration error (exit 2) that names keys only.
set +e
"$preflight" --env-file "$tmp/s3-base.env" >/dev/null 2>"$tmp/s3-missing.err"
s3_missing_rc=$?
set -e
test "$s3_missing_rc" -eq 2
grep -Fq 'BUZZ_S3_ACCESS_KEY' "$tmp/s3-missing.err"

kill "$fake_pid" 2>/dev/null || true
wait "$fake_pid" 2>/dev/null || true
fake_pid=

printf 'PASS: release pointer and rollback contract\n'
printf 'PASS: unit/launcher fail closed without an immutable release\n'
printf 'PASS: NIP-42 transport alias declared as a bounded scheme list\n'
printf 'PASS: launcher clamps production logging unless explicitly opted out\n'
printf 'PASS: no-op rollback refuses instead of reporting success\n'
printf 'PASS: unit and env are preserved and restored across rollback\n'
printf 'PASS: rollback metadata is recorded and value-safe\n'
printf 'PASS: S3 auth preflight gates the release swap and is value-safe\n'
