#!/usr/bin/env bash
# Install a prebuilt Buzz relay as an immutable, atomically selected release.
# Values in the runtime env file are copied byte-for-byte and never printed.
set -euo pipefail

root=/opt/buzz-relay
source_dir=
revision=
env_source=/root/buzz/.env
env_dest=/etc/buzz-relay/relay.env
unit_dest=/etc/systemd/system/buzz-relay.service
apply=false
rollback=false
restart=false
systemd=true
# root-jk1sw Gate4 Major 2: post-restart verification.
#
# The deployed release crash-looped 97 times in ten minutes and the installer
# reported success, because its only check was `systemctl is-active` sampled
# immediately after `systemctl restart` — while the doomed process was still
# alive. It also could not see that the relay was up but refusing every fleet
# NIP-42 handshake. Verification now soaks: it watches the restart counter,
# the real health endpoints, and the fleet-auth failure rate, and rolls back
# automatically when any of them says the release is bad.
verify=true
soak_seconds=45
health_base=http://127.0.0.1:8080
# Fleet-auth guard: how many NIP-42 relay-tag rejections during the soak mean
# "this release locked the fleet out". Zero is the healthy steady state, so a
# small non-zero bound tolerates an in-flight stale handshake without
# tolerating a systematic lockout.
max_auth_mismatch=3
unit_name=buzz-relay.service

usage() {
    printf '%s\n' 'usage: buzz-relay-release.sh --apply --source DIR --revision SHA [--restart]'
}

while (($#)); do
    case "$1" in
        --apply) apply=true ;;
        --rollback) rollback=true ;;
        --restart) restart=true ;;
        --no-systemd) systemd=false ;;
        --no-verify) verify=false ;;
        --root|--source|--revision|--env-source|--env-dest|--unit-dest|--soak-seconds|--health-base|--max-auth-mismatch|--unit-name)
            (($# >= 2)) || { usage >&2; exit 64; }
            case "$1" in
                --root) root=$2 ;;
                --source) source_dir=$2 ;;
                --revision) revision=$2 ;;
                --env-source) env_source=$2 ;;
                --env-dest) env_dest=$2 ;;
                --unit-dest) unit_dest=$2 ;;
                --soak-seconds) soak_seconds=$2 ;;
                --health-base) health_base=$2 ;;
                --max-auth-mismatch) max_auth_mismatch=$2 ;;
                --unit-name) unit_name=$2 ;;
            esac
            shift
            ;;
        *) usage >&2; exit 64 ;;
    esac
    shift
done

$apply || { usage >&2; exit 64; }
[[ "$root" = /* && "$root" != / ]] || { printf '%s\n' 'release root must be an absolute non-root path' >&2; exit 64; }

installer_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
rollback_dir="$root/rollback"
current="$root/current"

install_unit() {
    install -D -m 0644 "$installer_dir/buzz-relay.service" "$unit_dest"
}

# True when the given unit file is an immutable-release unit for THIS root —
# i.e. it launches out of "$root/current" rather than from a pre-release
# location. Used so a release unit is never mistaken for the legacy unit.
is_release_unit() {
    local candidate=$1
    [[ -r "$candidate" ]] || return 1
    grep -Fq "ExecStart=$root/current/" "$candidate"
}

# Value-safe rollback metadata.
#
# Records only what is needed to reason about a rollback: which revision was
# installed, which one it displaced, and whether a unit/env snapshot exists.
# Never the env file's contents — presence and mode only.
write_rollback_metadata() {
    local installed=$1
    local previous='' env_backup=no unit_backup=no legacy_unit=no
    [[ -r "$rollback_dir/previous-release" ]] && previous=$(<"$rollback_dir/previous-release")
    [[ -e "$rollback_dir/relay.env" ]] && env_backup=yes
    [[ -e "$rollback_dir/previous-buzz-relay.service" ]] && unit_backup=yes
    [[ -e "$rollback_dir/legacy-buzz-relay.service" ]] && legacy_unit=yes
    umask 077
    cat >"$rollback_dir/metadata" <<META
installed_revision=$installed
previous_revision=$previous
env_backup=$env_backup
previous_unit_backup=$unit_backup
legacy_unit_recorded=$legacy_unit
recorded_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
META
    chmod 0600 "$rollback_dir/metadata"
}

# root-jk1sw Gate4 Major 1: a rollback that cannot change anything must fail
# loudly instead of printing success.
#
# The deployed release's rollback exited 0 with "rolled back relay
# release=legacy-unit" while leaving the system byte-identical, because the
# only recorded artifact was a copy of the unit already installed. An operator
# reading that output would believe production had been reverted.
refuse_noop_rollback() {
    local target_unit=$1
    if [[ -e "$unit_dest" && -r "$target_unit" ]] && cmp -s "$target_unit" "$unit_dest"; then
        printf '%s\n' \
            'rollback refused: the recorded unit is identical to the installed unit and no
prior release is recorded — there is nothing to roll back to' >&2
        exit 66
    fi
}

restart_unit() {
    $systemd || return 0
    systemctl daemon-reload
    if $restart; then
        local restarts_before
        restarts_before=$(systemctl show "$unit_name" -p NRestarts --value 2>/dev/null || printf 0)
        local since
        since=$(date '+%Y-%m-%d %H:%M:%S')
        systemctl restart "$unit_name"
        # `is-active` alone is exactly the check that passed on a
        # crash-looping release; it is kept only as a fast first gate.
        systemctl is-active --quiet "$unit_name"
        if $verify; then
            verify_deployment "$restarts_before" "$since"
        fi
    fi
}

# Poll one health endpoint. Returns non-zero on any non-2xx or transport error.
probe_health() {
    local path=$1
    curl --fail --silent --show-error --max-time 5 -o /dev/null "$health_base$path"
}

# Soak the freshly restarted unit and report whether the release is healthy.
#
# Checks, all of which the previous installer was blind to:
#   1. the unit stays active for the whole soak, not just at t=0;
#   2. the systemd restart counter does not advance (crash-loop detection);
#   3. /_liveness and /_readiness answer, and /_status is reachable;
#   4. NIP-42 relay-tag rejections stay at or below the configured bound, so a
#      release that is "up" but refusing the whole fleet still fails.
# Only counts are read from the journal — never message bodies, never values.
verify_deployment() {
    local restarts_before=$1 since=$2
    local deadline=$((SECONDS + soak_seconds))
    local liveness_ok=false readiness_ok=false status_ok=false
    local failure=''

    while ((SECONDS < deadline)); do
        if ! systemctl is-active --quiet "$unit_name"; then
            failure="unit left the active state during the soak"
            break
        fi
        local restarts_now
        restarts_now=$(systemctl show "$unit_name" -p NRestarts --value 2>/dev/null || printf 0)
        if ((restarts_now > restarts_before)); then
            failure="restart counter advanced ${restarts_before} -> ${restarts_now} (crash loop)"
            break
        fi
        probe_health /_liveness >/dev/null 2>&1 && liveness_ok=true
        probe_health /_readiness >/dev/null 2>&1 && readiness_ok=true
        probe_health /_status >/dev/null 2>&1 && status_ok=true
        if $liveness_ok && $readiness_ok && $status_ok; then
            break
        fi
        sleep 3
    done

    if [[ -z "$failure" ]]; then
        $liveness_ok || failure="/_liveness never answered within ${soak_seconds}s"
    fi
    if [[ -z "$failure" ]]; then
        $readiness_ok || failure="/_readiness never answered within ${soak_seconds}s"
    fi
    if [[ -z "$failure" ]]; then
        $status_ok || failure="/_status never answered within ${soak_seconds}s"
    fi

    # Fleet-auth guard. A relay that is healthy to itself but rejecting every
    # client's NIP-42 relay tag is the Gate4 blocker; count-only, no bodies.
    if [[ -z "$failure" ]]; then
        local mismatches
        mismatches=$(journalctl -u "$unit_name" --since "$since" --no-pager 2>/dev/null |
            grep -c 'relay url mismatch' || true)
        mismatches=${mismatches:-0}
        if ((mismatches > max_auth_mismatch)); then
            failure="NIP-42 relay-tag rejections during soak: ${mismatches} (> ${max_auth_mismatch}) — clients cannot authenticate against this release"
        fi
    fi

    if [[ -n "$failure" ]]; then
        printf 'deployment verification FAILED: %s\n' "$failure" >&2
        auto_rollback
        exit 70
    fi
    printf 'deployment verification passed: soak=%ss liveness=ok readiness=ok status=ok auth_mismatch<=%s\n' \
        "$soak_seconds" "$max_auth_mismatch"
}

# Roll back automatically after a failed verification.
#
# Re-invokes this installer's rollback path so there is exactly one rollback
# implementation. Verification is disabled for the rollback restart: the point
# is to get the previous release back in place, and a second soak failure must
# not recurse.
auto_rollback() {
    printf 'rolling back automatically after failed verification\n' >&2
    local args=(--apply --rollback --root "$root" --unit-dest "$unit_dest" --env-dest "$env_dest"
        --unit-name "$unit_name" --no-verify)
    $systemd || args+=(--no-systemd)
    $restart && args+=(--restart)
    if "$installer_dir/buzz-relay-release.sh" "${args[@]}"; then
        printf 'automatic rollback completed\n' >&2
    else
        printf 'automatic rollback FAILED — the relay needs manual attention\n' >&2
    fi
}

if $rollback; then
    previous_file="$rollback_dir/previous-release"
    if [[ -r "$previous_file" ]]; then
        previous=$(<"$previous_file")
        [[ "$previous" =~ ^[A-Za-z0-9._-]+$ && -x "$root/releases/$previous/target/release/buzz-relay" ]] || {
            printf '%s\n' 'rollback refused: recorded release is invalid' >&2; exit 65;
        }
        [[ -r "$rollback_dir/relay.env" ]] || { printf '%s\n' 'rollback refused: runtime env backup unavailable' >&2; exit 65; }
        install -D -m 0600 "$rollback_dir/relay.env" "$env_dest"
        ln -s "releases/$previous" "$current.next"
        mv -Tf "$current.next" "$current"
        # root-jk1sw Gate4 Major 1: restore the unit that shipped with the
        # previous release, not the unit of the release being rolled back.
        # Reinstalling the current unit would leave its settings (pool,
        # handler ceiling, relay alias schemes) in force against older bytes.
        if [[ -r "$rollback_dir/previous-buzz-relay.service" ]]; then
            install -D -m 0644 "$rollback_dir/previous-buzz-relay.service" "$unit_dest"
        else
            install_unit
        fi
        restart_unit
        printf 'rolled back relay release=%s unit=%s env=restored\n' "$previous" \
            "$([[ -r "$rollback_dir/previous-buzz-relay.service" ]] && printf previous || printf current)"
        exit 0
    fi
    [[ -r "$rollback_dir/legacy-buzz-relay.service" ]] || {
        printf '%s\n' 'rollback refused: no prior immutable release or legacy unit recorded' >&2; exit 65;
    }
    # With no previous release, restoring the legacy unit is the entire
    # rollback — so it must actually differ from what is installed.
    refuse_noop_rollback "$rollback_dir/legacy-buzz-relay.service"
    install -D -m 0644 "$rollback_dir/legacy-buzz-relay.service" "$unit_dest"
    # Restore the pre-release runtime env alongside the legacy unit when one
    # was captured; a legacy unit paired with release-era env is a third state
    # that was never tested.
    if [[ -r "$rollback_dir/relay.env" ]]; then
        install -D -m 0600 "$rollback_dir/relay.env" "$env_dest"
    fi
    restart_unit
    printf '%s\n' 'rolled back relay release=legacy-unit'
    exit 0
fi

[[ -n "$source_dir" && -n "$revision" ]] || { usage >&2; exit 64; }
[[ "$revision" =~ ^[A-Za-z0-9._-]+$ ]] || { printf '%s\n' 'revision contains unsafe characters' >&2; exit 64; }
binary="$source_dir/target/release/buzz-relay"
[[ -x "$binary" ]] || { printf '%s\n' 'release refused: prebuilt target/release/buzz-relay is required' >&2; exit 66; }
[[ -d "$source_dir/web/dist" ]] || { printf '%s\n' 'release refused: prebuilt web/dist is required' >&2; exit 66; }
[[ -r "$env_source" ]] || { printf '%s\n' 'release refused: runtime env source is unavailable' >&2; exit 66; }
if git -C "$source_dir" rev-parse --verify HEAD >/dev/null 2>&1; then
    source_revision=$(git -C "$source_dir" rev-parse HEAD)
    [[ "$source_revision" = "$revision" ]] || {
        printf '%s\n' 'release refused: revision does not match source HEAD' >&2; exit 66;
    }
fi

release="$root/releases/$revision"
if [[ -e "$release" ]]; then
    [[ -x "$release/target/release/buzz-relay" ]] || { printf '%s\n' 'release path exists but is incomplete' >&2; exit 66; }
else
    stage="$root/releases/.${revision}.staging.$$"
    cleanup_stage() {
        status=$?
        rm -rf "$stage"
        exit "$status"
    }
    trap cleanup_stage EXIT
    install -d -m 0755 "$stage/target/release" "$stage/deploy/host/pepper"
    install -m 0755 "$binary" "$stage/target/release/buzz-relay"
    install -m 0755 "$installer_dir/buzz-relay-launch.sh" "$stage/deploy/host/pepper/buzz-relay-launch.sh"
    mkdir -p "$stage/web"
    cp -a "$source_dir/web/dist" "$stage/web/dist"
    binary_sha256=$(sha256sum "$stage/target/release/buzz-relay" | awk '{print $1}')
    printf 'revision=%s\nbinary_sha256=%s\n' "$revision" "$binary_sha256" >"$stage/manifest"
    mv -T "$stage" "$release"
    trap - EXIT
fi

install -d -m 0700 "$rollback_dir"
# root-jk1sw Gate4 Major 1: only a unit that is NOT an immutable-release unit
# may be recorded as the legacy unit.
#
# The deployed release recorded its own unit here — because the immutable unit
# had already been written to $unit_dest before the first --apply — so
# `--rollback` reinstalled the new unit and reported success while changing
# nothing. `is_release_unit` makes that impossible: a unit whose ExecStart
# points into this release root is by definition not the thing we are rolling
# back to.
if [[ ! -L "$current" && -e "$unit_dest" && ! -e "$rollback_dir/legacy-buzz-relay.service" ]]; then
    if is_release_unit "$unit_dest"; then
        printf 'note: not recording an immutable-release unit as the legacy unit\n' >&2
    else
        install -m 0644 "$unit_dest" "$rollback_dir/legacy-buzz-relay.service"
    fi
fi
# Always preserve the outgoing unit, release-shaped or not. Between two
# immutable releases the unit can change (pool/handler/alias settings live in
# it), so a release pointer alone is not a complete rollback target.
if [[ -e "$unit_dest" ]]; then
    install -m 0644 "$unit_dest" "$rollback_dir/previous-buzz-relay.service"
fi
if [[ -L "$current" ]]; then
    prior_target=$(readlink "$current")
    prior=${prior_target#releases/}
    [[ "$prior" =~ ^[A-Za-z0-9._-]+$ ]] || { printf '%s\n' 'current release target is invalid' >&2; exit 65; }
    printf '%s\n' "$prior" >"$rollback_dir/previous-release"
fi
if [[ -e "$env_dest" ]]; then
    install -m 0600 "$env_dest" "$rollback_dir/relay.env"
fi
write_rollback_metadata "$revision"
install -D -m 0600 "$env_source" "$env_dest"
ln -s "releases/$revision" "$current.next"
mv -Tf "$current.next" "$current"
install_unit
restart_unit
printf 'installed relay release=%s restart=%s\n' "$revision" "$restart"
