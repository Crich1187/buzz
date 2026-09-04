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

usage() {
    printf '%s\n' 'usage: buzz-relay-release.sh --apply --source DIR --revision SHA [--restart]'
}

while (($#)); do
    case "$1" in
        --apply) apply=true ;;
        --rollback) rollback=true ;;
        --restart) restart=true ;;
        --no-systemd) systemd=false ;;
        --root|--source|--revision|--env-source|--env-dest|--unit-dest)
            (($# >= 2)) || { usage >&2; exit 64; }
            case "$1" in
                --root) root=$2 ;;
                --source) source_dir=$2 ;;
                --revision) revision=$2 ;;
                --env-source) env_source=$2 ;;
                --env-dest) env_dest=$2 ;;
                --unit-dest) unit_dest=$2 ;;
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

restart_unit() {
    $systemd || return 0
    systemctl daemon-reload
    if $restart; then
        systemctl restart buzz-relay.service
        systemctl is-active --quiet buzz-relay.service
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
        install_unit
        restart_unit
        printf 'rolled back relay release=%s\n' "$previous"
        exit 0
    fi
    [[ -r "$rollback_dir/legacy-buzz-relay.service" ]] || {
        printf '%s\n' 'rollback refused: no prior immutable release or legacy unit recorded' >&2; exit 65;
    }
    install -D -m 0644 "$rollback_dir/legacy-buzz-relay.service" "$unit_dest"
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
if [[ ! -L "$current" && -e "$unit_dest" && ! -e "$rollback_dir/legacy-buzz-relay.service" ]]; then
    install -m 0644 "$unit_dest" "$rollback_dir/legacy-buzz-relay.service"
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
install -D -m 0600 "$env_source" "$env_dest"
ln -s "releases/$revision" "$current.next"
mv -Tf "$current.next" "$current"
install_unit
restart_unit
printf 'installed relay release=%s restart=%s\n' "$revision" "$restart"
