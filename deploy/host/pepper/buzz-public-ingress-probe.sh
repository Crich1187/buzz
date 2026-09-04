#!/usr/bin/env bash
# Probe the *public* Cloudflare ingress for buzz.lymarinc.com.
#
# Pepper pins buzz.lymarinc.com -> 127.0.0.1 in /etc/hosts so on-host ACP
# connectors can use ws://buzz.lymarinc.com via buzz-local-loopback (:80).
# That override makes naive https:// / wss:// probes from Pepper hit
# 127.0.0.1:443, where nothing listens — curl exit 7 / HTTP 000 — even when
# the real public path through Cloudflare is healthy.
#
# This script resolves A records via a public recursive resolver (default
# 1.1.1.1), then uses curl --resolve / a TLS WebSocket upgrade against that
# address with the correct SNI/Host. Status codes only; no bodies, no
# credentials, no NIP-42 AUTH.
#
# Exit 0: HTTPS health 2xx AND WSS upgrade returns 101.
# Exit 2: usage / missing tools.
# Exit 3: public DNS returned no usable A record.
# Exit 4: HTTPS health failed.
# Exit 5: WSS upgrade failed (no 101).
# Exit 6: hosts-shadowed naive probe would have failed (informational mode).
set -euo pipefail

HOST="${BUZZ_PUBLIC_HOST:-buzz.lymarinc.com}"
RESOLVER="${BUZZ_PUBLIC_DNS_RESOLVER:-1.1.1.1}"
HEALTH_PATH="${BUZZ_PUBLIC_HEALTH_PATH:-/_liveness}"
CONNECT_TIMEOUT="${BUZZ_PUBLIC_CONNECT_TIMEOUT:-8}"
MAX_TIME="${BUZZ_PUBLIC_MAX_TIME:-15}"
COMPARE_HOSTS="${BUZZ_PUBLIC_COMPARE_HOSTS:-1}"

usage() {
    cat <<'EOF'
Usage: buzz-public-ingress-probe.sh [--host NAME] [--resolver IP] [--no-compare-hosts]

Proves public Cloudflare HTTPS + WSS for the Buzz relay without being fooled
by Pepper's /etc/hosts loopback override. Prints status codes only.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            HOST=$2
            shift 2
            ;;
        --resolver)
            RESOLVER=$2
            shift 2
            ;;
        --no-compare-hosts)
            COMPARE_HOSTS=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

command -v dig >/dev/null || { printf 'missing dig\n' >&2; exit 2; }
command -v curl >/dev/null || { printf 'missing curl\n' >&2; exit 2; }

# Public A record (ignore /etc/hosts). Prefer the first IPv4 answer.
mapfile -t public_ips < <(dig +short @"$RESOLVER" "$HOST" A | grep -E '^[0-9.]+$' || true)
if ((${#public_ips[@]} == 0)); then
    printf 'public_dns: empty A for %s via %s\n' "$HOST" "$RESOLVER" >&2
    exit 3
fi
public_ip=${public_ips[0]}
printf 'public_dns: %s A via %s -> %s (n=%s)\n' "$HOST" "$RESOLVER" "$public_ip" "${#public_ips[@]}"

if [[ "$COMPARE_HOSTS" == "1" ]]; then
    hosts_ip=$(getent ahostsv4 "$HOST" 2>/dev/null | awk 'NR==1 {print $1; exit}' || true)
    hosts_https_code=$(
        curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 \
            "https://${HOST}${HEALTH_PATH}" 2>/dev/null || true
    )
    hosts_https_code=${hosts_https_code:-000}
    # curl may still emit 000 via -w on connect failure; normalize empties only.
    [[ -n "$hosts_https_code" ]] || hosts_https_code=000
    printf 'hosts_lookup: %s -> %s; naive_https_%s=%s\n' \
        "$HOST" "${hosts_ip:-none}" "$HEALTH_PATH" "$hosts_https_code"
    if [[ "${hosts_ip:-}" == "127.0.0.1" || "${hosts_ip:-}" == "::1" ]]; then
        printf 'note: /etc/hosts (or equivalent) shadows %s to loopback; naive https/wss from this host is not a public-ingress verdict\n' "$HOST"
    fi
fi

https_code=$(
    curl -sS -o /dev/null -w '%{http_code}' \
        --connect-timeout "$CONNECT_TIMEOUT" --max-time "$MAX_TIME" \
        --resolve "${HOST}:443:${public_ip}" \
        "https://${HOST}${HEALTH_PATH}" 2>/dev/null || true
)
https_code=${https_code:-000}
printf 'public_https: %s via %s -> %s\n' "$HEALTH_PATH" "$public_ip" "$https_code"
case "$https_code" in
    2??) ;;
    *)
        printf 'public HTTPS health failed (need 2xx)\n' >&2
        exit 4
        ;;
esac

# Minimal WebSocket upgrade; 101 = TLS+ingress accepts WSS before NIP-42.
# Force HTTP/1.1: curl's default HTTP/2 path returns 200 on this edge and never
# completes a WebSocket handshake, which falsely looks like "WSS down".
ws_code=$(
    curl -sS -o /dev/null -w '%{http_code}' \
        --http1.1 \
        --connect-timeout "$CONNECT_TIMEOUT" --max-time "$MAX_TIME" \
        --resolve "${HOST}:443:${public_ip}" \
        -H 'Connection: Upgrade' \
        -H 'Upgrade: websocket' \
        -H 'Sec-WebSocket-Version: 13' \
        -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
        "https://${HOST}/" 2>/dev/null || true
)
ws_code=${ws_code:-000}
printf 'public_wss_upgrade: / via %s -> %s\n' "$public_ip" "$ws_code"
if [[ "$ws_code" != "101" ]]; then
    # Some edges return 200 on a non-upgrade GET with Upgrade headers; require 101.
    printf 'public WSS upgrade failed (need HTTP 101 Switching Protocols, got %s)\n' "$ws_code" >&2
    exit 5
fi

printf 'public_ingress: PASS host=%s https=%s wss_upgrade=%s\n' "$HOST" "$https_code" "$ws_code"
exit 0
