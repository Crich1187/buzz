#!/usr/bin/env bash
# Fixture for buzz-public-ingress-probe.sh (root-jk1sw Gate4 ingress remediation).
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
probe="$repo_root/deploy/host/pepper/buzz-public-ingress-probe.sh"
readme="$repo_root/deploy/host/pepper/README.md"

test -x "$probe"
test -f "$readme"

# Contract: probe must bypass /etc/hosts via dig + curl --resolve.
grep -Fq 'dig +short' "$probe"
grep -Fq -- '--resolve' "$probe"
grep -Fq -- '--http1.1' "$probe"
grep -Fq 'Sec-WebSocket-Key' "$probe"
grep -Fq 'public_ingress: PASS' "$probe"
# Must not treat hosts-loopback HTTPS as the public verdict.
grep -Fq 'shadows' "$probe"
grep -Fq '/etc/hosts' "$probe"

# README must document the dual path + Gate4 probe requirement.
grep -Fq 'buzz-public-ingress-probe.sh' "$readme"
grep -Fq '/etc/hosts' "$readme"
grep -Fq '1.1.1.1' "$readme"

# Help exits cleanly.
"$probe" --help >/dev/null

# Live public probe (status codes only). Skip only when dig cannot resolve —
# never skip because naive hosts HTTPS failed (that is the bug under test).
if ! dig +short @1.1.1.1 buzz.lymarinc.com A | grep -Eq '^[0-9.]+$'; then
    printf 'SKIP live public probe: no public A from 1.1.1.1\n'
    exit 0
fi

"$probe" --host buzz.lymarinc.com --resolver 1.1.1.1
printf 'test-buzz-public-ingress-probe: PASS\n'
