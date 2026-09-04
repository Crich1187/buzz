#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
launcher="$root/bin/qwen-acp-agent"
override="$root/deploy/systemd/buzz-acp-fleet@qwen.service.d/10-qwen-mcp-allowlist.conf"

grep -Fq 'exec /root/.npm-global/bin/qwen --acp --allowed-mcp-server-names pepper-context' "$launcher"
grep -Fq -- '--agent-command /root/buzz/bin/qwen-acp-agent' "$override"
! rg -q 'atlaso' "$launcher" "$override"
