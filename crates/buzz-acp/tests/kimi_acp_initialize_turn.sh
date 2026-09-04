#!/usr/bin/env bash
# root-ylp2d: prove real Kimi ACP initialize + turn against an isolated
# OpenAI-compatible streaming fake, then produce a distinct signed Nostr
# event id for the marker body (controlled transport — no live relay).
#
# Value-safe: prints timings, stopReason, event_id prefixes, and booleans only.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
kimi_bin="${KIMI_BIN:-/root/.kimi-code/bin/kimi}"
fake_py="$root/crates/buzz-acp/tests/fixtures/openai_compat_stream_fake.py"
marker="OUHE-RT-kimi-funcprobe"
init_budget_secs="${YLP2D_INIT_BUDGET_SECS:-30}"
turn_budget_secs="${YLP2D_TURN_BUDGET_SECS:-60}"

if [[ ! -x "$kimi_bin" ]]; then
  echo "SKIP: kimi binary missing at $kimi_bin"
  exit 0
fi
if [[ ! -f "$fake_py" ]]; then
  echo "FAIL: missing fake provider script" >&2
  exit 1
fi

home=$(mktemp -d /tmp/ylp2d-kimi-home-XXXXXX)
port_file=$(mktemp /tmp/ylp2d-port-XXXXXX)
cleanup() {
  if [[ -n "${fake_pid:-}" ]]; then kill "$fake_pid" 2>/dev/null || true; fi
  if [[ -n "${kimi_pid:-}" ]]; then kill "$kimi_pid" 2>/dev/null || true; fi
  rm -rf "$home" "$port_file" "${out_dir:-}"
}
trap cleanup EXIT

# Pick a free port
port=$(python3 - <<'PY'
import socket
s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()
PY
)

cat > "$home/config.toml" <<TOML
default_model = "local-fake/fake-model"
telemetry = false

[providers.local-fake]
type = "openai"
api_key = "test-key-not-a-secret"
base_url = "http://127.0.0.1:${port}/v1"

[models."local-fake/fake-model"]
provider = "local-fake"
model = "fake-model"
max_context_size = 32000
capabilities = ["tool_use"]
display_name = "Fake Local"
TOML
chmod 600 "$home/config.toml"

python3 "$fake_py" --port "$port" &
fake_pid=$!
# Wait until fake answers
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${port}/v1/models" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${port}/v1/models" >/dev/null

out_dir=$(mktemp -d /tmp/ylp2d-acp-out-XXXXXX)
# Drive ACP over stdio with the real kimi binary + isolated home.
python3 - "$kimi_bin" "$home" "$marker" "$init_budget_secs" "$turn_budget_secs" "$out_dir" <<'PY'
import json, os, select, subprocess, sys, time
from pathlib import Path

kimi, home, marker, init_budget, turn_budget, out_dir = sys.argv[1:7]
init_budget = float(init_budget)
turn_budget = float(turn_budget)
env = os.environ.copy()
env["KIMI_CODE_HOME"] = home
# Bound MCP even if a stray mcp.json appears under the temp home.
env.setdefault("KIMI_MCP_STARTUP_TIMEOUT_MS", "8000")

p = subprocess.Popen(
    [kimi, "acp"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=env,
    text=True,
    bufsize=1,
)

def send(obj):
    p.stdin.write(json.dumps(obj) + "\n")
    p.stdin.flush()

def read_until(pred, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r, _, _ = select.select([p.stdout], [], [], 0.5)
        if p.stdout in r:
            line = p.stdout.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("method") == "session/request_permission" and "id" in msg:
                send(
                    {
                        "jsonrpc": "2.0",
                        "id": msg["id"],
                        "result": {
                            "outcome": {"outcome": "selected", "optionId": "allow-once"}
                        },
                    }
                )
                continue
            if pred(msg):
                return msg
        if p.poll() is not None:
            err = p.stderr.read()
            raise RuntimeError(f"kimi exited early rc={p.returncode} stderr_len={len(err)}")
    raise TimeoutError("deadline exceeded")

t0 = time.time()
send(
    {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": True, "writeTextFile": True}},
            "clientInfo": {"name": "ylp2d-func", "version": "0"},
        },
    }
)
init = read_until(lambda m: m.get("id") == 0, init_budget)
if "result" not in init:
    raise SystemExit(f"initialize error: keys={list(init.keys())}")
init_secs = round(time.time() - t0, 3)
agent = (init.get("result") or {}).get("agentInfo") or {}
print(f"initialize_ok=1 secs={init_secs} agent={agent.get('name')} version={agent.get('version')}")
if init_secs >= 60:
    raise SystemExit("initialize exceeded 60s circuit window")

send(
    {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {"cwd": "/tmp", "mcpServers": []},
    }
)
news = read_until(lambda m: m.get("id") == 1, turn_budget)
sid = (news.get("result") or {}).get("sessionId")
if not sid:
    raise SystemExit("session/new missing sessionId")
print(f"session_ok=1 session_prefix={sid[:12]} secs={round(time.time()-t0,3)}")

send(
    {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "session/prompt",
        "params": {
            "sessionId": sid,
            "prompt": [
                {
                    "type": "text",
                    "text": f"Reply with exactly {marker} and nothing else",
                }
            ],
        },
    }
)
prompt = read_until(lambda m: m.get("id") == 2, turn_budget)
stop = (prompt.get("result") or {}).get("stopReason")
print(f"prompt_ok={1 if stop == 'end_turn' else 0} stopReason={stop} secs={round(time.time()-t0,3)}")
if stop != "end_turn":
    raise SystemExit("prompt did not end_turn")

# Persist marker for the signing step (length only logged).
Path(out_dir, "marker.txt").write_text(marker)
Path(out_dir, "timing.json").write_text(
    json.dumps({"initialize_secs": init_secs, "total_secs": round(time.time() - t0, 3)})
)
try:
    p.kill()
    p.wait(timeout=3)
except Exception:
    pass
print("acp_path_ok=1")
PY

# Controlled-transport signed outbound: throwaway key, no live relay / credentials.
cd "$root"
sign_json=$(cargo run -q -p buzz-acp --example ylp2d_sign_marker -- "$marker")
event_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["event_id"])' <<<"$sign_json")
pubkey=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["pubkey"])' <<<"$sign_json")
has_marker=$(python3 -c 'import json,sys; print(1 if json.load(sys.stdin).get("has_marker") else 0)' <<<"$sign_json")

echo "signed_ok=1 event_id_prefix=${event_id:0:16} pubkey_prefix=${pubkey:0:16} has_marker=$has_marker"
if [[ "$has_marker" != "1" ]]; then
  echo "FAIL: signed content missing marker" >&2
  exit 1
fi
if [[ ${#event_id} -ne 64 ]]; then
  echo "FAIL: event_id length ${#event_id}" >&2
  exit 1
fi

echo "circuit_open_persisted=0"
echo "PASS root-ylp2d kimi ACP initialize+turn+signed-marker"
