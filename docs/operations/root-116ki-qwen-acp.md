# root-116ki Qwen ACP Atlaso isolation

`bin/qwen-acp-agent` is the tracked Qwen-only launcher for Pepper Buzz ACP.
It retains Qwen's normal user settings but permits only `pepper-context` MCP at
ACP startup, so a failed optional Atlaso server cannot prevent ACP admission.

After this commit is merged, an operator installs the launcher mode 0755 and
the Qwen-only drop-in at
`/etc/systemd/system/buzz-acp-fleet@qwen.service.d/10-qwen-mcp-allowlist.conf`,
runs `systemctl daemon-reload`, then uses the approved Buzz ACP operator/drill
path to restart only `buzz-acp-fleet@qwen.service`.

Rollback: stop via the same operator path, remove only that named drop-in,
run `systemctl daemon-reload`, and restart only the Qwen instance. Do not edit
the shared fleet template, global Qwen settings, or any other agent launcher.
