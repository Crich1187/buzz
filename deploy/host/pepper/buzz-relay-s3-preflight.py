#!/usr/bin/env python3
"""Value-safe S3 authentication preflight for the Pepper relay release.

Proves that the runtime environment a release is about to run with can
authenticate to the object store *before* the release pointer is swapped, so a
credential mismatch is a refused release instead of a crash-looping relay.
(2026-09-20: the relay's startup git object-store conformance probe hit HTTP
403 SignatureDoesNotMatch on both the candidate and the rollback target,
because the relay's S3 secret had silently drifted from the MinIO container's.)

The environment is assembled the way buzz-relay-launch.sh will see it:

  1. ``EnvironmentFile=`` entries of the systemd unit (``--unit``), in order;
     a leading ``-`` marks an optional file, exactly as systemd reads it;
  2. then each ``--env-file``, in order, later files overriding earlier ones,
     which is what ``set -a; source`` of the runtime env does at launch.

The request is a SigV4-signed ListObjectsV2 with ``max-keys=1`` — the same
permission the relay's storage sweep needs. Only status codes, S3 error codes
and the non-secret endpoint host / bucket are ever printed; credential values
never leave this process.

Exit status: 0 authenticated, 1 rejected or unreachable, 2 configuration error.
Python standard library only; no third-party dependency on the host.
"""

import argparse
import datetime
import hashlib
import hmac
import os
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

REQUIRED = ("BUZZ_S3_ENDPOINT", "BUZZ_S3_BUCKET", "BUZZ_S3_ACCESS_KEY", "BUZZ_S3_SECRET_KEY")
KEY_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def config_error(message: str) -> "NoReturn":  # type: ignore[name-defined]
    print(f"s3 preflight: configuration error: {message}", file=sys.stderr)
    raise SystemExit(2)


def parse_env_file(path: str, env: dict) -> None:
    """Load KEY=VALUE lines. Matching quotes are stripped as a shell would."""
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError as exc:
        config_error(f"cannot read env file {path}: {type(exc).__name__}")
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not KEY_RE.fullmatch(key):
            continue
        value = value.strip()
        if value and value[0] in "\"'":
            try:
                parts = shlex.split(value, posix=True)
                value = parts[0] if parts else ""
            except ValueError:
                value = value.strip(value[0])
        env[key] = value


def unit_env_files(unit: str) -> list:
    """EnvironmentFile= entries of a unit, as (path, optional) tuples."""
    try:
        result = subprocess.run(["systemctl", "cat", unit], capture_output=True, text=True, check=False)
    except OSError as exc:
        config_error(f"cannot run systemctl: {type(exc).__name__}")
    if result.returncode != 0:
        print(f"s3 preflight: note: unit {unit} is not installed yet; no EnvironmentFile= entries", file=sys.stderr)
        return []
    files = []
    for raw in result.stdout.splitlines():
        line = raw.strip()
        if not line.startswith("EnvironmentFile="):
            continue
        path = line.split("=", 1)[1].strip()
        optional = path.startswith("-")
        files.append((path.lstrip("-"), optional))
    return files


def sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret: str, date: str, region: str, service: str) -> bytes:
    key = sign(("AWS4" + secret).encode("utf-8"), date)
    key = sign(key, region)
    key = sign(key, service)
    return sign(key, "aws4_request")


def build_request(env: dict) -> "tuple[urllib.request.Request, str, str]":
    endpoint = env["BUZZ_S3_ENDPOINT"]
    bucket = env["BUZZ_S3_BUCKET"]
    region = env.get("BUZZ_S3_REGION") or "us-east-1"
    addressing = (env.get("BUZZ_S3_ADDRESSING_STYLE") or "path").lower()
    parts = urllib.parse.urlsplit(endpoint)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        config_error("BUZZ_S3_ENDPOINT is not an http(s) URL")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        config_error("BUZZ_S3_BUCKET is not a valid bucket name")
    if addressing not in ("path", "virtual"):
        config_error("BUZZ_S3_ADDRESSING_STYLE must be 'path' or 'virtual'")
    # Host for the wire and for the signature; never the URL's userinfo.
    netloc = parts.hostname + (f":{parts.port}" if parts.port else "")
    base_path = parts.path.rstrip("/")
    if addressing == "virtual":
        host = f"{bucket}.{netloc}"
        path = f"{base_path}/"
    else:
        host = netloc
        path = f"{base_path}/{bucket}/"
    query = "list-type=2&max-keys=1"  # already in canonical (sorted) order
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_uri = urllib.parse.quote(path, safe="/~")
    canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(["GET", canonical_uri, query, canonical_headers, signed_headers, payload_hash])
    scope = f"{date}/{region}/s3/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(
        signing_key(env["BUZZ_S3_SECRET_KEY"], date, region, "s3"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={env['BUZZ_S3_ACCESS_KEY']}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    url = f"{parts.scheme}://{host}{canonical_uri}?{query}"
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Host": host,
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            "Authorization": authorization,
        },
    )
    # What may be printed: scheme://host (no userinfo, no path) and the bucket.
    return request, f"{parts.scheme}://{netloc}", bucket


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--unit", help="systemd unit whose EnvironmentFile= entries are loaded first")
    parser.add_argument("--env-file", action="append", default=[], help="runtime env file (repeatable; later overrides earlier)")
    parser.add_argument("--timeout", type=float, default=10.0, help="request timeout in seconds")
    args = parser.parse_args(argv)
    if not args.unit and not args.env_file:
        config_error("nothing to check: pass --unit and/or --env-file")

    env: dict = {}
    if args.unit:
        for path, optional in unit_env_files(args.unit):
            if optional and not os.path.exists(path):
                continue  # systemd skips a missing optional file silently; so do we
            parse_env_file(path, env)
    for path in args.env_file:
        parse_env_file(path, env)

    missing = [name for name in REQUIRED if not env.get(name)]
    if missing:
        config_error("missing or empty: " + ", ".join(missing))

    request, shown_endpoint, bucket = build_request(env)
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            print(f"s3 preflight ok: endpoint={shown_endpoint} bucket={bucket} http={response.status}")
            return 0
    except urllib.error.HTTPError as exc:
        body = exc.read(4096).decode("utf-8", "replace")
        match = re.search(r"<Code>([A-Za-z0-9]+)</Code>", body)
        code = match.group(1) if match else "unknown"
        print(f"s3 preflight FAILED: endpoint={shown_endpoint} bucket={bucket} http={exc.code} code={code}", file=sys.stderr)
        return 1
    except (urllib.error.URLError, OSError) as exc:
        reason = getattr(exc, "reason", None)
        kind = type(reason).__name__ if reason is not None else type(exc).__name__
        print(f"s3 preflight FAILED: endpoint={shown_endpoint} bucket={bucket} transport={kind}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
