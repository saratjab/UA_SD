#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 - .env.railway <<'PY'
from pathlib import Path
import ssl
import sys

env_path = Path(sys.argv[1])
if not env_path.is_file():
    print("Missing .env.railway. Copy .env.railway.example and fill in the Railway settings.", file=sys.stderr)
    raise SystemExit(1)

values = {}
for line in env_path.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    if line.startswith("export "):
        line = line[7:].lstrip()
    if "=" not in line:
        print("Invalid .env.railway line; expected KEY=value.", file=sys.stderr)
        raise SystemExit(1)
    key, value = line.split("=", 1)
    key, value = key.strip(), value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    values[key] = value

required = (
    "KAFKA_HOST", "KAFKA_PORT", "CENTRAL_HOST", "CENTRAL_PORT",
    "KAFKA_REQUIRE_SECURITY", "KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "KAFKA_CA_CERT_PATH",
    "KAFKA_SSL_CA_LOCATION",
)
missing = [key for key in required if not values.get(key)]
if missing:
    print("Missing required setting(s): " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)

placeholders = [key for key in required if "<" in values[key] or ">" in values[key] or "GENERATED_" in values[key]]
if placeholders:
    print("Replace placeholder value(s) for: " + ", ".join(placeholders), file=sys.stderr)
    raise SystemExit(1)

for key in ("KAFKA_HOST", "CENTRAL_HOST"):
    if any(char.isspace() for char in values[key]):
        print(f"{key} must be a hostname without spaces.", file=sys.stderr)
        raise SystemExit(1)
for key in ("KAFKA_PORT", "CENTRAL_PORT"):
    try:
        port = int(values[key])
    except ValueError:
        port = 0
    if not 1 <= port <= 65535:
        print(f"{key} must be a port from 1 to 65535.", file=sys.stderr)
        raise SystemExit(1)

if values["KAFKA_REQUIRE_SECURITY"].lower() != "true":
    print("KAFKA_REQUIRE_SECURITY must be true for Railway Kafka.", file=sys.stderr)
    raise SystemExit(1)
if values["KAFKA_SECURITY_PROTOCOL"].upper() != "SASL_SSL":
    print("KAFKA_SECURITY_PROTOCOL must be SASL_SSL for Railway Kafka.", file=sys.stderr)
    raise SystemExit(1)
if values["KAFKA_SASL_MECHANISM"].upper() != "PLAIN":
    print("KAFKA_SASL_MECHANISM must be PLAIN for the configured Railway broker.", file=sys.stderr)
    raise SystemExit(1)
if values["KAFKA_SSL_CA_LOCATION"] != "/run/secrets/kafka/ca.pem":
    print("KAFKA_SSL_CA_LOCATION must be /run/secrets/kafka/ca.pem.", file=sys.stderr)
    raise SystemExit(1)
if values.get("KAFKA_SSL_CA_PEM", ""):
    print("Leave KAFKA_SSL_CA_PEM empty; Railway mode mounts the CA certificate as a file.", file=sys.stderr)
    raise SystemExit(1)

cert_path = Path(values["KAFKA_CA_CERT_PATH"])
if not cert_path.is_absolute():
    cert_path = Path.cwd() / cert_path
if not cert_path.is_file() or not cert_path.stat().st_size:
    print("KAFKA_CA_CERT_PATH must point to a readable CA certificate file.", file=sys.stderr)
    raise SystemExit(1)
resolved_cert = cert_path.resolve()
repository = Path.cwd().resolve()
try:
    resolved_cert.relative_to(repository)
    inside_repository = True
except ValueError:
    inside_repository = False
if inside_repository:
    try:
        resolved_cert.relative_to(repository / "secrets")
    except ValueError:
        print("Store an in-repository CA certificate under secrets/ so Git ignores it.", file=sys.stderr)
        raise SystemExit(1)
try:
    ssl.create_default_context(cafile=str(resolved_cert))
except (OSError, ssl.SSLError, ValueError):
    print("The CA certificate file could not be parsed as a trusted PEM certificate bundle.", file=sys.stderr)
    raise SystemExit(1)

print("Railway settings and CA certificate checks passed.")
PY

if ! docker compose -f docker-compose.local.yml config --quiet >/dev/null 2>&1; then
  printf '%s\n' "Local PLAINTEXT Compose configuration is invalid." >&2
  exit 1
fi
if ! docker compose --env-file .env.railway \
  -f docker-compose.local.yml -f docker-compose.railway.yml config --quiet >/dev/null 2>&1; then
  printf '%s\n' "Railway SASL_SSL Compose configuration is invalid." >&2
  exit 1
fi
printf '%s\n' "Both local PLAINTEXT and Railway SASL_SSL Compose configurations are valid."
