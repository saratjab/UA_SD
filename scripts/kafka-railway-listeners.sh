#!/usr/bin/env bash
# Generate and validate the Railway Kafka listener configuration.
set -euo pipefail

fail() { echo "Kafka startup configuration error: $1" >&2; exit 78; }
required() { [[ -n "${!1:-}" ]] || fail "$1 is required"; }
valid_host() { [[ "$1" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ && "$1" != *..* ]]; }

required KAFKA_PRIVATE_HOST
valid_host "$KAFKA_PRIVATE_HOST" || fail "KAFKA_PRIVATE_HOST must be a DNS hostname"

external_enabled="${KAFKA_PUBLIC_LISTENER_ENABLED:-false}"
[[ "$external_enabled" == "true" || "$external_enabled" == "false" ]] \
    || fail "KAFKA_PUBLIC_LISTENER_ENABLED must be true or false"

if [[ "$external_enabled" == "true" ]]; then
    required KAFKA_PUBLIC_HOST
    required KAFKA_PUBLIC_PORT
    valid_host "$KAFKA_PUBLIC_HOST" || fail "KAFKA_PUBLIC_HOST must be a DNS hostname"
    [[ "$KAFKA_PUBLIC_PORT" =~ ^[1-9][0-9]{0,4}$ ]] && (( KAFKA_PUBLIC_PORT <= 65535 )) \
        || fail "KAFKA_PUBLIC_PORT must be a valid TCP port"
    expected_listeners='INTERNAL://0.0.0.0:29092,HOST://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093'
    expected_protocols='INTERNAL:SASL_SSL,HOST:SASL_SSL,CONTROLLER:PLAINTEXT'
    expected_advertised="INTERNAL://${KAFKA_PRIVATE_HOST}:29092,HOST://${KAFKA_PUBLIC_HOST}:${KAFKA_PUBLIC_PORT}"
else
    [[ -z "${KAFKA_PUBLIC_HOST:-}${KAFKA_PUBLIC_PORT:-}" ]] \
        || fail "public Kafka endpoints require KAFKA_PUBLIC_LISTENER_ENABLED=true"
    expected_listeners='INTERNAL://0.0.0.0:29092,CONTROLLER://127.0.0.1:9093'
    expected_protocols='INTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT'
    expected_advertised="INTERNAL://${KAFKA_PRIVATE_HOST}:29092"
fi

# Reject stale dashboard overrides instead of letting Kafka start with a
# listener set that hides the internal or public client endpoint.
[[ "${KAFKA_LISTENERS:-$expected_listeners}" == "$expected_listeners" ]] \
    || fail "KAFKA_LISTENERS must contain the complete enabled listener set: $expected_listeners"
[[ "${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:-$expected_protocols}" == "$expected_protocols" ]] \
    || fail "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP must be exactly: $expected_protocols"

export KAFKA_LISTENERS="$expected_listeners"
export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="$expected_protocols"
export KAFKA_ADVERTISED_LISTENERS="$expected_advertised"
export KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
export KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
export KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093'
export KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
export KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
export KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https

# Validate the final values after generation, including addresses, protocol
# map, and the absence of a client-advertised controller endpoint.
[[ "$KAFKA_LISTENERS" == "$expected_listeners" ]] \
    || fail "generated KAFKA_LISTENERS is incomplete"
[[ "$KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" == "$expected_protocols" ]] \
    || fail "generated client listener protocols must use SASL_SSL"
[[ "$KAFKA_ADVERTISED_LISTENERS" == "$expected_advertised" && "$KAFKA_ADVERTISED_LISTENERS" != *CONTROLLER* ]] \
    || fail "generated advertised listeners are invalid or include the controller"
[[ "$KAFKA_INTER_BROKER_LISTENER_NAME" == INTERNAL && "$KAFKA_CONTROLLER_LISTENER_NAMES" == CONTROLLER ]] \
    || fail "generated inter-broker/controller listener names are invalid"

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    printf 'KAFKA_LISTENERS=%s\n' "$KAFKA_LISTENERS"
    printf 'KAFKA_ADVERTISED_LISTENERS=%s\n' "$KAFKA_ADVERTISED_LISTENERS"
    printf 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=%s\n' "$KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
fi
