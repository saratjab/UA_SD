#!/usr/bin/env bash
# Validate production-only Kafka security and materialize short-lived files.
set -euo pipefail
umask 077

fail() { echo "Kafka startup configuration error: $1" >&2; exit 78; }
required() { [[ -n "${!1:-}" ]] || fail "$1 is required"; }
valid_host() { [[ "$1" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ && "$1" != *..* ]]; }

for name in KAFKA_PRIVATE_HOST KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD KAFKA_TLS_KEYSTORE_B64 \
            KAFKA_TLS_KEYSTORE_PASSWORD KAFKA_TLS_CA_CERT_B64; do required "$name"; done

valid_host "$KAFKA_PRIVATE_HOST" || fail "KAFKA_PRIVATE_HOST must be a DNS hostname"
[[ "$KAFKA_SASL_USERNAME" =~ ^[A-Za-z0-9_.-]{1,64}$ ]] || fail "KAFKA_SASL_USERNAME contains unsupported characters"
(( ${#KAFKA_SASL_PASSWORD} >= 32 && ${#KAFKA_SASL_PASSWORD} <= 256 )) && [[ "$KAFKA_SASL_PASSWORD" =~ ^[A-Za-z0-9_.+]+$ ]] || fail "KAFKA_SASL_PASSWORD must be 32-256 URL-safe characters"
[[ ${#KAFKA_TLS_KEYSTORE_PASSWORD} -ge 12 && "$KAFKA_TLS_KEYSTORE_PASSWORD" != *$'\n'* ]] || fail "KAFKA_TLS_KEYSTORE_PASSWORD must be at least 12 characters"

[[ "${KAFKA_SASL_MECHANISM:-PLAIN}" == "PLAIN" ]] || fail "Kafka 3.8 single-node configuration supports only the configured PLAIN mechanism here"
external_enabled="${KAFKA_PUBLIC_LISTENER_ENABLED:-false}"
[[ "$external_enabled" == "true" || "$external_enabled" == "false" ]] || fail "KAFKA_PUBLIC_LISTENER_ENABLED must be true or false"
if [[ "$external_enabled" == "true" ]]; then
    required KAFKA_PUBLIC_HOST
    required KAFKA_PUBLIC_PORT
    valid_host "$KAFKA_PUBLIC_HOST" || fail "KAFKA_PUBLIC_HOST must be a DNS hostname"
    [[ "$KAFKA_PUBLIC_PORT" =~ ^[1-9][0-9]{0,4}$ ]] && (( KAFKA_PUBLIC_PORT <= 65535 )) || fail "KAFKA_PUBLIC_PORT must be a valid TCP port"
    expected_listeners='INTERNAL://:29092,HOST://:9092,CONTROLLER://127.0.0.1:9093'
    expected_protocols='INTERNAL:SASL_SSL,HOST:SASL_SSL,CONTROLLER:PLAINTEXT'
else
    [[ -z "${KAFKA_PUBLIC_HOST:-}${KAFKA_PUBLIC_PORT:-}" ]] || fail "public Kafka endpoints require KAFKA_PUBLIC_LISTENER_ENABLED=true"
    expected_listeners='INTERNAL://:29092,CONTROLLER://127.0.0.1:9093'
    expected_protocols='INTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT'
fi
[[ "${KAFKA_LISTENERS:-$expected_listeners}" == "$expected_listeners" ]] || fail "KAFKA_LISTENERS does not match the enabled secure listener set"
[[ "${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:-$expected_protocols}" == "$expected_protocols" ]] || fail "All enabled client listeners must use SASL_SSL"

export KAFKA_LISTENERS="$expected_listeners"
export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="$expected_protocols"
export KAFKA_ADVERTISED_LISTENERS="INTERNAL://${KAFKA_PRIVATE_HOST}:29092"
if [[ "$external_enabled" == "true" ]]; then
    export KAFKA_ADVERTISED_LISTENERS+=",HOST://${KAFKA_PUBLIC_HOST}:${KAFKA_PUBLIC_PORT}"
fi
export KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
export KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
export KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093'
export KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
export KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
export KAFKA_SSL_CLIENT_AUTH=none
export KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https

security_dir="$(mktemp -d /tmp/wm-kafka-security.XXXXXX)"
trap 'rm -rf "$security_dir"' EXIT
printf '%s' "$KAFKA_TLS_KEYSTORE_B64" | base64 -d > "$security_dir/kafka.p12" || fail "KAFKA_TLS_KEYSTORE_B64 is not valid base64"
printf '%s' "$KAFKA_TLS_CA_CERT_B64" | base64 -d > "$security_dir/ca.pem" || fail "KAFKA_TLS_CA_CERT_B64 is not valid base64"
grep -q '^-----BEGIN CERTIFICATE-----' "$security_dir/ca.pem" || fail "KAFKA_TLS_CA_CERT_B64 must contain a PEM certificate"

cert_hosts=("$KAFKA_PRIVATE_HOST")
if [[ "$external_enabled" == "true" ]]; then cert_hosts+=("$KAFKA_PUBLIC_HOST"); fi
host_csv="$(IFS=,; printf '%s' "${cert_hosts[*]}")"
KAFKA_TLS_KEYSTORE_PASSWORD="$KAFKA_TLS_KEYSTORE_PASSWORD" \
    java -cp /opt/wm VerifyKafkaCertificate "$security_dir/kafka.p12" "$security_dir/ca.pem" "$host_csv" \
    || fail "TLS certificate must chain to the configured CA and contain exact DNS SANs for enabled listeners"
unset KAFKA_TLS_KEYSTORE_B64 KAFKA_TLS_CA_CERT_B64

keytool -importcert -noprompt -alias wm-kafka-ca -file "$security_dir/ca.pem" \
    -keystore "$security_dir/truststore.p12" -storetype PKCS12 \
    -storepass "$KAFKA_TLS_KEYSTORE_PASSWORD" >/dev/null 2>&1 || fail "CA certificate could not be imported"

export KAFKA_SSL_KEYSTORE_LOCATION="$security_dir/kafka.p12"
export KAFKA_SSL_KEYSTORE_TYPE=PKCS12
export KAFKA_SSL_KEYSTORE_PASSWORD="$KAFKA_TLS_KEYSTORE_PASSWORD"
export KAFKA_SSL_KEY_PASSWORD="$KAFKA_TLS_KEYSTORE_PASSWORD"
export KAFKA_SSL_TRUSTSTORE_LOCATION="$security_dir/truststore.p12"
export KAFKA_SSL_TRUSTSTORE_TYPE=PKCS12
export KAFKA_SSL_TRUSTSTORE_PASSWORD="$KAFKA_TLS_KEYSTORE_PASSWORD"

cat > "$security_dir/kafka_server_jaas.conf" <<EOF
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="$KAFKA_SASL_USERNAME"
  password="$KAFKA_SASL_PASSWORD"
  user_${KAFKA_SASL_USERNAME}="$KAFKA_SASL_PASSWORD";
};
EOF
chmod 0600 "$security_dir/kafka_server_jaas.conf"
export KAFKA_OPTS="${KAFKA_OPTS:-} -Djava.security.auth.login.config=$security_dir/kafka_server_jaas.conf"

exec /etc/kafka/docker/run
