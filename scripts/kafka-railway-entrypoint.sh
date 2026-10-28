#!/usr/bin/env bash
# Validate production-only Kafka security and materialize short-lived files.
set -euo pipefail
umask 077

fail() { echo "Kafka startup configuration error: $1" >&2; exit 78; }
required() { [[ -n "${!1:-}" ]] || fail "$1 is required"; }

for name in KAFKA_PRIVATE_HOST KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD KAFKA_TLS_KEYSTORE_B64 \
            KAFKA_TLS_KEYSTORE_PASSWORD KAFKA_TLS_CA_CERT_B64; do required "$name"; done

[[ "$KAFKA_SASL_USERNAME" =~ ^[A-Za-z0-9_.-]{1,64}$ ]] || fail "KAFKA_SASL_USERNAME contains unsupported characters"
(( ${#KAFKA_SASL_PASSWORD} >= 32 && ${#KAFKA_SASL_PASSWORD} <= 256 )) && [[ "$KAFKA_SASL_PASSWORD" =~ ^[A-Za-z0-9_.+]+$ ]] || fail "KAFKA_SASL_PASSWORD must be 32-256 URL-safe characters"
[[ ${#KAFKA_TLS_KEYSTORE_PASSWORD} -ge 12 && "$KAFKA_TLS_KEYSTORE_PASSWORD" != *$'\n'* ]] || fail "KAFKA_TLS_KEYSTORE_PASSWORD must be at least 12 characters"

[[ "${KAFKA_SASL_MECHANISM:-PLAIN}" == "PLAIN" ]] || fail "Kafka 3.8 single-node configuration supports only the configured PLAIN mechanism here"
external_enabled="${KAFKA_PUBLIC_LISTENER_ENABLED:-false}"
[[ "$external_enabled" == "true" || "$external_enabled" == "false" ]] || fail "KAFKA_PUBLIC_LISTENER_ENABLED must be true or false"
if [[ -r /opt/wm/kafka-railway-listeners.sh ]]; then
    source /opt/wm/kafka-railway-listeners.sh
else
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    source "$script_dir/kafka-railway-listeners.sh"
fi
export KAFKA_SSL_CLIENT_AUTH=none

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
