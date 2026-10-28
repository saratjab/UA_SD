#!/usr/bin/env bash
set -euo pipefail

bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
partitions="${KAFKA_PARTITIONS:-3}"
replication="${KAFKA_REPLICATION_FACTOR:-1}"
kafka_topics="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
client_config=""
security_dir=""
require_security="${KAFKA_REQUIRE_SECURITY:-false}"
[[ "$require_security" == "true" || "$require_security" == "false" ]] || { echo "KAFKA_REQUIRE_SECURITY must be true or false" >&2; exit 2; }
if [[ "$require_security" == "true" && "${KAFKA_SECURITY_PROTOCOL:-}" != "SASL_SSL" ]]; then
    echo "KAFKA_REQUIRE_SECURITY=true requires KAFKA_SECURITY_PROTOCOL=SASL_SSL" >&2
    exit 2
fi

cleanup() { [[ -z "$security_dir" ]] || rm -rf "$security_dir"; }
trap cleanup EXIT

if [[ "${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}" == "SASL_SSL" ]]; then
    for name in KAFKA_SASL_MECHANISM KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD; do
        [[ -n "${!name:-}" ]] || { echo "$name is required for SASL_SSL topic initialization" >&2; exit 2; }
    done
    [[ "$KAFKA_SASL_MECHANISM" == PLAIN ]] || { echo "KAFKA_SASL_MECHANISM must be PLAIN" >&2; exit 2; }
    [[ "$KAFKA_SASL_USERNAME" =~ ^[A-Za-z0-9_.-]{1,64}$ ]] || { echo "KAFKA_SASL_USERNAME contains unsupported characters" >&2; exit 2; }
    (( ${#KAFKA_SASL_PASSWORD} >= 32 && ${#KAFKA_SASL_PASSWORD} <= 256 )) && [[ "$KAFKA_SASL_PASSWORD" =~ ^[A-Za-z0-9_.+]+$ ]] || { echo "KAFKA_SASL_PASSWORD must be 32-256 URL-safe characters" >&2; exit 2; }
    [[ -n "${KAFKA_SSL_CA_PEM:-}" || -n "${KAFKA_SSL_CA_LOCATION:-}" ]] || { echo "SASL_SSL requires KAFKA_SSL_CA_PEM or KAFKA_SSL_CA_LOCATION" >&2; exit 2; }
    security_dir="$(mktemp -d)"
    chmod 700 "$security_dir"
    if [[ -n "${KAFKA_SSL_CA_PEM:-}" ]]; then
        printf '%s\n' "$KAFKA_SSL_CA_PEM" > "$security_dir/ca.pem"
        ca_file="$security_dir/ca.pem"
    else
        [[ -r "$KAFKA_SSL_CA_LOCATION" ]] || { echo "KAFKA_SSL_CA_LOCATION is not readable" >&2; exit 2; }
        ca_file="$KAFKA_SSL_CA_LOCATION"
    fi
    grep -q '^-----BEGIN CERTIFICATE-----' "$ca_file" || { echo "Kafka CA must be PEM encoded" >&2; exit 2; }
    trust_password="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
    "${KAFKA_KEYTOOL_BIN:-keytool}" -importcert -noprompt -alias wm-kafka-ca -file "$ca_file" \
        -keystore "$security_dir/truststore.p12" -storetype PKCS12 -storepass "$trust_password" >/dev/null 2>&1 \
        || { echo "Could not create temporary Kafka CA truststore" >&2; exit 2; }
    client_config="$security_dir/client.properties"
    {
        printf 'security.protocol=SASL_SSL\n'
        printf 'sasl.mechanism=PLAIN\n'
        printf 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";\n' "$KAFKA_SASL_USERNAME" "$KAFKA_SASL_PASSWORD"
        printf 'ssl.truststore.location=%s\n' "$security_dir/truststore.p12"
        printf 'ssl.truststore.type=PKCS12\n'
        printf 'ssl.truststore.password=%s\n' "$trust_password"
        printf 'ssl.endpoint.identification.algorithm=https\n'
    } > "$client_config"
    chmod 600 "$client_config"
elif [[ "${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}" != "PLAINTEXT" ]]; then
    echo "Topic initialization supports PLAINTEXT or SASL_SSL" >&2
    exit 2
fi

run_topics() {
    if [[ -n "$client_config" ]]; then
        "$kafka_topics" "$@" --command-config "$client_config"
    else
        "$kafka_topics" "$@"
    fi
}

[[ "$partitions" =~ ^[1-9][0-9]*$ ]] || { echo "KAFKA_PARTITIONS must be a positive integer" >&2; exit 2; }
[[ "$replication" =~ ^[1-9][0-9]*$ ]] || { echo "KAFKA_REPLICATION_FACTOR must be a positive integer" >&2; exit 2; }

for topic in wm.activation.requests wm.ws.commands wm.ws.events wm.operator.events; do
    # --list is the authoritative existence check. In Kafka 3.8, --describe for
    # a missing topic can exit successfully while printing Optional[topic] text.
    topic_list="$(run_topics --bootstrap-server "$bootstrap" --list)"
    if ! printf '%s\n' "$topic_list" | grep -Fqx -- "$topic"; then
        run_topics --bootstrap-server "$bootstrap" --create --if-not-exists \
            --topic "$topic" --partitions "$partitions" --replication-factor "$replication"
        topic_list="$(run_topics --bootstrap-server "$bootstrap" --list)"
        if ! printf '%s\n' "$topic_list" | grep -Fqx -- "$topic"; then
            echo "Topic creation was not confirmed for $topic" >&2
            exit 1
        fi
    fi

    description="$(run_topics --bootstrap-server "$bootstrap" --describe --topic "$topic")"

    current="$(printf '%s\n' "$description" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p' | head -n 1)"
    if [[ -z "$current" ]]; then
        echo "Could not read partition count for existing topic $topic" >&2
        exit 1
    fi
    if (( current < partitions )); then
        # Kafka only supports increasing partition counts. Existing records and
        # consumer offsets are left untouched; old keyed records may map to new
        # partitions differently after this explicit migration.
        run_topics --bootstrap-server "$bootstrap" --alter --topic "$topic" --partitions "$partitions"
    fi
done
