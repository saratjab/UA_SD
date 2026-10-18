# AI prompt record

This is a curated record of the substantial AI-assisted work themes for Release
1, not a complete conversation transcript. Summaries below are reconstructions
from the task records and resulting project; they are not presented as exact
historical wording. The original text is included only where it is available in
the linked prompt file.

## Existing prompt record

- [`kafka-irrigation-lifecycle.md`](kafka-irrigation-lifecycle.md) — records the
  Central–Engine Kafka lifecycle work, the constraint to preserve the Monitor
  sockets and SQLite, and the contract documentation. Its original prompt is not
  available verbatim; the file labels the text as a summary.

## Main prompt themes

1. **Architecture and irrigation lifecycle — reconstructed summary.** Trace the
   four applications and preserve the Monitor–Central and Monitor–Engine socket
   protocols. Connect FO requests to Central authorization, Engine commands,
   telemetry, and operator results through Kafka; retain Central's SQLite state
   and document the message contract. The existing implementation record is
   linked above rather than duplicated.
2. **Reliability, faults, and restart recovery — reconstructed summary.** Audit
   component disconnects and failures individually; keep valve safety and
   station status correct; reconnect only the affected components; preserve
   pending station events across Central restarts; and avoid duplicate state
   transitions or notifications.
3. **FO crash and result recovery — reconstructed summary.** Let a returning FO
   using the same operator ID retrieve a completion produced while it was down,
   including request ID and final measurements, without replaying all historical
   events or exposing another operator's data.
4. **Real Kafka testing and command delivery — reconstructed summary.** Exercise
   the real-broker flow with an established Monitor–Engine handshake, investigate
   command-consumption delay separately from Kafka lag, retain rejection reason
   validation, and verify prompt dispatch through final result delivery.
5. **Submission documentation — reconstructed summary.** Prepare a concise
   project README, explain design and failure handling for assessment, organize
   the AI prompt record, preserve student names, and distinguish local Docker
   evidence from Railway and two-PC acceptance that remain pending.

For system operation and design decisions, see the [deployment guide](../docs/local-docker-deployment.md),
[Kafka contract](../docs/kafka-message-contract.md), [state-model ADR](../docs/adr/ADR-001-state-model-and-schema.md),
and [implementation report](../docs/report.md).
