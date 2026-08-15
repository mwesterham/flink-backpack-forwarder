# Flink BackpackTF Forwarder

A high-performance Apache Flink application that processes Team Fortress 2 trading data from BackpackTF. Consists of two independent Flink jobs that run simultaneously — one for real-time listing updates and one for on-demand market data backfill.

## Jobs

| Job | Entry Class | Purpose |
|-----|-------------|---------|
| **WebSocketForwarderNatsJob** | `me.matthew.flink.backpacktfforward.WebSocketForwarderNatsJob` | Real-time listing updates from NATS JetStream (production) |
| **BackfillJob** | `me.matthew.flink.backpacktfforward.BackfillJob` | On-demand market data backfill, sourced from NATS JetStream |

Both jobs were originally Kafka-sourced; that path (and `flink-connector-kafka`)
has since been removed entirely — NATS JetStream is now the only source for
both. `WebSocketForwarderNatsJob` was the "verification" job during the
migration; it's the sole production listings job now.

All jobs are packaged into the same JAR and run as separate `FlinkDeployment` resources in Kubernetes.

## Features

- **Real-time Processing**: Consumes trading data from NATS JetStream and processes listing updates
- **Backfill System**: Multiple specialized handlers for refreshing market data from BackpackTF API
- **API Integration**: BackpackTF and Steam Web API integration with rate limiting
- **Database Persistence**: PostgreSQL storage with upsert and delete operations
- **Monitoring**: Comprehensive metrics via Prometheus integration
- **At-least-once delivery**: this deployment does not actually have Flink checkpointing enabled (`execution.checkpointing.interval` is unset), so both jobs' NATS sources ack per-message via `AckingUtf8StringSourceConverter`, decoupled from Flink state/checkpoints, rather than assume a guarantee that isn't actually wired up. On a true cold start (a fresh durable consumer with no delivery history), the listings job starts from "now" rather than replaying the stream's retention window — see `NatsListingSource`/`BackfillRequestNatsSource` javadoc.

## Quick Start

### Prerequisites

- Java 17+
- Apache Flink 1.20.2
- PostgreSQL database
- NATS JetStream cluster

### Build

```bash
git clone <repository-url>
cd flink-backpack-tf-forwarder
mvn clean package
```

### WebSocketForwarderNatsJob — Environment Variables

```bash
export NATS_URL="localhost:4222"
export NATS_STREAM="LISTINGS"
export NATS_SUBJECT="bptf.listing.update"
export NATS_CONSUMER_NAME="flink-listings-nats-source"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"
```

| Variable | Description | Default |
|---|---|---|
| `NATS_URL` | Comma-separated NATS server addresses | Required |
| `NATS_STREAM` | JetStream stream name | Required |
| `NATS_SUBJECT` | Subject to consume | Required |
| `NATS_CONSUMER_NAME` | Durable consumer name — must be one this job owns (see NatsListingSource javadoc for why it can't reuse nack's pre-existing consumers) | Required |
| `NATS_ACK_WAIT_MS` | How long before an unacked message is redelivered | `300000` (5 min) |

```bash
flink run -d -c me.matthew.flink.backpacktfforward.WebSocketForwarderNatsJob target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

### BackfillJob — Environment Variables

```bash
export NATS_URL="localhost:4222"
export NATS_STREAM="BACKFILL"
export NATS_SUBJECT="bptf.backfill.request"
export NATS_CONSUMER_NAME="flink-backfill-nats-source"
export NATS_ACK_WAIT_MS="2100000"
export BACKPACK_TF_API_TOKEN="your-backpack-tf-api-token"
export STEAM_API_KEY="your-steam-api-key"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"
```

`NATS_ACK_WAIT_MS` defaults to 5 minutes (see `NatsSourceConfiguration`), but
BackfillJob needs it well above its own 30-minute per-item async API-call
timeout so a slow backpack.tf/Steam call doesn't trigger a mid-flight
redelivery — 35 minutes (`2100000`) in the k8s deployment.

**Rate limit variables** (defaults scale correctly for parallelism=1):

| Variable | Default | Notes |
|---|---|---|
| `BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS` | `10` | Multiply by parallelism if > 1 |
| `BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS` | `1` | Multiply by parallelism if > 1 |
| `STEAM_API_RATE_LIMIT_SECONDS` | `10` | Multiply by parallelism if > 1 |

```bash
flink run -d --class me.matthew.flink.backpacktfforward.BackfillJob \
  target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

## Backfill System

The backfill job processes requests from a dedicated NATS subject one at a time (parallelism=1) so it never affects the main listing update stream. It supports four request types:

| Type | Purpose | API Usage | Speed |
|------|---------|-----------|-------|
| `FULL` | Complete refresh (buy + sell) | High | Slow |
| `BUY_ONLY` | Buy orders only | Low-Medium | Fast |
| `SELL_ONLY` | Sell orders only | Medium-High | Medium |
| `SINGLE_ID` | Individual listing | Minimal | Fastest |

### Example Backfill Request

```json
{
  "data": {
    "request_type": "FULL",
    "item_defindex": 190,
    "item_quality_id": 11
  },
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "backfill-request-id"
}
```

## Docker Deployment

Both jobs share the same image:

```bash
mvn clean package && \
docker build -t tf2-ingest-flink-job:1.0 . && \
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest && \
docker push mwesterham/tf2-ingest-flink-job:latest
```

The Kubernetes deployments use `spec.job.entryClass` to select which job to run from the shared JAR.

## Fault Tolerance

`execution.checkpointing.interval` is not set in either k8s deployment and `enableCheckpointing()` is never called in code, so despite `state.checkpoints.dir`/`execution.checkpointing.timeout` being configured, Flink checkpointing is not actually running — those settings are currently inert. This works out fine in practice because both jobs are stateless streaming ETL (no windows, no keyed aggregations), so there's no meaningful Flink operator state to lose on restart anyway.

What actually provides resilience: both jobs' NATS sources ack per message via `AckingUtf8StringSourceConverter`, decoupled from Flink state, rather than `AckBehavior.AckAll`'s checkpoint-gated acking (which would never fire here since checkpointing isn't enabled). This deliberately mirrors the old Kafka consumers' `enable.auto.commit=true` behavior, which this pipeline was originally built around before the Kafka -> NATS migration.

`upgradeMode: last-state` on both k8s deployments restores the operator-managed job graph across restarts, but — per the above — there's no real checkpoint underneath it for these jobs; it's not doing more than a stateless restart would.

## Monitoring

Both jobs expose Prometheus metrics on port 9249. In the k8s cluster, each job has its own `Service` and `ServiceMonitor` resources in the `tf2-auto-bot` namespace.

```bash
# Check processing status (listings job)
curl http://localhost:9249/metrics | grep nats_messages_consumed

# Monitor backfill operations
curl http://localhost:9249/metrics | grep backfill_requests

# Check NATS consumer backlog — from the NATS Prometheus exporter (port 7777
# on the nats-* pods, not this job's own /metrics), since Flink's own
# per-operator metrics don't know about JetStream consumer state
curl http://<nats-pod>:7777/metrics | grep 'nats_consumer_num_pending{consumer_name="flink-listings-nats-source"'
```

## Architecture

```
[NATS: listing updates] → WebSocketForwarderNatsJob → PostgreSQL
[NATS: backfill requests] → BackfillJob → BackpackTF API → Steam API → PostgreSQL
```

The two jobs run as completely independent Flink deployments with separate checkpoint stores. This guarantees that slow backfill API calls (which can take 30–120 seconds per item) never block checkpoint barriers in the real-time listing update stream.

## Documentation

Detailed documentation is available in the [`docs/`](docs/) directory:

- **[Setup and Configuration](docs/setup-configuration.md)** - Complete environment setup and configuration options
- **[Backfill System](docs/backfill-system.md)** - Comprehensive guide to backfill handlers and usage patterns
- **[API Integration](docs/api-integration.md)** - BackpackTF and Steam API configuration, authentication, and rate limiting
- **[Monitoring and Metrics](docs/monitoring-metrics.md)** - Available metrics, monitoring commands, and troubleshooting
- **[Development Guide](docs/development-guide.md)** - Local development setup, testing, and debugging

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `mvn test`
5. Submit a pull request
