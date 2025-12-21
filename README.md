### Setup flink

- Download
```
wget https://dlcdn.apache.org/flink/flink-1.20.2/flink-1.20.2-bin-scala_2.12.tgz
tar -xzf flink-1.20.2-bin-scala_2.12.tgz
```

- Add to path
```
vi .bashrc
export FLINK_HOME=/home/mwesterham/flink-1.20.2
export PATH=$FLINK_HOME/bin:$PATH
```

```
flink -v
```

- Setup windows cluster
Put this in `$FLINK_HOME/conf/config.yaml`
```
vi $FLINK_HOME/conf/config.yaml
```
```
metrics:
  reporters: prom
  reporter:
    prom:
      factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
      port: 9249-9259
      filter.includes: "*"  # or "taskmanager.*" for TaskManager only
```

### Kafka Configuration

This application consumes messages from Kafka instead of directly from WebSocket. The following environment variables are required:

#### Required Environment Variables

- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses (e.g., "localhost:9092" or "broker1:9092,broker2:9092")
- `KAFKA_TOPIC`: Name of the Kafka topic to consume from (e.g., "backpack-tf-relay-egress-queue-topic")
- `KAFKA_CONSUMER_GROUP`: Consumer group ID for offset management (e.g., "flink-backpack-tf-consumer")

#### Optional Kafka Consumer Properties

Additional Kafka consumer properties can be set using environment variables with the `KAFKA_CONSUMER_` prefix:

- `KAFKA_CONSUMER_AUTO_OFFSET_RESET`: What to do when there is no initial offset (earliest/latest/none)
- `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT`: Whether to enable auto-commit of offsets (true/false)
- `KAFKA_CONSUMER_SESSION_TIMEOUT_MS`: Session timeout in milliseconds
- `KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS`: Heartbeat interval in milliseconds

#### Timestamp-Based Consumer Start

- `KAFKA_START_TIMESTAMP`: Unix timestamp in milliseconds to start consuming from (optional)
  - If not set: Consumer starts from latest messages (default behavior)
  - If set: Consumer starts from first message at or after the specified timestamp
  - If timestamp is before earliest available message: Consumer starts from beginning of topic
  - Example values:
    - `0`: Start from beginning of topic (equivalent to earliest)
    - `1703936200000`: Start from December 20, 2024 10:30:00 UTC
    - `echo $(date -d '1 hour ago' +%s)000`: Start from 1 hour ago (bash)

#### Example Kafka Configuration

```bash
# Basic configuration
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic"
export KAFKA_CONSUMER_GROUP="flink-backpack-tf-consumer"

# Optional consumer properties
export KAFKA_CONSUMER_AUTO_OFFSET_RESET="earliest"
export KAFKA_CONSUMER_ENABLE_AUTO_COMMIT="false"
export KAFKA_CONSUMER_SESSION_TIMEOUT_MS="30000"
export KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS="3000"

# Timestamp-based start (optional)
export KAFKA_START_TIMESTAMP="1703936200000"  # Start from specific timestamp
# export KAFKA_START_TIMESTAMP="0"            # Start from beginning of topic
# export KAFKA_START_TIMESTAMP="$(date -d '1 hour ago' +%s)000"  # Start from 1 hour ago
```

#### Kafka Message Format

The application expects Kafka messages with the following JSON structure:

```json
{
  "data": [/* Original WebSocket ListingUpdate array */],
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "unique-message-id",
  "source": "websocket"
}
```

The `data` field contains the original WebSocket payload that would have been received directly from the BackpackTF WebSocket API.

### Migration from WebSocket to Kafka

This application has been migrated from direct WebSocket consumption to Kafka-based message consumption. The migration provides:

- **Improved Reliability**: Kafka provides message persistence and delivery guarantees
- **Better Scalability**: Multiple consumer instances can process messages in parallel
- **Operational Benefits**: Kafka's monitoring, offset management, and consumer group coordination
- **Decoupling**: Separation between WebSocket connection management and message processing

#### Migration Steps

1. **Deploy Kafka Infrastructure**: Set up Kafka cluster and create the required topic
2. **Deploy WebSocket Bridge**: Deploy a service that consumes from WebSocket and produces to Kafka
3. **Update Application Configuration**: Change environment variables from WebSocket URL to Kafka configuration
4. **Deploy Updated Application**: Deploy this Kafka-enabled version of the Flink application
5. **Monitor and Validate**: Ensure messages flow correctly and all metrics are healthy

#### Rollback Procedures

If rollback is needed:

1. **Preserve WebSocket Bridge**: Keep the WebSocket-to-Kafka bridge running to maintain message flow
2. **Revert Configuration**: Change environment variables back to WebSocket configuration
3. **Deploy Previous Version**: Deploy the WebSocket-based version of the application
4. **Validate Functionality**: Ensure direct WebSocket consumption is working correctly

Note: The WebSocket bridge can continue running during rollback to maintain Kafka message history.

### Building the docker file

- One-liner

```
mvn clean package && \
docker build -t tf2-ingest-flink-job:1.0 . && \
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest && \
docker push mwesterham/tf2-ingest-flink-job:latest
```

- Separate

```
mvn clean package
```

```
docker build -t tf2-ingest-flink-job:1.0 .
```

```
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest
```

```
docker push mwesterham/tf2-ingest-flink-job:latest
```

### Running locally via cluster

- Setup the test sink

```
docker run --name flink-postgres -e POSTGRES_USER=testuser -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=testdb -p 5432:5432 -d postgres:16
```

```
docker exec -it flink-postgres psql -U testuser -d testdb

DROP TABLE IF EXISTS listings;
CREATE TABLE listings (
    id TEXT PRIMARY KEY,
    steamid TEXT NOT NULL,
    item_defindex INT NOT NULL,
    item_quality_id INT,
    intent TEXT NOT NULL,
    appid INT,
    metal DOUBLE PRECISION,
    keys INT,
    raw_value DOUBLE PRECISION,
    short_value TEXT,
    long_value TEXT,
    details TEXT,
    listed_at TIMESTAMP,
    market_name TEXT,
    status TEXT,
    user_agent_client TEXT,
    user_name TEXT,
    user_premium BOOLEAN,
    user_online BOOLEAN,
    user_banned BOOLEAN,
    user_trade_offer_url TEXT,
    item_tradable BOOLEAN,
    item_craftable BOOLEAN,
    item_quality_color TEXT,
    item_particle_name TEXT,
    item_particle_type TEXT,
    bumped_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

- Build the package

```
mvn clean package
```

- Startup the cluster, this is located in $FLINK_HOME
```
start-cluster.sh
```

- Startup the job
```
mvn clean package && \
KAFKA_BROKERS="localhost:9092" \
KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic" \
KAFKA_CONSUMER_GROUP="flink-backpack-tf-test-consumer" \
DB_URL="jdbc:postgresql://localhost:5432/testdb" \
DB_USERNAME="testuser" \
DB_PASSWORD="testpass" \
flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

To start from a specific timestamp, add `KAFKA_START_TIMESTAMP`:
```
mvn clean package && \
KAFKA_BROKERS="localhost:9092" \
KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic" \
KAFKA_CONSUMER_GROUP="flink-backpack-tf-test-consumer" \
KAFKA_START_TIMESTAMP="0" \
DB_URL="jdbc:postgresql://localhost:5432/testdb" \
DB_USERNAME="testuser" \
DB_PASSWORD="testpass" \
flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

- Observe the job

Go to http://localhost:8081 and to `Task Managers -> pick your task manager -> Logs`

- View/cancel running jobs

```
flink list
flink cancel <job_id>
```

- Observe the sink

```
docker exec -it flink-postgres psql -U testuser -d testdb

SELECT * FROM listings;

SELECT * FROM listings WHERE item_defindex = '5021' AND item_quality_id = '6' AND intent = 'sell' AND is_deleted = false ORDER BY raw_value;

SELECT * FROM listings WHERE id = '440_16358814163';
```

- Stop the cluster

```
stop-cluster.sh
```

### Adjustting task manager ram / prometheus ports / etc...

```
code /home/mwesterham/flink-1.20.2/conf/config.yaml
```

### Checking metrics

```
curl http://localhost:9249/metrics
```

```
curl http://localhost:9250/metrics | grep kafka_messages
```

#### Available Metrics

**Kafka Source Metrics:**
- `kafka_messages_consumed`: Total messages consumed from Kafka
- `kafka_messages_parsed_success`: Successfully parsed messages
- `kafka_messages_parsed_failed`: Failed message parsing attempts
- `kafka_consumer_lag`: Consumer lag behind latest offset
- `kafka_consumer_rebalances`: Consumer group rebalancing events
- `kafka_offset_commits_success`: Successful offset commits
- `kafka_offset_commits_failed`: Failed offset commits
- `kafka_connection_failures`: Kafka connection failures
- `kafka_reconnect_attempts`: Kafka reconnection attempts
- `kafka_topic_validation_failures`: Topic validation failures

**Database Sink Metrics (unchanged):**
- `listing_upserts`: Successful listing upsert operations
- `listing_upsert_retries`: Listing upsert retry attempts
- `deleted_listings`: Successful listing delete operations
- `deleted_listings_retries`: Listing delete retry attempts

### Debugging

#### Flink is highlighted RED in code?

- Right-click `pom.xml`
- `Maven -> Sync Project`