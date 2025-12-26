# Setup and Configuration

## Environment Variables

### Core Application Configuration

**Required:**
- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses (e.g., "localhost:9092" or "broker1:9092,broker2:9092")
- `KAFKA_TOPIC`: Name of the Kafka topic to consume from (e.g., "backpack-tf-relay-egress-queue-topic")
- `KAFKA_CONSUMER_GROUP`: Consumer group ID for offset management (e.g., "flink-backpack-tf-consumer")
- `DB_URL`: Database connection URL (e.g., "jdbc:postgresql://localhost:5432/testdb")
- `DB_USERNAME`: Database username
- `DB_PASSWORD`: Database password

### Optional Kafka Consumer Properties

Additional Kafka consumer properties can be set using environment variables with the `KAFKA_CONSUMER_` prefix:

- `KAFKA_CONSUMER_AUTO_OFFSET_RESET`: What to do when there is no initial offset (earliest/latest/none)
- `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT`: Whether to enable auto-commit of offsets (true/false)
- `KAFKA_CONSUMER_SESSION_TIMEOUT_MS`: Session timeout in milliseconds
- `KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS`: Heartbeat interval in milliseconds

### Timestamp-Based Consumer Start

- `KAFKA_START_TIMESTAMP`: Unix timestamp in milliseconds to start consuming from (optional)
  - If not set: Consumer starts from latest messages (default behavior)
  - If set: Consumer starts from first message at or after the specified timestamp
  - If timestamp is before earliest available message: Consumer starts from beginning of topic
  - Example values:
    - `0`: Start from beginning of topic (equivalent to earliest)
    - `1703936200000`: Start from December 20, 2024 10:30:00 UTC
    - `echo $(date -d '1 hour ago' +%s)000`: Start from 1 hour ago (bash)

### Backfill Configuration

**Required for Backfill (all must be set to enable backfill):**
- `BACKFILL_KAFKA_TOPIC`: Kafka topic for backfill requests (e.g., "backpack-tf-backfill-requests")
- `BACKFILL_KAFKA_CONSUMER_GROUP`: Consumer group for backfill requests (e.g., "flink-backfill-consumer")
- `BACKPACK_TF_API_TOKEN`: API token for backpack.tf snapshot API (obtain from https://backpack.tf/developer)
- `STEAM_API_KEY`: Steam Web API key for inventory scanning (obtain from https://steamcommunity.com/dev/apikey)

**Optional Backfill Configuration:**
- `BACKPACK_TF_API_TIMEOUT_SECONDS`: Timeout for BackpackTF API calls in seconds (default: 30)
- `BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS`: Delay between snapshot API calls in seconds (default: 10)
- `BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS`: Delay between getListing API calls in seconds (default: 1)
- `STEAM_API_TIMEOUT_SECONDS`: Timeout for Steam API calls in seconds (default: 30)
- `STEAM_API_RATE_LIMIT_SECONDS`: Delay between Steam API calls in seconds (default: 10)

## Example Configurations

### Basic Configuration (No Backfill)

```bash
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic"
export KAFKA_CONSUMER_GROUP="flink-backpack-tf-consumer"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"

# Optional consumer properties
export KAFKA_CONSUMER_AUTO_OFFSET_RESET="earliest"
export KAFKA_CONSUMER_ENABLE_AUTO_COMMIT="false"
```

### Full Configuration with Backfill

```bash
# Core application
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic"
export KAFKA_CONSUMER_GROUP="flink-backpack-tf-consumer"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"

# Backfill functionality
export BACKFILL_KAFKA_TOPIC="backpack-tf-backfill-requests"
export BACKFILL_KAFKA_CONSUMER_GROUP="flink-backfill-consumer"
export BACKPACK_TF_API_TOKEN="your-backpack-tf-api-token-here"
export STEAM_API_KEY="your-steam-api-key-here"

# Optional backfill tuning
export BACKPACK_TF_API_TIMEOUT_SECONDS="30"
export BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS="10"
export BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS="1"
export STEAM_API_TIMEOUT_SECONDS="30"
export STEAM_API_RATE_LIMIT_SECONDS="10"

# Timestamp-based start (optional)
export KAFKA_START_TIMESTAMP="0"  # Start from beginning
```

## Kafka Message Formats

### Main Topic Message Format

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

### Backfill Topic Message Format

Backfill requests should be sent to the backfill Kafka topic with this JSON structure:

```json
{
  "data": {
    "request_type": "FULL",
    "item_defindex": 463,
    "item_quality_id": 5,
    "listing_id": null,
    "max_inventory_size": null
  },
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "unique-backfill-request-id"
}
```

## Database Setup

### PostgreSQL Setup

```bash
# Start PostgreSQL container
docker run --name flink-postgres \
  -e POSTGRES_USER=testuser \
  -e POSTGRES_PASSWORD=testpass \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 -d postgres:16
```

### Database Schema

```sql
-- Connect to database
docker exec -it flink-postgres psql -U testuser -d testdb

-- Create listings table
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
    listed_at BIGINT,
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
    bumped_at BIGINT,
    spell_ids TEXT[],
    strange_part_ids TEXT[],
    is_deleted BOOLEAN DEFAULT false,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT
);

-- Create update trigger function
CREATE OR REPLACE FUNCTION set_updated_at_epoch_ms()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at := (EXTRACT(EPOCH FROM now() AT TIME ZONE 'UTC') * 1000)::BIGINT;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger
CREATE TRIGGER listings_set_updated_at
BEFORE UPDATE ON listings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_epoch_ms();

-- Verify trigger
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'listings'::regclass AND NOT tgisinternal;
```

## Migration from WebSocket to Kafka

This application has been migrated from direct WebSocket consumption to Kafka-based message consumption. The migration provides:

- **Improved Reliability**: Kafka provides message persistence and delivery guarantees
- **Better Scalability**: Multiple consumer instances can process messages in parallel
- **Operational Benefits**: Kafka's monitoring, offset management, and consumer group coordination
- **Decoupling**: Separation between WebSocket connection management and message processing

### Migration Steps

1. **Deploy Kafka Infrastructure**: Set up Kafka cluster and create the required topic
2. **Deploy WebSocket Bridge**: Deploy a service that consumes from WebSocket and produces to Kafka
3. **Update Application Configuration**: Change environment variables from WebSocket URL to Kafka configuration
4. **Deploy Updated Application**: Deploy this Kafka-enabled version of the Flink application
5. **Monitor and Validate**: Ensure messages flow correctly and all metrics are healthy

### Rollback Procedures

If rollback is needed:

1. **Preserve WebSocket Bridge**: Keep the WebSocket-to-Kafka bridge running to maintain message flow
2. **Revert Configuration**: Change environment variables back to WebSocket configuration
3. **Deploy Previous Version**: Deploy the WebSocket-based version of the application
4. **Validate Functionality**: Ensure direct WebSocket consumption is working correctly

Note: The WebSocket bridge can continue running during rollback to maintain Kafka message history.