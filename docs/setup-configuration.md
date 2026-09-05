# Setup and Configuration

## Environment Variables

### Core Application Configuration (WebSocketForwarderNatsJob)

**Required:**
- `NATS_URL`: Comma-separated list of NATS server addresses (e.g., "localhost:4222" or "nats-0:4222,nats-1:4222")
- `NATS_STREAM`: JetStream stream name to consume from (e.g., "LISTINGS")
- `NATS_SUBJECT`: Subject to consume (e.g., "bptf.listing.update")
- `NATS_CONSUMER_NAME`: Durable consumer name this job owns and creates itself — must not be a pre-existing consumer created by something else (e.g. a `nack` CRD), since the connector always issues a create-or-update call and JetStream rejects changing `deliverPolicy` on an already-existing consumer. See `NatsListingSource` javadoc.
- `DB_URL`: Database connection URL (e.g., "jdbc:postgresql://localhost:5432/testdb")
- `DB_USERNAME`: Database username
- `DB_PASSWORD`: Database password

**Optional:**
- `NATS_ACK_WAIT_MS`: How long before an unacked message is redelivered, in milliseconds (default: `300000`, 5 minutes)

### Timestamp-Based Consumer Start

The NATS source approximates the old Kafka consumer's cold-start "latest" behavior automatically: on a **true** cold start (a fresh durable consumer with no delivery history — see `NATS_CONSUMER_NAME` above), it starts from "now" rather than replaying the stream's whole retention window. There's no environment variable for this; it's set in code via `.startTime(ZonedDateTime.now())` in `NatsListingSource`/`BackfillRequestNatsSource`, since it only takes effect on a genuinely fresh consumer anyway.

### Backfill Configuration (BackfillJob)

**Required for Backfill (all must be set to enable backfill):**
- `NATS_URL`: Comma-separated list of NATS server addresses
- `NATS_STREAM`: JetStream stream name for backfill requests (e.g., "BACKFILL")
- `NATS_SUBJECT`: Subject to consume (e.g., "bptf.backfill.request")
- `NATS_CONSUMER_NAME`: Durable consumer name this job owns and creates itself (same constraint as above)
- `BACKPACK_TF_API_TOKEN`: API token for backpack.tf snapshot API (obtain from https://backpack.tf/developer)
- `STEAM_API_KEY`: Steam Web API key for inventory scanning (obtain from https://steamcommunity.com/dev/apikey)

**Optional:**
- `NATS_ACK_WAIT_MS`: How long before an unacked message is redelivered (default: `300000`, 5 minutes). BackfillJob needs this well above its own 30-minute per-item async API-call timeout so a slow backpack.tf/Steam call doesn't trigger a mid-flight redelivery — the k8s deployment sets it to `2100000` (35 minutes).
- `BACKPACK_TF_API_TIMEOUT_SECONDS`: Timeout for BackpackTF API calls in seconds (default: 30)
- `BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS`: Delay between snapshot API calls in seconds (default: 10)
- `BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS`: Delay between getListing API calls in seconds (default: 1)
- `STEAM_API_TIMEOUT_SECONDS`: Timeout for Steam API calls in seconds (default: 30)
- `STEAM_API_RATE_LIMIT_SECONDS`: Delay between Steam API calls in seconds (default: 10)

## Example Configurations

### Listings Job (WebSocketForwarderNatsJob)

```bash
export NATS_URL="localhost:4222"
export NATS_STREAM="LISTINGS"
export NATS_SUBJECT="bptf.listing.update"
export NATS_CONSUMER_NAME="flink-listings-nats-source"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"
```

### Backfill Job (BackfillJob)

```bash
export NATS_URL="localhost:4222"
export NATS_STREAM="BACKFILL"
export NATS_SUBJECT="bptf.backfill.request"
export NATS_CONSUMER_NAME="flink-backfill-nats-source"
export NATS_ACK_WAIT_MS="2100000"
export BACKPACK_TF_API_TOKEN="your-backpack-tf-api-token-here"
export STEAM_API_KEY="your-steam-api-key-here"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"

# Optional backfill tuning
export BACKPACK_TF_API_TIMEOUT_SECONDS="30"
export BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS="10"
export BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS="1"
export STEAM_API_TIMEOUT_SECONDS="30"
export STEAM_API_RATE_LIMIT_SECONDS="10"
```

## NATS Message Formats

### Listings Subject Message Format

The application expects NATS messages with the following JSON structure:

```json
{
  "data": [/* Original WebSocket ListingUpdate array */],
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "unique-message-id",
  "source": "websocket"
}
```

The `data` field contains the original WebSocket payload that would have been received directly from the BackpackTF WebSocket API.

### Backfill Subject Message Format

Backfill requests should be published to the backfill NATS subject with this JSON structure:

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
    metal_half_scrap BIGINT GENERATED ALWAYS AS (ROUND(metal * 18)) STORED,
    keys BIGINT,
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
    paint_id INT,
    paint_name TEXT,
    paint_color TEXT,
    paint_secondary_hex TEXT,
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

-- Composite index for item lookups
-- Every pricing-path query (tf2-custom-pricer-java's ListingRepository) filters
-- on item_defindex + item_quality_id. Without this index those queries fall
-- back to a sequential scan of the whole table, which is what's serving reads
-- off the TrueNAS-backed iSCSI volume — a single price lookup can otherwise
-- pull the item's entire listing history over the wire instead of just its rows.
-- CONCURRENTLY avoids locking the table against the live NATS-fed writers.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_listings_defindex_quality
ON listings (item_defindex, item_quality_id);

-- Verify index
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'listings';
```

## NATS JetStream Setup

Both jobs consume from streams/subjects managed via the `nack` (NATS Controllers for Kubernetes) JetStream operator — see `nats-streams.yaml` in the `k8s-mwesterham-homelab` repo for the declarative `Stream` definitions (`LISTINGS`, `BACKFILL`). Each job's own durable consumer (`NATS_CONSUMER_NAME` above) is created by the connector itself on first startup, not pre-provisioned via `nack` — see the `Core Application Configuration` note above for why.
