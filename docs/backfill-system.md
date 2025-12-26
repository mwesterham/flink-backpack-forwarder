# Backfill System

The application features a comprehensive backfill system that refreshes listing data from the BackpackTF API. The system uses a modular handler-based architecture to support different backfill strategies optimized for various use cases.

## Backfill Handler Types

The system supports multiple backfill strategies through specialized handlers:

### 1. Full Backfill Handler (`FULL`)

**Purpose**: Complete refresh of all listings (both buy and sell orders) for an item.

**Use Cases**:
- Comprehensive data refresh for high-value items
- Initial data population for new items
- Periodic full synchronization

**Process**:
1. Queries database for all existing listings (buy + sell)
2. Fetches BackpackTF snapshot API for the item
3. Processes buy orders directly (no Steam inventory needed)
4. Scans Steam inventories for sell listings
5. Generates updates for current listings
6. Identifies and deletes stale listings

**API Calls**: Moderate to high (snapshot + getListing for each active listing)

### 2. Buy-Only Backfill Handler (`BUY_ONLY`)

**Purpose**: Refresh only buy orders for an item.

**Use Cases**:
- Fast refresh for buy order monitoring
- Price tracking for purchase opportunities
- Reduced API usage when sell data isn't needed

**Process**:
1. Queries database for existing buy listings only
2. Fetches BackpackTF snapshot API for the item
3. Filters to buy orders only
4. Generates listing IDs using steamid + item name hash
5. Calls getListing API for each buy order
6. Deletes stale buy listings not in current API response

**API Calls**: Low to moderate (snapshot + getListing for buy orders only)
**Performance**: Fast (no Steam inventory scanning required)

### 3. Sell-Only Backfill Handler (`SELL_ONLY`)

**Purpose**: Refresh only sell listings for an item.

**Use Cases**:
- Inventory-based filtering (skip users with too many items)
- Focus on sell-side market analysis
- Reduced processing when buy data isn't needed

**Process**:
1. Queries database for existing sell listings only
2. Fetches BackpackTF snapshot API for the item
3. Filters to sell orders only
4. Scans Steam inventory for each seller
5. Matches inventory items by defindex/quality
6. Calls getListing API for matched items
7. Supports inventory size filtering (skip large inventories)
8. Deletes stale sell listings

**API Calls**: Moderate to high (snapshot + Steam inventory + getListing per sell order)
**Features**: Inventory size filtering, Steam inventory validation

### 4. Single ID Backfill Handler (`SINGLE_ID`)

**Purpose**: Refresh or validate a specific listing by its ID.

**Use Cases**:
- Real-time listing validation
- Targeted refresh for specific listings
- Error recovery for individual listings
- User-reported listing issues

**Process**:
1. Queries database for the specific listing ID
2. Calls BackpackTF getListing API directly with the ID
3. Generates update if listing exists, delete if not found
4. No snapshot API call needed (most efficient)

**API Calls**: Minimal (single getListing call)
**Performance**: Fastest (direct ID lookup)

## Performance Characteristics

| Handler Type | API Calls | Steam Inventory | Processing Speed | Use Case |
|--------------|-----------|-----------------|------------------|----------|
| `FULL` | High | Yes (sells) | Slow | Complete refresh |
| `BUY_ONLY` | Low-Medium | No | Fast | Buy order monitoring |
| `SELL_ONLY` | Medium-High | Yes | Medium | Sell-side analysis |
| `SINGLE_ID` | Minimal | No | Fastest | Individual validation |

## Request Format and Examples

### Basic Request Structure

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

### Handler Selection Logic

The system automatically selects the appropriate handler based on the `request_type` field:

- **`FULL`** → FullBackfillHandler (processes both buy and sell)
- **`BUY_ONLY`** → BuyOnlyBackfillHandler (buy orders only)
- **`SELL_ONLY`** → SellOnlyBackfillHandler (sell orders only)  
- **`SINGLE_ID`** → SingleIdBackfillHandler (specific listing)

If `request_type` is not specified, defaults to `FULL` for backward compatibility.

### Request Type Examples

**Full Backfill (Complete Refresh)**:
```json
{
  "data": {
    "request_type": "FULL",
    "item_defindex": 190,
    "item_quality_id": 11,
    "max_inventory_size": 50
  },
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "full-backfill-strange-bat"
}
```

**Buy-Only Backfill (Fast Buy Order Refresh)**:
```json
{
  "data": {
    "request_type": "BUY_ONLY", 
    "item_defindex": 5021,
    "item_quality_id": 6
  },
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "buy-only-unusual-horsemann"
}
```

**Sell-Only Backfill with Inventory Filtering**:
```json
{
  "data": {
    "request_type": "SELL_ONLY",
    "item_defindex": 190,
    "item_quality_id": 11,
    "max_inventory_size": 50
  },
  "timestamp": "2024-01-01T12:00:00.000Z", 
  "messageId": "sell-only-filtered-strange-bat"
}
```

**Single Listing Validation**:
```json
{
  "data": {
    "request_type": "SINGLE_ID",
    "listing_id": "440_16525961480"
  },
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "validate-listing-16525961480"
}
```

## Inventory Size Filtering

The `SELL_ONLY` handler supports inventory size filtering to skip users with large inventories:

```json
{
  "data": {
    "request_type": "SELL_ONLY",
    "item_defindex": 190,
    "item_quality_id": 11,
    "max_inventory_size": 20
  }
}
```

**Behavior**:
- Scans each seller's Steam inventory
- Counts matching items (same defindex + quality)
- Skips sellers with more than `max_inventory_size` matching items
- Prevents deletion of their existing listings (preserves data)
- Reduces processing time for items with many collectors

## Backfill Process Flow

**General Flow (All Handlers)**:
1. **Request Processing**: Consumes backfill requests from dedicated Kafka topic
2. **Handler Selection**: Routes to appropriate handler based on request_type
3. **Validation**: Validates request parameters for the selected handler
4. **Database Query**: Retrieves existing listings (scope depends on handler)
5. **API Integration**: Calls BackpackTF and/or Steam APIs as needed
6. **Data Processing**: Maps API responses to ListingUpdate events
7. **Stale Detection**: Identifies outdated listings for deletion
8. **Event Emission**: Outputs listing-update and listing-delete events
9. **Database Updates**: Uses existing sinks for persistence

**Handler-Specific Variations**:
- **Full**: Processes both buy and sell listings sequentially
- **Buy-Only**: Skips Steam inventory scanning, uses listing ID generation
- **Sell-Only**: Includes inventory scanning and optional size filtering
- **Single-ID**: Direct API call, no snapshot or inventory scanning

## Testing Backfill Handlers

Once the application is running with backfill enabled, you can test different backfill types by sending messages to the backfill Kafka topic:

### Test Full Backfill (Strange Bat)

```bash
echo '{
  "data": {
    "request_type": "FULL",
    "item_defindex": 190,
    "item_quality_id": 11
  },
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "messageId": "test-full-backfill-'$(date +%s)'"
}' | kafka-console-producer --bootstrap-server localhost:9092 --topic backpack-tf-backfill-requests
```

### Test Buy-Only Backfill (Unusual Horseless Headless Horsemann's Headtaker)

```bash
echo '{
  "data": {
    "request_type": "BUY_ONLY",
    "item_defindex": 266,
    "item_quality_id": 5
  },
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "messageId": "test-buy-only-'$(date +%s)'"
}' | kafka-console-producer --bootstrap-server localhost:9092 --topic backpack-tf-backfill-requests
```

### Test Sell-Only Backfill with Inventory Filtering

```bash
echo '{
  "data": {
    "request_type": "SELL_ONLY",
    "item_defindex": 190,
    "item_quality_id": 11,
    "max_inventory_size": 10
  },
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "messageId": "test-sell-only-filtered-'$(date +%s)'"
}' | kafka-console-producer --bootstrap-server localhost:9092 --topic backpack-tf-backfill-requests
```

### Test Single ID Backfill

```bash
echo '{
  "data": {
    "request_type": "SINGLE_ID",
    "listing_id": "440_16525961480"
  },
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "messageId": "test-single-id-'$(date +%s)'"
}' | kafka-console-producer --bootstrap-server localhost:9092 --topic backpack-tf-backfill-requests
```

### Monitor Backfill Processing

```bash
# Watch backfill metrics
watch -n 2 "curl -s http://localhost:9250/metrics | grep backfill"

# Check database for new/updated listings
docker exec -it flink-postgres psql -U testuser -d testdb -c "
SELECT COUNT(*) as total_listings, 
       COUNT(*) FILTER (WHERE updated_at > (EXTRACT(EPOCH FROM now() - interval '5 minutes') * 1000)) as recent_updates
FROM listings 
WHERE item_defindex = 190 AND item_quality_id = 11;"
```