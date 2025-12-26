# API Integration

The application integrates with external APIs for backfill functionality. These APIs require authentication and have configurable rate limiting.

## BackpackTF API Configuration

**Required:**
- `BACKPACK_TF_API_TOKEN`: API token for backpack.tf (obtain from https://backpack.tf/developer)

**Optional:**
- `BACKPACK_TF_API_TIMEOUT_SECONDS`: HTTP timeout for API calls (default: 30)
- `BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS`: Delay between snapshot API calls (default: 10 seconds, 6 requests/minute)
- `BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS`: Delay between getListing API calls (default: 1 second, 60 requests/minute)

### BackpackTF API Endpoints

The application uses two main BackpackTF API endpoints:

#### 1. Snapshot API
- **Endpoint**: `GET /api/IGetMarketPrices/v1`
- **Purpose**: Fetch current market listings for a specific item
- **Rate Limit**: 6 requests per minute (configurable)
- **Usage**: Used by all handlers except `SINGLE_ID`

#### 2. GetListing API
- **Endpoint**: `GET /api/classifieds/listings/{listing_id}`
- **Purpose**: Get detailed information for a specific listing
- **Rate Limit**: 60 requests per minute (configurable)
- **Usage**: Used by all handlers to get complete listing data

## Steam Web API Configuration

**Required:**
- `STEAM_API_KEY`: Steam Web API key for inventory access (obtain from https://steamcommunity.com/dev/apikey)

**Optional:**
- `STEAM_API_TIMEOUT_SECONDS`: HTTP timeout for Steam API calls (default: 30)
- `STEAM_API_RATE_LIMIT_SECONDS`: Delay between Steam API calls (default: 10 seconds, 6 requests/minute)

### Steam API Endpoints

#### GetPlayerItems API
- **Endpoint**: `GET /IEconItems_440/GetPlayerItems/v1`
- **Purpose**: Fetch a user's Team Fortress 2 inventory
- **Rate Limit**: 6 requests per minute (configurable)
- **Usage**: Used by `FULL` and `SELL_ONLY` handlers for inventory scanning

## API Rate Limiting

The application implements rate limiting to comply with API terms of service:

- **BackpackTF Snapshot API**: 6 requests per minute (configurable)
- **BackpackTF GetListing API**: 60 requests per minute (configurable)  
- **Steam Web API**: 6 requests per minute (configurable)

Rate limits are enforced globally across all application instances to prevent API abuse.

### Rate Limiting Implementation

Rate limiting is implemented using a token bucket algorithm with the following features:

- **Global Rate Limiting**: Shared across all application instances
- **Configurable Delays**: Adjustable via environment variables
- **Graceful Degradation**: Continues processing with delays when rate limits are hit
- **Retry Logic**: Automatic retry with exponential backoff for failed requests

### Configuring Rate Limits

```bash
# BackpackTF API rate limits
export BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS="10"    # 6 requests/minute
export BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS="1"  # 60 requests/minute

# Steam API rate limits
export STEAM_API_RATE_LIMIT_SECONDS="10"               # 6 requests/minute

# API timeouts
export BACKPACK_TF_API_TIMEOUT_SECONDS="30"
export STEAM_API_TIMEOUT_SECONDS="30"
```

## API Authentication

### BackpackTF API Token

1. Visit https://backpack.tf/developer
2. Log in with your Steam account
3. Create a new API application
4. Copy the generated API token
5. Set the `BACKPACK_TF_API_TOKEN` environment variable

### Steam Web API Key

1. Visit https://steamcommunity.com/dev/apikey
2. Log in with your Steam account
3. Register a new API key with your domain
4. Copy the generated API key
5. Set the `STEAM_API_KEY` environment variable

## Error Handling

The application includes comprehensive error handling for API interactions:

### Common API Errors

1. **Rate Limiting (429)**: Automatic retry with increased delay
2. **Authentication (401/403)**: Log error and skip processing
3. **Not Found (404)**: Handle gracefully (expected for deleted listings)
4. **Timeout**: Retry with exponential backoff
5. **Network Errors**: Retry with exponential backoff

### Error Recovery Strategies

- **Transient Errors**: Automatic retry with exponential backoff
- **Authentication Errors**: Log and continue (may indicate invalid API keys)
- **Rate Limit Errors**: Respect rate limits and retry after delay
- **Permanent Errors**: Log and skip individual requests

### Monitoring API Health

```bash
# Check API call success rates
curl http://localhost:9250/metrics | grep api_calls_success
curl http://localhost:9250/metrics | grep api_calls_failed

# Monitor API call latency
curl http://localhost:9250/metrics | grep api_call_latency

# Check rate limiting
curl http://localhost:9250/metrics | grep rate_limit
```

## API Usage Patterns by Handler

### Full Backfill Handler
- **BackpackTF Snapshot**: 1 call per request
- **Steam Inventory**: 1 call per unique seller
- **BackpackTF GetListing**: 1 call per active listing
- **Total**: High API usage (comprehensive refresh)

### Buy-Only Backfill Handler
- **BackpackTF Snapshot**: 1 call per request
- **Steam Inventory**: 0 calls (not needed for buy orders)
- **BackpackTF GetListing**: 1 call per buy order
- **Total**: Low-medium API usage (efficient for buy orders)

### Sell-Only Backfill Handler
- **BackpackTF Snapshot**: 1 call per request
- **Steam Inventory**: 1 call per unique seller
- **BackpackTF GetListing**: 1 call per matched sell listing
- **Total**: Medium-high API usage (inventory scanning required)

### Single ID Backfill Handler
- **BackpackTF Snapshot**: 0 calls (not needed)
- **Steam Inventory**: 0 calls (not needed)
- **BackpackTF GetListing**: 1 call per request
- **Total**: Minimal API usage (most efficient)

## Best Practices

### API Key Security
- Store API keys as environment variables, never in code
- Use different API keys for different environments
- Rotate API keys periodically
- Monitor API key usage for unusual activity

### Rate Limit Management
- Start with conservative rate limits and increase gradually
- Monitor API response times and adjust timeouts accordingly
- Use appropriate handlers for your use case to minimize API calls
- Consider using `SINGLE_ID` handler for real-time validation

### Error Handling
- Always check API response status codes
- Implement proper retry logic with exponential backoff
- Log API errors for monitoring and debugging
- Have fallback strategies for API unavailability

### Performance Optimization
- Use `BUY_ONLY` handler when Steam inventory data isn't needed
- Implement inventory size filtering for `SELL_ONLY` handler
- Batch requests when possible to reduce API overhead
- Cache API responses when appropriate (respecting cache headers)