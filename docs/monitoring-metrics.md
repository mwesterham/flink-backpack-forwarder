# Monitoring and Metrics

## Available Metrics

### Application Metrics

- `nats_messages_consumed`: Total messages consumed from NATS (listings job, `NatsMessageParser`)
- `nats_messages_parsed_success`: Successfully parsed listing messages
- `nats_messages_parsed_failed`: Failed listing message parsing attempts
- `backfill_messages_consumed`: Total messages consumed from NATS (backfill job, `BackfillMessageParser`) — a separate counter from the listings job's, despite the similar name
- `backfill_messages_parsed_success`: Successfully parsed backfill request messages
- `backfill_messages_parsed_failed`: Failed backfill request message parsing attempts
- `incoming_events`: Total incoming events processed

### Backfill Metrics

**General Backfill Metrics:**
- `backfill_requests_consumed`: Total backfill requests consumed
- `backfill_requests_processed`: Successfully processed backfill requests
- `backfill_requests_failed`: Failed backfill request processing attempts
- `backfill_api_calls_success`: Successful API calls to backpack.tf
- `backfill_api_calls_failed`: Failed API calls to backpack.tf
- `backfill_api_call_latency`: API call latency timing
- `backfill_stale_listings_detected`: Number of stale listings identified for deletion
- `backfill_listings_updated`: Number of listings updated from API data

**Handler-Specific Metrics:**
- `backfill_full_requests`: Full backfill requests processed
- `backfill_buy_only_requests`: Buy-only backfill requests processed
- `backfill_sell_only_requests`: Sell-only backfill requests processed
- `backfill_single_id_requests`: Single-ID backfill requests processed
- `backfill_inventory_scans`: Steam inventory scans performed
- `backfill_inventory_filtered`: Users skipped due to inventory size limits
- `backfill_steam_api_calls`: Steam API calls made for inventory scanning
- `backfill_listing_id_generations`: Buy listing IDs generated

### NATS Consumer Backlog (from the NATS server, not this job)

Unlike Kafka, `flink-connector-nats` doesn't expose per-consumer lag through Flink's own metrics system. Consumer backlog instead comes from the `natsio/prometheus-nats-exporter` sidecar already deployed on the `nats-*` pods (port 7777, `-jsz=all` enabled), scraped independently of this job:

- `nats_consumer_num_pending{consumer_name="..."}`: Undelivered backlog for a given durable consumer (e.g. `flink-listings-nats-source`, `flink-backfill-nats-source`) — the direct equivalent of Kafka's consumer lag
- `nats_consumer_num_ack_pending{consumer_name="..."}`: Messages delivered but not yet acked
- `nats_consumer_num_redelivered{consumer_name="..."}`: Redelivery count — a rising value usually means the consumer is stalled or crashing mid-processing

### Database Sink Metrics

- `listing_upserts`: Successful listing upsert operations
- `listing_upsert_retries`: Listing upsert retry attempts
- `deleted_listings`: Successful listing delete operations
- `deleted_listings_retries`: Listing delete retry attempts

## Monitoring Commands

### Basic Metrics Check

```bash
# Check all metrics
curl http://localhost:9250/metrics

# Check specific metric categories
curl http://localhost:9250/metrics | grep nats_messages
curl http://localhost:9250/metrics | grep backfill
```

### Backfill Monitoring

**Check Backfill Processing:**
```bash
curl http://localhost:9250/metrics | grep backfill_requests
```

**Monitor API Call Success Rates:**
```bash
curl http://localhost:9250/metrics | grep backfill_api_calls
```

**Track Handler Usage:**
```bash
curl http://localhost:9250/metrics | grep "backfill_.*_requests"
```

**Monitor Steam API Usage:**
```bash
curl http://localhost:9250/metrics | grep backfill_steam_api
```

### NATS Consumer Health

```bash
# Check consumer backlog — from the NATS pod's exporter (port 7777), not
# this job's own /metrics endpoint (port 9250/9249)
curl http://<nats-pod>:7777/metrics | grep 'nats_consumer_num_pending{consumer_name="flink-listings-nats-source"'

# Monitor message processing rates
curl http://localhost:9250/metrics | grep nats_messages_consumed

# Check parsing success/failure rates
curl http://localhost:9250/metrics | grep nats_messages_parsed
```

### Database Performance

```bash
# Check upsert performance
curl http://localhost:9250/metrics | grep listing_upserts

# Monitor retry rates
curl http://localhost:9250/metrics | grep retry
```

## Continuous Monitoring

### Watch Metrics in Real-Time

```bash
# Watch all backfill metrics
watch -n 2 "curl -s http://localhost:9250/metrics | grep backfill"

# Monitor specific handler usage
watch -n 5 "curl -s http://localhost:9250/metrics | grep -E 'backfill_(full|buy_only|sell_only|single_id)_requests'"

# Track API call rates
watch -n 10 "curl -s http://localhost:9250/metrics | grep -E 'backfill_api_calls_(success|failed)'"
```

### Database Monitoring

```bash
# Monitor recent listing updates
docker exec -it flink-postgres psql -U testuser -d testdb -c "
SELECT 
  COUNT(*) as total_listings,
  COUNT(*) FILTER (WHERE updated_at > (EXTRACT(EPOCH FROM now() - interval '1 hour') * 1000)) as last_hour_updates,
  COUNT(*) FILTER (WHERE created_at > (EXTRACT(EPOCH FROM now() - interval '1 hour') * 1000)) as last_hour_new
FROM listings;"

# Check backfill effectiveness for specific items
docker exec -it flink-postgres psql -U testuser -d testdb -c "
SELECT 
  item_defindex,
  item_quality_id,
  COUNT(*) as total_listings,
  COUNT(*) FILTER (WHERE intent = 'buy') as buy_listings,
  COUNT(*) FILTER (WHERE intent = 'sell') as sell_listings,
  MAX(updated_at) as last_update
FROM listings 
WHERE item_defindex IN (190, 266, 463)
GROUP BY item_defindex, item_quality_id
ORDER BY item_defindex, item_quality_id;"
```

## Troubleshooting

### Common Issues

1. **Missing API Keys**: Ensure both `BACKPACK_TF_API_TOKEN` and `STEAM_API_KEY` are set
2. **Rate Limiting**: Increase rate limit delays if getting 429 responses
3. **Steam Inventory Private**: Some users have private inventories (expected behavior)
4. **Invalid Item Parameters**: Verify item_defindex and item_quality_id are correct
5. **Network Timeouts**: Increase API timeout values for slow connections

### Debug Commands

**Check if backfill is enabled:**
```bash
curl http://localhost:9250/metrics | grep backfill_requests_consumed
```

**Monitor failed requests:**
```bash
curl http://localhost:9250/metrics | grep backfill_requests_failed
```

**Check API call failures:**
```bash
curl http://localhost:9250/metrics | grep backfill_api_calls_failed
```

**Verify handler selection:**
```bash
# Should show increasing counts for the handlers you're testing
curl http://localhost:9250/metrics | grep -E "backfill_(full|buy_only|sell_only|single_id)_requests"
```

### Performance Troubleshooting

**High API Call Latency:**
```bash
# Check API call timing
curl http://localhost:9250/metrics | grep backfill_api_call_latency

# Possible solutions:
# - Increase API timeout values
# - Check network connectivity
# - Verify API service status
```

**High Consumer Backlog:**
```bash
# Check NATS consumer backlog (from the NATS pod's exporter, port 7777)
curl http://<nats-pod>:7777/metrics | grep 'nats_consumer_num_pending{consumer_name="flink-listings-nats-source"'

# Possible solutions:
# - Scale up Flink parallelism
# - Optimize backfill processing
# - Check database performance
```

**Database Performance Issues:**
```bash
# Check retry rates
curl http://localhost:9250/metrics | grep retry

# Monitor database connections
docker exec -it flink-postgres psql -U testuser -d testdb -c "
SELECT count(*) as active_connections 
FROM pg_stat_activity 
WHERE state = 'active';"
```

### Error Investigation

**Check Flink Logs:**
```bash
# View task manager logs
# Go to http://localhost:8081 -> Task Managers -> Select task manager -> Logs
```

**Database Query Debugging:**
```bash
# Check for recent errors in PostgreSQL logs
docker logs flink-postgres --tail 100

# Verify table structure
docker exec -it flink-postgres psql -U testuser -d testdb -c "\d listings"
```

**API Response Debugging:**
```bash
# Test BackpackTF API manually
curl -H "Authorization: Bearer YOUR_API_TOKEN" \
  "https://backpack.tf/api/IGetMarketPrices/v1?appid=440&market_name=Strange%20Bat"

# Test Steam API manually  
curl "https://api.steampowered.com/IEconItems_440/GetPlayerItems/v1/?key=YOUR_STEAM_KEY&steamid=76561199574661225"
```

## Alerting Recommendations

### Critical Alerts

1. **Backfill Processing Failures**: `backfill_requests_failed` increasing
2. **API Authentication Failures**: `backfill_api_calls_failed` with 401/403 errors
3. **High Consumer Backlog**: `nats_consumer_num_pending` > threshold (NATS exporter, not this job's own metrics)
4. **Database Connection Issues**: `listing_upsert_retries` increasing rapidly

### Warning Alerts

1. **High API Latency**: `backfill_api_call_latency` > threshold
2. **Rate Limiting**: `backfill_api_calls_failed` with 429 errors
3. **Inventory Filtering**: High `backfill_inventory_filtered` rates
4. **Processing Delays**: Backfill requests taking longer than expected

### Monitoring Dashboard Suggestions

**Key Metrics to Display:**
- Backfill request processing rate and success rate
- API call success/failure rates by endpoint
- NATS consumer backlog and message processing rates
- Database operation success rates and latency
- Handler usage distribution (Full vs Buy-Only vs Sell-Only vs Single-ID)

**Useful Graphs:**
- Time series of backfill requests by handler type
- API call latency percentiles
- NATS consumer backlog over time
- Database operation rates and retry counts