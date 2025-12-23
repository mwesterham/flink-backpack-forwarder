package me.matthew.flink.backpacktfforward.metrics;

public final class Metrics {

    // General application metrics
    public static final String INCOMING_EVENTS = "incoming_events";

    // Kafka source metrics
    public static final String KAFKA_MESSAGES_CONSUMED = "kafka_messages_consumed";
    public static final String KAFKA_MESSAGES_PARSED_SUCCESS = "kafka_messages_parsed_success";
    public static final String KAFKA_MESSAGES_PARSED_FAILED = "kafka_messages_parsed_failed";

    // Database sink metrics
    public static final String DELETED_LISTINGS = "deleted_listings";
    public static final String DELETED_LISTINGS_RETRIES = "listing_delete_retries";
    public static final String LISTING_UPSERTS = "listing_upserts";
    public static final String LISTING_UPSERT_RETRIES = "listing_upsert_retries";

    // Backfill operation metrics
    public static final String BACKFILL_REQUESTS_CONSUMED = "backfill_requests_consumed";
    public static final String BACKFILL_REQUESTS_PROCESSED = "backfill_requests_processed";
    public static final String BACKFILL_REQUESTS_FAILED = "backfill_requests_failed";
    public static final String BACKFILL_API_CALLS_SUCCESS = "backfill_api_calls_success";
    public static final String BACKFILL_API_CALLS_FAILED = "backfill_api_calls_failed";
    public static final String BACKFILL_API_CALL_LATENCY = "backfill_api_call_latency";
    public static final String BACKFILL_STALE_LISTINGS_DETECTED = "backfill_stale_listings_detected";
    public static final String BACKFILL_LISTINGS_UPDATED = "backfill_listings_updated";
}
