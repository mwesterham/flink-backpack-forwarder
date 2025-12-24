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
    public static final String BACKFILL_STALE_LISTINGS_DETECTED = "backfill_stale_listings_detected";
    public static final String BACKFILL_LISTINGS_UPDATED = "backfill_listings_updated";

    // Steam API metrics
    public static final String STEAM_API_CALLS_SUCCESS = "steam_api_calls_success";
    public static final String STEAM_API_CALLS_FAILED = "steam_api_calls_failed";

    // BackpackTF getListing API metrics
    public static final String GET_LISTING_API_CALLS_SUCCESS = "get_listing_api_calls_success";
    public static final String GET_LISTING_API_CALLS_FAILED = "get_listing_api_calls_failed";

    // Database operation metrics
    public static final String DATABASE_QUERIES_SUCCESS = "database_queries_success";
    public static final String DATABASE_QUERIES_FAILED = "database_queries_failed";

    // Item processing metrics
    public static final String ITEMS_MATCHED = "items_matched";
    public static final String SOURCE_OF_TRUTH_LISTINGS_CREATED = "source_of_truth_listings_created";

    // Performance tracking gauge metrics
    public static final String BACKFILL_LAST_API_CALL_LATENCY = "backfill_last_api_call_latency";
    public static final String BACKFILL_LAST_PROCESSING_TIME = "backfill_last_processing_time";
}
