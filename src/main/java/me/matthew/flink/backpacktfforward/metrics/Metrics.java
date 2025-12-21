package me.matthew.flink.backpacktfforward.metrics;

public final class Metrics {

    // General application metrics (updated to reflect Kafka source)
    public static final String INCOMING_EVENTS = "incoming_events";

    // Kafka source metrics
    public static final String KAFKA_MESSAGES_CONSUMED = "kafka_messages_consumed";
    public static final String KAFKA_MESSAGES_PARSED_SUCCESS = "kafka_messages_parsed_success";
    public static final String KAFKA_MESSAGES_PARSED_FAILED = "kafka_messages_parsed_failed";
    
    // Kafka consumer group coordination metrics
    public static final String KAFKA_CONSUMER_LAG = "kafka_consumer_lag";
    public static final String KAFKA_CONSUMER_REBALANCES = "kafka_consumer_rebalances";
    public static final String KAFKA_OFFSET_COMMITS_SUCCESS = "kafka_offset_commits_success";
    public static final String KAFKA_OFFSET_COMMITS_FAILED = "kafka_offset_commits_failed";
    
    // Kafka connection and error handling metrics
    public static final String KAFKA_CONNECTION_FAILURES = "kafka_connection_failures";
    public static final String KAFKA_RECONNECT_ATTEMPTS = "kafka_reconnect_attempts";
    public static final String KAFKA_TOPIC_VALIDATION_FAILURES = "kafka_topic_validation_failures";

    // Database sink metrics (unchanged)
    public static final String DELETED_LISTINGS = "deleted_listings";
    public static final String DELETED_LISTINGS_RETRIES = "listing_delete_retries";
    public static final String LISTING_UPSERTS = "listing_upserts";
    public static final String LISTING_UPSERT_RETRIES = "listing_upsert_retries";
}
