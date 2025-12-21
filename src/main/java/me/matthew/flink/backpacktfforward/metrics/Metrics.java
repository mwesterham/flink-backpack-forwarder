package me.matthew.flink.backpacktfforward.metrics;

public final class Metrics {

    // General application metrics (updated to reflect Kafka source)
    public static final String INCOMING_EVENTS = "incoming_events";
    public static final String LAST_RECONNECT_BACKOFF_MS = "last_reconnect_backoff_ms";

    // Legacy WebSocket metrics (deprecated - kept for backward compatibility)
    @Deprecated
    public static final String INCOMING_WS_EVENTS = "incoming_ws_events";
    @Deprecated
    public static final String WS_MESSAGES_RECEIVED = "websocket_source_messages_received";
    @Deprecated
    public static final String WS_MESSAGES_DROPPED = "websocket_source_messages_dropped";
    @Deprecated
    public static final String WS_CONNECTIONS_OPENED = "websocket_source_connections_opened";
    @Deprecated
    public static final String WS_CONNECTIONS_CLOSED = "websocket_source_connections_closed";
    @Deprecated
    public static final String WS_CONNECTION_FAILURES = "websocket_source_connection_failures";
    @Deprecated
    public static final String WS_RECONNECT_ATTEMPTS = "websocket_source_reconnect_attempts";
    @Deprecated
    public static final String WS_HEARTBEAT_FAILURES = "websocket_source_heartbeat_failures";

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
