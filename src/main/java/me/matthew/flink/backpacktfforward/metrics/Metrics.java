package me.matthew.flink.backpacktfforward.metrics;

public final class Metrics {

    public static final String INCOMING_WS_EVENTS = "incoming_ws_events";
    public static final String LAST_RECONNECT_BACKOFF_MS = "last_reconnect_backoff_ms";

    public static final String WS_MESSAGES_RECEIVED = "websocket_source_messages_received";
    public static final String WS_MESSAGES_DROPPED = "websocket_source_messages_dropped";
    public static final String WS_CONNECTIONS_OPENED = "websocket_source_connections_opened";
    public static final String WS_CONNECTIONS_CLOSED = "websocket_source_connections_closed";
    public static final String WS_CONNECTION_FAILURES = "websocket_source_connection_failures";
    public static final String WS_RECONNECT_ATTEMPTS = "websocket_source_reconnect_attempts";
    public static final String WS_HEARTBEAT_FAILURES = "websocket_source_heartbeat_failures";

    public static final String DELETED_LISTINGS = "deleted_listings";
    public static final String DELETED_LISTINGS_RETRIES = "listing_delete_retries";

    public static final String LISTING_UPSERTS = "listing_upserts";
    public static final String LISTING_UPSERT_RETRIES = "listing_upsert_retries";

    // Kafka message parsing metrics
    public static final String KAFKA_MESSAGES_PARSED_SUCCESS = "kafka_messages_parsed_success";
    public static final String KAFKA_MESSAGES_PARSED_FAILED = "kafka_messages_parsed_failed";
    
    // Kafka consumer group coordination metrics
    public static final String KAFKA_CONSUMER_LAG = "kafka_consumer_lag";
    public static final String KAFKA_CONSUMER_REBALANCES = "kafka_consumer_rebalances";
    public static final String KAFKA_OFFSET_COMMITS_SUCCESS = "kafka_offset_commits_success";
    public static final String KAFKA_OFFSET_COMMITS_FAILED = "kafka_offset_commits_failed";
    public static final String KAFKA_MESSAGES_CONSUMED = "kafka_messages_consumed";
}
