package me.matthew.flink.backpacktfforward.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaMetrics class.
 * Tests the metrics constants and basic functionality.
 */
class KafkaMetricsTest {

    @Test
    void testMetricsConstants() {
        // Test that all Kafka metrics constants are properly defined
        assertNotNull(Metrics.KAFKA_MESSAGES_CONSUMED);
        assertNotNull(Metrics.KAFKA_MESSAGES_PARSED_SUCCESS);
        assertNotNull(Metrics.KAFKA_MESSAGES_PARSED_FAILED);
        assertNotNull(Metrics.KAFKA_CONSUMER_LAG);
        assertNotNull(Metrics.KAFKA_CONSUMER_REBALANCES);
        assertNotNull(Metrics.KAFKA_OFFSET_COMMITS_SUCCESS);
        assertNotNull(Metrics.KAFKA_OFFSET_COMMITS_FAILED);
        assertNotNull(Metrics.KAFKA_CONNECTION_FAILURES);
        assertNotNull(Metrics.KAFKA_RECONNECT_ATTEMPTS);
        assertNotNull(Metrics.KAFKA_TOPIC_VALIDATION_FAILURES);
    }

    @Test
    void testMetricsNaming() {
        // Test that metrics have appropriate naming conventions
        assertTrue(Metrics.KAFKA_MESSAGES_CONSUMED.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_MESSAGES_PARSED_SUCCESS.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_MESSAGES_PARSED_FAILED.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_CONSUMER_LAG.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_CONSUMER_REBALANCES.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_OFFSET_COMMITS_SUCCESS.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_OFFSET_COMMITS_FAILED.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_CONNECTION_FAILURES.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_RECONNECT_ATTEMPTS.startsWith("kafka_"));
        assertTrue(Metrics.KAFKA_TOPIC_VALIDATION_FAILURES.startsWith("kafka_"));
    }

    @Test
    void testDeprecatedMetricsStillExist() {
        // Test that deprecated WebSocket metrics still exist for backward compatibility
        assertNotNull(Metrics.INCOMING_WS_EVENTS);
        assertNotNull(Metrics.WS_MESSAGES_RECEIVED);
        assertNotNull(Metrics.WS_MESSAGES_DROPPED);
        assertNotNull(Metrics.WS_CONNECTIONS_OPENED);
        assertNotNull(Metrics.WS_CONNECTIONS_CLOSED);
        assertNotNull(Metrics.WS_CONNECTION_FAILURES);
        assertNotNull(Metrics.WS_RECONNECT_ATTEMPTS);
        assertNotNull(Metrics.WS_HEARTBEAT_FAILURES);
    }

    @Test
    void testNewGeneralMetrics() {
        // Test that new general metrics exist
        assertNotNull(Metrics.INCOMING_EVENTS);
        assertEquals("incoming_events", Metrics.INCOMING_EVENTS);
    }

    @Test
    void testSinkMetricsUnchanged() {
        // Test that existing sink metrics are unchanged
        assertNotNull(Metrics.DELETED_LISTINGS);
        assertNotNull(Metrics.DELETED_LISTINGS_RETRIES);
        assertNotNull(Metrics.LISTING_UPSERTS);
        assertNotNull(Metrics.LISTING_UPSERT_RETRIES);
    }
}