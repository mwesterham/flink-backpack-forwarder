package me.matthew.flink.backpacktfforward.source;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaMessageSource class.
 * Tests the factory methods for creating Kafka sources.
 */
class KafkaMessageSourceTest {

    @Test
    void testCreateSource_WhenMissingConfiguration_ThrowsException() {
        // Test that createSource throws exception when required Kafka configuration is missing
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            KafkaMessageSource::createSource
        );
        
        // Should mention one of the required environment variables
        String message = exception.getMessage();
        assertTrue(
            message.contains("KAFKA_BROKERS") || 
            message.contains("KAFKA_TOPIC") || 
            message.contains("KAFKA_CONSUMER_GROUP")
        );
    }

    @Test
    void testCreateSourceWithOffsetInitializer_WhenMissingConfiguration_ThrowsException() {
        // Test that createSource with offset initializer throws exception when required config is missing
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> KafkaMessageSource.createSource(null)
        );
        
        // Should mention one of the required environment variables
        String message = exception.getMessage();
        assertTrue(
            message.contains("KAFKA_BROKERS") || 
            message.contains("KAFKA_TOPIC") || 
            message.contains("KAFKA_CONSUMER_GROUP")
        );
    }
}