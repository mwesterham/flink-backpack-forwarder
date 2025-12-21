package me.matthew.flink.backpacktfforward.source;

import org.apache.kafka.common.KafkaException;
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
        KafkaException exception = assertThrows(
            KafkaException.class,
            KafkaMessageSource::createSource
        );
        
        // Should mention configuration failure
        String message = exception.getMessage();
        assertTrue(message.contains("Failed to create Kafka source"));
        
        // The cause should be the original IllegalArgumentException
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        String causeMessage = exception.getCause().getMessage();
        assertTrue(
            causeMessage.contains("KAFKA_BROKERS") || 
            causeMessage.contains("KAFKA_TOPIC") || 
            causeMessage.contains("KAFKA_CONSUMER_GROUP")
        );
    }

    @Test
    void testCreateSourceWithOffsetInitializer_WhenMissingConfiguration_ThrowsException() {
        // Test that createSource with offset initializer throws exception when required config is missing
        KafkaException exception = assertThrows(
            KafkaException.class,
            () -> KafkaMessageSource.createSource(null)
        );
        
        // Should mention configuration failure
        String message = exception.getMessage();
        assertTrue(message.contains("Failed to create Kafka source"));
        
        // The cause should be the original IllegalArgumentException
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        String causeMessage = exception.getCause().getMessage();
        assertTrue(
            causeMessage.contains("KAFKA_BROKERS") || 
            causeMessage.contains("KAFKA_TOPIC") || 
            causeMessage.contains("KAFKA_CONSUMER_GROUP")
        );
    }
}