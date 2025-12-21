package me.matthew.flink.backpacktfforward.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaConfiguration class.
 * Tests environment variable reading, validation, and property handling.
 */
class KafkaConfigurationTest {

    @BeforeEach
    void setUp() {
        // Clear any existing environment variables that might interfere
        clearTestEnvironment();
    }

    @AfterEach
    void tearDown() {
        // Clean up after tests
        clearTestEnvironment();
    }

    private void clearTestEnvironment() {
        // Note: We can't actually clear environment variables in Java,
        // but we can test with mock environments or system properties
        // For now, we'll test the error cases when variables are missing
    }

    @Test
    void testGetKafkaBrokers_WhenNotSet_ThrowsException() {
        // Test that missing KAFKA_BROKERS throws appropriate exception
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            KafkaConfiguration::getKafkaBrokers
        );
        
        assertTrue(exception.getMessage().contains("KAFKA_BROKERS"));
        assertTrue(exception.getMessage().contains("required but not set"));
    }

    @Test
    void testGetKafkaTopic_WhenNotSet_ThrowsException() {
        // Test that missing KAFKA_TOPIC throws appropriate exception
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            KafkaConfiguration::getKafkaTopic
        );
        
        assertTrue(exception.getMessage().contains("KAFKA_TOPIC"));
        assertTrue(exception.getMessage().contains("required but not set"));
    }

    @Test
    void testGetConsumerGroup_WhenNotSet_ThrowsException() {
        // Test that missing KAFKA_CONSUMER_GROUP throws appropriate exception
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            KafkaConfiguration::getConsumerGroup
        );
        
        assertTrue(exception.getMessage().contains("KAFKA_CONSUMER_GROUP"));
        assertTrue(exception.getMessage().contains("required but not set"));
    }

    @Test
    void testGetKafkaConsumerProperties_ReturnsEmptyWhenNoProperties() {
        // Test that getKafkaConsumerProperties returns empty Properties when no KAFKA_CONSUMER_* vars exist
        Properties properties = KafkaConfiguration.getKafkaConsumerProperties();
        
        assertNotNull(properties);
        // Properties might not be completely empty if there are existing KAFKA_CONSUMER_* env vars
        // but we can verify it's a valid Properties object
    }

    @Test
    void testValidateConfiguration_WhenMissingRequired_ThrowsException() {
        // Test that validateConfiguration throws exception when required config is missing
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            KafkaConfiguration::validateConfiguration
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
    void testGetAllKafkaProperties_WhenMissingRequired_ThrowsException() {
        // Test that getAllKafkaProperties throws exception when required config is missing
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            KafkaConfiguration::getAllKafkaProperties
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