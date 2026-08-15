package me.matthew.flink.backpacktfforward.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NatsSourceConfiguration class.
 * Mirrors KafkaConfigurationTest's pattern.
 */
class NatsSourceConfigurationTest {

    @Test
    void testGetNatsUrl_WhenNotSet_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            NatsSourceConfiguration::getNatsUrl
        );

        assertTrue(exception.getMessage().contains("NATS_URL"));
        assertTrue(exception.getMessage().contains("required but not set"));
    }

    @Test
    void testGetStream_WhenNotSet_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            NatsSourceConfiguration::getStream
        );

        assertTrue(exception.getMessage().contains("NATS_STREAM"));
    }

    @Test
    void testGetSubject_WhenNotSet_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            NatsSourceConfiguration::getSubject
        );

        assertTrue(exception.getMessage().contains("NATS_SUBJECT"));
    }

    @Test
    void testGetConsumerName_WhenNotSet_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            NatsSourceConfiguration::getConsumerName
        );

        assertTrue(exception.getMessage().contains("NATS_CONSUMER_NAME"));
    }

    @Test
    void testGetAckWaitMillis_WhenNotSet_ReturnsDefault() {
        assertEquals(300_000L, NatsSourceConfiguration.getAckWaitMillis());
    }

    @Test
    void testValidateConfiguration_WhenMissingRequired_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            NatsSourceConfiguration::validateConfiguration
        );

        String message = exception.getMessage();
        assertTrue(
            message.contains("NATS_URL") ||
            message.contains("NATS_STREAM") ||
            message.contains("NATS_SUBJECT") ||
            message.contains("NATS_CONSUMER_NAME")
        );
    }
}
