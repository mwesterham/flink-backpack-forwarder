package me.matthew.flink.backpacktfforward.config;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for managing NATS JetStream source configuration from environment variables.
 * Mirrors KafkaConfiguration's pattern for the Phase 3 (Kafka -> NATS migration) verification source.
 */
@Slf4j
public class NatsSourceConfiguration {

    private static final String NATS_URL_ENV = "NATS_URL";
    private static final String NATS_STREAM_ENV = "NATS_STREAM";
    private static final String NATS_SUBJECT_ENV = "NATS_SUBJECT";
    private static final String NATS_CONSUMER_NAME_ENV = "NATS_CONSUMER_NAME";
    private static final String NATS_ACK_WAIT_MS_ENV = "NATS_ACK_WAIT_MS";
    private static final long DEFAULT_ACK_WAIT_MS = 300_000L; // 5 minutes, matches nats-streams.yaml

    private NatsSourceConfiguration() {
    }

    public static String getNatsUrl() {
        return requireEnv(NATS_URL_ENV);
    }

    public static String getStream() {
        return requireEnv(NATS_STREAM_ENV);
    }

    public static String getSubject() {
        return requireEnv(NATS_SUBJECT_ENV);
    }

    public static String getConsumerName() {
        return requireEnv(NATS_CONSUMER_NAME_ENV);
    }

    public static long getAckWaitMillis() {
        String value = System.getenv(NATS_ACK_WAIT_MS_ENV);
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT_ACK_WAIT_MS;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s must be a valid number of milliseconds, got: %s",
                    NATS_ACK_WAIT_MS_ENV, value), e
            );
        }
    }

    public static void validateConfiguration() {
        log.info("Validating NATS source configuration...");

        String url = getNatsUrl();
        String stream = getStream();
        String subject = getSubject();
        String consumerName = getConsumerName();
        long ackWaitMillis = getAckWaitMillis();

        log.info("NATS source configuration validated successfully:");
        log.info("  URL: {}", url);
        log.info("  Stream: {}", stream);
        log.info("  Subject: {}", subject);
        log.info("  Consumer name: {}", consumerName);
        log.info("  Ack wait (ms): {}", ackWaitMillis);
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", name)
            );
        }
        return value.trim();
    }
}
