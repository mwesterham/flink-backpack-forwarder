package me.matthew.flink.backpacktfforward.config;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * Utility class for managing Kafka configuration from environment variables.
 * Handles reading and validation of Kafka connection parameters and consumer properties.
 */
@Slf4j
public class KafkaConfiguration {

    // Required environment variables
    private static final String KAFKA_BROKERS_ENV = "KAFKA_BROKERS";
    private static final String KAFKA_TOPIC_ENV = "KAFKA_TOPIC";
    private static final String KAFKA_CONSUMER_GROUP_ENV = "KAFKA_CONSUMER_GROUP";
    
    // Prefix for additional Kafka consumer properties
    private static final String KAFKA_CONSUMER_PREFIX = "KAFKA_CONSUMER_";

    /**
     * Reads Kafka broker addresses from environment variable.
     * @return Comma-separated list of Kafka broker addresses
     * @throws IllegalArgumentException if KAFKA_BROKERS is not set or empty
     */
    public static String getKafkaBrokers() {
        String brokers = System.getenv(KAFKA_BROKERS_ENV);
        if (brokers == null || brokers.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", KAFKA_BROKERS_ENV)
            );
        }
        return brokers.trim();
    }

    /**
     * Reads Kafka topic name from environment variable.
     * @return Kafka topic name
     * @throws IllegalArgumentException if KAFKA_TOPIC is not set or empty
     */
    public static String getKafkaTopic() {
        String topic = System.getenv(KAFKA_TOPIC_ENV);
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", KAFKA_TOPIC_ENV)
            );
        }
        return topic.trim();
    }

    /**
     * Reads Kafka consumer group ID from environment variable.
     * @return Kafka consumer group ID
     * @throws IllegalArgumentException if KAFKA_CONSUMER_GROUP is not set or empty
     */
    public static String getConsumerGroup() {
        String consumerGroup = System.getenv(KAFKA_CONSUMER_GROUP_ENV);
        if (consumerGroup == null || consumerGroup.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", KAFKA_CONSUMER_GROUP_ENV)
            );
        }
        return consumerGroup.trim();
    }

    /**
     * Reads additional Kafka consumer properties from environment variables with KAFKA_CONSUMER_ prefix.
     * Environment variables like KAFKA_CONSUMER_AUTO_OFFSET_RESET become consumer property auto.offset.reset.
     * @return Properties object containing additional Kafka consumer configuration
     */
    public static Properties getKafkaConsumerProperties() {
        Properties properties = new Properties();
        
        System.getenv().forEach((key, value) -> {
            if (key.startsWith(KAFKA_CONSUMER_PREFIX) && key.length() > KAFKA_CONSUMER_PREFIX.length()) {
                // Extract the property name after the prefix
                String propertyName = key.substring(KAFKA_CONSUMER_PREFIX.length());
                
                // Convert from UPPER_CASE to lower.case format
                String kafkaPropertyName = propertyName.toLowerCase().replace('_', '.');
                
                properties.setProperty(kafkaPropertyName, value);
                log.debug("Added Kafka consumer property: {} = {}", kafkaPropertyName, value);
            }
        });
        
        return properties;
    }

    /**
     * Validates that all required Kafka configuration is present and valid.
     * @throws IllegalArgumentException if any required configuration is missing or invalid
     */
    public static void validateConfiguration() {
        log.info("Validating Kafka configuration...");
        
        try {
            // Validate required environment variables by calling getters
            String brokers = getKafkaBrokers();
            String topic = getKafkaTopic();
            String consumerGroup = getConsumerGroup();
            
            log.info("Kafka configuration validated successfully:");
            log.info("  Brokers: {}", brokers);
            log.info("  Topic: {}", topic);
            log.info("  Consumer Group: {}", consumerGroup);
            
            // Log additional consumer properties if any
            Properties additionalProps = getKafkaConsumerProperties();
            if (!additionalProps.isEmpty()) {
                log.info("  Additional consumer properties: {}", additionalProps.size());
                additionalProps.forEach((key, value) -> 
                    log.debug("    {} = {}", key, value)
                );
            }
            
        } catch (IllegalArgumentException e) {
            log.error("Kafka configuration validation failed: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Creates a complete Properties object with all Kafka consumer configuration.
     * Includes both the core configuration (brokers, topic, consumer group) and 
     * any additional properties from KAFKA_CONSUMER_* environment variables.
     * @return Complete Properties object for Kafka consumer configuration
     */
    public static Properties getAllKafkaProperties() {
        Properties properties = getKafkaConsumerProperties();
        
        // Add core configuration
        properties.setProperty("bootstrap.servers", getKafkaBrokers());
        properties.setProperty("group.id", getConsumerGroup());
        
        // Set default properties if not already specified
        if (!properties.containsKey("key.deserializer")) {
            properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        if (!properties.containsKey("value.deserializer")) {
            properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        
        return properties;
    }
}