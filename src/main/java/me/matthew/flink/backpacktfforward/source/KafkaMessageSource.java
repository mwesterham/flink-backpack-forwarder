package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.KafkaConfiguration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Factory class for creating configured Kafka source instances.
 * Handles Kafka source configuration using environment variables and provides
 * a properly configured KafkaSource for consuming messages from the specified topic.
 */
@Slf4j
public class KafkaMessageSource {

    /**
     * Creates a configured KafkaSource instance for consuming string messages.
     * The source is configured with:
     * - String deserialization for Kafka message values
     * - Consumer group and topic subscription from environment variables
     * - Additional consumer properties from KAFKA_CONSUMER_* environment variables
     * - Automatic offset initialization (latest for new consumer groups)
     * 
     * @return Configured KafkaSource<String> ready for use in Flink streaming job
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     */
    public static KafkaSource<String> createSource() {
        log.info("Creating Kafka source...");
        
        // Validate configuration first - this will throw if anything is missing
        KafkaConfiguration.validateConfiguration();
        
        // Get configuration values
        String kafkaBrokers = KafkaConfiguration.getKafkaBrokers();
        String kafkaTopic = KafkaConfiguration.getKafkaTopic();
        String consumerGroup = KafkaConfiguration.getConsumerGroup();
        Properties kafkaProperties = KafkaConfiguration.getKafkaConsumerProperties();
        
        log.info("Configuring Kafka source with brokers: {}, topic: {}, consumer group: {}", 
                kafkaBrokers, kafkaTopic, consumerGroup);
        
        // Build the KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(kafkaTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProperties)
                .build();
        
        log.info("Kafka source created successfully");
        return kafkaSource;
    }
    
    /**
     * Creates a configured KafkaSource instance with custom offset initialization.
     * Allows specifying how to initialize offsets for new consumer groups.
     * 
     * @param offsetsInitializer Strategy for initializing offsets (earliest, latest, specific offsets, etc.)
     * @return Configured KafkaSource<String> with custom offset initialization
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     */
    public static KafkaSource<String> createSource(OffsetsInitializer offsetsInitializer) {
        log.info("Creating Kafka source with custom offset initializer...");
        
        // Validate configuration first
        KafkaConfiguration.validateConfiguration();
        
        // Get configuration values
        String kafkaBrokers = KafkaConfiguration.getKafkaBrokers();
        String kafkaTopic = KafkaConfiguration.getKafkaTopic();
        String consumerGroup = KafkaConfiguration.getConsumerGroup();
        Properties kafkaProperties = KafkaConfiguration.getKafkaConsumerProperties();
        
        log.info("Configuring Kafka source with brokers: {}, topic: {}, consumer group: {}", 
                kafkaBrokers, kafkaTopic, consumerGroup);
        
        // Build the KafkaSource with custom offset initializer
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(kafkaTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProperties)
                .build();
        
        log.info("Kafka source created successfully with custom offset initializer");
        return kafkaSource;
    }
}