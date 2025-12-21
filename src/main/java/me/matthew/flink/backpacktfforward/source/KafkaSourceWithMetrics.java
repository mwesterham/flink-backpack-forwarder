package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.metrics.KafkaMetrics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_CONSUMED;

/**
 * Wrapper for Kafka source that adds comprehensive metrics collection.
 * This class provides a way to add metrics to the Kafka source stream
 * without modifying the core KafkaSource implementation.
 */
@Slf4j
public class KafkaSourceWithMetrics {
    
    /**
     * Adds Kafka-specific metrics to a Kafka source data stream.
     * This method wraps the source stream with a map function that collects
     * metrics for messages consumed and integrates with the KafkaMetrics system.
     * 
     * @param sourceStream The original Kafka source data stream
     * @return A new data stream with metrics collection added
     */
    public static SingleOutputStreamOperator<String> addMetrics(DataStream<String> sourceStream) {
        return sourceStream.map(new KafkaMetricsCollector())
                .name("KafkaMetricsCollector");
    }
    
    /**
     * Map function that collects Kafka consumption metrics.
     * This function passes through all messages unchanged while collecting
     * metrics about message consumption and Kafka source health.
     */
    public static class KafkaMetricsCollector extends RichMapFunction<String, String> {
        
        private transient Counter messagesConsumed;
        private transient KafkaMetrics kafkaMetrics;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize message consumption counter
            this.messagesConsumed = getRuntimeContext()
                    .getMetricGroup()
                    .counter(KAFKA_MESSAGES_CONSUMED);
            
            // Initialize Kafka-specific metrics
            this.kafkaMetrics = new KafkaMetrics(getRuntimeContext().getMetricGroup());
            
            log.info("Kafka metrics collector initialized");
        }
        
        @Override
        public String map(String message) throws Exception {
            // Record message consumption
            messagesConsumed.inc();
            
            // Pass through the message unchanged
            return message;
        }
        
        /**
         * Gets the KafkaMetrics instance for external use.
         * This allows other components to record Kafka-specific events.
         * 
         * @return The KafkaMetrics instance, or null if not initialized
         */
        public KafkaMetrics getKafkaMetrics() {
            return kafkaMetrics;
        }
    }
}