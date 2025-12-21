package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_CONSUMED;

/**
 * Wrapper for Kafka source that adds message consumption metrics.
 * This class provides a way to add metrics to the Kafka source stream
 * without modifying the core KafkaSource implementation.
 */
@Slf4j
public class KafkaSourceWithMetrics {
    
    /**
     * Adds message consumption metrics to a Kafka source data stream.
     * 
     * @param sourceStream The original Kafka source data stream
     * @return A new data stream with metrics collection added
     */
    public static SingleOutputStreamOperator<String> addMetrics(DataStream<String> sourceStream) {
        return sourceStream.map(new KafkaMetricsCollector())
                .name("KafkaMetricsCollector");
    }
    
    /**
     * Map function that collects Kafka message consumption metrics.
     * This function passes through all messages unchanged while counting them.
     */
    public static class KafkaMetricsCollector extends RichMapFunction<String, String> {
        
        private transient Counter messagesConsumed;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize message consumption counter
            this.messagesConsumed = getRuntimeContext()
                    .getMetricGroup()
                    .counter(KAFKA_MESSAGES_CONSUMED);
            
            log.info("Kafka message counter initialized");
        }
        
        @Override
        public String map(String message) throws Exception {
            // Record message consumption
            messagesConsumed.inc();
            
            // Pass through the message unchanged
            return message;
        }
    }
}