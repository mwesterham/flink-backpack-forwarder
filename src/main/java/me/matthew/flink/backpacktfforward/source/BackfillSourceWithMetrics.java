package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.BACKFILL_REQUESTS_CONSUMED;

/**
 * Wrapper for backfill source that adds request consumption metrics.
 * This class provides a way to add metrics to the backfill source stream
 * following the same patterns as KafkaSourceWithMetrics.
 */
@Slf4j
public class BackfillSourceWithMetrics {
    
    /**
     * Adds backfill request consumption metrics to a backfill source data stream.
     * 
     * @param sourceStream The original backfill source data stream
     * @return A new data stream with metrics collection added
     */
    public static SingleOutputStreamOperator<BackfillRequest> addMetrics(DataStream<BackfillRequest> sourceStream) {
        return sourceStream.map(new BackfillMetricsCollector())
                .name("BackfillMetricsCollector");
    }
    
    /**
     * Map function that collects backfill request consumption metrics.
     * This function passes through all requests unchanged while counting them.
     */
    public static class BackfillMetricsCollector extends RichMapFunction<BackfillRequest, BackfillRequest> {
        
        private transient Counter requestsConsumed;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize backfill request consumption counter
            this.requestsConsumed = getRuntimeContext()
                    .getMetricGroup()
                    .counter(BACKFILL_REQUESTS_CONSUMED);
            
            log.info("Backfill request counter initialized");
        }
        
        @Override
        public BackfillRequest map(BackfillRequest request) throws Exception {
            // Record request consumption
            requestsConsumed.inc();
            
            // Pass through the request unchanged
            return request;
        }
    }
}