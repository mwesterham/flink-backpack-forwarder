package me.matthew.flink.backpacktfforward.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.BackfillKafkaMessage;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_PARSED_FAILED;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_PARSED_SUCCESS;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_CONSUMED;

/**
 * Parses Kafka messages containing backfill request data and extracts BackfillRequest objects.
 * Handles JSON deserialization with error handling and metrics collection following existing patterns.
 */
@Slf4j
public class BackfillMessageParser extends RichFlatMapFunction<String, BackfillRequest> {
    
    private transient ObjectMapper objectMapper;
    private transient Counter successfulParses;
    private transient Counter failedParses;
    private transient Counter messagesConsumed;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize ObjectMapper
        this.objectMapper = new ObjectMapper();
        
        // Initialize metrics following existing patterns
        this.successfulParses = getRuntimeContext()
                .getMetricGroup()
                .counter(KAFKA_MESSAGES_PARSED_SUCCESS);
                
        this.failedParses = getRuntimeContext()
                .getMetricGroup()
                .counter(KAFKA_MESSAGES_PARSED_FAILED);
                
        this.messagesConsumed = getRuntimeContext()
                .getMetricGroup()
                .counter(KAFKA_MESSAGES_CONSUMED);
    }
    
    @Override
    public void flatMap(String kafkaMessageValue, Collector<BackfillRequest> out) throws Exception {
        // Record that we consumed a message from Kafka (if metrics are available)
        if (messagesConsumed != null) {
            messagesConsumed.inc();
        }
        
        // Validate input
        if (kafkaMessageValue == null) {
            log.error("Received null Kafka message value, skipping");
            if (failedParses != null) {
                failedParses.inc();
            }
            return;
        }
        
        if (kafkaMessageValue.trim().isEmpty()) {
            log.error("Received empty Kafka message value, skipping");
            if (failedParses != null) {
                failedParses.inc();
            }
            return;
        }
        
        try {
            // Parse the Kafka message wrapper
            BackfillKafkaMessage wrapper;
            try {
                wrapper = objectMapper.readValue(kafkaMessageValue, BackfillKafkaMessage.class);
            } catch (Exception jsonException) {
                log.error("Failed to parse JSON from Kafka message. Error = {}, Raw message = {}", 
                         jsonException.getMessage(), kafkaMessageValue, jsonException);
                if (failedParses != null) {
                    failedParses.inc();
                }
                return;
            }
            
            if (wrapper == null) {
                log.error("Parsed backfill Kafka message wrapper is null. Raw message = {}", kafkaMessageValue);
                if (failedParses != null) {
                    failedParses.inc();
                }
                return;
            }
            
            if (wrapper.getData() == null) {
                log.error("Backfill Kafka message wrapper data field is null. Raw message = {}", kafkaMessageValue);
                if (failedParses != null) {
                    failedParses.inc();
                }
                return;
            }
            
            BackfillRequest request = wrapper.getData();
            
            // Validate required fields with detailed error messages
            if (request.getItemDefindex() != null && request.getItemDefindex() <= 0) {
                log.error("Invalid item_defindex in backfill request: {}. Must be positive integer. Raw message = {}", 
                         request.getItemDefindex(), kafkaMessageValue);
                if (failedParses != null) {
                    failedParses.inc();
                }
                return;
            }
            
            if (request.getItemQualityId() != null && request.getItemQualityId() <= 0) {
                log.error("Invalid item_quality_id in backfill request: {}. Must be positive integer. Raw message = {}", 
                         request.getItemQualityId(), kafkaMessageValue);
                if (failedParses != null) {
                    failedParses.inc();
                }
                return;
            }
            
            // Emit the BackfillRequest
            out.collect(request);
            if (successfulParses != null) {
                successfulParses.inc();
            }
            
            log.debug("Successfully parsed backfill request: item_defindex={}, item_quality_id={}", 
                     request.getItemDefindex(), request.getItemQualityId());
            
        } catch (Exception e) {
            log.error("Unexpected error parsing backfill Kafka message. Error = {}, Raw message = {}", 
                     e.getMessage(), kafkaMessageValue, e);
            if (failedParses != null) {
                failedParses.inc();
            }
            // Don't rethrow - continue processing other messages
        }
    }
}