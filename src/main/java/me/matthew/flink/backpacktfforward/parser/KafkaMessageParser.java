package me.matthew.flink.backpacktfforward.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.KafkaMessageWrapper;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import java.util.List;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_PARSED_FAILED;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_PARSED_SUCCESS;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.KAFKA_MESSAGES_CONSUMED;

/**
 * Parses Kafka messages containing WebSocket data and extracts ListingUpdate objects.
 * Handles JSON deserialization with error handling and metrics collection.
 */
@Slf4j
public class KafkaMessageParser extends RichFlatMapFunction<String, ListingUpdate> {
    
    private transient ObjectMapper objectMapper;
    private transient Counter successfulParses;
    private transient Counter failedParses;
    private transient Counter messagesConsumed;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize ObjectMapper
        this.objectMapper = new ObjectMapper();
        
        // Initialize metrics
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
    public void flatMap(String kafkaMessageValue, Collector<ListingUpdate> out) throws Exception {
        // Record that we consumed a message from Kafka
        messagesConsumed.inc();
        
        try {
            // Parse the Kafka message wrapper
            KafkaMessageWrapper wrapper = objectMapper.readValue(kafkaMessageValue, KafkaMessageWrapper.class);
            
            if (wrapper == null) {
                log.error("Parsed Kafka message wrapper is null. Raw message = {}", kafkaMessageValue);
                failedParses.inc();
                return;
            }
            
            if (wrapper.getData() == null) {
                log.error("Kafka message wrapper data field is null. Raw message = {}", kafkaMessageValue);
                failedParses.inc();
                return;
            }
            
            // Extract and parse the original WebSocket data
            String dataJson = objectMapper.writeValueAsString(wrapper.getData());
            List<ListingUpdate> updates = objectMapper.readValue(dataJson, new TypeReference<List<ListingUpdate>>() {});
            
            if (updates == null) {
                log.error("Parsed WebSocket data returned null list. Wrapper data = {}, Raw message = {}", 
                         wrapper.getData(), kafkaMessageValue);
                failedParses.inc();
                return;
            }
            
            // Emit each ListingUpdate
            for (ListingUpdate update : updates) {
                if (update == null) {
                    log.error("Parsed WebSocket data contains null element. Wrapper data = {}, Raw message = {}", 
                             wrapper.getData(), kafkaMessageValue);
                } else {
                    out.collect(update);
                }
            }
            
            successfulParses.inc();
            
        } catch (Exception e) {
            log.error("Failed to parse Kafka message. Error = {}, Raw message = {}", e.getMessage(), kafkaMessageValue, e);
            failedParses.inc();
        }
    }
}