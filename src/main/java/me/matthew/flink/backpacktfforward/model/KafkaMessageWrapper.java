package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Wrapper class for Kafka messages containing WebSocket data.
 * Matches the JSON structure produced by the WebSocket-to-Kafka bridge service.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaMessageWrapper {
    
    /**
     * Original WebSocket payload containing ListingUpdate array
     */
    @JsonProperty("data")
    private Object data;
    
    /**
     * ISO timestamp from the bridge service
     */
    @JsonProperty("timestamp")
    private String timestamp;
    
    /**
     * Unique message identifier
     */
    @JsonProperty("messageId")
    private String messageId;
    
    /**
     * Source identifier, always "websocket" for WebSocket bridge messages
     */
    @JsonProperty("source")
    private String source;
}