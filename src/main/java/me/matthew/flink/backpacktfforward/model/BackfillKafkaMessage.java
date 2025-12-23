package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * Wrapper class for Kafka messages containing backfill request data.
 * Matches the JSON structure expected for backfill operations.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackfillKafkaMessage {
    
    /**
     * Backfill request data containing item identifiers
     */
    @JsonProperty("data")
    private BackfillRequest data;
    
    /**
     * ISO timestamp when the backfill request was created
     */
    @JsonProperty("timestamp")
    private String timestamp;
    
    /**
     * Unique message identifier for tracking
     */
    @JsonProperty("messageId")
    private String messageId;
    
    /**
     * Source identifier for backfill requests
     */
    @JsonProperty("source")
    private String source;
}