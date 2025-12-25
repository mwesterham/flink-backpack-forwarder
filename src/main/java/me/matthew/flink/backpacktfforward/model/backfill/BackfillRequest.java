package me.matthew.flink.backpacktfforward.model.backfill;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * Model class for backfill request messages consumed from Kafka.
 * Contains item identifiers needed to trigger API-based data refresh.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackfillRequest {
    
    /**
     * Item definition index identifying the specific item type
     */
    @JsonProperty("item_defindex")
    private int itemDefindex;
    
    /**
     * Item quality ID identifying the quality/rarity of the item
     */
    @JsonProperty("item_quality_id")
    private int itemQualityId;
    
    /**
     * Market name for the item (retrieved from database during processing)
     * This field is populated during processing and not part of the Kafka message
     */
    @JsonIgnore
    private String marketName;
}