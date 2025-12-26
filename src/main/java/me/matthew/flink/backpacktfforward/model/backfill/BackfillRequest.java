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
 * Extended to support multiple request types with optional parameters.
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
    private Integer itemDefindex;
    
    /**
     * Item quality ID identifying the quality/rarity of the item
     */
    @JsonProperty("item_quality_id")
    private Integer itemQualityId;
    
    /**
     * Market name for the item (retrieved from database during processing)
     * This field is populated during processing and not part of the Kafka message
     */
    @JsonIgnore
    private String marketName;
    
    /**
     * Type of backfill request to perform. Defaults to FULL for backward compatibility.
     * Optional field that determines the processing strategy and filtering behavior.
     */
    @JsonProperty("request_type")
    private BackfillRequestType requestType;
    
    /**
     * Specific listing ID for SINGLE_ID request types.
     * When present, only this specific listing will be processed.
     */
    @JsonProperty("listing_id")
    private String listingId;
    
    /**
     * Maximum inventory size threshold for INVENTORY_FILTERED request types.
     * Users with more than this number of matching items will be skipped.
     */
    @JsonProperty("max_inventory_size")
    private Integer maxInventorySize;
}