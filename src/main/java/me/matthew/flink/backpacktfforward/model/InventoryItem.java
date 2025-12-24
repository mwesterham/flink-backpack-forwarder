package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Represents an item found in a Steam user's inventory.
 * Maps to the Steam Web API GetPlayerItems response structure.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class InventoryItem {
    private long id;                    // Steam item ID
    @JsonProperty("original_id")
    private long originalId;            // Original item ID
    private int defindex;               // Item definition index
    private int level;                  // Item level
    private int quality;                // Item quality ID
    private long inventory;             // Inventory position
    private int quantity;               // Item quantity
    private int origin;                 // Item origin
    @JsonProperty("flag_cannot_trade")
    private Boolean flagCannotTrade;    // Trade restriction flag
    @JsonProperty("flag_cannot_craft")
    private Boolean flagCannotCraft;    // Craft restriction flag
    private List<ItemAttribute> attributes; // Item attributes
    
    /**
     * Represents an attribute of an inventory item.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ItemAttribute {
        private int defindex;           // Attribute definition index
        private long value;             // Attribute value (integer)
        @JsonProperty("float_value")
        private Float floatValue;       // Attribute value (float)
    }
}