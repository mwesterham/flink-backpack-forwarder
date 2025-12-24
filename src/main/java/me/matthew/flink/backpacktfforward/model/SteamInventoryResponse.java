package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Represents the response from Steam Web API inventory calls.
 * Maps to the GetPlayerItems API response structure.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SteamInventoryResponse {
    private SteamResult result;
    
    /**
     * Represents the result section of the Steam API response.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SteamResult {
        private int status;                     // API call status (1 = success)
        private List<InventoryItem> items;      // List of inventory items
        @JsonProperty("num_backpack_slots")
        private Integer numBackpackSlots;       // Total backpack slots
    }
}