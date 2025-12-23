package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Model class representing the response from the backpack.tf snapshot API.
 * Maps the JSON structure returned by the API to Java objects.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackpackTfApiResponse {
    
    /**
     * Array of listings returned by the API
     */
    private List<ApiListing> listings;
    
    /**
     * Steam application ID (typically 440 for TF2)
     */
    private int appid;
    
    /**
     * The SKU (market name) that was requested
     */
    private String sku;
    
    /**
     * Timestamp when the response was created
     */
    private long createdAt;
    
    /**
     * Individual listing data from the API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiListing {
        
        /**
         * Steam ID of the user who created the listing
         */
        private String steamid;
        
        /**
         * Number of offers on this listing
         */
        private int offers;
        
        /**
         * Whether this is a buyout listing (0 or 1)
         */
        private int buyout;
        
        /**
         * Additional details/description for the listing
         */
        private String details;
        
        /**
         * Intent of the listing (buy/sell)
         */
        private String intent;
        
        /**
         * Timestamp when the listing was created
         */
        private long timestamp;
        
        /**
         * Price of the listing
         */
        private double price;
        
        /**
         * Item information for the listing
         */
        private ApiItem item;
        
        /**
         * Currency information (keys, metal, etc.)
         */
        private ApiCurrencies currencies;
        
        /**
         * Timestamp when the listing was last bumped (optional)
         */
        private Long bump;
        
        /**
         * User agent information (optional)
         */
        private ApiUserAgent userAgent;
    }
    
    /**
     * Item information from the API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiItem {
        
        /**
         * Item definition index
         */
        private int defindex;
        
        /**
         * Item quality ID
         */
        private int quality;
        
        /**
         * Item attributes (optional, can be complex structure)
         */
        private Object attributes;
        
        /**
         * Quantity of the item
         */
        private String quantity;
    }
    
    /**
     * Currency information from the API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiCurrencies {
        
        /**
         * Number of keys in the price
         */
        private Integer keys;
        
        /**
         * Amount of metal in the price
         */
        private Double metal;
    }
    
    /**
     * User agent information from the API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiUserAgent {
        
        /**
         * Timestamp of last pulse from the user's client
         */
        private Long lastPulse;
        
        /**
         * Client name/identifier
         */
        private String client;
    }
}