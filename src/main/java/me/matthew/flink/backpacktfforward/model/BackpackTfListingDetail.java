package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Model class representing the detailed response from the backpack.tf getListing API.
 * Maps the complete JSON structure returned by the getListing endpoint to Java objects.
 * This provides the actual listing ID and complete item data needed for accurate updates.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackpackTfListingDetail {
    
    /**
     * The actual listing ID - this is the key field we need for accurate updates
     */
    private String id;
    
    /**
     * Steam ID of the user who created the listing
     */
    private String steamid;
    
    /**
     * Steam application ID (typically 440 for TF2)
     */
    private int appid;
    
    /**
     * Currency information for the listing price
     */
    private Map<String, Object> currencies;
    
    /**
     * Value information including raw price and formatted strings
     */
    private ApiValue value;
    
    /**
     * Whether trade offers are preferred for this listing
     */
    @JsonProperty("tradeOffersPreferred")
    private Boolean tradeOffersPreferred;
    
    /**
     * Whether this is a buyout-only listing
     */
    private Boolean buyoutOnly;
    
    /**
     * Additional details/description provided by the user
     */
    private String details;
    
    /**
     * Timestamp when the listing was created
     */
    private Long listedAt;
    
    /**
     * Timestamp when the listing was last bumped
     */
    private Long bumpedAt;
    
    /**
     * Intent of the listing (buy/sell)
     */
    private String intent;
    
    /**
     * Number of items in the listing
     */
    private Integer count;
    
    /**
     * Status of the listing (active, inactive, etc.)
     */
    private String status;
    
    /**
     * Source of the listing (user, automatic, etc.)
     */
    private String source;
    
    /**
     * Detailed item information
     */
    private ApiItemDetail item;
    
    /**
     * User information for the listing creator
     */
    private ApiUser user;
    
    /**
     * Value information from the getListing API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiValue {
        
        /**
         * Raw numeric value of the price
         */
        private Double raw;
        
        /**
         * Short formatted string representation of the price
         */
        @JsonProperty("short")
        private String shortStr;
        
        /**
         * Long formatted string representation of the price
         */
        @JsonProperty("long")
        private String longStr;
        
        /**
         * USD equivalent value
         */
        private Double usd;
    }
    
    /**
     * Detailed item information from the getListing API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiItemDetail {
        
        /**
         * Steam application ID
         */
        private Integer appid;
        
        /**
         * Base name of the item
         */
        private String baseName;
        
        /**
         * Item definition index
         */
        private Integer defindex;
        
        /**
         * Item ID from the API
         */
        private String id;
        
        /**
         * URL to the item's image
         */
        private String imageUrl;
        
        /**
         * Market name of the item
         */
        private String marketName;
        
        /**
         * Display name of the item
         */
        private String name;
        
        /**
         * Origin information (can be complex object)
         */
        private Object origin;
        
        /**
         * Original item ID
         */
        private String originalId;
        
        /**
         * Quality information for the item
         */
        private ApiQuality quality;
        
        /**
         * Summary description of the item
         */
        private String summary;
        
        /**
         * Price information (can be complex object)
         */
        private Object price;
        
        /**
         * Item level
         */
        private Integer level;
        
        /**
         * Classes that can use this item
         */
        @JsonProperty("class")
        private List<String> clazz;
        
        /**
         * Equipment slot for the item
         */
        private String slot;
        
        /**
         * Kill eater information (for strange items)
         */
        private List<Object> killEaters;
        
        /**
         * Whether the item is tradable
         */
        private Boolean tradable;
        
        /**
         * Whether the item is craftable
         */
        private Boolean craftable;
        
        /**
         * Tag information (can be complex object)
         */
        private Object tag;
    }
    
    /**
     * Quality information from the getListing API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiQuality {
        
        /**
         * Quality ID
         */
        private Integer id;
        
        /**
         * Quality name
         */
        private String name;
        
        /**
         * Quality color (hex color code)
         */
        private String color;
    }
    
    /**
     * User information from the getListing API response
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApiUser {
        
        /**
         * User's Steam ID
         */
        private String id;
        
        /**
         * User's display name
         */
        private String name;
        
        /**
         * URL to user's avatar (medium size)
         */
        private String avatar;
        
        /**
         * URL to user's avatar (full size)
         */
        private String avatarFull;
        
        /**
         * Whether the user has premium status
         */
        private Boolean premium;
        
        /**
         * Whether the user is currently online
         */
        private Boolean online;
        
        /**
         * Whether the user is banned
         */
        private Boolean banned;
        
        /**
         * Custom name styling
         */
        private String customNameStyle;
        
        /**
         * User class information
         */
        @JsonProperty("class")
        private String clazz;
        
        /**
         * User style information
         */
        private String style;
        
        /**
         * User role information (can be complex object)
         */
        private Object role;
        
        /**
         * Trade offer URL for the user
         */
        private String tradeOfferUrl;
        
        /**
         * Ban information (list of ban objects)
         */
        private List<Object> bans;
    }
}