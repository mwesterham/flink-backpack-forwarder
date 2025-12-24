package me.matthew.flink.backpacktfforward.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * Represents a complete source of truth listing that combines data from multiple sources:
 * - BackpackTF API listing (basic market data)
 * - Steam inventory item (actual item data)
 * - BackpackTF getListing detail (complete listing information with actual ID)
 * 
 * This class provides convenience methods for accessing key data and handles cases
 * where some data may be missing.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SourceOfTruthListing {
    
    /**
     * Original API listing data from BackpackTF snapshot API
     */
    private BackpackTfApiResponse.ApiListing apiListing;
    
    /**
     * Matched Steam inventory item data
     */
    private InventoryItem inventoryItem;
    
    /**
     * Complete listing details from BackpackTF getListing API
     */
    private BackpackTfListingDetail listingDetail;
    
    /**
     * Gets the intent (buy/sell) from the API listing.
     * Falls back to listing detail if API listing is not available.
     * 
     * @return the intent string ("buy" or "sell"), or null if not available
     */
    public String getIntent() {
        if (apiListing != null && apiListing.getIntent() != null) {
            return apiListing.getIntent();
        }
        if (listingDetail != null && listingDetail.getIntent() != null) {
            return listingDetail.getIntent();
        }
        return null;
    }
    
    /**
     * Gets the actual listing ID from the getListing API response.
     * This is the authoritative ID that should be used for database operations.
     * 
     * @return the actual listing ID, or null if not available
     */
    public String getActualListingId() {
        if (listingDetail != null && listingDetail.getId() != null) {
            return listingDetail.getId();
        }
        return null;
    }
    
    /**
     * Gets the Steam ID from the API listing.
     * Falls back to listing detail if API listing is not available.
     * 
     * @return the Steam ID, or null if not available
     */
    public String getSteamId() {
        if (apiListing != null && apiListing.getSteamid() != null) {
            return apiListing.getSteamid();
        }
        if (listingDetail != null && listingDetail.getSteamid() != null) {
            return listingDetail.getSteamid();
        }
        return null;
    }
    
    /**
     * Gets the Steam item ID from the inventory item.
     * This is used for correlating with the Steam inventory.
     * 
     * @return the Steam item ID, or null if inventory item is not available
     */
    public Long getSteamItemId() {
        if (inventoryItem != null) {
            return inventoryItem.getId();
        }
        return null;
    }
    
    /**
     * Gets the defindex from the inventory item.
     * Falls back to API listing item if inventory item is not available.
     * 
     * @return the defindex, or null if not available
     */
    public Integer getDefindex() {
        if (inventoryItem != null) {
            return inventoryItem.getDefindex();
        }
        if (apiListing != null && apiListing.getItem() != null) {
            return apiListing.getItem().getDefindex();
        }
        return null;
    }
    
    /**
     * Gets the quality from the inventory item.
     * Falls back to API listing item if inventory item is not available.
     * 
     * @return the quality, or null if not available
     */
    public Integer getQuality() {
        if (inventoryItem != null) {
            return inventoryItem.getQuality();
        }
        if (apiListing != null && apiListing.getItem() != null) {
            return apiListing.getItem().getQuality();
        }
        return null;
    }
    
    /**
     * Checks if this source of truth listing has complete data.
     * Complete data means we have all three components: API listing, inventory item, and listing detail.
     * 
     * @return true if all components are present, false otherwise
     */
    public boolean isComplete() {
        return apiListing != null && inventoryItem != null && listingDetail != null;
    }
    
    /**
     * Checks if this source of truth listing has the minimum required data for processing.
     * Minimum data means we have at least the listing detail (which contains the actual ID).
     * 
     * @return true if listing detail is present, false otherwise
     */
    public boolean hasMinimumData() {
        return listingDetail != null && listingDetail.getId() != null;
    }
    
    /**
     * Gets a description of what data is missing from this source of truth listing.
     * Useful for debugging and logging.
     * 
     * @return a string describing missing components, or "Complete" if all data is present
     */
    public String getMissingDataDescription() {
        if (isComplete()) {
            return "Complete";
        }
        
        StringBuilder missing = new StringBuilder();
        if (apiListing == null) {
            missing.append("API listing, ");
        }
        if (inventoryItem == null) {
            missing.append("Inventory item, ");
        }
        if (listingDetail == null) {
            missing.append("Listing detail, ");
        }
        
        // Remove trailing comma and space
        if (missing.length() > 0) {
            missing.setLength(missing.length() - 2);
        }
        
        return "Missing: " + missing.toString();
    }
}