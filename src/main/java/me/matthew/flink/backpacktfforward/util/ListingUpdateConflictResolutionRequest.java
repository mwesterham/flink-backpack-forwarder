package me.matthew.flink.backpacktfforward.util;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;

/**
 * Implementation of ConflictResolutionRequest that wraps a ListingUpdate object.
 * Provides access to all timestamp fields needed for conflict resolution.
 */
public class ListingUpdateConflictResolutionRequest implements ConflictResolutionRequest {
    
    private final ListingUpdate listingUpdate;
    
    public ListingUpdateConflictResolutionRequest(ListingUpdate listingUpdate) {
        this.listingUpdate = listingUpdate;
    }
    
    @Override
    public String getListingId() {
        return listingUpdate.getPayload() != null ? listingUpdate.getPayload().getId() : null;
    }
    
    @Override
    public Long getGenerationTimestamp() {
        return listingUpdate.getGenerationTimestamp();
    }
    
    @Override
    public Long getListedAt() {
        return listingUpdate.getPayload() != null ? listingUpdate.getPayload().getListedAt() : null;
    }
    
    @Override
    public Long getBumpedAt() {
        return listingUpdate.getPayload() != null ? listingUpdate.getPayload().getBumpedAt() : null;
    }
}