package me.matthew.flink.backpacktfforward.util;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.SourceOfTruthListing;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * Utility class for mapping between different listing data structures.
 * Handles conversion from API responses to internal ListingUpdate format.
 */
@Slf4j
public class ListingUpdateMapper {
    
    /**
     * Helper method to require non-null values with descriptive error messages.
     */
    private static <T> T requireNonNull(T obj, String fieldName) {
        return Objects.requireNonNull(obj, fieldName + " cannot be null");
    }
    
    /**
     * Maps a SourceOfTruthListing to a ListingUpdate object.
     * This handles the complex logic of determining whether to create an update or delete event.
     * 
     * @param sotListing the source of truth listing containing snapshot, inventory, and API data
     * @param generationTimestamp the timestamp when this backfill data was generated (null for real-time data)
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the listing data is invalid
     */
    public static ListingUpdate mapFromSourceOfTruth(SourceOfTruthListing sotListing, Long generationTimestamp) {
        if (sotListing == null) {
            throw new IllegalArgumentException("SourceOfTruthListing cannot be null");
        }
        
        if (!sotListing.hasMinimumData()) {
            throw new IllegalArgumentException("SourceOfTruthListing must have minimum data (listing detail with ID): " 
                    + sotListing.getMissingDataDescription());
        }
        
        return mapFromListingDetail(sotListing.getListingDetail(), generationTimestamp);
    }
    
    /**
     * Maps a ListingUpdate.Payload directly to a ListingUpdate object.
     * This is the simplified approach that just maps the fields directly.
     * 
     * @param listingDetail the listing payload to map
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the listing detail is null or missing required data
     */
    public static ListingUpdate mapFromListingDetail(ListingUpdate.Payload listingDetail) {
        return mapFromListingDetail(listingDetail, null);
    }
    
    /**
     * Maps a ListingUpdate.Payload directly to a ListingUpdate object with generation timestamp.
     * This is the simplified approach that just maps the fields directly.
     * 
     * @param listingDetail the listing payload to map
     * @param generationTimestamp the timestamp when this backfill data was generated (null for real-time data)
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the listing detail is null or missing required data
     */
    public static ListingUpdate mapFromListingDetail(ListingUpdate.Payload listingDetail, Long generationTimestamp) {
        if (listingDetail == null) {
            throw new IllegalArgumentException("ListingUpdate.Payload cannot be null");
        }
        
        if (listingDetail.id == null || listingDetail.id.trim().isEmpty()) {
            throw new IllegalArgumentException("ListingUpdate.Payload must have a valid ID");
        }
        
        log.debug("Mapping ListingUpdate.Payload to ListingUpdate for listing ID: {} with generation_timestamp: {}", 
                listingDetail.id, generationTimestamp);
        
        ListingUpdate update = new ListingUpdate();
        update.id = listingDetail.id;
        update.event = "listing-update";
        update.generationTimestamp = generationTimestamp;
        update.payload = listingDetail;
        
        log.debug("Successfully mapped ListingUpdate.Payload to ListingUpdate for listing ID: {}, intent: {}", 
                listingDetail.id, listingDetail.intent);
        
        return update;
    }
    
    /**
     * Creates a ListingUpdate object for deleting a stale listing.
     * Uses event="listing-delete" and includes sufficient identification data.
     * 
     * @param listingId the ID of the listing to delete
     * @param steamId the Steam ID associated with the listing
     * @param generationTimestamp the timestamp when this backfill data was generated (null for real-time data)
     * @return a ListingUpdate object with delete event
     */
    public static ListingUpdate createDeleteUpdate(String listingId, String steamId, Long generationTimestamp) {
        requireNonNull(listingId, "Listing ID");
        requireNonNull(steamId, "Steam ID");
        
        log.debug("Creating delete update for listing ID: {} with generation_timestamp: {}", listingId, generationTimestamp);
        
        ListingUpdate update = new ListingUpdate();
        update.id = listingId;
        update.event = "listing-delete";
        update.generationTimestamp = generationTimestamp;
        
        // Create minimal payload with identification data for delete events
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.id = listingId;
        payload.steamid = steamId;
        
        update.payload = payload;
        
        log.debug("Successfully created delete update for listing ID: {}", listingId);
        
        return update;
    }
}