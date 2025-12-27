package me.matthew.flink.backpacktfforward.util;

import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.SourceOfTruthListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class for mapping SourceOfTruthListing objects to ListingUpdate objects.
 * Handles the transformation of data from the combined source of truth (BackpackTF API + Steam inventory + getListing detail)
 * into the format expected by the existing sink infrastructure.
 */
public class ListingUpdateMapper {
    
    private static final Logger log = LoggerFactory.getLogger(ListingUpdateMapper.class);
    
    /**
     * Maps a SourceOfTruthListing to a ListingUpdate object for database updates.
     * Uses the actual listing ID from the getListing API response as the primary key.
     * Handles both buy and sell intents with event="listing-update".
     * 
     * @param sourceOfTruth the source of truth listing containing all data
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the source of truth doesn't have minimum required data
     */
    public static ListingUpdate mapToListingUpdate(SourceOfTruthListing sourceOfTruth) {
        return mapToListingUpdate(sourceOfTruth, null);
    }
    
    /**
     * Maps a SourceOfTruthListing to a ListingUpdate object for database updates with generation timestamp.
     * Uses the actual listing ID from the getListing API response as the primary key.
     * Handles both buy and sell intents with event="listing-update".
     * 
     * @param sourceOfTruth the source of truth listing containing all data
     * @param generationTimestamp the timestamp when this backfill data was generated (null for real-time data)
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the source of truth doesn't have minimum required data
     */
    public static ListingUpdate mapToListingUpdate(SourceOfTruthListing sourceOfTruth, Long generationTimestamp) {
        if (sourceOfTruth == null) {
            throw new IllegalArgumentException("SourceOfTruthListing cannot be null");
        }
        
        if (!sourceOfTruth.hasMinimumData()) {
            throw new IllegalArgumentException("SourceOfTruthListing must have minimum data (listing detail with ID): " 
                    + sourceOfTruth.getMissingDataDescription());
        }
        
        return mapFromListingDetail(sourceOfTruth.getListingDetail(), generationTimestamp);
    }
    
    /**
     * Maps a BackpackTfListingDetail directly to a ListingUpdate object.
     * This is the simplified approach that just maps the fields directly.
     * 
     * @param listingDetail the BackpackTF listing detail to map
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the listing detail is null or missing required data
     */
    public static ListingUpdate mapFromListingDetail(BackpackTfListingDetail listingDetail) {
        return mapFromListingDetail(listingDetail, null);
    }
    
    /**
     * Maps a BackpackTfListingDetail directly to a ListingUpdate object with generation timestamp.
     * This is the simplified approach that just maps the fields directly.
     * 
     * @param listingDetail the BackpackTF listing detail to map
     * @param generationTimestamp the timestamp when this backfill data was generated (null for real-time data)
     * @return a ListingUpdate object ready for processing by sinks
     * @throws IllegalArgumentException if the listing detail is null or missing required data
     */
    public static ListingUpdate mapFromListingDetail(BackpackTfListingDetail listingDetail, Long generationTimestamp) {
        if (listingDetail == null) {
            throw new IllegalArgumentException("BackpackTfListingDetail cannot be null");
        }
        
        if (listingDetail.getId() == null || listingDetail.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("BackpackTfListingDetail must have a valid ID");
        }
        
        log.debug("Mapping BackpackTfListingDetail to ListingUpdate for listing ID: {} with generation_timestamp: {}", 
                listingDetail.getId(), generationTimestamp);
        
        ListingUpdate update = new ListingUpdate();
        update.setId(listingDetail.getId());
        update.setEvent("listing-update");
        update.setGenerationTimestamp(generationTimestamp);
        
        // Create and populate the payload - simple direct mapping
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        
        // Direct field mapping - fail explicitly if required fields are missing
        payload.setId(requireNonNull(listingDetail.getId(), "Listing ID"));
        payload.setSteamid(requireNonNull(listingDetail.getSteamid(), "Steam ID"));
        payload.setAppid(listingDetail.getAppid());
        payload.setTradeOffersPreferred(listingDetail.getTradeOffersPreferred());
        payload.setBuyoutOnly(listingDetail.getBuyoutOnly());
        payload.setDetails(listingDetail.getDetails());
        payload.setListedAt(requireNonNull(listingDetail.getListedAt(), "Listed at timestamp"));
        payload.setBumpedAt(requireNonNull(listingDetail.getBumpedAt(), "Bumped at timestamp"));
        payload.setIntent(requireNonNull(listingDetail.getIntent(), "Intent"));
        payload.setCount(requireNonNull(listingDetail.getCount(), "Count"));
        payload.setStatus(listingDetail.getStatus());
        payload.setSource(listingDetail.getSource());
        
        // Map complex nested objects
        payload.setCurrencies(mapCurrencies(listingDetail.getCurrencies()));
        payload.setValue(mapValue(listingDetail.getValue()));
        payload.setItem(mapItem(listingDetail.getItem()));
        payload.setUser(mapUser(listingDetail.getUser()));
        
        update.setPayload(payload);
        
        log.debug("Successfully mapped BackpackTfListingDetail to ListingUpdate for listing ID: {}, intent: {}", 
                listingDetail.getId(), listingDetail.getIntent());
        
        return update;
    }
    
    /**
     * Creates a ListingUpdate object for deleting a stale listing.
     * Uses event="listing-delete" and includes sufficient identification data.
     * 
     * @param listingId the ID of the listing to delete
     * @param steamId the Steam ID associated with the listing
     * @return a ListingUpdate object for deletion
     */
    public static ListingUpdate createDeleteEvent(String listingId, String steamId) {
        return createDeleteEvent(listingId, steamId, null);
    }
    
    /**
     * Creates a ListingUpdate object for deleting a stale listing with generation timestamp.
     * Uses event="listing-delete" and includes sufficient identification data.
     * 
     * @param listingId the ID of the listing to delete
     * @param steamId the Steam ID associated with the listing
     * @param generationTimestamp the timestamp when this backfill data was generated (null for real-time data)
     * @return a ListingUpdate object for deletion
     */
    public static ListingUpdate createDeleteEvent(String listingId, String steamId, Long generationTimestamp) {
        if (listingId == null || listingId.trim().isEmpty()) {
            throw new IllegalArgumentException("Listing ID cannot be null or empty for delete event");
        }
        
        log.debug("Creating delete event for listing ID: {}, Steam ID: {} with generation_timestamp: {}", 
                listingId, steamId, generationTimestamp);
        
        ListingUpdate deleteUpdate = new ListingUpdate();
        deleteUpdate.setId(listingId);
        deleteUpdate.setEvent("listing-delete");
        deleteUpdate.setGenerationTimestamp(generationTimestamp);
        
        // Create minimal payload with identification data
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId(listingId);
        payload.setSteamid(steamId);
        
        deleteUpdate.setPayload(payload);
        
        return deleteUpdate;
    }
    
    /**
     * Helper method to explicitly fail when required fields are null.
     * 
     * @param value the value to check
     * @param fieldName the name of the field for error messages
     * @param <T> the type of the value
     * @return the value if not null
     * @throws IllegalArgumentException if the value is null
     */
    private static <T> T requireNonNull(T value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("Required field '" + fieldName + "' cannot be null");
        }
        return value;
    }
    
    /**
     * Maps currency information from BackpackTfListingDetail to ListingUpdate format.
     */
    private static ListingUpdate.Currencies mapCurrencies(Map<String, Object> currencies) {
        if (currencies == null) {
            return null;
        }
        
        ListingUpdate.Currencies mappedCurrencies = new ListingUpdate.Currencies();
        
        // Extract metal value
        Object metalValue = currencies.get("metal");
        if (metalValue instanceof Number) {
            mappedCurrencies.setMetal(((Number) metalValue).doubleValue());
        }
        
        // Extract keys value
        Object keysValue = currencies.get("keys");
        if (keysValue instanceof Number) {
            long keysLong = ((Number) keysValue).longValue();
            mappedCurrencies.setKeys(keysLong);
        }
        
        return mappedCurrencies;
    }
    
    /**
     * Maps value information from BackpackTfListingDetail to ListingUpdate format.
     */
    private static ListingUpdate.Value mapValue(BackpackTfListingDetail.ApiValue apiValue) {
        if (apiValue == null) {
            return null;
        }
        
        ListingUpdate.Value mappedValue = new ListingUpdate.Value();
        mappedValue.setRaw(requireNonNull(apiValue.getRaw(), "Value raw"));
        mappedValue.setShortStr(apiValue.getShortStr());
        mappedValue.setLongStr(apiValue.getLongStr());
        
        // Note: Steam, community, and suggested prices are not available in getListing response
        // These would need to be populated from other sources if required
        
        return mappedValue;
    }
    
    /**
     * Maps item information from BackpackTfListingDetail to ListingUpdate format.
     */
    private static ListingUpdate.Item mapItem(BackpackTfListingDetail.ApiItemDetail apiItem) {
        if (apiItem == null) {
            return null;
        }
        
        ListingUpdate.Item mappedItem = new ListingUpdate.Item();
        
        // Basic item information - fail explicitly for required fields
        mappedItem.setAppid(requireNonNull(apiItem.getAppid(), "Item appid"));
        mappedItem.setBaseName(apiItem.getBaseName());
        mappedItem.setDefindex(requireNonNull(apiItem.getDefindex(), "Item defindex"));
        mappedItem.setId(apiItem.getId());
        mappedItem.setImageUrl(apiItem.getImageUrl());
        mappedItem.setMarketName(apiItem.getMarketName());
        mappedItem.setName(apiItem.getName());
        mappedItem.setOrigin(mapOrigin(apiItem.getOrigin()));
        mappedItem.setOriginalId(apiItem.getOriginalId());
        mappedItem.setSummary(apiItem.getSummary());
        mappedItem.setSlot(apiItem.getSlot());
        mappedItem.setTradable(apiItem.getTradable());
        mappedItem.setCraftable(apiItem.getCraftable());
        
        // Quality information
        if (apiItem.getQuality() != null) {
            ListingUpdate.Quality quality = new ListingUpdate.Quality();
            quality.setId(requireNonNull(apiItem.getQuality().getId(), "Quality id"));
            quality.setName(apiItem.getQuality().getName());
            quality.setColor(apiItem.getQuality().getColor());
            mappedItem.setQuality(quality);
        }
        
        // Class information
        mappedItem.setClazz(apiItem.getClazz());
        
        // Note: Some fields like price, particle, and priceindex are not directly available
        // in the getListing response and would need to be populated from other sources if required
        
        return mappedItem;
    }
    
    /**
     * Maps user information from BackpackTfListingDetail to ListingUpdate format.
     */
    private static ListingUpdate.User mapUser(BackpackTfListingDetail.ApiUser apiUser) {
        if (apiUser == null) {
            return null;
        }
        
        ListingUpdate.User mappedUser = new ListingUpdate.User();
        
        mappedUser.setId(apiUser.getId());
        mappedUser.setName(apiUser.getName());
        mappedUser.setAvatar(apiUser.getAvatar());
        mappedUser.setAvatarFull(apiUser.getAvatarFull());
        mappedUser.setPremium(apiUser.getPremium());
        mappedUser.setOnline(apiUser.getOnline());
        mappedUser.setBanned(apiUser.getBanned());
        mappedUser.setCustomNameStyle(apiUser.getCustomNameStyle());
        mappedUser.setClazz(apiUser.getClazz());
        mappedUser.setStyle(apiUser.getStyle());
        mappedUser.setRole(apiUser.getRole());
        mappedUser.setTradeOfferUrl(apiUser.getTradeOfferUrl());
        mappedUser.setBans(apiUser.getBans());
        
        return mappedUser;
    }
    
    /**
     * Maps origin information from Object to ListingUpdate.Origin format.
     * Handles the case where origin might be null or a Map with id/name fields.
     */
    private static ListingUpdate.Origin mapOrigin(Object originObj) {
        if (originObj == null) {
            return null;
        }
        
        // If it's already a Map, extract id and name
        if (originObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> originMap = (Map<String, Object>) originObj;
            
            ListingUpdate.Origin origin = new ListingUpdate.Origin();
            
            Object idObj = originMap.get("id");
            if (idObj instanceof Number) {
                origin.setId(((Number) idObj).intValue());
            }
            
            Object nameObj = originMap.get("name");
            if (nameObj instanceof String) {
                origin.setName((String) nameObj);
            }
            
            return origin;
        }
        
        // If it's some other type, log and return null
        log.warn("Unexpected origin type: {}, returning null", originObj.getClass().getSimpleName());
        return null;
    }
}