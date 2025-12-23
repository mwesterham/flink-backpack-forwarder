package me.matthew.flink.backpacktfforward.processor;

import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Flink processor that handles backfill requests by fetching data from the backpack.tf API
 * and emitting ListingUpdate events for both new/updated listings and stale data deletion.
 * 
 * This processor:
 * 1. Uses DatabaseHelper to query for market_name using item identifiers
 * 2. Calls the backpack.tf API to fetch current listings
 * 3. Maps API response to ListingUpdate objects
 * 4. Uses DatabaseHelper to identify stale listings and emits delete events
 */
@Slf4j
public class BackfillProcessor extends RichFlatMapFunction<BackfillRequest, ListingUpdate> {
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    private transient DatabaseHelper databaseHelper;
    private transient BackpackTfApiClient apiClient;
    
    /**
     * Creates a new BackfillProcessor with database connection parameters.
     * 
     * @param jdbcUrl Database JDBC URL
     * @param username Database username
     * @param password Database password
     */
    public BackfillProcessor(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize retry policy using existing SqlRetryMetrics patterns
        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(),
                "backfill_db_retries"
        );
        RetryPolicy<Object> retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);
        
        // Initialize database helper with existing connection patterns
        databaseHelper = new DatabaseHelper(jdbcUrl, username, password, retryPolicy);
        
        // Initialize API client
        apiClient = new BackpackTfApiClient();
        
        log.info("BackfillProcessor initialized with DatabaseHelper and API client");
    }
    
    @Override
    public void flatMap(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        log.debug("Processing backfill request for item_defindex={}, item_quality_id={}", 
                request.getItemDefindex(), request.getItemQualityId());
        
        try {
            // Step 1: Query database for market_name using DatabaseHelper
            String marketName = null;
            try {
                marketName = databaseHelper.getMarketName(request.getItemDefindex(), request.getItemQualityId());
            } catch (Exception e) {
                log.error("Database error while querying market_name for item_defindex={}, item_quality_id={}: {}. " +
                         "Skipping backfill request to prevent job failure.", 
                         request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
                return; // Skip this request but continue processing others
            }
            
            if (marketName == null) {
                log.warn("No market_name found for item_defindex={}, item_quality_id={}. " +
                        "This may indicate missing reference data in the database.", 
                        request.getItemDefindex(), request.getItemQualityId());
                return;
            }
            
            log.debug("Found market_name: {} for item_defindex={}, item_quality_id={}", 
                    marketName, request.getItemDefindex(), request.getItemQualityId());
            
            // Step 2: Call backpack.tf API with error handling
            BackpackTfApiResponse apiResponse = null;
            try {
                apiResponse = apiClient.fetchSnapshot(marketName, 440);
            } catch (Exception e) {
                log.error("API error while fetching snapshot for market_name={}, item_defindex={}, item_quality_id={}: {}. " +
                         "Skipping entire backfill request due to API failure.", 
                         marketName, request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
                
                // Complete no-op when API fails - skip both API processing and stale data deletion
                return;
            }
            
            if (apiResponse == null) {
                log.warn("API returned null response for market_name: {}. Skipping entire backfill request.", marketName);
                // Complete no-op when API returns null
                return;
            }
            
            if (apiResponse.getListings() == null || apiResponse.getListings().isEmpty()) {
                log.info("No listings returned from API for market_name: {}. Skipping entire backfill request.", marketName);
                // Complete no-op when no listings are returned
                return;
            }
            
            log.debug("API returned {} listings for market_name: {}", 
                    apiResponse.getListings().size(), marketName);
            
            // Step 3: Process API listings and emit update events with error handling
            List<String> apiSteamIds = new ArrayList<>();
            for (BackpackTfApiResponse.ApiListing apiListing : apiResponse.getListings()) {
                try {
                    ListingUpdate updateEvent = mapApiListingToListingUpdate(apiListing, marketName);
                    out.collect(updateEvent);
                    apiSteamIds.add(apiListing.getSteamid());
                } catch (Exception mappingException) {
                    log.error("Error mapping API listing to ListingUpdate for steamid={}, market_name={}: {}. " +
                             "Skipping this listing but continuing with others.", 
                             apiListing != null ? apiListing.getSteamid() : "null", 
                             marketName, mappingException.getMessage(), mappingException);
                    // Continue processing other listings
                }
            }
            
            // Step 4: Handle stale data deletion using DatabaseHelper with error handling
            try {
                handleStaleDataDeletion(request.getItemDefindex(), request.getItemQualityId(), 
                        apiSteamIds, out);
            } catch (Exception staleDataException) {
                log.error("Database error during stale data deletion for item_defindex={}, item_quality_id={}: {}. " +
                         "Stale data may not be properly cleaned up for this request.", 
                         request.getItemDefindex(), request.getItemQualityId(), staleDataException.getMessage(), staleDataException);
                // Don't rethrow - we've already processed the API data successfully
            }
            
        } catch (Exception e) {
            log.error("Unexpected error processing backfill request for item_defindex={}, item_quality_id={}: {}. " +
                     "This request will be skipped to prevent job failure.", 
                     request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
            // Don't rethrow - continue processing other requests to maintain job stability
        }
    }
    
    /**
     * Queries existing listings using DatabaseHelper and identifies stale listings that should be deleted.
     * 
     * @param itemDefindex Item definition index
     * @param itemQualityId Item quality ID
     * @param apiSteamIds List of Steam IDs from the API response
     * @param out Collector to emit delete events
     * @throws Exception if database query fails
     */
    private void handleStaleDataDeletion(int itemDefindex, int itemQualityId, 
            List<String> apiSteamIds, Collector<ListingUpdate> out) throws Exception {
        
        List<DatabaseHelper.ExistingListing> existingListings;
        try {
            existingListings = databaseHelper.getExistingListings(itemDefindex, itemQualityId);
        } catch (Exception e) {
            log.error("Database error while querying existing listings for item_defindex={}, item_quality_id={}: {}. " +
                     "Stale data detection will be skipped for this request.", 
                     itemDefindex, itemQualityId, e.getMessage(), e);
            throw e; // Rethrow to let caller handle appropriately
        }
        
        if (existingListings == null || existingListings.isEmpty()) {
            log.debug("No existing listings found in database for item_defindex={}, item_quality_id={}", 
                     itemDefindex, itemQualityId);
            return;
        }
        
        Set<String> apiSteamIdSet = new HashSet<>(apiSteamIds);
        int staleListingsCount = 0;
        
        for (DatabaseHelper.ExistingListing existingListing : existingListings) {
            try {
                // If this listing's steamid is not in the API response, it's stale
                if (!apiSteamIdSet.contains(existingListing.getSteamid())) {
                    ListingUpdate deleteEvent = createDeleteEvent(existingListing.getId(), existingListing.getSteamid());
                    out.collect(deleteEvent);
                    staleListingsCount++;
                    log.debug("Emitting delete event for stale listing: id={}, steamid={}", 
                            existingListing.getId(), existingListing.getSteamid());
                }
            } catch (Exception e) {
                log.error("Error creating delete event for stale listing id={}, steamid={}: {}. " +
                         "This stale listing will not be deleted.", 
                         existingListing.getId(), existingListing.getSteamid(), e.getMessage(), e);
                // Continue processing other stale listings
            }
        }
        
        if (staleListingsCount > 0) {
            log.info("Identified {} stale listings for deletion for item_defindex={}, item_quality_id={}", 
                    staleListingsCount, itemDefindex, itemQualityId);
        }
    }
    
    /**
     * Creates a ListingUpdate event for deleting a stale listing.
     * 
     * @param listingId The ID of the listing to delete
     * @param steamId The Steam ID associated with the listing
     * @return ListingUpdate with event="listing-delete"
     */
    private ListingUpdate createDeleteEvent(String listingId, String steamId) {
        ListingUpdate deleteEvent = new ListingUpdate();
        deleteEvent.setEvent("listing-delete");
        deleteEvent.setId(UUID.randomUUID().toString());
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId(listingId);
        payload.setSteamid(steamId);
        
        deleteEvent.setPayload(payload);
        
        return deleteEvent;
    }
    
    /**
     * Maps an API listing to a ListingUpdate object with event="listing-update".
     * 
     * @param apiListing The API listing to map
     * @param marketName The market name for the item
     * @return ListingUpdate object ready for database persistence
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    private ListingUpdate mapApiListingToListingUpdate(BackpackTfApiResponse.ApiListing apiListing, 
            String marketName) {
        
        if (apiListing == null) {
            throw new IllegalArgumentException("API listing cannot be null");
        }
        
        if (apiListing.getSteamid() == null || apiListing.getSteamid().trim().isEmpty()) {
            throw new IllegalArgumentException("API listing must have a valid steamid");
        }
        
        if (marketName == null || marketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Market name cannot be null or empty");
        }
        
        try {
            ListingUpdate listingUpdate = new ListingUpdate();
            listingUpdate.setEvent("listing-update");
            listingUpdate.setId(UUID.randomUUID().toString());
            
            ListingUpdate.Payload payload = new ListingUpdate.Payload();
            
            // Map basic listing information
            // Try to get existing listing ID from database, generate if not found
            String listingId = getOrGenerateListingId(apiListing, marketName);
            payload.setId(listingId);
            payload.setSteamid(apiListing.getSteamid());
            payload.setAppid(440); // TF2 app ID
            payload.setIntent(apiListing.getIntent() != null ? apiListing.getIntent() : "unknown");
            payload.setDetails(apiListing.getDetails());
            
            // Map timestamps - API uses seconds, ListingUpdate expects seconds
            payload.setListedAt(apiListing.getTimestamp());
            payload.setBumpedAt(apiListing.getBump() != null ? apiListing.getBump() : apiListing.getTimestamp());
            
            // Map currencies with null safety
            if (apiListing.getCurrencies() != null) {
                ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
                currencies.setKeys(apiListing.getCurrencies().getKeys());
                currencies.setMetal(apiListing.getCurrencies().getMetal());
                payload.setCurrencies(currencies);
            }
            
            // Map item information with null safety
            if (apiListing.getItem() != null) {
                ListingUpdate.Item item = new ListingUpdate.Item();
                item.setAppid(440);
                item.setDefindex(apiListing.getItem().getDefindex());
                item.setMarketName(marketName);
                
                // Map item quality with null safety
                ListingUpdate.Quality quality = new ListingUpdate.Quality();
                quality.setId(apiListing.getItem().getQuality());
                item.setQuality(quality);
                
                payload.setItem(item);
            } else {
                log.warn("API listing has null item information for steamid={}, using defaults", apiListing.getSteamid());
                // Create minimal item information
                ListingUpdate.Item item = new ListingUpdate.Item();
                item.setAppid(440);
                item.setMarketName(marketName);
                payload.setItem(item);
            }
            
            // Map user agent information with null safety
            if (apiListing.getUserAgent() != null) {
                ListingUpdate.UserAgent userAgent = new ListingUpdate.UserAgent();
                userAgent.setClient(apiListing.getUserAgent().getClient());
                userAgent.setLastPulse(apiListing.getUserAgent().getLastPulse() != null ? 
                        apiListing.getUserAgent().getLastPulse() : 0);
                payload.setUserAgent(userAgent);
            }
            
            // Set default values for required fields
            payload.setStatus("active"); // Default status for API listings
            payload.setSource("backpack.tf"); // Identify source as API
            
            listingUpdate.setPayload(payload);
            
            return listingUpdate;
            
        } catch (Exception e) {
            log.error("Error mapping API listing to ListingUpdate for steamid={}, market_name={}: {}", 
                     apiListing.getSteamid(), marketName, e.getMessage(), e);
            throw new RuntimeException("Failed to map API listing to ListingUpdate", e);
        }
    }
    
    /**
     * Gets the real listing ID from database by querying market_name/steamId combination,
     * or generates a new one if it doesn't exist.
     * 
     * @param apiListing The API listing
     * @param marketName The market name for the item
     * @return Existing listing ID from database or generated ID if not found
     */
    private String getOrGenerateListingId(BackpackTfApiResponse.ApiListing apiListing, String marketName) {
        try {
            // First try to get existing listing ID from database
            String existingId = databaseHelper.getExistingListingId(marketName, apiListing.getSteamid());
            if (existingId != null) {
                log.debug("Using existing listing ID: {} for market_name={}, steamid={}", 
                        existingId, marketName, apiListing.getSteamid());
                return existingId;
            }
            
            // If no existing ID found, generate a new one
            String generatedId = generateListingId(apiListing, marketName);
            log.debug("Generated new listing ID: {} for market_name={}, steamid={}", 
                    generatedId, marketName, apiListing.getSteamid());
            return generatedId;
            
        } catch (Exception e) {
            log.warn("Error querying existing listing ID for market_name={}, steamid={}: {}. " +
                    "Falling back to generated ID.", marketName, apiListing.getSteamid(), e.getMessage());
            // Fallback to generated ID if database query fails
            return generateListingId(apiListing, marketName);
        }
    }
    
    /**
     * Generates a unique listing ID for API listings since the API doesn't provide IDs.
     * Uses a combination of steamid, defindex, quality, and timestamp to create a unique identifier.
     * 
     * @param apiListing The API listing
     * @param marketName The market name for the item
     * @return A unique listing ID
     */
    private String generateListingId(BackpackTfApiResponse.ApiListing apiListing, String marketName) {
        // Create a deterministic ID based on listing characteristics
        // This ensures the same listing gets the same ID across multiple API calls
        String baseId = String.format("%s_%d_%d_%d", 
                apiListing.getSteamid(),
                apiListing.getItem() != null ? apiListing.getItem().getDefindex() : 0,
                apiListing.getItem() != null ? apiListing.getItem().getQuality() : 0,
                apiListing.getTimestamp());
        
        // Use a hash to create a shorter, more manageable ID
        return "api_" + Math.abs(baseId.hashCode());
    }
    
    @Override
    public void close() throws Exception {
        // DatabaseHelper manages its own connections, no cleanup needed
        super.close();
    }
}