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
            String marketName = databaseHelper.getMarketName(request.getItemDefindex(), request.getItemQualityId());
            if (marketName == null) {
                log.warn("No market_name found for item_defindex={}, item_quality_id={}", 
                        request.getItemDefindex(), request.getItemQualityId());
                return;
            }
            
            log.debug("Found market_name: {} for item_defindex={}, item_quality_id={}", 
                    marketName, request.getItemDefindex(), request.getItemQualityId());
            
            // Step 2: Call backpack.tf API
            BackpackTfApiResponse apiResponse = apiClient.fetchSnapshot(marketName, 440);
            
            if (apiResponse.getListings() == null || apiResponse.getListings().isEmpty()) {
                log.info("No listings returned from API for market_name: {}", marketName);
                // Still need to check for stale data deletion
                handleStaleDataDeletion(request.getItemDefindex(), request.getItemQualityId(), 
                        new ArrayList<>(), out);
                return;
            }
            
            log.debug("API returned {} listings for market_name: {}", 
                    apiResponse.getListings().size(), marketName);
            
            // Step 3: Process API listings and emit update events
            List<String> apiSteamIds = new ArrayList<>();
            for (BackpackTfApiResponse.ApiListing apiListing : apiResponse.getListings()) {
                ListingUpdate updateEvent = mapApiListingToListingUpdate(apiListing, marketName);
                out.collect(updateEvent);
                apiSteamIds.add(apiListing.getSteamid());
            }
            
            // Step 4: Handle stale data deletion using DatabaseHelper
            handleStaleDataDeletion(request.getItemDefindex(), request.getItemQualityId(), 
                    apiSteamIds, out);
            
        } catch (Exception e) {
            log.error("Error processing backfill request for item_defindex={}, item_quality_id={}: {}", 
                    request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
            // Don't rethrow - continue processing other requests
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
        
        List<DatabaseHelper.ExistingListing> existingListings = 
                databaseHelper.getExistingListings(itemDefindex, itemQualityId);
        
        Set<String> apiSteamIdSet = new HashSet<>(apiSteamIds);
        
        for (DatabaseHelper.ExistingListing existingListing : existingListings) {
            // If this listing's steamid is not in the API response, it's stale
            if (!apiSteamIdSet.contains(existingListing.getSteamid())) {
                ListingUpdate deleteEvent = createDeleteEvent(existingListing.getId(), existingListing.getSteamid());
                out.collect(deleteEvent);
                log.debug("Emitting delete event for stale listing: id={}, steamid={}", 
                        existingListing.getId(), existingListing.getSteamid());
            }
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
     */
    private ListingUpdate mapApiListingToListingUpdate(BackpackTfApiResponse.ApiListing apiListing, 
            String marketName) {
        
        ListingUpdate listingUpdate = new ListingUpdate();
        listingUpdate.setEvent("listing-update");
        listingUpdate.setId(UUID.randomUUID().toString());
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        
        // Map basic listing information
        payload.setId(apiListing.getId());
        payload.setSteamid(apiListing.getSteamid());
        payload.setAppid(440); // TF2 app ID
        payload.setIntent(apiListing.getIntent());
        payload.setDetails(apiListing.getDetails());
        
        // Map timestamps - API uses seconds, ListingUpdate expects seconds
        payload.setListedAt(apiListing.getTimestamp());
        payload.setBumpedAt(apiListing.getBump() != null ? apiListing.getBump() : apiListing.getTimestamp());
        
        // Map currencies
        if (apiListing.getCurrencies() != null) {
            ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
            currencies.setKeys(apiListing.getCurrencies().getKeys());
            currencies.setMetal(apiListing.getCurrencies().getMetal());
            payload.setCurrencies(currencies);
        }
        
        // Map item information
        if (apiListing.getItem() != null) {
            ListingUpdate.Item item = new ListingUpdate.Item();
            item.setAppid(440);
            item.setDefindex(apiListing.getItem().getDefindex());
            item.setMarketName(marketName);
            
            // Map item quality
            ListingUpdate.Quality quality = new ListingUpdate.Quality();
            quality.setId(apiListing.getItem().getQuality());
            item.setQuality(quality);
            
            payload.setItem(item);
        }
        
        // Map user agent information
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
    }
    
    @Override
    public void close() throws Exception {
        // DatabaseHelper manages its own connections, no cleanup needed
        super.close();
    }
}