package me.matthew.flink.backpacktfforward.processor;

import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.client.SteamApi;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;
import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;
import me.matthew.flink.backpacktfforward.model.InventoryItem;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.SourceOfTruthListing;
import me.matthew.flink.backpacktfforward.model.SteamInventoryResponse;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Flink processor that handles backfill requests using the new Steam integration approach.
 * 
 * This processor implements a 7-step data flow:
 * 1. Query database for ALL rows matching defindex/quality combination
 * 2. Fetch BackpackTF market data using market name
 * 3. Scan Steam user inventories for matching items
 * 4. Match items by defindex and quality
 * 5. Retrieve detailed listing data via getListing API
 * 6. Detect stale data by comparing database with source of truth
 * 7. Generate ListingUpdate events for updates and deletes
 * 
 * The new approach addresses the missing ID issue by using Steam inventory scanning
 * to correlate BackpackTF listings with actual Steam items, then using the getListing
 * API to retrieve complete listing data with proper IDs.
 */
@Slf4j
public class BackfillProcessor extends RichFlatMapFunction<BackfillRequest, ListingUpdate> {
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    private transient DatabaseHelper databaseHelper;
    private transient BackpackTfApiClient apiClient;
    private transient SteamApi steamApi;
    
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
        
        // Initialize Steam API client
        steamApi = new SteamApi();
        
        log.info("BackfillProcessor initialized with DatabaseHelper, BackpackTF API client, and Steam API client");
    }
    
    @Override
    public void flatMap(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        log.debug("Processing backfill request for item_defindex={}, item_quality_id={}", 
                request.getItemDefindex(), request.getItemQualityId());
        
        try {
            // Step 1: Query database for ALL rows matching defindex/quality combination
            List<DatabaseHelper.ExistingListing> allDbListings = null;
            try {
                allDbListings = databaseHelper.getAllListingsForItem(request.getItemDefindex(), request.getItemQualityId());
                log.debug("Found {} existing database listings for item_defindex={}, item_quality_id={}", 
                        allDbListings != null ? allDbListings.size() : 0, 
                        request.getItemDefindex(), request.getItemQualityId());
            } catch (Exception e) {
                log.error("Database error while querying all listings for item_defindex={}, item_quality_id={}: {}. " +
                         "Skipping backfill request to prevent job failure.", 
                         request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
                return; // Skip this request but continue processing others
            }
            
            // Get market name from database listings (needed for BackpackTF API call)
            String marketName = getMarketNameFromDbListings(allDbListings);
            if (marketName == null) {
                log.warn("No market_name found for item_defindex={}, item_quality_id={}. " +
                        "This may indicate missing reference data in the database.", 
                        request.getItemDefindex(), request.getItemQualityId());
                return;
            }
            
            log.debug("Using market_name: {} for BackpackTF API call", marketName);
            
            // Step 2: Query BackpackTF API with market name to get source of truth listings
            BackpackTfApiResponse apiResponse = null;
            try {
                apiResponse = apiClient.fetchSnapshot(marketName, 440);
                log.debug("BackpackTF API returned {} listings for market_name: {}", 
                        apiResponse != null && apiResponse.getListings() != null ? apiResponse.getListings().size() : 0, 
                        marketName);
            } catch (Exception e) {
                log.error("BackpackTF API error while fetching snapshot for market_name={}, item_defindex={}, item_quality_id={}: {}. " +
                         "Skipping entire backfill request due to API failure.", 
                         marketName, request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
                return; // Skip this request but continue processing others
            }
            
            if (apiResponse == null) {
                log.warn("BackpackTF API returned null response for market_name: {}. " +
                        "Performing complete no-op - no updates or deletes will be processed.", marketName);
                return; // Complete no-op when API returns null
            }
            
            if (apiResponse.getListings() == null || apiResponse.getListings().isEmpty()) {
                log.info("No listings returned from BackpackTF API for market_name: {}. " +
                        "Proceeding with stale data detection only.", marketName);
                // Still need to handle stale data detection when API returns empty listings
                handleStaleDataDetection(allDbListings, new ArrayList<>(), out);
                return;
            }
            
            // Step 3-5: Process each BackpackTF listing through Steam inventory scan and getListing API
            List<SourceOfTruthListing> sourceOfTruthListings = new ArrayList<>();
            
            for (BackpackTfApiResponse.ApiListing apiListing : apiResponse.getListings()) {
                try {
                    // Step 3: Use SteamApi to scan inventory for matching items
                    SteamInventoryResponse inventory = null;
                    try {
                        inventory = steamApi.getPlayerItems(apiListing.getSteamid());
                        log.debug("Steam API returned inventory for steamid: {} with {} items", 
                                apiListing.getSteamid(), 
                                inventory != null && inventory.getResult() != null && inventory.getResult().getItems() != null 
                                        ? inventory.getResult().getItems().size() : 0);
                    } catch (Exception steamError) {
                        log.warn("Steam API error for steamid={}: {}. Skipping this listing but continuing with others.", 
                                apiListing.getSteamid(), steamError.getMessage());
                        continue; // Skip this listing but continue processing others
                    }
                    
                    if (inventory == null || inventory.getResult() == null || inventory.getResult().getItems() == null) {
                        log.debug("No inventory data available for steamid: {}. Skipping this listing.", 
                                apiListing.getSteamid());
                        continue;
                    }
                    
                    // Step 4: Match items by defindex and quality
                    List<InventoryItem> matchingItems = steamApi.findMatchingItems(
                            inventory, request.getItemDefindex(), request.getItemQualityId());
                    
                    log.debug("Found {} matching items in inventory for steamid: {}, defindex: {}, quality: {}", 
                            matchingItems.size(), apiListing.getSteamid(), 
                            request.getItemDefindex(), request.getItemQualityId());
                    
                    // Step 5: For each matching item, call getListing API to get complete data
                    for (InventoryItem matchingItem : matchingItems) {
                        try {
                            BackpackTfListingDetail listingDetail = 
                                    apiClient.getListing(String.valueOf(matchingItem.getId()));
                            
                            if (listingDetail != null && listingDetail.getId() != null) {
                                // Create source of truth entry with complete data
                                SourceOfTruthListing sotListing = new SourceOfTruthListing(
                                        apiListing, matchingItem, listingDetail);
                                sourceOfTruthListings.add(sotListing);
                                
                                log.debug("Successfully created source of truth listing for item ID: {}, listing ID: {}", 
                                        matchingItem.getId(), listingDetail.getId());
                            } else {
                                log.warn("getListing API returned null or incomplete data for item ID: {}. Skipping this item.", 
                                        matchingItem.getId());
                            }
                        } catch (Exception getListingError) {
                            log.warn("getListing API error for item ID {}: {}. Skipping this item but continuing with others.", 
                                    matchingItem.getId(), getListingError.getMessage());
                            // Continue processing other items
                        }
                    }
                    
                } catch (Exception listingProcessingError) {
                    log.error("Error processing API listing for steamid={}: {}. Skipping this listing but continuing with others.", 
                            apiListing.getSteamid(), listingProcessingError.getMessage(), listingProcessingError);
                    // Continue processing other listings
                }
            }
            
            log.info("Successfully processed {} source of truth listings for item_defindex={}, item_quality_id={}", 
                    sourceOfTruthListings.size(), request.getItemDefindex(), request.getItemQualityId());
            
            // Step 6: Generate listing-update events for source of truth
            for (SourceOfTruthListing sotListing : sourceOfTruthListings) {
                try {
                    ListingUpdate updateEvent = ListingUpdateMapper.mapToListingUpdate(sotListing);
                    out.collect(updateEvent);
                    log.debug("Emitted listing-update event for listing ID: {}, intent: {}", 
                            sotListing.getActualListingId(), sotListing.getIntent());
                } catch (Exception mappingError) {
                    log.error("Error mapping source of truth listing to ListingUpdate for listing ID {}: {}. " +
                             "Skipping this update but continuing with others.", 
                             sotListing.getActualListingId(), mappingError.getMessage(), mappingError);
                    // Continue processing other listings
                }
            }
            
            // Step 7: Detect stale data and generate listing-delete events
            handleStaleDataDetection(allDbListings, sourceOfTruthListings, out);
            
        } catch (Exception e) {
            log.error("Unexpected error processing backfill request for item_defindex={}, item_quality_id={}: {}. " +
                     "This request will be skipped to prevent job failure.", 
                     request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
            // Don't rethrow - continue processing other requests to maintain job stability
        }
    }
    
    /**
     * Extracts market name from database listings.
     * 
     * @param allDbListings List of existing database listings
     * @return market name if available, null otherwise
     */
    private String getMarketNameFromDbListings(List<DatabaseHelper.ExistingListing> allDbListings) {
        if (allDbListings == null || allDbListings.isEmpty()) {
            return null;
        }
        
        // Get market name from the first listing (all should have the same market name for the same defindex/quality)
        return allDbListings.get(0).getMarketName();
    }
    
    /**
     * Handles stale data detection by comparing database listings with source of truth listings.
     * Generates ListingUpdate objects with event="listing-delete" for stale data.
     * 
     * @param allDbListings All database listings for the item combination
     * @param sourceOfTruthListings Complete source of truth dataset from APIs
     * @param out Collector to emit delete events
     */
    private void handleStaleDataDetection(List<DatabaseHelper.ExistingListing> allDbListings, 
            List<SourceOfTruthListing> sourceOfTruthListings, Collector<ListingUpdate> out) {
        
        if (allDbListings == null || allDbListings.isEmpty()) {
            log.debug("No existing database listings found, no stale data to detect");
            return;
        }
        
        // Create set of actual listing IDs from source of truth for efficient lookup
        Set<String> sourceOfTruthIds = sourceOfTruthListings.stream()
                .map(SourceOfTruthListing::getActualListingId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        
        log.debug("Comparing {} database listings against {} source of truth listings", 
                allDbListings.size(), sourceOfTruthIds.size());
        
        int staleListingsCount = 0;
        
        for (DatabaseHelper.ExistingListing dbListing : allDbListings) {
            try {
                // If this database listing's ID is not in the source of truth, it's stale
                if (!sourceOfTruthIds.contains(dbListing.getId())) {
                    ListingUpdate deleteEvent = ListingUpdateMapper.createDeleteEvent(
                            dbListing.getId(), dbListing.getSteamid());
                    out.collect(deleteEvent);
                    staleListingsCount++;
                    log.debug("Emitting delete event for stale listing: id={}, steamid={}", 
                            dbListing.getId(), dbListing.getSteamid());
                }
            } catch (Exception e) {
                log.error("Error creating delete event for stale listing id={}, steamid={}: {}. " +
                         "This stale listing will not be deleted.", 
                         dbListing.getId(), dbListing.getSteamid(), e.getMessage(), e);
                // Continue processing other stale listings
            }
        }
        
        if (staleListingsCount > 0) {
            log.info("Identified {} stale listings for deletion", staleListingsCount);
        } else {
            log.debug("No stale listings identified for deletion");
        }
    }
    
    @Override
    public void close() throws Exception {
        // DatabaseHelper and API clients manage their own connections, no cleanup needed
        super.close();
    }
}