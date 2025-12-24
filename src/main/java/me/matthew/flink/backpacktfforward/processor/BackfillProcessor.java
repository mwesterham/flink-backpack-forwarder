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
import me.matthew.flink.backpacktfforward.util.ListingIdGenerator;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.*;

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
    
    // Metrics for comprehensive monitoring
    private transient Counter backfillRequestsProcessed;
    private transient Counter backfillRequestsFailed;
    private transient Counter backfillApiCallsSuccess;
    private transient Counter backfillApiCallsFailed;
    private transient Counter backfillStaleListingsDetected;
    private transient Counter backfillListingsUpdated;
    
    // Performance tracking - using simple counters for latency tracking
    private volatile long lastApiCallLatency = 0;
    private volatile long lastProcessingTime = 0;
    
    // Additional detailed metrics
    private transient Counter steamApiCallsSuccess;
    private transient Counter steamApiCallsFailed;
    private transient Counter getListingApiCallsSuccess;
    private transient Counter getListingApiCallsFailed;
    private transient Counter databaseQueriesSuccess;
    private transient Counter databaseQueriesFailed;
    private transient Counter itemsMatched;
    private transient Counter sourceOfTruthListingsCreated;
    
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
        
        // Initialize metrics
        var metricGroup = getRuntimeContext().getMetricGroup();
        
        // Existing metrics
        backfillRequestsProcessed = metricGroup.counter(BACKFILL_REQUESTS_PROCESSED);
        backfillRequestsFailed = metricGroup.counter(BACKFILL_REQUESTS_FAILED);
        backfillApiCallsSuccess = metricGroup.counter(BACKFILL_API_CALLS_SUCCESS);
        backfillApiCallsFailed = metricGroup.counter(BACKFILL_API_CALLS_FAILED);
        backfillStaleListingsDetected = metricGroup.counter(BACKFILL_STALE_LISTINGS_DETECTED);
        backfillListingsUpdated = metricGroup.counter(BACKFILL_LISTINGS_UPDATED);
        
        // Performance tracking gauges
        metricGroup.gauge("backfill_last_api_call_latency", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastApiCallLatency;
            }
        });
        
        metricGroup.gauge("backfill_last_processing_time", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastProcessingTime;
            }
        });
        steamApiCallsSuccess = metricGroup.counter("steam_api_calls_success");
        steamApiCallsFailed = metricGroup.counter("steam_api_calls_failed");
        getListingApiCallsSuccess = metricGroup.counter("get_listing_api_calls_success");
        getListingApiCallsFailed = metricGroup.counter("get_listing_api_calls_failed");
        databaseQueriesSuccess = metricGroup.counter("database_queries_success");
        databaseQueriesFailed = metricGroup.counter("database_queries_failed");
        itemsMatched = metricGroup.counter("items_matched");
        sourceOfTruthListingsCreated = metricGroup.counter("source_of_truth_listings_created");
        
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
        log.info("Metrics initialized: processing_time, api_latency, success/failure counters for all operations");
    }
    
    @Override
    public void flatMap(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        long processingStartTime = System.currentTimeMillis();
        
        log.info("Starting backfill processing for item_defindex={}, item_quality_id={}", 
                request.getItemDefindex(), request.getItemQualityId());
        
        try {
            // Step 1: Query database for ALL rows matching defindex/quality combination
            List<DatabaseHelper.ExistingListing> allDbListings = null;
            long dbQueryStartTime = System.currentTimeMillis();
            try {
                allDbListings = databaseHelper.getAllListingsForItem(request.getItemDefindex(), request.getItemQualityId());
                long dbQueryDuration = System.currentTimeMillis() - dbQueryStartTime;
                databaseQueriesSuccess.inc();
                
                log.info("Database query completed in {}ms. Found {} existing listings for item_defindex={}, item_quality_id={}", 
                        dbQueryDuration, allDbListings != null ? allDbListings.size() : 0, 
                        request.getItemDefindex(), request.getItemQualityId());
            } catch (Exception e) {
                long dbQueryDuration = System.currentTimeMillis() - dbQueryStartTime;
                databaseQueriesFailed.inc();
                
                log.error("Database query failed after {}ms for item_defindex={}, item_quality_id={}: {}. " +
                         "Skipping backfill request to prevent job failure.", 
                         dbQueryDuration, request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
                backfillRequestsFailed.inc();
                return; // Skip this request but continue processing others
            }
            
            // Get market name from database listings (needed for BackpackTF API call)
            String marketName = databaseHelper.getMarketName(request.getItemDefindex(), request.getItemQualityId());
            if (marketName == null) {
                log.warn("No market_name found for item_defindex={}, item_quality_id={}. " +
                        "This may indicate missing reference data in the database.", 
                        request.getItemDefindex(), request.getItemQualityId());
                backfillRequestsFailed.inc();
                return;
            }
            
            log.debug("Using market_name: {} for BackpackTF API call", marketName);
            
            // Step 2: Query BackpackTF API with market name to get source of truth listings
            BackpackTfApiResponse apiResponse = null;
            long backpackTfApiStartTime = System.currentTimeMillis();
            try {
                apiResponse = apiClient.fetchSnapshot(marketName, 440);
                long backpackTfApiDuration = System.currentTimeMillis() - backpackTfApiStartTime;
                backfillApiCallsSuccess.inc();
                lastApiCallLatency = backpackTfApiDuration;
                
                log.info("BackpackTF API call completed in {}ms. Returned {} listings for market_name: {}", 
                        backpackTfApiDuration, 
                        apiResponse != null && apiResponse.getListings() != null ? apiResponse.getListings().size() : 0, 
                        marketName);
            } catch (Exception e) {
                long backpackTfApiDuration = System.currentTimeMillis() - backpackTfApiStartTime;
                backfillApiCallsFailed.inc();
                lastApiCallLatency = backpackTfApiDuration;
                
                log.error("BackpackTF API call failed after {}ms for market_name={}, item_defindex={}, item_quality_id={}: {}. " +
                         "Skipping entire backfill request due to API failure.", 
                         backpackTfApiDuration, marketName, request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
                backfillRequestsFailed.inc();
                return; // Skip this request but continue processing others
            }
            
            if (apiResponse == null) {
                log.warn("BackpackTF API returned null response for market_name: {}. " +
                        "Performing complete no-op - no updates or deletes will be processed.", marketName);
                backfillRequestsFailed.inc();
                return; // Complete no-op when API returns null
            }
            
            if (apiResponse.getListings() == null || apiResponse.getListings().isEmpty()) {
                log.info("No listings returned from BackpackTF API for market_name: {}. " +
                        "Proceeding with stale data detection only.", marketName);
                // Still need to handle stale data detection when API returns empty listings
                int deletesGenerated = handleStaleDataDetection(allDbListings, new ArrayList<>(), out);
                backfillRequestsProcessed.inc();
                
                long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
                lastProcessingTime = totalProcessingTime;
                log.info("Backfill processing completed in {}ms (stale data detection only). Generated {} deletes", 
                        totalProcessingTime, deletesGenerated);
                return;
            }
            
            // Step 3-5: Process each BackpackTF listing through Steam inventory scan and getListing API
            List<SourceOfTruthListing> sourceOfTruthListings = new ArrayList<>();
            int steamApiCallCount = 0;
            int getListingApiCallCount = 0;
            int totalItemsMatched = 0;
            
            log.info("Processing {} BackpackTF listings through Steam inventory scan", apiResponse.getListings().size());
            
            for (BackpackTfApiResponse.ApiListing apiListing : apiResponse.getListings()) {
                try {
                    // Step 3: Use SteamApi to scan inventory for matching items
                    SteamInventoryResponse inventory = null;
                    long steamApiStartTime = System.currentTimeMillis();
                    try {
                        inventory = steamApi.getPlayerItems(apiListing.getSteamid());
                        long steamApiDuration = System.currentTimeMillis() - steamApiStartTime;
                        steamApiCallsSuccess.inc();
                        steamApiCallCount++;
                        
                        log.debug("Steam API call completed in {}ms for steamid: {} with {} items", 
                                steamApiDuration, apiListing.getSteamid(), 
                                inventory != null && inventory.getResult() != null && inventory.getResult().getItems() != null 
                                        ? inventory.getResult().getItems().size() : 0);
                    } catch (Exception steamError) {
                        long steamApiDuration = System.currentTimeMillis() - steamApiStartTime;
                        steamApiCallsFailed.inc();
                        steamApiCallCount++;
                        
                        log.warn("Steam API call failed after {}ms for steamid={}: {}. Skipping this listing but continuing with others.", 
                                steamApiDuration, apiListing.getSteamid(), steamError.getMessage());
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
                    
                    totalItemsMatched += matchingItems.size();
                    itemsMatched.inc(matchingItems.size());
                    
                    log.debug("Found {} matching items in inventory for steamid: {}, defindex: {}, quality: {}", 
                            matchingItems.size(), apiListing.getSteamid(), 
                            request.getItemDefindex(), request.getItemQualityId());
                    
                    // Step 5: For each matching item, call getListing API to get complete data
                    for (InventoryItem matchingItem : matchingItems) {
                        long getListingStartTime = System.currentTimeMillis();
                        try {
                            // Generate the correct listing ID based on intent
                            String listingId;
                            if ("sell".equalsIgnoreCase(apiListing.getIntent())) {
                                // For sell listings: {appid}_{assetid}
                                listingId = ListingIdGenerator.generateSellListingId(440, String.valueOf(matchingItem.getId()));
                            } else if ("buy".equalsIgnoreCase(apiListing.getIntent())) {
                                // For buy listings: {appid}_{steamid}_{md5 hash of item name}
                                // We need the market name for buy listings
                                listingId = ListingIdGenerator.generateBuyListingId(440, apiListing.getSteamid(), marketName);
                            } else {
                                log.warn("Unknown intent '{}' for steamid={}. Skipping this listing.", 
                                        apiListing.getIntent(), apiListing.getSteamid());
                                continue;
                            }
                            
                            BackpackTfListingDetail listingDetail = apiClient.getListing(listingId);
                            long getListingDuration = System.currentTimeMillis() - getListingStartTime;
                            getListingApiCallsSuccess.inc();
                            getListingApiCallCount++;
                            
                            if (listingDetail != null && listingDetail.getId() != null) {
                                // Create source of truth entry with complete data
                                SourceOfTruthListing sotListing = new SourceOfTruthListing(
                                        apiListing, matchingItem, listingDetail);
                                sourceOfTruthListings.add(sotListing);
                                sourceOfTruthListingsCreated.inc();
                                
                                log.debug("Successfully created source of truth listing in {}ms for item ID: {}, listing ID: {}, intent: {}", 
                                        getListingDuration, matchingItem.getId(), listingDetail.getId(), apiListing.getIntent());
                            } else {
                                log.warn("getListing API returned null or incomplete data after {}ms for listing ID: {}. Skipping this item.", 
                                        getListingDuration, listingId);
                            }
                        } catch (Exception getListingError) {
                            long getListingDuration = System.currentTimeMillis() - getListingStartTime;
                            getListingApiCallsFailed.inc();
                            getListingApiCallCount++;
                            
                            log.warn("getListing API call failed after {}ms for listing ID generation (intent: {}, steamid: {}, item: {}): {}. Skipping this item but continuing with others.", 
                                    getListingDuration, apiListing.getIntent(), apiListing.getSteamid(), matchingItem.getId(), getListingError.getMessage());
                            // Continue processing other items
                        }
                    }
                    
                } catch (Exception listingProcessingError) {
                    log.error("Error processing API listing for steamid={}: {}. Skipping this listing but continuing with others.", 
                            apiListing.getSteamid(), listingProcessingError.getMessage(), listingProcessingError);
                    // Continue processing other listings
                }
            }
            
            log.info("API processing completed: {} Steam API calls, {} getListing API calls, {} items matched, {} source of truth listings created", 
                    steamApiCallCount, getListingApiCallCount, totalItemsMatched, sourceOfTruthListings.size());
            
            // Step 6: Generate listing-update events for source of truth
            int updatesGenerated = 0;
            for (SourceOfTruthListing sotListing : sourceOfTruthListings) {
                try {
                    ListingUpdate updateEvent = ListingUpdateMapper.mapToListingUpdate(sotListing);
                    out.collect(updateEvent);
                    updatesGenerated++;
                    backfillListingsUpdated.inc();
                    
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
            int deletesGenerated = handleStaleDataDetection(allDbListings, sourceOfTruthListings, out);
            
            // Mark request as successfully processed
            backfillRequestsProcessed.inc();
            
            long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
            lastProcessingTime = totalProcessingTime;
            
            log.info("Backfill processing completed successfully in {}ms for item_defindex={}, item_quality_id={}. " +
                    "Generated {} updates and {} deletes", 
                    totalProcessingTime, request.getItemDefindex(), request.getItemQualityId(), 
                    updatesGenerated, deletesGenerated);
            
        } catch (Exception e) {
            backfillRequestsFailed.inc();
            long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
            lastProcessingTime = totalProcessingTime;
            
            log.error("Unexpected error processing backfill request after {}ms for item_defindex={}, item_quality_id={}: {}. " +
                     "This request will be skipped to prevent job failure.", 
                     totalProcessingTime, request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
            // Don't rethrow - continue processing other requests to maintain job stability
        }
    }
    
    /**
     * Handles stale data detection by comparing database listings with source of truth listings.
     * Generates ListingUpdate objects with event="listing-delete" for stale data.
     * 
     * @param allDbListings All database listings for the item combination
     * @param sourceOfTruthListings Complete source of truth dataset from APIs
     * @param out Collector to emit delete events
     * @return Number of delete events generated
     */
    private int handleStaleDataDetection(List<DatabaseHelper.ExistingListing> allDbListings, 
            List<SourceOfTruthListing> sourceOfTruthListings, Collector<ListingUpdate> out) {
        
        if (allDbListings == null || allDbListings.isEmpty()) {
            log.debug("No existing database listings found, no stale data to detect");
            return 0;
        }
        
        // Create set of actual listing IDs from source of truth for efficient lookup
        Set<String> sourceOfTruthIds = sourceOfTruthListings.stream()
                .map(SourceOfTruthListing::getActualListingId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        
        log.info("Starting stale data detection: comparing {} database listings against {} source of truth listings", 
                allDbListings.size(), sourceOfTruthIds.size());
        
        int staleListingsCount = 0;
        long staleDetectionStartTime = System.currentTimeMillis();
        
        for (DatabaseHelper.ExistingListing dbListing : allDbListings) {
            try {
                // If this database listing's ID is not in the source of truth, it's stale
                if (!sourceOfTruthIds.contains(dbListing.getId())) {
                    ListingUpdate deleteEvent = ListingUpdateMapper.createDeleteEvent(
                            dbListing.getId(), dbListing.getSteamid());
                    out.collect(deleteEvent);
                    staleListingsCount++;
                    backfillStaleListingsDetected.inc();
                    
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
        
        long staleDetectionDuration = System.currentTimeMillis() - staleDetectionStartTime;
        
        if (staleListingsCount > 0) {
            log.info("Stale data detection completed in {}ms: identified {} stale listings for deletion", 
                    staleDetectionDuration, staleListingsCount);
        } else {
            log.info("Stale data detection completed in {}ms: no stale listings identified for deletion", 
                    staleDetectionDuration);
        }
        
        return staleListingsCount;
    }
    
    @Override
    public void close() throws Exception {
        // DatabaseHelper and API clients manage their own connections, no cleanup needed
        super.close();
    }
}