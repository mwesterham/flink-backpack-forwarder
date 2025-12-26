package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.client.SteamApi;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;

import org.apache.flink.util.Collector;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;

import java.util.List;

@Slf4j
public class FullBackfillHandler implements BackfillRequestHandler {
    private transient DatabaseHelper databaseHelper;
    private transient BackpackTfApiClient apiClient;
    private transient SteamApi steamApi;

    public FullBackfillHandler(
            DatabaseHelper databaseHelper,
            BackpackTfApiClient apiClient,
            SteamApi steamApi) {
        this.databaseHelper = databaseHelper;
        this.apiClient = apiClient;
        this.steamApi = steamApi;
        log.info("FullBackfillHandler initialized with database, API client, and Steam API dependencies");
    }

    @Override
    public void process(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        long generationTimestamp = System.currentTimeMillis();

        log.info(
                "Starting full backfill processing for item_defindex={}, item_quality_id={} with generation_timestamp={}",
                request.getItemDefindex(), request.getItemQualityId(), generationTimestamp);

        // Step 1: Query database for ALL rows matching defindex/quality combination
        long dbQueryStartTime = System.currentTimeMillis();
        List<DatabaseHelper.ExistingListing> allDbListings = databaseHelper
                .getAllListingsForItem(request.getItemDefindex(), request.getItemQualityId());
        long dbQueryDuration = System.currentTimeMillis() - dbQueryStartTime;
        log.info(
                "Database query completed in {}ms. Found {} existing listings for item_defindex={}, item_quality_id={}",
                dbQueryDuration, allDbListings != null ? allDbListings.size() : 0,
                request.getItemDefindex(), request.getItemQualityId());

        // Get market name from database listings (needed for BackpackTF API call)
        String marketName = databaseHelper.getMarketName(request.getItemDefindex(), request.getItemQualityId());
        if (marketName == null) {
            log.warn("No market_name found for item_defindex={}, item_quality_id={}. " +
                    "This may indicate missing reference data in the database.",
                    request.getItemDefindex(), request.getItemQualityId());
            throw new IllegalStateException("No market_name found for item");
        }
        log.debug("Using market_name: {} for BackpackTF API call", marketName);

        // Step 2: Query BackpackTF API with market name to get source of truth listings
        long backpackTfApiStartTime = System.currentTimeMillis();
        BackpackTfApiResponse snapshotResponse = apiClient.fetchSnapshot(marketName, 440);
        long backpackTfApiDuration = System.currentTimeMillis() - backpackTfApiStartTime;
        log.info("BackpackTF API call completed in {}ms. Returned {} listings for market_name: {}",
                backpackTfApiDuration,
                snapshotResponse != null && snapshotResponse.getListings() != null
                        ? snapshotResponse.getListings().size()
                        : 0,
                marketName);

        if (snapshotResponse == null) {
            log.warn("BackpackTF API returned null response for market_name: {}. " +
                    "Performing complete no-op - no updates or deletes will be processed.", marketName);
            throw new IllegalStateException("BackpackTF API returned null response");
        }

        log.info("Processing {} BackpackTF listings with optimized approach (buy orders skip Steam API)",
                snapshotResponse.getListings().size());
        BackfillRequestHandler buyHandler = new BuyOnlyBackfillHandler(
            this.databaseHelper, 
            this.apiClient,
            allDbListings,
            marketName,
            snapshotResponse
        );
        BackfillRequestHandler sellHandler = new SellOnlyBackfillHandler(
            this.databaseHelper, 
            this.apiClient, 
            this.steamApi,
            allDbListings,
            marketName,
            snapshotResponse
        );
        if(buyHandler.canHandle(request)) buyHandler.process(request, out);
        if(sellHandler.canHandle(request)) sellHandler.process(request, out);
        log.debug("FULL backfill request processing completed for item_defindex={}, item_quality_id={}",
                request.getItemDefindex(), request.getItemQualityId());
    }

    @Override
    public BackfillRequestType getRequestType() {
        return BackfillRequestType.FULL;
    }

    @Override
    public boolean canHandle(BackfillRequest request) {
        if (request == null) {
            log.warn("Cannot handle null BackfillRequest");
            return false;
        }

        // Validate basic required parameters for FULL requests
        if (request.getItemDefindex() <= 0) {
            log.warn("Cannot handle FULL request with invalid itemDefindex: {}",
                    request.getItemDefindex());
            return false;
        }

        if (request.getItemQualityId() < 0) {
            log.warn("Cannot handle FULL request with invalid itemQualityId: {}",
                    request.getItemQualityId());
            return false;
        }

        log.debug("FULL backfill request validation passed for item_defindex={}, item_quality_id={}",
                request.getItemDefindex(), request.getItemQualityId());
        return true;
    }
}