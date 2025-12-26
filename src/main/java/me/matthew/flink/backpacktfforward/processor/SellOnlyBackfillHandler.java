package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.client.SteamApi;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;
import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;
import me.matthew.flink.backpacktfforward.model.InventoryItem;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.SourceOfTruthListing;
import me.matthew.flink.backpacktfforward.model.SteamInventoryResponse;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import me.matthew.flink.backpacktfforward.util.ListingIdGenerator;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

@Slf4j
public class SellOnlyBackfillHandler implements BackfillRequestHandler {
    @NonNull
    private final DatabaseHelper databaseHelper;
    @NonNull
    private final BackpackTfApiClient apiClient;
    @NonNull
    private final SteamApi steamApi;

    @Nullable
    private List<DatabaseHelper.ExistingListing> allDbListings;
    @Nullable
    private String marketName;
    @Nullable
    private BackpackTfApiResponse snapshotResponse;

    public SellOnlyBackfillHandler(DatabaseHelper databaseHelper, BackpackTfApiClient apiClient, SteamApi steamApi) {
        this.databaseHelper = databaseHelper;
        this.apiClient = apiClient;
        this.steamApi = steamApi;
        log.info("SellOnlyBackfillHandler initialized with database, API client, and Steam API dependencies");
    }

    public SellOnlyBackfillHandler(
            DatabaseHelper databaseHelper,
            BackpackTfApiClient apiClient,
            SteamApi steamApi,
            List<DatabaseHelper.ExistingListing> allDbListings,
            String marketName,
            BackpackTfApiResponse snapshotResponse) {
        this.databaseHelper = databaseHelper;
        this.apiClient = apiClient;
        this.steamApi = steamApi;
        this.allDbListings = allDbListings;
        this.marketName = marketName;
        this.snapshotResponse = snapshotResponse;
    }

    @Override
    public void process(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        long processingStartTime = System.currentTimeMillis();
        long generationTimestamp = System.currentTimeMillis();
        log.info(
                "Starting SELL_ONLY backfill processing for item_defindex={}, item_quality_id={} with generation_timestamp={}",
                request.getItemDefindex(), request.getItemQualityId(), generationTimestamp);

        // Step 1: Query database for ALL rows matching defindex/quality combination
        if (this.allDbListings == null) {
            this.allDbListings = databaseHelper.getAllListingsForItem(
                    request.getItemDefindex(), request.getItemQualityId());
            log.info("Database query completed. Found {} existing listings for item_defindex={}, item_quality_id={}",
                    this.allDbListings != null ? this.allDbListings.size() : 0,
                    request.getItemDefindex(), request.getItemQualityId());
        }

        // Get market name from database listings (needed for BackpackTF API call)
        if (this.marketName == null) {
            this.marketName = databaseHelper.getMarketName(request.getItemDefindex(), request.getItemQualityId());
        }
        if (this.marketName == null) {
            log.warn("No market_name found for item_defindex={}, item_quality_id={}. " +
                    "This may indicate missing reference data in the database.",
                    request.getItemDefindex(), request.getItemQualityId());
            return;
        }

        log.debug("Using market_name: {} for BackpackTF API call", this.marketName);

        // Step 2: Query BackpackTF API with market name to get source of truth listings
        if (this.snapshotResponse == null) {
            this.snapshotResponse = apiClient.fetchSnapshot(this.marketName, 440);
        }
        if (this.snapshotResponse == null) {
            log.warn("BackpackTF API returned null response for market_name: {}. " +
                    "Performing complete no-op - no updates or deletes will be processed.", this.marketName);
            return;
        }

        // Step 3: Filter API response to sell listings only
        List<BackpackTfApiResponse.ApiListing> snapshotSellListings = this.snapshotResponse.getListings().stream()
                .filter(listing -> "sell".equalsIgnoreCase(listing.getIntent()))
                .collect(Collectors.toList());

        // Step 4: Process sell listings with Steam inventory scanning
        List<SourceOfTruthListing> sourceOfTruthSellListings = new ArrayList<>();
        List<String> skippedSteamIds = new ArrayList<>();
        for (BackpackTfApiResponse.ApiListing snapshotSellListing : snapshotSellListings) {
            SteamInventoryResponse inventory = steamApi.getPlayerItems(snapshotSellListing.getSteamid());
            List<InventoryItem> matchingItems = steamApi.findMatchingItems(
                    inventory, request.getItemDefindex(), request.getItemQualityId());

            if (request.getMaxInventorySize() != null && matchingItems.size() > request.getMaxInventorySize()) {
                log.info(
                        "Skipping sell listing for user steamid={} due to inventory size filter: {} matching items > {} threshold",
                        snapshotSellListing.getSteamid(), matchingItems.size(), request.getMaxInventorySize());
                skippedSteamIds.add(snapshotSellListing.getSteamid());
                continue;
            }

            // For each matching item, call getListing API to get complete data. Only 1 will match so break after finding it.
            // We need this since the snapshot does not contain the primary key so we must find it in the steam inventory
            for (InventoryItem matchingItem : matchingItems) {
                String listingId = ListingIdGenerator.generateSellListingId(440, String.valueOf(matchingItem.getId()));

                BackpackTfListingDetail listingDetail = apiClient.getListing(listingId);
                if(listingDetail != null) {
                    SourceOfTruthListing sotSellListing = new SourceOfTruthListing(snapshotSellListing, matchingItem,
                            listingDetail);
                    sourceOfTruthSellListings.add(sotSellListing);
                    break;
                }
            }
        }

        int updatesGenerated = 0;
        for (SourceOfTruthListing sotSellListing : sourceOfTruthSellListings) {
            ListingUpdate sellUpdateEvent = ListingUpdateMapper.mapToListingUpdate(sotSellListing, generationTimestamp);
            out.collect(sellUpdateEvent);
            updatesGenerated++;
        }
        int deletesGenerated = handleSellOnlyStaleDataDetection(sourceOfTruthSellListings, generationTimestamp,
                skippedSteamIds, out);

        long totalProcessingTime = System.currentTimeMillis() - processingStartTime;

        log.info(
                "SELL_ONLY backfill processing completed successfully in {}ms for item_defindex={}, item_quality_id={}. "
                        +
                        "Generated {} updates and {} deletes",
                totalProcessingTime, request.getItemDefindex(), request.getItemQualityId(),
                updatesGenerated, deletesGenerated);

    }

    private int handleSellOnlyStaleDataDetection(
            List<SourceOfTruthListing> sourceOfTruthSellListings,
            Long generationTimestamp,
            List<String> skipListingDeleteForSteamIdList,
            Collector<ListingUpdate> out) {
        if (this.allDbListings == null || this.allDbListings.isEmpty()) {
            log.debug("No existing database listings found, no stale data to detect");
            return 0;
        }
        Set<String> sourceOfTruthSellListingIds = sourceOfTruthSellListings.stream()
                .map(SourceOfTruthListing::getActualListingId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        List<DatabaseHelper.ExistingListing> sellDbListings = this.allDbListings.stream()
                .filter(sellDbListing -> isSellListing(sellDbListing))
                .collect(Collectors.toList());

        int staleListingsCount = 0;
        for (DatabaseHelper.ExistingListing sellDbListing : sellDbListings) {
            boolean shouldSkip = skipListingDeleteForSteamIdList.contains(sellDbListing.getSteamid());
            boolean matchesWithSourceOfTruth = sourceOfTruthSellListingIds.contains(sellDbListing.getId());
            if (!matchesWithSourceOfTruth && !shouldSkip) {
                ListingUpdate deleteEvent = ListingUpdateMapper.createDeleteEvent(
                        sellDbListing.getId(), sellDbListing.getSteamid(), generationTimestamp);
                out.collect(deleteEvent);
                staleListingsCount++;

                log.debug(
                        "Emitting delete event for stale sell listing: id={}, steamid={} with generation_timestamp: {}",
                        sellDbListing.getId(), sellDbListing.getSteamid(), generationTimestamp);
            }
        }

        return staleListingsCount;
    }

    private boolean isSellListing(DatabaseHelper.ExistingListing sellDbListing) {
        if (sellDbListing.getId() == null) {
            return false;
        }

        String listingId = sellDbListing.getId();

        long underscoreCount = listingId.chars().filter(ch -> ch == '_').count();
        return underscoreCount >= 2;
    }

    @Override
    public BackfillRequestType getRequestType() {
        return BackfillRequestType.SELL_ONLY;
    }

    @Override
    public boolean canHandle(BackfillRequest request) {
        if (request == null) {
            log.warn("Cannot handle null BackfillRequest");
            return false;
        }

        // Validate basic required parameters for SELL_ONLY requests
        if (request.getItemDefindex() <= 0) {
            log.warn("Cannot handle SELL_ONLY request with invalid itemDefindex: {}",
                    request.getItemDefindex());
            return false;
        }

        if (request.getItemQualityId() < 0) {
            log.warn("Cannot handle SELL_ONLY request with invalid itemQualityId: {}",
                    request.getItemQualityId());
            return false;
        }

        log.debug("SELL_ONLY backfill request validation passed for item_defindex={}, item_quality_id={}",
                request.getItemDefindex(), request.getItemQualityId());
        return true;
    }
}