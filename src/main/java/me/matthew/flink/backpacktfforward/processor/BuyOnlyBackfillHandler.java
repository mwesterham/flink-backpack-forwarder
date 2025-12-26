package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;
import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.SourceOfTruthListing;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import me.matthew.flink.backpacktfforward.util.ListingIdGenerator;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import org.apache.flink.util.Collector;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

@Slf4j
public class BuyOnlyBackfillHandler implements BackfillRequestHandler {
    @NonNull
    private final DatabaseHelper databaseHelper;
    @NonNull
    private final BackpackTfApiClient apiClient;

    @Nullable
    private List<DatabaseHelper.ExistingListing> allDbListings;
    @Nullable
    private String marketName;
    @Nullable
    private BackpackTfApiResponse snapshotResponse;

    public BuyOnlyBackfillHandler(DatabaseHelper databaseHelper, BackpackTfApiClient apiClient) {
        this.databaseHelper = databaseHelper;
        this.apiClient = apiClient;
        log.info("BuyOnlyBackfillHandler initialized with database and API client dependencies");
    }

    public BuyOnlyBackfillHandler(
            DatabaseHelper databaseHelper,
            BackpackTfApiClient apiClient,
            List<DatabaseHelper.ExistingListing> allDbListings,
            String marketName,
            BackpackTfApiResponse snapshotResponse) {
        this.databaseHelper = databaseHelper;
        this.apiClient = apiClient;
        this.allDbListings = allDbListings;
        this.marketName = marketName;
        this.snapshotResponse = snapshotResponse;
    }

    @Override
    public void process(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        long processingStartTime = System.currentTimeMillis();
        long generationTimestamp = System.currentTimeMillis();
        log.info(
                "Starting BUY_ONLY backfill processing for item_defindex={}, item_quality_id={} with generation_timestamp={}",
                request.getItemDefindex(), request.getItemQualityId(), generationTimestamp);

        // Step 1: Query database for ALL rows matching defindex/quality combination
        if (this.allDbListings == null) {
            this.allDbListings = databaseHelper.getAllListingsForItem(
                    request.getItemDefindex(), request.getItemQualityId());
            log.info("Database query completed. Found {} existing listings for item_defindex={}, item_quality_id={}",
                    this.allDbListings != null ? this.allDbListings.size() : 0,
                    request.getItemDefindex(), request.getItemQualityId());
        }

        if (this.marketName == null) {
            this.marketName = databaseHelper.getMarketName(request.getItemDefindex(), request.getItemQualityId());
            log.debug("Using market_name: {} for BackpackTF API call", this.marketName);
        }
        if (this.marketName == null) {
            log.warn("No market_name found for item_defindex={}, item_quality_id={}. " +
                        "This may indicate missing reference data in the database.", 
                        request.getItemDefindex(), request.getItemQualityId());
            return;
        }

        // Step 2: Query BackpackTF API with market name to get source of truth listings
        if (this.snapshotResponse == null) {
            this.snapshotResponse = apiClient.fetchSnapshot(this.marketName, 440);
        }

        if (this.snapshotResponse == null) {
            log.warn("BackpackTF API returned null response for market_name: {}. " +
                    "Performing complete no-op - no updates or deletes will be processed.", this.marketName);
            return;
        }

        // Step 3: Filter API response to buy listings only
        List<BackpackTfApiResponse.ApiListing> snapshotBuyListings = this.snapshotResponse.getListings().stream()
                .filter(listing -> "buy".equalsIgnoreCase(listing.getIntent()))
                .collect(Collectors.toList());

        // Step 4: Process buy listings
        List<SourceOfTruthListing> sourceOfTruthBuyListings = new ArrayList<>();

        for (BackpackTfApiResponse.ApiListing snapshotBuyListing : snapshotBuyListings) {
            // For buy orders: construct listing ID directly without Steam API call
            String listingId = ListingIdGenerator.generateBuyListingId(440, snapshotBuyListing.getSteamid(), this.marketName);
            BackpackTfListingDetail listingDetail = apiClient.getListing(listingId);
            SourceOfTruthListing sotBuyListing = new SourceOfTruthListing(snapshotBuyListing, null, listingDetail);
            sourceOfTruthBuyListings.add(sotBuyListing);
        }

        log.info("BUY_ONLY API processing completed: {} source of truth listings created",
                sourceOfTruthBuyListings.size());

        // Step 5: Generate listing-update events for source of truth
        int updatesGenerated = 0;
        for (SourceOfTruthListing sotBuyListing : sourceOfTruthBuyListings) {
            ListingUpdate updateEvent = ListingUpdateMapper.mapToListingUpdate(sotBuyListing, generationTimestamp);
            out.collect(updateEvent);
            updatesGenerated++;
            log.debug("Emitted listing-update event for buy listing ID: {} with generation_timestamp: {}",
                    sotBuyListing.getActualListingId(), generationTimestamp);
        }

        // Step 6: Detect stale data for buy listings only and generate listing-delete
        // events
        int deletesGenerated = handleBuyOnlyStaleDataDetection(sourceOfTruthBuyListings, generationTimestamp, out);
        long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
        log.info(
                "BUY_ONLY backfill processing completed successfully in {}ms for item_defindex={}, item_quality_id={}. "
                        +
                        "Generated {} updates and {} deletes",
                totalProcessingTime, request.getItemDefindex(), request.getItemQualityId(),
                updatesGenerated, deletesGenerated);

    }

    private int handleBuyOnlyStaleDataDetection(
            List<SourceOfTruthListing> sourceOfTruthBuyListings,
            Long generationTimestamp,
            Collector<ListingUpdate> out) throws Exception {

        if (this.allDbListings == null || this.allDbListings.isEmpty()) {
            log.debug("No existing database listings found, no stale data to detect");
            return 0;
        }

        Set<String> sourceOfTruthIds = sourceOfTruthBuyListings.stream()
                .map(SourceOfTruthListing::getActualListingId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        List<DatabaseHelper.ExistingListing> buyDbListings = this.allDbListings.stream()
                .filter(dbListing -> isBuyListing(dbListing))
                .collect(Collectors.toList());
        log.info(
                "Starting BUY_ONLY stale data detection: comparing {} buy database listings against {} source of truth listings with generation_timestamp: {}",
                buyDbListings.size(), sourceOfTruthIds.size(), generationTimestamp);

        int staleListingsCount = 0;

        for (DatabaseHelper.ExistingListing dbListing : buyDbListings) {
            boolean buyListingMatchesWithSource = sourceOfTruthIds.contains(dbListing.getId());
            if (!buyListingMatchesWithSource) {
                ListingUpdate deleteEvent = ListingUpdateMapper.createDeleteEvent(
                        dbListing.getId(), dbListing.getSteamid(), generationTimestamp);
                out.collect(deleteEvent);
                staleListingsCount++;

                log.debug(
                        "Emitting delete event for stale buy listing: id={}, steamid={} with generation_timestamp: {}",
                        dbListing.getId(), dbListing.getSteamid(), generationTimestamp);
            }
        }

        return staleListingsCount;
    }

    private boolean isBuyListing(DatabaseHelper.ExistingListing dbListing) {
        if (dbListing.getId() == null) {
            return false;
        }

        String listingId = dbListing.getId();
        long underscoreCount = listingId.chars().filter(ch -> ch == '_').count();
        return underscoreCount == 2;
    }

    @Override
    public BackfillRequestType getRequestType() {
        return BackfillRequestType.BUY_ONLY;
    }

    @Override
    public boolean canHandle(BackfillRequest request) {
        if (request == null) {
            log.warn("Cannot handle null BackfillRequest");
            return false;
        }

        // Validate basic required parameters for BUY_ONLY requests
        if (request.getItemDefindex() <= 0) {
            log.warn("Cannot handle BUY_ONLY request with invalid itemDefindex: {}",
                    request.getItemDefindex());
            return false;
        }

        if (request.getItemQualityId() < 0) {
            log.warn("Cannot handle BUY_ONLY request with invalid itemQualityId: {}",
                    request.getItemQualityId());
            return false;
        }

        log.debug("BUY_ONLY backfill request validation passed for item_defindex={}, item_quality_id={}",
                request.getItemDefindex(), request.getItemQualityId());
        return true;
    }
}