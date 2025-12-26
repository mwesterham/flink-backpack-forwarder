package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import org.apache.flink.util.Collector;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SingleIdBackfillHandler implements BackfillRequestHandler {
    private final DatabaseHelper databaseHelper;
    private final BackpackTfApiClient apiClient;

    public SingleIdBackfillHandler(DatabaseHelper databaseHelper, BackpackTfApiClient apiClient) {
        this.databaseHelper = databaseHelper;
        this.apiClient = apiClient;
        log.info("SingleIdBackfillHandler initialized with database and API client dependencies");
    }

    @Override
    public void process(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        long processingStartTime = System.currentTimeMillis();
        long generationTimestamp = System.currentTimeMillis();

        String listingId = request.getListingId();
        log.info("Starting SINGLE_ID backfill processing for listing_id={} with generation_timestamp={}",
                listingId, generationTimestamp);

        // Step 1: Query database for the specific listing ID
        DatabaseHelper.ExistingListing dbListing = databaseHelper.getSingleListingById(listingId);
        if (dbListing == null) {
            log.info("Listing ID {} not found in database or is deleted. No processing needed.", listingId);
            long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
            log.info("SINGLE_ID backfill processing completed in {}ms (no database record found)", totalProcessingTime);
            return;
        }

        log.info("Found database listing: id={}, steamid={}, market_name={}, item_defindex={}, item_quality_id={}",
                dbListing.getId(), dbListing.getSteamid(), dbListing.getMarketName(),
                dbListing.getItemDefindex(), dbListing.getItemQualityId());

        // Step 2: Call BackpackTF getListing API directly with the provided ID
        BackpackTfListingDetail listingDetail = apiClient.getListing(listingId);

        // Step 3: Generate single update or delete event based on listing existence
        if (listingDetail != null && listingDetail.getId() != null) {
            ListingUpdate updateEvent = ListingUpdateMapper.mapFromListingDetail(
                    listingDetail, generationTimestamp);
            out.collect(updateEvent);
            log.debug("Emitted listing-update event for buy listing ID: {} with generation_timestamp: {}",
                    listingId, generationTimestamp);
        } else {
            ListingUpdate deleteEvent = ListingUpdateMapper.createDeleteEvent(
                    listingId, dbListing.getSteamid(), generationTimestamp);
            out.collect(deleteEvent);
            log.debug("Emitted listing-delete event for buy listing ID: {} with generation_timestamp: {}",
                    listingId, generationTimestamp);
        }

        long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
        log.info("SINGLE_ID backfill processing completed successfully in {}ms for listing_id={}",
                totalProcessingTime, listingId);

    }

    @Override
    public BackfillRequestType getRequestType() {
        return BackfillRequestType.SINGLE_ID;
    }

    @Override
    public boolean canHandle(BackfillRequest request) {
        if (request == null) {
            log.warn("Cannot handle null BackfillRequest");
            return false;
        }

        // Validate that listing_id parameter is present and valid for SINGLE_ID
        // requests
        if (request.getListingId() == null || request.getListingId().trim().isEmpty()) {
            log.warn("Cannot handle SINGLE_ID request without valid listing_id parameter");
            return false;
        }

        // SINGLE_ID requests should not have item defindex/quality parameters
        // (these will be retrieved from the database based on the listing ID)
        if (request.getItemDefindex() > 0 || request.getItemQualityId() >= 0) {
            log.warn("SINGLE_ID request should not include item_defindex or item_quality_id parameters. " +
                    "These will be retrieved from the database based on listing_id: {}",
                    request.getListingId());
            // This is a warning but not a validation failure - we'll ignore these
            // parameters
        }

        // SINGLE_ID requests should not have max_inventory_size parameter
        if (request.getMaxInventorySize() != null) {
            log.warn("Cannot handle SINGLE_ID request with max_inventory_size parameter: {}. " +
                    "Use INVENTORY_FILTERED request type for inventory size filtering.",
                    request.getMaxInventorySize());
            return false;
        }

        log.debug("SINGLE_ID backfill request validation passed for listing_id={}",
                request.getListingId());
        return true;
    }
}