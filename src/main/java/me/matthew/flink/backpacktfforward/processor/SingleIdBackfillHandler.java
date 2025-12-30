package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
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

        // Step 2: Call BackpackTF getListing API directly with the provided ID
        ListingUpdate.Payload listingDetail = apiClient.getListing(listingId);

        // Step 3: Generate single update or delete event based on listing existence
        if (listingDetail != null && listingDetail.getId() != null) {
            ListingUpdate updateEvent = ListingUpdateMapper.mapFromListingDetail(
                    listingDetail, generationTimestamp);
            out.collect(updateEvent);
            log.debug("Emitted listing-update event for buy listing ID: {} with generation_timestamp: {}",
                    listingId, generationTimestamp);
        } else if (dbListing != null) {
            ListingUpdate deleteEvent = ListingUpdateMapper.createDeleteUpdate(
                    listingId, dbListing.getSteamid(), generationTimestamp);
            out.collect(deleteEvent);
            log.debug("Emitted listing-delete event for buy listing ID: {} with generation_timestamp: {}",
                    listingId, generationTimestamp);
        } else {
            log.debug("Both backpack tf and the database are in sync and have no listing ID: {}.",
                    listingId);
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

        log.debug("SINGLE_ID backfill request validation passed for listing_id={}",
                request.getListingId());
        return true;
    }
}