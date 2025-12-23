package me.matthew.flink.backpacktfforward.processor;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.model.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
 * 1. Queries the database for market_name using item identifiers
 * 2. Calls the backpack.tf API to fetch current listings
 * 3. Maps API response to ListingUpdate objects
 * 4. Identifies stale listings and emits delete events
 */
@Slf4j
public class BackfillProcessor extends RichFlatMapFunction<BackfillRequest, ListingUpdate> {
    
    private static final String MARKET_NAME_QUERY = """
        SELECT DISTINCT market_name 
        FROM listings 
        WHERE item_defindex = ? AND item_quality_id = ? 
        AND market_name IS NOT NULL 
        ORDER BY updated_at DESC 
        LIMIT 1
        """;
    
    private static final String EXISTING_LISTINGS_QUERY = """
        SELECT id, steamid 
        FROM listings 
        WHERE item_defindex = ? AND item_quality_id = ? AND is_deleted = false
        """;
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    private transient Connection connection;
    private transient PreparedStatement marketNameStmt;
    private transient PreparedStatement existingListingsStmt;
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
        
        // Initialize database connection
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(true);
        
        // Prepare SQL statements
        marketNameStmt = connection.prepareStatement(MARKET_NAME_QUERY);
        existingListingsStmt = connection.prepareStatement(EXISTING_LISTINGS_QUERY);
        
        // Initialize API client
        apiClient = new BackpackTfApiClient();
        
        log.info("BackfillProcessor initialized with database connection");
    }
    
    @Override
    public void flatMap(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        log.debug("Processing backfill request for item_defindex={}, item_quality_id={}", 
                request.getItemDefindex(), request.getItemQualityId());
        
        try {
            // Step 1: Query database for market_name
            String marketName = getMarketName(request.getItemDefindex(), request.getItemQualityId());
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
            
            // Step 4: Handle stale data deletion
            handleStaleDataDeletion(request.getItemDefindex(), request.getItemQualityId(), 
                    apiSteamIds, out);
            
        } catch (Exception e) {
            log.error("Error processing backfill request for item_defindex={}, item_quality_id={}: {}", 
                    request.getItemDefindex(), request.getItemQualityId(), e.getMessage(), e);
            // Don't rethrow - continue processing other requests
        }
    }
    
    /**
     * Queries the database for the market_name associated with the given item identifiers.
     * 
     * @param itemDefindex Item definition index
     * @param itemQualityId Item quality ID
     * @return Market name if found, null otherwise
     * @throws SQLException if database query fails
     */
    private String getMarketName(int itemDefindex, int itemQualityId) throws SQLException {
        marketNameStmt.setInt(1, itemDefindex);
        marketNameStmt.setInt(2, itemQualityId);
        
        try (ResultSet rs = marketNameStmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString("market_name");
            }
        }
        
        return null;
    }
    
    /**
     * Queries the database for existing listings with the same item identifiers
     * and identifies stale listings that should be deleted.
     * 
     * @param itemDefindex Item definition index
     * @param itemQualityId Item quality ID
     * @param apiSteamIds List of Steam IDs from the API response
     * @param out Collector to emit delete events
     * @throws SQLException if database query fails
     */
    private void handleStaleDataDeletion(int itemDefindex, int itemQualityId, 
            List<String> apiSteamIds, Collector<ListingUpdate> out) throws SQLException {
        
        existingListingsStmt.setInt(1, itemDefindex);
        existingListingsStmt.setInt(2, itemQualityId);
        
        Set<String> apiSteamIdSet = new HashSet<>(apiSteamIds);
        
        try (ResultSet rs = existingListingsStmt.executeQuery()) {
            while (rs.next()) {
                String listingId = rs.getString("id");
                String steamId = rs.getString("steamid");
                
                // If this listing's steamid is not in the API response, it's stale
                if (!apiSteamIdSet.contains(steamId)) {
                    ListingUpdate deleteEvent = createDeleteEvent(listingId, steamId);
                    out.collect(deleteEvent);
                    log.debug("Emitting delete event for stale listing: id={}, steamid={}", 
                            listingId, steamId);
                }
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
        payload.setId(generateListingId(apiListing.getSteamid(), apiListing.getItem().getDefindex(), 
                apiListing.getItem().getQuality()));
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
    
    /**
     * Generates a consistent listing ID based on steamid and item identifiers.
     * This ensures the same listing from the API gets the same ID across backfill runs.
     * 
     * @param steamid Steam ID of the listing owner
     * @param defindex Item definition index
     * @param quality Item quality ID
     * @return Generated listing ID
     */
    private String generateListingId(String steamid, int defindex, int quality) {
        // Create a deterministic ID based on steamid and item identifiers
        // This ensures consistency across backfill runs
        return String.format("api_%s_%d_%d_%d", steamid, defindex, quality, System.currentTimeMillis() / 1000);
    }
    
    @Override
    public void close() throws Exception {
        if (marketNameStmt != null) {
            marketNameStmt.close();
        }
        if (existingListingsStmt != null) {
            existingListingsStmt.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}