package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.model.*;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for SingleIdBackfillHandler.
 * Tests the complete flow for processing individual listing IDs.
 */
@ExtendWith(MockitoExtension.class)
class SingleIdBackfillHandlerIntegrationTest {

    @Mock
    private DatabaseHelper mockDatabaseHelper;
    
    @Mock
    private BackpackTfApiClient mockApiClient;
    
    private SingleIdBackfillHandler handler;
    private TestCollector collector;
    
    @BeforeEach
    void setUp() {
        handler = new SingleIdBackfillHandler(mockDatabaseHelper, mockApiClient);
        collector = new TestCollector();
    }
    
    @Test
    void testSuccessfulSingleIdBackfillWithExistingListing() throws Exception {
        // Arrange
        String listingId = "440_16525961480";
        BackfillRequest request = createBackfillRequest(listingId);
        
        // Mock database response - listing exists
        DatabaseHelper.ExistingListing dbListing = new DatabaseHelper.ExistingListing(
            listingId, "76561199574661225", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(listingId)).thenReturn(dbListing);
        
        // Mock successful API response
        ListingUpdate.Payload listingDetail = createListingDetail(listingId, "76561199574661225", "sell");
        when(mockApiClient.getListing(listingId)).thenReturn(listingDetail);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertEquals(1, results.size(), "Should generate exactly 1 listing update");
        
        ListingUpdate update = results.get(0);
        assertEquals("listing-update", update.getEvent());
        assertEquals(listingId, update.getId());
        assertEquals("76561199574661225", update.getPayload().getSteamid());
        assertEquals("sell", update.getPayload().getIntent());
        assertNotNull(update.getGenerationTimestamp());
        
        // Verify API calls
        verify(mockDatabaseHelper).getSingleListingById(listingId);
        verify(mockApiClient).getListing(listingId);
    }
    
    @Test
    void testSingleIdBackfillWithBuyListing() throws Exception {
        // Arrange
        String buyListingId = "440_76561199574661225_Strange_Bat";
        BackfillRequest request = createBackfillRequest(buyListingId);
        
        // Mock database response - buy listing exists
        DatabaseHelper.ExistingListing dbListing = new DatabaseHelper.ExistingListing(
            buyListingId, "76561199574661225", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(buyListingId)).thenReturn(dbListing);
        
        // Mock successful API response for buy listing
        ListingUpdate.Payload listingDetail = createListingDetail(buyListingId, "76561199574661225", "buy");
        when(mockApiClient.getListing(buyListingId)).thenReturn(listingDetail);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertEquals(1, results.size(), "Should generate exactly 1 listing update");
        
        ListingUpdate update = results.get(0);
        assertEquals("listing-update", update.getEvent());
        assertEquals(buyListingId, update.getId());
        assertEquals("76561199574661225", update.getPayload().getSteamid());
        assertEquals("buy", update.getPayload().getIntent());
        assertNotNull(update.getGenerationTimestamp());
        
        verify(mockDatabaseHelper).getSingleListingById(buyListingId);
        verify(mockApiClient).getListing(buyListingId);
    }
    
    @Test
    void testSingleIdBackfillWithNonExistentDatabaseListing() throws Exception {
        // Arrange
        String listingId = "440_99999999999";
        BackfillRequest request = createBackfillRequest(listingId);
        
        // Mock database response - listing not found
        when(mockDatabaseHelper.getSingleListingById(listingId)).thenReturn(null);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertTrue(results.isEmpty(), "Should not generate any events when listing not found in database");
        
        verify(mockDatabaseHelper).getSingleListingById(listingId);
        verify(mockApiClient).getListing(listingId);
    }
    
    @Test
    void testSingleIdBackfillWithNullApiResponse() throws Exception {
        // Arrange
        String listingId = "440_16525961480";
        BackfillRequest request = createBackfillRequest(listingId);
        
        // Mock database response - listing exists
        DatabaseHelper.ExistingListing dbListing = new DatabaseHelper.ExistingListing(
            listingId, "76561199574661225", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(listingId)).thenReturn(dbListing);
        
        // Mock API response - listing no longer exists (returns null)
        when(mockApiClient.getListing(listingId)).thenReturn(null);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertEquals(1, results.size(), "Should generate exactly 1 listing delete event");
        
        ListingUpdate delete = results.get(0);
        assertEquals("listing-delete", delete.getEvent());
        assertEquals(listingId, delete.getId());
        assertEquals("76561199574661225", delete.getPayload().getSteamid());
        assertNotNull(delete.getGenerationTimestamp());
        
        verify(mockDatabaseHelper).getSingleListingById(listingId);
        verify(mockApiClient).getListing(listingId);
    }
    
    @Test
    void testSingleIdBackfillWithEmptyListingDetailId() throws Exception {
        // Arrange
        String listingId = "440_16525961480";
        BackfillRequest request = createBackfillRequest(listingId);
        
        // Mock database response - listing exists
        DatabaseHelper.ExistingListing dbListing = new DatabaseHelper.ExistingListing(
            listingId, "76561199574661225", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(listingId)).thenReturn(dbListing);
        
        // Mock API response with null ID (should trigger delete)
        ListingUpdate.Payload listingDetail = createListingDetail(listingId, "76561199574661225", "sell");
        listingDetail.id = null;
        when(mockApiClient.getListing(listingId)).thenReturn(listingDetail);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertEquals(1, results.size(), "Should generate exactly 1 listing delete event");
        
        ListingUpdate delete = results.get(0);
        assertEquals("listing-delete", delete.getEvent());
        assertEquals(listingId, delete.getId());
        assertEquals("76561199574661225", delete.getPayload().getSteamid());
        
        verify(mockDatabaseHelper).getSingleListingById(listingId);
        verify(mockApiClient).getListing(listingId);
    }
    
    @Test
    void testSingleIdBackfillWithMultipleListingIds() throws Exception {
        // Test processing multiple different listing IDs sequentially
        
        // Test Case 1: Sell listing that exists
        String sellListingId = "440_16525961480";
        BackfillRequest sellRequest = createBackfillRequest(sellListingId);
        
        DatabaseHelper.ExistingListing sellDbListing = new DatabaseHelper.ExistingListing(
            sellListingId, "76561199574661225", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(sellListingId)).thenReturn(sellDbListing);
        
        ListingUpdate.Payload sellListingDetail = createListingDetail(sellListingId, "76561199574661225", "sell");
        when(mockApiClient.getListing(sellListingId)).thenReturn(sellListingDetail);
        
        // Process sell listing
        TestCollector sellCollector = new TestCollector();
        handler.process(sellRequest, sellCollector);
        
        // Test Case 2: Buy listing that exists
        String buyListingId = "440_76561199574661226_Strange_Bat";
        BackfillRequest buyRequest = createBackfillRequest(buyListingId);
        
        DatabaseHelper.ExistingListing buyDbListing = new DatabaseHelper.ExistingListing(
            buyListingId, "76561199574661226", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(buyListingId)).thenReturn(buyDbListing);
        
        ListingUpdate.Payload buyListingDetail = createListingDetail(buyListingId, "76561199574661226", "buy");
        when(mockApiClient.getListing(buyListingId)).thenReturn(buyListingDetail);
        
        // Process buy listing
        TestCollector buyCollector = new TestCollector();
        handler.process(buyRequest, buyCollector);
        
        // Test Case 3: Listing that no longer exists
        String deletedListingId = "440_99999999999";
        BackfillRequest deletedRequest = createBackfillRequest(deletedListingId);
        
        DatabaseHelper.ExistingListing deletedDbListing = new DatabaseHelper.ExistingListing(
            deletedListingId, "76561199574661227", "Strange Bat", 190, 11
        );
        when(mockDatabaseHelper.getSingleListingById(deletedListingId)).thenReturn(deletedDbListing);
        when(mockApiClient.getListing(deletedListingId)).thenReturn(null);
        
        // Process deleted listing
        TestCollector deletedCollector = new TestCollector();
        handler.process(deletedRequest, deletedCollector);
        
        // Assert results
        List<ListingUpdate> sellResults = sellCollector.getCollectedItems();
        assertEquals(1, sellResults.size());
        assertEquals("listing-update", sellResults.get(0).getEvent());
        assertEquals("sell", sellResults.get(0).getPayload().getIntent());
        
        List<ListingUpdate> buyResults = buyCollector.getCollectedItems();
        assertEquals(1, buyResults.size());
        assertEquals("listing-update", buyResults.get(0).getEvent());
        assertEquals("buy", buyResults.get(0).getPayload().getIntent());
        
        List<ListingUpdate> deletedResults = deletedCollector.getCollectedItems();
        assertEquals(1, deletedResults.size());
        assertEquals("listing-delete", deletedResults.get(0).getEvent());
        
        // Verify all API calls were made
        verify(mockDatabaseHelper).getSingleListingById(sellListingId);
        verify(mockDatabaseHelper).getSingleListingById(buyListingId);
        verify(mockDatabaseHelper).getSingleListingById(deletedListingId);
        verify(mockApiClient).getListing(sellListingId);
        verify(mockApiClient).getListing(buyListingId);
        verify(mockApiClient).getListing(deletedListingId);
    }
    
    @Test
    void testSingleIdBackfillPerformanceWithRapidSequentialCalls() throws Exception {
        // Test performance with rapid sequential calls to simulate high-throughput scenarios
        
        List<String> listingIds = Arrays.asList(
            "440_16525961480", "440_16525961481", "440_16525961482", 
            "440_16525961483", "440_16525961484"
        );
        
        // Mock database and API responses for all listings
        for (int i = 0; i < listingIds.size(); i++) {
            String listingId = listingIds.get(i);
            String steamId = "7656119957466122" + i;
            
            DatabaseHelper.ExistingListing dbListing = new DatabaseHelper.ExistingListing(
                listingId, steamId, "Strange Bat", 190, 11
            );
            when(mockDatabaseHelper.getSingleListingById(listingId)).thenReturn(dbListing);
            
            ListingUpdate.Payload listingDetail = createListingDetail(listingId, steamId, "sell");
            when(mockApiClient.getListing(listingId)).thenReturn(listingDetail);
        }
        
        // Process all listings and measure time
        long startTime = System.currentTimeMillis();
        List<ListingUpdate> allResults = new ArrayList<>();
        
        for (String listingId : listingIds) {
            BackfillRequest request = createBackfillRequest(listingId);
            TestCollector testCollector = new TestCollector();
            handler.process(request, testCollector);
            allResults.addAll(testCollector.getCollectedItems());
        }
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        // Assert results
        assertEquals(5, allResults.size(), "Should process all 5 listings");
        assertTrue(allResults.stream().allMatch(r -> "listing-update".equals(r.getEvent())), 
                  "All results should be updates");
        
        // Performance assertion
        assertTrue(processingTime < 1000, 
            "Processing 5 single listings should complete within 1 second, took: " + processingTime + "ms");
        
        // Verify all API calls were made
        for (String listingId : listingIds) {
            verify(mockDatabaseHelper).getSingleListingById(listingId);
            verify(mockApiClient).getListing(listingId);
        }
    }
    
    @Test
    void testCanHandleValidation() {
        // Valid requests
        assertTrue(handler.canHandle(createBackfillRequest("440_16525961480")));
        assertTrue(handler.canHandle(createBackfillRequest("440_76561199574661225_Strange_Bat")));
        assertTrue(handler.canHandle(createBackfillRequest("custom_listing_id")));
        
        // Invalid requests
        assertFalse(handler.canHandle(null));
        assertFalse(handler.canHandle(createBackfillRequest(null)));
        assertFalse(handler.canHandle(createBackfillRequest("")));
        assertFalse(handler.canHandle(createBackfillRequest("   ")));
        
        // Requests with invalid additional parameters
        BackfillRequest requestWithInventorySize = createBackfillRequest("440_16525961480");
        requestWithInventorySize.setMaxInventorySize(10);
        assertTrue(handler.canHandle(requestWithInventorySize));
        
        // Requests with item parameters (should be warnings but still valid)
        BackfillRequest requestWithItemParams = createBackfillRequest("440_16525961480");
        requestWithItemParams.setItemDefindex(190);
        requestWithItemParams.setItemQualityId(11);
        assertTrue(handler.canHandle(requestWithItemParams)); // Should still be valid despite warnings
    }
    
    @Test
    void testGetRequestType() {
        assertEquals(BackfillRequestType.SINGLE_ID, handler.getRequestType());
    }
    
    @Test
    void testSingleIdBackfillWithVariousListingIdFormats() throws Exception {
        // Test various listing ID formats to ensure compatibility
        
        Map<String, String> listingFormats = Map.of(
            "440_16525961480", "sell", // Standard sell listing
            "440_76561199574661225_Strange_Bat", "buy", // Standard buy listing
            "custom_format_123", "sell", // Custom format
            "440_16525961480_extra_data", "sell", // Sell with extra data
            "tf2_special_listing", "buy" // Non-standard format
        );
        
        for (Map.Entry<String, String> entry : listingFormats.entrySet()) {
            String listingId = entry.getKey();
            String intent = entry.getValue();
            
            BackfillRequest request = createBackfillRequest(listingId);
            
            DatabaseHelper.ExistingListing dbListing = new DatabaseHelper.ExistingListing(
                listingId, "76561199574661225", "Test Item", 190, 11
            );
            when(mockDatabaseHelper.getSingleListingById(listingId)).thenReturn(dbListing);
            
            ListingUpdate.Payload listingDetail = createListingDetail(listingId, "76561199574661225", intent);
            when(mockApiClient.getListing(listingId)).thenReturn(listingDetail);
            
            TestCollector testCollector = new TestCollector();
            handler.process(request, testCollector);
            
            List<ListingUpdate> results = testCollector.getCollectedItems();
            assertEquals(1, results.size(), "Should process listing with format: " + listingId);
            assertEquals("listing-update", results.get(0).getEvent());
            assertEquals(listingId, results.get(0).getId());
            assertEquals(intent, results.get(0).getPayload().getIntent());
        }
        
        // Verify all API calls were made
        for (String listingId : listingFormats.keySet()) {
            verify(mockDatabaseHelper).getSingleListingById(listingId);
            verify(mockApiClient).getListing(listingId);
        }
    }
    
    // Helper methods
    
    private BackfillRequest createBackfillRequest(String listingId) {
        BackfillRequest request = new BackfillRequest();
        request.setListingId(listingId);
        request.setRequestType(BackfillRequestType.SINGLE_ID);
        return request;
    }
    
    private ListingUpdate.Payload createListingDetail(String listingId, String steamId, String intent) {
        ListingUpdate.Payload detail = new ListingUpdate.Payload();
        detail.id = listingId;
        detail.steamid = steamId;
        detail.appid = 440;
        detail.intent = intent;
        detail.count = 1;
        detail.status = "active";
        detail.source = "user";
        detail.listedAt = System.currentTimeMillis() / 1000;
        detail.bumpedAt = System.currentTimeMillis() / 1000;
        
        // Create item detail
        ListingUpdate.Item itemDetail = new ListingUpdate.Item();
        itemDetail.appid = 440;
        itemDetail.defindex = 190;
        itemDetail.marketName = "Strange Bat";
        itemDetail.name = "Strange Bat";
        itemDetail.level = 1;
        itemDetail.baseName = "Bat";
        itemDetail.imageUrl = "https://steamcdn-a.akamaihd.net/apps/440/icons/c_bat.png";
        itemDetail.summary = "Level 1 Bat";
        itemDetail.tradable = true;
        itemDetail.craftable = true;
        
        // Set item ID based on listing type
        if ("sell".equals(intent) && listingId.startsWith("440_")) {
            String[] parts = listingId.split("_");
            if (parts.length >= 2) {
                itemDetail.id = parts[1]; // Extract item ID for sell listings
            }
        }
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.id = 11;
        quality.name = "Strange";
        quality.color = "#CF6A32";
        itemDetail.quality = quality;
        
        detail.item = itemDetail;
        
        // Create currencies
        ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
        if ("buy".equals(intent)) {
            currencies.metal = 6.0;
        } else {
            currencies.metal = 7.0;
        }
        detail.currencies = currencies;
        
        // Create value
        ListingUpdate.Value value = new ListingUpdate.Value();
        double price = "buy".equals(intent) ? 6.0 : 7.0;
        value.raw = price;
        value.shortStr = price + " ref";
        value.longStr = price + " ref";
        detail.value = value;
        
        // Create user information
        ListingUpdate.User user = new ListingUpdate.User();
        user.id = steamId;
        user.name = "buy".equals(intent) ? "Test Buyer" : "Test Seller";
        user.avatar = "https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb_medium.jpg";
        user.premium = false;
        user.online = "buy".equals(intent);
        user.banned = false;
        detail.user = user;
        
        return detail;
    }
    
    /**
     * Test collector implementation to capture emitted ListingUpdate objects
     */
    private static class TestCollector implements Collector<ListingUpdate> {
        private final List<ListingUpdate> collectedItems = new ArrayList<>();
        
        @Override
        public void collect(ListingUpdate record) {
            collectedItems.add(record);
        }
        
        @Override
        public void close() {
            // No-op
        }
        
        public List<ListingUpdate> getCollectedItems() {
            return new ArrayList<>(collectedItems);
        }
    }
}