package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.client.SteamApi;
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
 * Integration tests for FullBackfillHandler.
 * Tests the complete flow that processes both buy and sell listings using delegated handlers.
 */
@ExtendWith(MockitoExtension.class)
class FullBackfillHandlerIntegrationTest {

    @Mock
    private DatabaseHelper mockDatabaseHelper;
    
    @Mock
    private BackpackTfApiClient mockApiClient;
    
    @Mock
    private SteamApi mockSteamApi;
    
    private FullBackfillHandler handler;
    private TestCollector collector;
    
    @BeforeEach
    void setUp() {
        handler = new FullBackfillHandler(mockDatabaseHelper, mockApiClient, mockSteamApi);
        collector = new TestCollector();
    }
    
    @Test
    void testSuccessfulFullBackfillWithBothBuyAndSellListings() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        // Mock database listings (mix of buy and sell listings)
        List<DatabaseHelper.ExistingListing> existingListings = Arrays.asList(
            // Buy listings (identified by underscore count <= 1)
            new DatabaseHelper.ExistingListing("buy_listing_1", "76561199574661225", "Strange Bat"),
            new DatabaseHelper.ExistingListing("buy_listing_2", "76561199574661226", "Strange Bat"),
            // Sell listings (identified by underscore count >= 2)
            new DatabaseHelper.ExistingListing("440_16525961480", "76561199574661227", "Strange Bat"),
            new DatabaseHelper.ExistingListing("440_16525961481", "76561199574661228", "Strange Bat"),
            // Stale listings that will be deleted
            new DatabaseHelper.ExistingListing("stale_buy_1", "76561199574661229", "Strange Bat"),
            new DatabaseHelper.ExistingListing("440_99999", "76561199574661230", "Strange Bat")
        );
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(existingListings);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Mock BackpackTF API snapshot response (both buy and sell listings)
        BackpackTfApiResponse apiResponse = createMixedApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        // Mock buy listing details (using generated IDs)
        String buyListingId1 = me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661225", "Strange Bat");
        String buyListingId2 = me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661226", "Strange Bat");
        
        ListingUpdate.Payload buyListing1 = createBuyListingDetail(buyListingId1, "76561199574661225");
        ListingUpdate.Payload buyListing2 = createBuyListingDetail(buyListingId2, "76561199574661226");
        when(mockApiClient.getListing(buyListingId1)).thenReturn(buyListing1);
        when(mockApiClient.getListing(buyListingId2)).thenReturn(buyListing2);
        
        // Mock Steam inventory responses for sell listings
        SteamInventoryResponse inventory1 = createSteamInventory("76561199574661227", "16525961480");
        SteamInventoryResponse inventory2 = createSteamInventory("76561199574661228", "16525961481");
        when(mockSteamApi.getPlayerItems("76561199574661227")).thenReturn(inventory1);
        when(mockSteamApi.getPlayerItems("76561199574661228")).thenReturn(inventory2);
        
        // Mock item matching for sell listings
        List<InventoryItem> matchingItems1 = Arrays.asList(createInventoryItem("16525961480", 190, 11));
        List<InventoryItem> matchingItems2 = Arrays.asList(createInventoryItem("16525961481", 190, 11));
        when(mockSteamApi.findMatchingItems(inventory1, 190, 11)).thenReturn(matchingItems1);
        when(mockSteamApi.findMatchingItems(inventory2, 190, 11)).thenReturn(matchingItems2);
        
        // Mock sell listing details
        ListingUpdate.Payload sellListing1 = createSellListingDetail("440_16525961480", "76561199574661227");
        ListingUpdate.Payload sellListing2 = createSellListingDetail("440_16525961481", "76561199574661228");
        when(mockApiClient.getListing("440_16525961480")).thenReturn(sellListing1);
        when(mockApiClient.getListing("440_16525961481")).thenReturn(sellListing2);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertFalse(results.isEmpty(), "Should generate listing updates");
        
        // Separate updates and deletes
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        List<ListingUpdate> deletes = results.stream()
            .filter(r -> "listing-delete".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should have updates for both buy and sell listings
        assertEquals(4, updates.size(), "Should generate 4 listing updates (2 buy + 2 sell)");
        
        // Verify buy and sell updates
        List<ListingUpdate> buyUpdates = updates.stream()
            .filter(u -> "buy".equals(u.getPayload().getIntent()))
            .collect(Collectors.toList());
        List<ListingUpdate> sellUpdates = updates.stream()
            .filter(u -> "sell".equals(u.getPayload().getIntent()))
            .collect(Collectors.toList());
        
        assertEquals(2, buyUpdates.size(), "Should generate 2 buy listing updates");
        assertEquals(2, sellUpdates.size(), "Should generate 2 sell listing updates");
        
        // Should have deletes for stale listings
        assertTrue(deletes.size() >= 2, "Should generate deletes for stale listings");
        
        // Verify API calls for both buy and sell processing
        verify(mockDatabaseHelper, times(1)).getAllListingsForItem(190, 11); // Called by both handlers
        verify(mockDatabaseHelper, times(1)).getMarketName(190, 11); // Called by both handlers
        verify(mockApiClient, times(1)).fetchSnapshot("Strange Bat", 440); // Called by both handlers
        
        // Buy listing API calls
        verify(mockApiClient).getListing("440_76561199574661225_a7832b3c6f42413c90bff860597f6c45");
        verify(mockApiClient).getListing("440_76561199574661225_a7832b3c6f42413c90bff860597f6c45");
        
        // Sell listing Steam API calls
        verify(mockSteamApi).getPlayerItems("76561199574661227");
        verify(mockSteamApi).getPlayerItems("76561199574661228");
        verify(mockSteamApi).findMatchingItems(inventory1, 190, 11);
        verify(mockSteamApi).findMatchingItems(inventory2, 190, 11);
        
        // Sell listing API calls
        verify(mockApiClient).getListing("440_16525961480");
        verify(mockApiClient).getListing("440_16525961481");
    }
    
    @Test
    void testFullBackfillWithOnlyBuyListings() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(266, 5);
        
        when(mockDatabaseHelper.getAllListingsForItem(266, 5)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(266, 5)).thenReturn("Unusual Horseless Headless Horsemann's Headtaker");
        
        // API response with only buy listings
        BackpackTfApiResponse buyOnlyResponse = createBuyOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Unusual Horseless Headless Horsemann's Headtaker", 440)).thenReturn(buyOnlyResponse);
        
        ListingUpdate.Payload buyListing = createBuyListingDetail("440_76561199574661225_Unusual Horseless Headless Horsemann's Headtaker", "76561199574661225");
        when(mockApiClient.getListing(anyString())).thenReturn(buyListing);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should only have buy updates
        assertTrue(updates.size() > 0, "Should generate buy listing updates");
        assertTrue(updates.stream().allMatch(u -> "buy".equals(u.getPayload().getIntent())), 
                  "All updates should be buy listings");
        
        // Should not call Steam API when no sell listings
        verifyNoInteractions(mockSteamApi);
    }
    
    @Test
    void testFullBackfillWithOnlySellListings() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // API response with only sell listings
        BackpackTfApiResponse sellOnlyResponse = createSellOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(sellOnlyResponse);
        
        // Mock Steam inventory for sell listing
        SteamInventoryResponse inventory = createSteamInventory("76561199574661225", "16525961480");
        when(mockSteamApi.getPlayerItems("76561199574661225")).thenReturn(inventory);
        
        List<InventoryItem> matchingItems = Arrays.asList(createInventoryItem("16525961480", 190, 11));
        when(mockSteamApi.findMatchingItems(inventory, 190, 11)).thenReturn(matchingItems);
        
        ListingUpdate.Payload sellListing = createSellListingDetail("440_16525961480", "76561199574661225");
        when(mockApiClient.getListing("440_16525961480")).thenReturn(sellListing);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should only have sell updates
        assertTrue(updates.size() > 0, "Should generate sell listing updates");
        assertTrue(updates.stream().allMatch(u -> "sell".equals(u.getPayload().getIntent())), 
                  "All updates should be sell listings");
        
        // Should call Steam API for sell listings
        verify(mockSteamApi).getPlayerItems("76561199574661225");
        verify(mockSteamApi).findMatchingItems(inventory, 190, 11);
    }
    
    @Test
    void testFullBackfillWithMissingMarketName() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(999, 99);
        
        when(mockDatabaseHelper.getAllListingsForItem(999, 99)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(999, 99)).thenReturn(null);
        
        // Act & Assert
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            handler.process(request, collector);
        });
        
        assertEquals("No market_name found for item", exception.getMessage());
        
        verify(mockDatabaseHelper).getAllListingsForItem(999, 99);
        verify(mockDatabaseHelper).getMarketName(999, 99);
        verifyNoInteractions(mockApiClient);
        verifyNoInteractions(mockSteamApi);
    }
    
    @Test
    void testFullBackfillWithNullApiResponse() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(null);
        
        // Act & Assert
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            handler.process(request, collector);
        });
        
        assertEquals("BackpackTF API returned null response", exception.getMessage());
        
        verify(mockDatabaseHelper).getAllListingsForItem(190, 11);
        verify(mockDatabaseHelper).getMarketName(190, 11);
        verify(mockApiClient).fetchSnapshot("Strange Bat", 440);
        verifyNoInteractions(mockSteamApi);
    }
    
    @Test
    void testFullBackfillPerformanceWithLargeDataset() throws Exception {
        // Arrange - Test with larger dataset to verify performance
        BackfillRequest request = createBackfillRequest(190, 11);
        
        // Create large dataset (20 buy + 20 sell listings)
        List<DatabaseHelper.ExistingListing> largeDataset = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            // Buy listings (using simple format with 1 underscore)
            largeDataset.add(new DatabaseHelper.ExistingListing(
                "buy_listing_" + i, 
                "7656119957466" + String.format("%04d", i), 
                "Strange Bat"
            ));
            // Sell listings (using format with 2 underscores)
            largeDataset.add(new DatabaseHelper.ExistingListing(
                "440_1652596" + String.format("%04d", i), 
                "7656119957466" + String.format("%04d", i + 100), 
                "Strange Bat"
            ));
        }
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(largeDataset);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Create large API response
        BackpackTfApiResponse largeApiResponse = createLargeApiResponse(20, 20);
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(largeApiResponse);
        
        // Mock buy listing details
        for (int i = 0; i < 20; i++) {
            String steamId = "7656119957466" + String.format("%04d", i);
            String listingId = me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, steamId, "Strange Bat");
            ListingUpdate.Payload buyListing = createBuyListingDetail(listingId, steamId);
            when(mockApiClient.getListing(listingId)).thenReturn(buyListing);
        }
        
        // Mock Steam API responses for sell listings
        for (int i = 0; i < 20; i++) {
            String steamId = "7656119957466" + String.format("%04d", i + 100);
            String itemId = "1652596" + String.format("%04d", i);
            
            SteamInventoryResponse inventory = createSteamInventory(steamId, itemId);
            when(mockSteamApi.getPlayerItems(steamId)).thenReturn(inventory);
            
            List<InventoryItem> matchingItems = Arrays.asList(createInventoryItem(itemId, 190, 11));
            when(mockSteamApi.findMatchingItems(inventory, 190, 11)).thenReturn(matchingItems);
            
            ListingUpdate.Payload sellListing = createSellListingDetail("440_" + itemId, steamId);
            when(mockApiClient.getListing("440_" + itemId)).thenReturn(sellListing);
        }
        
        // Act
        long startTime = System.currentTimeMillis();
        handler.process(request, collector);
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should process all 40 listings (20 buy + 20 sell)
        assertEquals(40, updates.size(), "Should process all 40 listings");
        
        // Verify buy/sell distribution
        long buyCount = updates.stream().filter(u -> "buy".equals(u.getPayload().getIntent())).count();
        long sellCount = updates.stream().filter(u -> "sell".equals(u.getPayload().getIntent())).count();
        assertEquals(20, buyCount, "Should have 20 buy updates");
        assertEquals(20, sellCount, "Should have 20 sell updates");
        
        // Performance assertion (should complete within reasonable time)
        assertTrue(processingTime < 10000, 
            "Processing 40 items should complete within 10 seconds, took: " + processingTime + "ms");
    }
    
    @Test
    void testCanHandleValidation() {
        // Valid requests
        assertTrue(handler.canHandle(createBackfillRequest(190, 11)));
        assertTrue(handler.canHandle(createBackfillRequest(1, 0)));
        
        // Invalid requests
        assertFalse(handler.canHandle(null));
        assertFalse(handler.canHandle(createBackfillRequest(0, 11)));
        assertFalse(handler.canHandle(createBackfillRequest(-1, 11)));
        assertFalse(handler.canHandle(createBackfillRequest(190, -1)));
    }
    
    @Test
    void testGetRequestType() {
        assertEquals(BackfillRequestType.FULL, handler.getRequestType());
    }
    
    // Helper methods
    
    private BackfillRequest createBackfillRequest(int defindex, int qualityId) {
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(defindex);
        request.setItemQualityId(qualityId);
        request.setRequestType(BackfillRequestType.FULL);
        return request;
    }
    
    private BackpackTfApiResponse createMixedApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        // Buy listings
        BackpackTfApiResponse.ApiListing buyListing1 = new BackpackTfApiResponse.ApiListing();
        buyListing1.setSteamid("76561199574661225");
        buyListing1.setIntent("buy");
        
        BackpackTfApiResponse.ApiListing buyListing2 = new BackpackTfApiResponse.ApiListing();
        buyListing2.setSteamid("76561199574661226");
        buyListing2.setIntent("buy");
        
        // Sell listings
        BackpackTfApiResponse.ApiListing sellListing1 = new BackpackTfApiResponse.ApiListing();
        sellListing1.setSteamid("76561199574661227");
        sellListing1.setIntent("sell");
        
        BackpackTfApiResponse.ApiListing sellListing2 = new BackpackTfApiResponse.ApiListing();
        sellListing2.setSteamid("76561199574661228");
        sellListing2.setIntent("sell");
        
        response.setListings(Arrays.asList(buyListing1, buyListing2, sellListing1, sellListing2));
        return response;
    }
    
    private BackpackTfApiResponse createBuyOnlyApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing buyListing = new BackpackTfApiResponse.ApiListing();
        buyListing.setSteamid("76561199574661225");
        buyListing.setIntent("buy");
        
        response.setListings(Arrays.asList(buyListing));
        return response;
    }
    
    private BackpackTfApiResponse createSellOnlyApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing sellListing = new BackpackTfApiResponse.ApiListing();
        sellListing.setSteamid("76561199574661225");
        sellListing.setIntent("sell");
        
        response.setListings(Arrays.asList(sellListing));
        return response;
    }
    
    private BackpackTfApiResponse createLargeApiResponse(int buyCount, int sellCount) {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        List<BackpackTfApiResponse.ApiListing> listings = new ArrayList<>();
        
        // Add buy listings
        for (int i = 0; i < buyCount; i++) {
            BackpackTfApiResponse.ApiListing buyListing = new BackpackTfApiResponse.ApiListing();
            buyListing.setSteamid("7656119957466" + String.format("%04d", i));
            buyListing.setIntent("buy");
            listings.add(buyListing);
        }
        
        // Add sell listings
        for (int i = 0; i < sellCount; i++) {
            BackpackTfApiResponse.ApiListing sellListing = new BackpackTfApiResponse.ApiListing();
            sellListing.setSteamid("7656119957466" + String.format("%04d", i + 100));
            sellListing.setIntent("sell");
            listings.add(sellListing);
        }
        
        response.setListings(listings);
        return response;
    }
    
    private SteamInventoryResponse createSteamInventory(String steamId, String itemId) {
        SteamInventoryResponse response = new SteamInventoryResponse();
        SteamInventoryResponse.SteamResult result = new SteamInventoryResponse.SteamResult();
        result.setStatus(1);
        result.setItems(Arrays.asList(createInventoryItem(itemId, 190, 11)));
        response.setResult(result);
        return response;
    }
    
    private InventoryItem createInventoryItem(String itemId, int defindex, int quality) {
        InventoryItem item = new InventoryItem();
        item.setId(Long.parseLong(itemId));
        item.setDefindex(defindex);
        item.setQuality(quality);
        item.setLevel(1);
        item.setQuantity(1);
        return item;
    }
    
    private ListingUpdate.Payload createBuyListingDetail(String listingId, String steamId) {
        ListingUpdate.Payload detail = new ListingUpdate.Payload();
        detail.id = listingId;
        detail.steamid = steamId;
        detail.appid = 440;
        detail.intent = "buy";
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
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.id = 11;
        quality.name = "Strange";
        quality.color = "#CF6A32";
        itemDetail.quality = quality;
        
        detail.item = itemDetail;
        
        // Create currencies for buy order
        ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
        currencies.metal = 6.0;
        detail.currencies = currencies;
        
        // Create value
        ListingUpdate.Value value = new ListingUpdate.Value();
        value.raw = 6.0;
        value.shortStr = "6 ref";
        value.longStr = "6 ref";
        detail.value = value;
        
        // Create user information
        ListingUpdate.User user = new ListingUpdate.User();
        user.id = steamId;
        user.name = "Test Buyer";
        user.avatar = "https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb_medium.jpg";
        user.premium = false;
        user.online = true;
        user.banned = false;
        detail.user = user;
        
        return detail;
    }
    
    private ListingUpdate.Payload createSellListingDetail(String listingId, String steamId) {
        ListingUpdate.Payload detail = new ListingUpdate.Payload();
        detail.id = listingId;
        detail.steamid = steamId;
        detail.appid = 440;
        detail.intent = "sell";
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
        itemDetail.id = listingId.split("_")[1]; // Extract item ID from listing ID
        itemDetail.imageUrl = "https://steamcdn-a.akamaihd.net/apps/440/icons/c_bat.png";
        itemDetail.summary = "Level 1 Bat";
        itemDetail.tradable = true;
        itemDetail.craftable = true;
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.id = 11;
        quality.name = "Strange";
        quality.color = "#CF6A32";
        itemDetail.quality = quality;
        
        detail.item = itemDetail;
        
        // Create currencies for sell listing
        ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
        currencies.metal = 7.0;
        detail.currencies = currencies;
        
        // Create value
        ListingUpdate.Value value = new ListingUpdate.Value();
        value.raw = 7.0;
        value.shortStr = "7 ref";
        value.longStr = "7 ref";
        detail.value = value;
        
        // Create user information
        ListingUpdate.User user = new ListingUpdate.User();
        user.id = steamId;
        user.name = "Test Seller";
        user.avatar = "https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb_medium.jpg";
        user.premium = false;
        user.online = false;
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