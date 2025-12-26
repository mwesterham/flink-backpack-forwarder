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
 * Integration tests for SellOnlyBackfillHandler.
 * Tests the complete flow including Steam inventory scanning and sell listing processing.
 */
@ExtendWith(MockitoExtension.class)
class SellOnlyBackfillHandlerIntegrationTest {

    @Mock
    private DatabaseHelper mockDatabaseHelper;
    
    @Mock
    private BackpackTfApiClient mockApiClient;
    
    @Mock
    private SteamApi mockSteamApi;
    
    private SellOnlyBackfillHandler handler;
    private TestCollector collector;
    
    @BeforeEach
    void setUp() {
        handler = new SellOnlyBackfillHandler(mockDatabaseHelper, mockApiClient, mockSteamApi);
        collector = new TestCollector();
    }
    
    @Test
    void testSuccessfulSellOnlyBackfillWithInventoryScanning() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        // Mock database listings (existing sell listings)
        List<DatabaseHelper.ExistingListing> existingListings = Arrays.asList(
            new DatabaseHelper.ExistingListing("440_16525961480", "76561199574661225", "Strange Bat"),
            new DatabaseHelper.ExistingListing("440_16525961481", "76561199574661226", "Strange Bat"),
            new DatabaseHelper.ExistingListing("staleSellListing440_99999", "76561199574661227", "Strange Bat") // Will be deleted
        );
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(existingListings);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Mock BackpackTF API snapshot response (only sell listings)
        BackpackTfApiResponse apiResponse = createSellOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        // Mock Steam inventory responses
        SteamInventoryResponse inventory1 = createSteamInventory("76561199574661225", "16525961480");
        SteamInventoryResponse inventory2 = createSteamInventory("76561199574661226", "16525961481");
        when(mockSteamApi.getPlayerItems("76561199574661225")).thenReturn(inventory1);
        when(mockSteamApi.getPlayerItems("76561199574661226")).thenReturn(inventory2);
        
        // Mock item matching
        List<InventoryItem> matchingItems1 = Arrays.asList(createInventoryItem("16525961480", 190, 11));
        List<InventoryItem> matchingItems2 = Arrays.asList(createInventoryItem("16525961481", 190, 11));
        when(mockSteamApi.findMatchingItems(inventory1, 190, 11)).thenReturn(matchingItems1);
        when(mockSteamApi.findMatchingItems(inventory2, 190, 11)).thenReturn(matchingItems2);
        
        // Mock getListing API responses for sell listings
        BackpackTfListingDetail sellListing1 = createSellListingDetail("440_16525961480", "76561199574661225");
        BackpackTfListingDetail sellListing2 = createSellListingDetail("440_16525961481", "76561199574661226");
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
        
        // Verify updates for source of truth sell listings
        assertEquals(2, updates.size(), "Should generate 2 sell listing updates");
        
        for (ListingUpdate update : updates) {
            assertEquals("listing-update", update.getEvent());
            assertEquals("sell", update.getPayload().getIntent());
            assertNotNull(update.getPayload().getSteamid());
            assertNotNull(update.getGenerationTimestamp());
        }
        
        // Verify delete for stale sell listing
        assertEquals(1, deletes.size(), "Should generate 1 delete for stale sell listing");
        ListingUpdate delete = deletes.get(0);
        assertEquals("listing-delete", delete.getEvent());
        assertEquals("staleSellListing440_99999", delete.getId());
        
        // Verify API calls
        verify(mockDatabaseHelper).getAllListingsForItem(190, 11);
        verify(mockDatabaseHelper).getMarketName(190, 11);
        verify(mockApiClient).fetchSnapshot("Strange Bat", 440);
        verify(mockSteamApi).getPlayerItems("76561199574661225");
        verify(mockSteamApi).getPlayerItems("76561199574661226");
        verify(mockSteamApi).findMatchingItems(inventory1, 190, 11);
        verify(mockSteamApi).findMatchingItems(inventory2, 190, 11);
        verify(mockApiClient).getListing("440_16525961480");
        verify(mockApiClient).getListing("440_16525961481");
    }
    
    @Test
    void testSellOnlyBackfillWithInventorySizeFiltering() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        request.setMaxInventorySize(5); // Set inventory size limit
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        BackpackTfApiResponse apiResponse = createSellOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        // Mock Steam inventory with too many matching items (should be skipped)
        SteamInventoryResponse largeInventory = createSteamInventory("76561199574661225", "16525961480");
        when(mockSteamApi.getPlayerItems("76561199574661225")).thenReturn(largeInventory);
        
        // Mock large inventory (more than maxInventorySize)
        List<InventoryItem> tooManyItems = Arrays.asList(
            createInventoryItem("16525961480", 190, 11),
            createInventoryItem("16525961481", 190, 11),
            createInventoryItem("16525961482", 190, 11),
            createInventoryItem("16525961483", 190, 11),
            createInventoryItem("16525961484", 190, 11),
            createInventoryItem("16525961485", 190, 11) // 6 items > 5 limit
        );
        when(mockSteamApi.findMatchingItems(largeInventory, 190, 11)).thenReturn(tooManyItems);
        
        // Mock normal inventory for second user
        SteamInventoryResponse normalInventory = createSteamInventory("76561199574661226", "16525961490");
        when(mockSteamApi.getPlayerItems("76561199574661226")).thenReturn(normalInventory);
        
        List<InventoryItem> normalItems = Arrays.asList(createInventoryItem("16525961490", 190, 11));
        when(mockSteamApi.findMatchingItems(normalInventory, 190, 11)).thenReturn(normalItems);
        
        BackpackTfListingDetail sellListing = createSellListingDetail("440_16525961490", "76561199574661226");
        when(mockApiClient.getListing("440_16525961490")).thenReturn(sellListing);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should only process the user with normal inventory size
        assertEquals(1, updates.size(), "Should only process user with inventory under limit");
        assertEquals("76561199574661226", updates.get(0).getPayload().getSteamid());
        
        // Verify Steam API calls
        verify(mockSteamApi).getPlayerItems("76561199574661225");
        verify(mockSteamApi).getPlayerItems("76561199574661226");
        verify(mockSteamApi).findMatchingItems(largeInventory, 190, 11);
        verify(mockSteamApi).findMatchingItems(normalInventory, 190, 11);
        
        // Should only call getListing for the non-filtered user
        verify(mockApiClient, times(1)).getListing(anyString());
        verify(mockApiClient).getListing("440_16525961490");
    }
    
    @Test
    void testSellOnlyBackfillWithNoMatchingInventoryItems() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        BackpackTfApiResponse apiResponse = createSellOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        // Mock Steam inventory with no matching items
        SteamInventoryResponse inventory = createSteamInventory("76561199574661225", "16525961480");
        when(mockSteamApi.getPlayerItems("76561199574661225")).thenReturn(inventory);
        when(mockSteamApi.findMatchingItems(inventory, 190, 11)).thenReturn(Collections.emptyList());
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should not generate any updates when no matching items found
        assertTrue(updates.isEmpty(), "Should not generate updates when no matching inventory items");
        
        verify(mockSteamApi).getPlayerItems("76561199574661225");
        verify(mockSteamApi).findMatchingItems(inventory, 190, 11);
        // Note: mockApiClient.fetchSnapshot is still called, but getListing should not be called
        verify(mockApiClient, never()).getListing(anyString());
    }
    
    @Test
    void testSellOnlyBackfillWithFailedListingDetailCall() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        BackpackTfApiResponse apiResponse = createSellOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        SteamInventoryResponse inventory = createSteamInventory("76561199574661225", "16525961480");
        when(mockSteamApi.getPlayerItems("76561199574661225")).thenReturn(inventory);
        
        List<InventoryItem> matchingItems = Arrays.asList(createInventoryItem("16525961480", 190, 11));
        when(mockSteamApi.findMatchingItems(inventory, 190, 11)).thenReturn(matchingItems);
        
        // Mock failed getListing call (returns null)
        when(mockApiClient.getListing("440_16525961480")).thenReturn(null);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should not generate updates when getListing returns null
        assertTrue(updates.isEmpty(), "Should not generate updates when getListing returns null");
        
        verify(mockApiClient).getListing("440_16525961480");
    }
    
    @Test
    void testSellOnlyBackfillWithMixedIntentListings() throws Exception {
        // Arrange - API response contains both buy and sell listings, but handler should only process sell
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Mixed API response with both buy and sell listings
        BackpackTfApiResponse mixedResponse = createMixedIntentApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(mixedResponse);
        
        // Only mock Steam API for sell listing user
        SteamInventoryResponse inventory = createSteamInventory("76561199574661226", "16525961480");
        when(mockSteamApi.getPlayerItems("76561199574661226")).thenReturn(inventory);
        
        List<InventoryItem> matchingItems = Arrays.asList(createInventoryItem("16525961480", 190, 11));
        when(mockSteamApi.findMatchingItems(inventory, 190, 11)).thenReturn(matchingItems);
        
        BackpackTfListingDetail sellListing = createSellListingDetail("440_16525961480", "76561199574661226");
        when(mockApiClient.getListing("440_16525961480")).thenReturn(sellListing);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should only process sell listings, ignore buy listings
        assertEquals(1, updates.size(), "Should only process sell listings from mixed response");
        assertEquals("sell", updates.get(0).getPayload().getIntent());
        
        // Should only call Steam API for sell listing user
        verify(mockSteamApi, times(1)).getPlayerItems(anyString());
        verify(mockSteamApi).getPlayerItems("76561199574661226");
        verify(mockSteamApi, never()).getPlayerItems("76561199574661225"); // Buy listing user
    }
    
    @Test
    void testSellListingIdentificationLogic() throws Exception {
        // Arrange - Test the sell listing identification logic with various listing ID formats
        BackfillRequest request = createBackfillRequest(190, 11);
        
        List<DatabaseHelper.ExistingListing> mixedListings = Arrays.asList(
            new DatabaseHelper.ExistingListing("440_16525961480", "76561199574661225", "Strange Bat"), // Sell listing (2 underscores)
            new DatabaseHelper.ExistingListing("buy_listing_1", "76561199574661226", "Strange Bat"), // Buy listing (1 underscore)
            new DatabaseHelper.ExistingListing("440_16525961481", "76561199574661227", "Strange Bat"), // Sell listing (2 underscores)
            new DatabaseHelper.ExistingListing("buy_listing_2", "76561199574661228", "Strange Bat") // Buy listing (1 underscore)
        );
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(mixedListings);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Empty API response to trigger all deletes
        BackpackTfApiResponse emptyResponse = new BackpackTfApiResponse();
        emptyResponse.setListings(Collections.emptyList());
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(emptyResponse);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        List<ListingUpdate> deletes = results.stream()
            .filter(r -> "listing-delete".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should identify and delete sell listings based on underscore count logic (>= 2)
        assertEquals(2, deletes.size(), "Should identify and delete sell listings");
        
        Set<String> deletedIds = deletes.stream().map(ListingUpdate::getId).collect(Collectors.toSet());
        assertTrue(deletedIds.contains("440_16525961480"), "Should delete sell listing with 2 underscores");
        assertTrue(deletedIds.contains("440_16525961481"), "Should delete sell listing with 2 underscores");
        assertFalse(deletedIds.contains("buy_listing_1"), "Should not delete buy listing");
        assertFalse(deletedIds.contains("buy_listing_2"), "Should not delete buy listing");
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
        assertEquals(BackfillRequestType.SELL_ONLY, handler.getRequestType());
    }
    
    // Helper methods
    
    private BackfillRequest createBackfillRequest(int defindex, int qualityId) {
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(defindex);
        request.setItemQualityId(qualityId);
        request.setRequestType(BackfillRequestType.SELL_ONLY);
        return request;
    }
    
    private BackpackTfApiResponse createSellOnlyApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing sellListing1 = new BackpackTfApiResponse.ApiListing();
        sellListing1.setSteamid("76561199574661225");
        sellListing1.setIntent("sell");
        
        BackpackTfApiResponse.ApiListing sellListing2 = new BackpackTfApiResponse.ApiListing();
        sellListing2.setSteamid("76561199574661226");
        sellListing2.setIntent("sell");
        
        response.setListings(Arrays.asList(sellListing1, sellListing2));
        return response;
    }
    
    private BackpackTfApiResponse createMixedIntentApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing buyListing = new BackpackTfApiResponse.ApiListing();
        buyListing.setSteamid("76561199574661225");
        buyListing.setIntent("buy");
        
        BackpackTfApiResponse.ApiListing sellListing = new BackpackTfApiResponse.ApiListing();
        sellListing.setSteamid("76561199574661226");
        sellListing.setIntent("sell");
        
        response.setListings(Arrays.asList(buyListing, sellListing));
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
    
    private BackpackTfListingDetail createSellListingDetail(String listingId, String steamId) {
        BackpackTfListingDetail detail = new BackpackTfListingDetail();
        detail.setId(listingId);
        detail.setSteamid(steamId);
        detail.setAppid(440);
        detail.setIntent("sell");
        detail.setCount(1);
        detail.setStatus("active");
        detail.setSource("user");
        detail.setListedAt(System.currentTimeMillis() / 1000);
        detail.setBumpedAt(System.currentTimeMillis() / 1000);
        
        // Create item detail
        BackpackTfListingDetail.ApiItemDetail itemDetail = new BackpackTfListingDetail.ApiItemDetail();
        itemDetail.setAppid(440);
        itemDetail.setDefindex(190);
        itemDetail.setMarketName("Strange Bat");
        itemDetail.setName("Strange Bat");
        itemDetail.setLevel(1);
        itemDetail.setBaseName("Bat");
        itemDetail.setId(listingId.split("_")[1]); // Extract item ID from listing ID
        itemDetail.setImageUrl("https://steamcdn-a.akamaihd.net/apps/440/icons/c_bat.png");
        itemDetail.setSummary("Level 1 Bat");
        itemDetail.setTradable(true);
        itemDetail.setCraftable(true);
        
        BackpackTfListingDetail.ApiQuality quality = new BackpackTfListingDetail.ApiQuality();
        quality.setId(11);
        quality.setName("Strange");
        quality.setColor("#CF6A32");
        itemDetail.setQuality(quality);
        
        detail.setItem(itemDetail);
        
        // Create currencies for sell listing
        Map<String, Object> currencies = new HashMap<>();
        currencies.put("metal", 7.0);
        detail.setCurrencies(currencies);
        
        // Create value
        BackpackTfListingDetail.ApiValue value = new BackpackTfListingDetail.ApiValue();
        value.setRaw(7.0);
        value.setShortStr("7 ref");
        value.setLongStr("7 ref");
        value.setUsd(2.33);
        detail.setValue(value);
        
        // Create user information
        BackpackTfListingDetail.ApiUser user = new BackpackTfListingDetail.ApiUser();
        user.setId(steamId);
        user.setName("Test Seller");
        user.setAvatar("https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb_medium.jpg");
        user.setPremium(false);
        user.setOnline(false);
        user.setBanned(false);
        detail.setUser(user);
        
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