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
 * Integration tests for BuyOnlyBackfillHandler.
 * Tests the complete flow from request processing to ListingUpdate generation.
 */
@ExtendWith(MockitoExtension.class)
class BuyOnlyBackfillHandlerIntegrationTest {

    @Mock
    private DatabaseHelper mockDatabaseHelper;
    
    @Mock
    private BackpackTfApiClient mockApiClient;
    
    private BuyOnlyBackfillHandler handler;
    private TestCollector collector;
    
    @BeforeEach
    void setUp() {
        handler = new BuyOnlyBackfillHandler(mockDatabaseHelper, mockApiClient);
        collector = new TestCollector();
    }
    
    @Test
    void testSuccessfulBuyOnlyBackfillWithUpdatesAndDeletes() throws Exception {
        
        // Mock getListing API responses for buy orders (using actual generated IDs)
        String buyListingId1 = me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661225", "Strange Bat");
        String buyListingId2 = me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661226", "Strange Bat");
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        // Mock database listings (existing buy listings with correct format)
        List<DatabaseHelper.ExistingListing> existingListings = Arrays.asList(
            new DatabaseHelper.ExistingListing(buyListingId1, "76561199574661225", "Strange Bat"),
            new DatabaseHelper.ExistingListing(buyListingId2, "76561199574661226", "Strange Bat"),
            new DatabaseHelper.ExistingListing("stale_buy_1", "76561199574661227", "Strange Bat")
        );
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(existingListings);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Mock BackpackTF API snapshot response (only buy listings)
        BackpackTfApiResponse apiResponse = createBuyOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        BackpackTfListingDetail buyListing1 = createBuyListingDetail(buyListingId1, "76561199574661225");
        BackpackTfListingDetail buyListing2 = createBuyListingDetail(buyListingId2, "76561199574661226");
        
        when(mockApiClient.getListing(buyListingId1)).thenReturn(buyListing1);
        when(mockApiClient.getListing(buyListingId2)).thenReturn(buyListing2);
        
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
        
        // Verify updates for source of truth buy listings
        assertEquals(2, updates.size(), "Should generate 2 buy listing updates");
        
        for (ListingUpdate update : updates) {
            assertEquals("listing-update", update.getEvent());
            assertEquals("buy", update.getPayload().getIntent());
            assertNotNull(update.getPayload().getSteamid());
            assertNotNull(update.getGenerationTimestamp());
        }
        
        // Verify delete for stale buy listing
        assertEquals(1, deletes.size(), "Should generate 1 delete for stale buy listing");
        ListingUpdate delete = deletes.get(0);
        assertEquals("listing-delete", delete.getEvent());
        assertEquals("stale_buy_1", delete.getId());
        assertEquals("76561199574661227", delete.getPayload().getSteamid());
        
        // Verify API calls
        verify(mockDatabaseHelper).getAllListingsForItem(190, 11);
        verify(mockDatabaseHelper).getMarketName(190, 11);
        verify(mockApiClient).fetchSnapshot("Strange Bat", 440);
        verify(mockApiClient).getListing(buyListingId1);
        verify(mockApiClient).getListing(buyListingId2);
    }
    
    @Test
    void testBuyOnlyBackfillWithNoExistingListings() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(266, 5);
        
        when(mockDatabaseHelper.getAllListingsForItem(266, 5)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(266, 5)).thenReturn("Unusual Horseless Headless Horsemann's Headtaker");
        
        BackpackTfApiResponse apiResponse = createBuyOnlyApiResponse();
        when(mockApiClient.fetchSnapshot("Unusual Horseless Headless Horsemann's Headtaker", 440)).thenReturn(apiResponse);
        
        BackpackTfListingDetail buyListing = createBuyListingDetail(me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661225", "Unusual Horseless Headless Horsemann's Headtaker"), "76561199574661225");
        when(mockApiClient.getListing(anyString())).thenReturn(buyListing);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        List<ListingUpdate> deletes = results.stream()
            .filter(r -> "listing-delete".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should have updates but no deletes (no existing listings)
        assertEquals(2, updates.size(), "Should generate updates for new buy listings");
        assertEquals(0, deletes.size(), "Should not generate deletes when no existing listings");
    }
    
    @Test
    void testBuyOnlyBackfillWithEmptyApiResponse() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(999, 6);
        
        List<DatabaseHelper.ExistingListing> existingListings = Arrays.asList(
            new DatabaseHelper.ExistingListing("buy_listing_old", "76561199574661228", "Test Item") // Buy listing format
        );
        when(mockDatabaseHelper.getAllListingsForItem(999, 6)).thenReturn(existingListings);
        when(mockDatabaseHelper.getMarketName(999, 6)).thenReturn("Test Item");
        
        // Empty API response
        BackpackTfApiResponse emptyResponse = new BackpackTfApiResponse();
        emptyResponse.setListings(Collections.emptyList());
        when(mockApiClient.fetchSnapshot("Test Item", 440)).thenReturn(emptyResponse);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        
        List<ListingUpdate> deletes = results.stream()
            .filter(r -> "listing-delete".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should delete all existing buy listings since API returned no listings
        assertEquals(1, deletes.size(), "Should delete existing buy listing when API returns empty");
        assertEquals("buy_listing_old", deletes.get(0).getId());
    }
    
    @Test
    void testBuyOnlyBackfillWithNullApiResponse() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(null);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertTrue(results.isEmpty(), "Should not generate any events when API returns null");
        
        verify(mockDatabaseHelper).getAllListingsForItem(190, 11);
        verify(mockDatabaseHelper).getMarketName(190, 11);
        verify(mockApiClient).fetchSnapshot("Strange Bat", 440);
        verifyNoMoreInteractions(mockApiClient);
    }
    
    @Test
    void testBuyOnlyBackfillWithMissingMarketName() throws Exception {
        // Arrange
        BackfillRequest request = createBackfillRequest(999, 99);
        
        when(mockDatabaseHelper.getAllListingsForItem(999, 99)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(999, 99)).thenReturn(null);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        assertTrue(results.isEmpty(), "Should not generate any events when market name is missing");
        
        verify(mockDatabaseHelper).getAllListingsForItem(999, 99);
        verify(mockDatabaseHelper).getMarketName(999, 99);
        verifyNoInteractions(mockApiClient);
    }
    
    @Test
    void testBuyOnlyBackfillWithMixedIntentListings() throws Exception {
        // Arrange - API response contains both buy and sell listings, but handler should only process buy
        BackfillRequest request = createBackfillRequest(190, 11);
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Mixed API response with both buy and sell listings
        BackpackTfApiResponse mixedResponse = createMixedIntentApiResponse();
        when(mockApiClient.fetchSnapshot("Strange Bat", 440)).thenReturn(mixedResponse);
        
        BackpackTfListingDetail buyListing = createBuyListingDetail(me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661225", "Strange Bat"), "76561199574661225");
        when(mockApiClient.getListing(me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661225", "Strange Bat"))).thenReturn(buyListing);
        
        // Act
        handler.process(request, collector);
        
        // Assert
        List<ListingUpdate> results = collector.getCollectedItems();
        
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should only process buy listings, ignore sell listings
        assertEquals(1, updates.size(), "Should only process buy listings from mixed response");
        assertEquals("buy", updates.get(0).getPayload().getIntent());
        
        // Should only call getListing for buy listings
        verify(mockApiClient, times(1)).getListing(anyString());
        verify(mockApiClient).getListing(me.matthew.flink.backpacktfforward.util.ListingIdGenerator.generateBuyListingId(440, "76561199574661225", "Strange Bat"));
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
        assertEquals(BackfillRequestType.BUY_ONLY, handler.getRequestType());
    }
    
    @Test
    void testBuyListingIdentificationLogic() throws Exception {
        // Arrange - Test the buy listing identification logic with various listing ID formats
        BackfillRequest request = createBackfillRequest(190, 11);
        
        List<DatabaseHelper.ExistingListing> mixedListings = Arrays.asList(
            new DatabaseHelper.ExistingListing("buy_listing_1", "76561199574661225", "Strange Bat"),
            new DatabaseHelper.ExistingListing("440_16525961480", "76561199574661226", "Strange Bat"),
            new DatabaseHelper.ExistingListing("buy_listing_2", "76561199574661227", "Strange Bat"),
            new DatabaseHelper.ExistingListing("stale_buy_1", "76561199574661228", "Strange Bat")
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
        
        // Should identify and delete buy listings based on underscore count logic
        assertTrue(deletes.size() >= 3, "Should identify and delete buy listings");
        
        Set<String> deletedIds = deletes.stream().map(ListingUpdate::getId).collect(Collectors.toSet());
        assertTrue(deletedIds.contains("buy_listing_1") || 
                  deletedIds.contains("stale_buy"), "Should delete buy listings based on ID format");
    }
    
    // Helper methods
    
    private BackfillRequest createBackfillRequest(int defindex, int qualityId) {
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(defindex);
        request.setItemQualityId(qualityId);
        request.setRequestType(BackfillRequestType.BUY_ONLY);
        return request;
    }
    
    private BackpackTfApiResponse createBuyOnlyApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing buyListing1 = new BackpackTfApiResponse.ApiListing();
        buyListing1.setSteamid("76561199574661225");
        buyListing1.setIntent("buy");
        
        BackpackTfApiResponse.ApiListing buyListing2 = new BackpackTfApiResponse.ApiListing();
        buyListing2.setSteamid("76561199574661226");
        buyListing2.setIntent("buy");
        
        response.setListings(Arrays.asList(buyListing1, buyListing2));
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
    
    private BackpackTfListingDetail createBuyListingDetail(String listingId, String steamId) {
        BackpackTfListingDetail detail = new BackpackTfListingDetail();
        detail.setId(listingId);
        detail.setSteamid(steamId);
        detail.setAppid(440);
        detail.setIntent("buy");
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
        
        // Create currencies for buy order
        Map<String, Object> currencies = new HashMap<>();
        currencies.put("metal", 6.0);
        detail.setCurrencies(currencies);
        
        // Create value
        BackpackTfListingDetail.ApiValue value = new BackpackTfListingDetail.ApiValue();
        value.setRaw(6.0);
        value.setShortStr("6 ref");
        value.setLongStr("6 ref");
        value.setUsd(2.00);
        detail.setValue(value);
        
        // Create user information
        BackpackTfListingDetail.ApiUser user = new BackpackTfListingDetail.ApiUser();
        user.setId(steamId);
        user.setName("Test Buyer");
        user.setAvatar("https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb_medium.jpg");
        user.setPremium(false);
        user.setOnline(true);
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