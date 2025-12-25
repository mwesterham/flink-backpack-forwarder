package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for timestamp functionality in BackfillProcessor and related components.
 */
class BackfillProcessorTimestampTest {
    
    @Test
    void testListingUpdateMapperWithGenerationTimestamp() {
        // Create a minimal BackpackTfListingDetail for testing
        BackpackTfListingDetail listingDetail = new BackpackTfListingDetail();
        listingDetail.setId("test-listing-id");
        listingDetail.setSteamid("76561198000000000");
        listingDetail.setAppid(440);
        listingDetail.setListedAt(System.currentTimeMillis());
        listingDetail.setBumpedAt(System.currentTimeMillis());
        listingDetail.setIntent("sell");
        listingDetail.setCount(1);
        
        // Create minimal value object
        BackpackTfListingDetail.ApiValue value = new BackpackTfListingDetail.ApiValue();
        value.setRaw(10.0);
        listingDetail.setValue(value);
        
        // Test with generation timestamp
        Long generationTimestamp = System.currentTimeMillis();
        ListingUpdate updateWithTimestamp = ListingUpdateMapper.mapFromListingDetail(listingDetail, generationTimestamp);
        
        assertNotNull(updateWithTimestamp);
        assertEquals("test-listing-id", updateWithTimestamp.getId());
        assertEquals("listing-update", updateWithTimestamp.getEvent());
        assertEquals(generationTimestamp, updateWithTimestamp.getGenerationTimestamp());
        
        // Test without generation timestamp (should be null)
        ListingUpdate updateWithoutTimestamp = ListingUpdateMapper.mapFromListingDetail(listingDetail, null);
        
        assertNotNull(updateWithoutTimestamp);
        assertEquals("test-listing-id", updateWithoutTimestamp.getId());
        assertEquals("listing-update", updateWithoutTimestamp.getEvent());
        assertNull(updateWithoutTimestamp.getGenerationTimestamp());
    }
    
    @Test
    void testDeleteEventWithGenerationTimestamp() {
        Long generationTimestamp = System.currentTimeMillis();
        
        // Test delete event with generation timestamp
        ListingUpdate deleteWithTimestamp = ListingUpdateMapper.createDeleteEvent(
                "test-listing-id", "76561198000000000", generationTimestamp);
        
        assertNotNull(deleteWithTimestamp);
        assertEquals("test-listing-id", deleteWithTimestamp.getId());
        assertEquals("listing-delete", deleteWithTimestamp.getEvent());
        assertEquals(generationTimestamp, deleteWithTimestamp.getGenerationTimestamp());
        
        // Test delete event without generation timestamp
        ListingUpdate deleteWithoutTimestamp = ListingUpdateMapper.createDeleteEvent(
                "test-listing-id", "76561198000000000", null);
        
        assertNotNull(deleteWithoutTimestamp);
        assertEquals("test-listing-id", deleteWithoutTimestamp.getId());
        assertEquals("listing-delete", deleteWithoutTimestamp.getEvent());
        assertNull(deleteWithoutTimestamp.getGenerationTimestamp());
    }
    
    @Test
    void testGenerationTimestampConsistency() {
        // Test that the same timestamp is used consistently
        Long generationTimestamp = System.currentTimeMillis();
        
        // Create multiple updates with the same timestamp
        BackpackTfListingDetail listingDetail1 = new BackpackTfListingDetail();
        listingDetail1.setId("listing-1");
        listingDetail1.setSteamid("76561198000000001");
        listingDetail1.setAppid(440);
        listingDetail1.setListedAt(System.currentTimeMillis());
        listingDetail1.setBumpedAt(System.currentTimeMillis());
        listingDetail1.setIntent("sell");
        listingDetail1.setCount(1);
        
        BackpackTfListingDetail.ApiValue value1 = new BackpackTfListingDetail.ApiValue();
        value1.setRaw(10.0);
        listingDetail1.setValue(value1);
        
        BackpackTfListingDetail listingDetail2 = new BackpackTfListingDetail();
        listingDetail2.setId("listing-2");
        listingDetail2.setSteamid("76561198000000002");
        listingDetail2.setAppid(440);
        listingDetail2.setListedAt(System.currentTimeMillis());
        listingDetail2.setBumpedAt(System.currentTimeMillis());
        listingDetail2.setIntent("buy");
        listingDetail2.setCount(1);
        
        BackpackTfListingDetail.ApiValue value2 = new BackpackTfListingDetail.ApiValue();
        value2.setRaw(15.0);
        listingDetail2.setValue(value2);
        
        ListingUpdate update1 = ListingUpdateMapper.mapFromListingDetail(listingDetail1, generationTimestamp);
        ListingUpdate update2 = ListingUpdateMapper.mapFromListingDetail(listingDetail2, generationTimestamp);
        
        // Both updates should have the same generation timestamp
        assertEquals(generationTimestamp, update1.getGenerationTimestamp());
        assertEquals(generationTimestamp, update2.getGenerationTimestamp());
        assertEquals(update1.getGenerationTimestamp(), update2.getGenerationTimestamp());
    }
}