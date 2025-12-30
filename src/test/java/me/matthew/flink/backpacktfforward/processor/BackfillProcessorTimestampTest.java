package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.util.ListingUpdateMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for timestamp functionality in BackfillProcessor and related components.
 */
class BackfillProcessorTimestampTest {
    
    @Test
    void testListingUpdateMapperWithGenerationTimestamp() {
        // Create a minimal ListingUpdate.Payload for testing
        ListingUpdate.Payload listingDetail = new ListingUpdate.Payload();
        listingDetail.id = "test-listing-id";
        listingDetail.steamid = "76561198000000000";
        listingDetail.appid = 440;
        listingDetail.listedAt = System.currentTimeMillis();
        listingDetail.bumpedAt = System.currentTimeMillis();
        listingDetail.intent = "sell";
        listingDetail.count = 1;
        
        // Create minimal value object
        ListingUpdate.Value value = new ListingUpdate.Value();
        value.raw = 10.0;
        listingDetail.value = value;
        
        // Test with generation timestamp
        Long generationTimestamp = System.currentTimeMillis();
        ListingUpdate updateWithTimestamp = ListingUpdateMapper.mapFromListingDetail(listingDetail, generationTimestamp);
        
        assertNotNull(updateWithTimestamp);
        assertEquals("test-listing-id", updateWithTimestamp.id);
        assertEquals("listing-update", updateWithTimestamp.event);
        assertEquals(generationTimestamp, updateWithTimestamp.generationTimestamp);
        
        // Test without generation timestamp (should be null)
        ListingUpdate updateWithoutTimestamp = ListingUpdateMapper.mapFromListingDetail(listingDetail, null);
        
        assertNotNull(updateWithoutTimestamp);
        assertEquals("test-listing-id", updateWithoutTimestamp.id);
        assertEquals("listing-update", updateWithoutTimestamp.event);
        assertNull(updateWithoutTimestamp.generationTimestamp);
    }
    
    @Test
    void testDeleteEventWithGenerationTimestamp() {
        Long generationTimestamp = System.currentTimeMillis();
        
        // Test delete event with generation timestamp
        ListingUpdate deleteWithTimestamp = ListingUpdateMapper.createDeleteUpdate(
                "test-listing-id", "76561198000000000", generationTimestamp);
        
        assertNotNull(deleteWithTimestamp);
        assertEquals("test-listing-id", deleteWithTimestamp.id);
        assertEquals("listing-delete", deleteWithTimestamp.event);
        assertEquals(generationTimestamp, deleteWithTimestamp.generationTimestamp);
        
        // Test delete event without generation timestamp
        ListingUpdate deleteWithoutTimestamp = ListingUpdateMapper.createDeleteUpdate(
                "test-listing-id", "76561198000000000", null);
        
        assertNotNull(deleteWithoutTimestamp);
        assertEquals("test-listing-id", deleteWithoutTimestamp.id);
        assertEquals("listing-delete", deleteWithoutTimestamp.event);
        assertNull(deleteWithoutTimestamp.generationTimestamp);
    }
    
    @Test
    void testGenerationTimestampConsistency() {
        // Test that the same timestamp is used consistently
        Long generationTimestamp = System.currentTimeMillis();
        
        // Create multiple updates with the same timestamp
        ListingUpdate.Payload listingDetail1 = new ListingUpdate.Payload();
        listingDetail1.id = "listing-1";
        listingDetail1.steamid = "76561198000000001";
        listingDetail1.appid = 440;
        listingDetail1.listedAt = System.currentTimeMillis();
        listingDetail1.bumpedAt = System.currentTimeMillis();
        listingDetail1.intent = "sell";
        listingDetail1.count = 1;
        
        ListingUpdate.Value value1 = new ListingUpdate.Value();
        value1.raw = 10.0;
        listingDetail1.value = value1;
        
        ListingUpdate.Payload listingDetail2 = new ListingUpdate.Payload();
        listingDetail2.id = "listing-2";
        listingDetail2.steamid = "76561198000000002";
        listingDetail2.appid = 440;
        listingDetail2.listedAt = System.currentTimeMillis();
        listingDetail2.bumpedAt = System.currentTimeMillis();
        listingDetail2.intent = "buy";
        listingDetail2.count = 1;
        
        ListingUpdate.Value value2 = new ListingUpdate.Value();
        value2.raw = 15.0;
        listingDetail2.value = value2;
        
        ListingUpdate update1 = ListingUpdateMapper.mapFromListingDetail(listingDetail1, generationTimestamp);
        ListingUpdate update2 = ListingUpdateMapper.mapFromListingDetail(listingDetail2, generationTimestamp);
        
        // Both updates should have the same generation timestamp
        assertEquals(generationTimestamp, update1.generationTimestamp);
        assertEquals(generationTimestamp, update2.generationTimestamp);
        assertEquals(update1.generationTimestamp, update2.generationTimestamp);
    }
}