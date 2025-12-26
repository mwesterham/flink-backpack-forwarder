package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BackfillRequest model class focusing on JSON serialization/deserialization.
 */
public class BackfillRequestTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testJsonSerialization() throws Exception {
        // Create a BackfillRequest using no-args constructor and setters for backward compatibility
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(463);
        request.setItemQualityId(5);
        request.setMarketName(null);
        
        // Serialize to JSON
        String json = objectMapper.writeValueAsString(request);
        
        // Verify JSON contains expected fields
        assertTrue(json.contains("\"item_defindex\":463"));
        assertTrue(json.contains("\"item_quality_id\":5"));
    }
    
    @Test
    public void testJsonDeserialization() throws Exception {
        // JSON string with backfill request data
        String json = "{\"item_defindex\":463,\"item_quality_id\":5}";
        
        // Deserialize from JSON
        BackfillRequest request = objectMapper.readValue(json, BackfillRequest.class);
        
        // Verify fields are correctly parsed
        assertEquals(463, request.getItemDefindex());
        assertEquals(5, request.getItemQualityId());
        assertNull(request.getMarketName()); // transient field should be null
    }
    
    @Test
    public void testJsonRoundTrip() throws Exception {
        // Create original request using no-args constructor and setters for backward compatibility
        BackfillRequest original = new BackfillRequest();
        original.setItemDefindex(123);
        original.setItemQualityId(6);
        original.setMarketName("Test Market Name");
        
        // Serialize and deserialize
        String json = objectMapper.writeValueAsString(original);
        BackfillRequest deserialized = objectMapper.readValue(json, BackfillRequest.class);
        
        // Verify core fields match (marketName is transient so won't be serialized)
        assertEquals(original.getItemDefindex(), deserialized.getItemDefindex());
        assertEquals(original.getItemQualityId(), deserialized.getItemQualityId());
        assertNull(deserialized.getMarketName()); // transient field
    }
    
    @Test
    public void testNewOptionalFieldsDeserialization() throws Exception {
        // JSON string with new optional fields
        String json = "{\"item_defindex\":463,\"item_quality_id\":5,\"request_type\":\"BUY_ONLY\",\"listing_id\":\"test_id\",\"max_inventory_size\":10}";
        
        // Deserialize from JSON
        BackfillRequest request = objectMapper.readValue(json, BackfillRequest.class);
        
        // Verify all fields are correctly parsed
        assertEquals(463, request.getItemDefindex());
        assertEquals(5, request.getItemQualityId());
        assertEquals("BUY_ONLY", request.getRequestType().name());
        assertEquals("test_id", request.getListingId());
        assertEquals(Integer.valueOf(10), request.getMaxInventorySize());
    }
    
    @Test
    public void testBackwardCompatibilityWithoutOptionalFields() throws Exception {
        // JSON string without new optional fields (existing format)
        String json = "{\"item_defindex\":463,\"item_quality_id\":5}";
        
        // Deserialize from JSON
        BackfillRequest request = objectMapper.readValue(json, BackfillRequest.class);
        
        // Verify existing fields work and new fields are null (backward compatible)
        assertEquals(463, request.getItemDefindex());
        assertEquals(5, request.getItemQualityId());
        assertNull(request.getRequestType()); // Should default to null for backward compatibility
        assertNull(request.getListingId());
        assertNull(request.getMaxInventorySize());
    }
}