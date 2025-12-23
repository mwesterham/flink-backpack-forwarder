package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BackfillRequest model class focusing on JSON serialization/deserialization.
 */
public class BackfillRequestTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testJsonSerialization() throws Exception {
        // Create a BackfillRequest
        BackfillRequest request = new BackfillRequest(463, 5, null);
        
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
        // Create original request
        BackfillRequest original = new BackfillRequest(123, 6, "Test Market Name");
        
        // Serialize and deserialize
        String json = objectMapper.writeValueAsString(original);
        BackfillRequest deserialized = objectMapper.readValue(json, BackfillRequest.class);
        
        // Verify core fields match (marketName is transient so won't be serialized)
        assertEquals(original.getItemDefindex(), deserialized.getItemDefindex());
        assertEquals(original.getItemQualityId(), deserialized.getItemQualityId());
        assertNull(deserialized.getMarketName()); // transient field
    }
}