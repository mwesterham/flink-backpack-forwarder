package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BackfillKafkaMessage wrapper class focusing on JSON serialization/deserialization.
 */
public class BackfillKafkaMessageTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testJsonSerialization() throws Exception {
        // Create a BackfillKafkaMessage with nested BackfillRequest
        BackfillRequest request = new BackfillRequest(463, 5, null);
        BackfillKafkaMessage message = new BackfillKafkaMessage(
            request, 
            "2023-12-22T10:30:00Z", 
            "msg-123", 
            "backfill-service"
        );
        
        // Serialize to JSON
        String json = objectMapper.writeValueAsString(message);
        
        // Verify JSON structure
        assertTrue(json.contains("\"data\":{"));
        assertTrue(json.contains("\"item_defindex\":463"));
        assertTrue(json.contains("\"item_quality_id\":5"));
        assertTrue(json.contains("\"timestamp\":\"2023-12-22T10:30:00Z\""));
        assertTrue(json.contains("\"messageId\":\"msg-123\""));
        assertTrue(json.contains("\"source\":\"backfill-service\""));
    }
    
    @Test
    public void testJsonDeserialization() throws Exception {
        // JSON string with complete backfill message structure
        String json = "{\n" +
            "  \"data\": {\n" +
            "    \"item_defindex\": 463,\n" +
            "    \"item_quality_id\": 5\n" +
            "  },\n" +
            "  \"timestamp\": \"2023-12-22T10:30:00Z\",\n" +
            "  \"messageId\": \"msg-123\",\n" +
            "  \"source\": \"backfill-service\"\n" +
            "}";
        
        // Deserialize from JSON
        BackfillKafkaMessage message = objectMapper.readValue(json, BackfillKafkaMessage.class);
        
        // Verify wrapper fields
        assertEquals("2023-12-22T10:30:00Z", message.getTimestamp());
        assertEquals("msg-123", message.getMessageId());
        assertEquals("backfill-service", message.getSource());
        
        // Verify nested data
        assertNotNull(message.getData());
        assertEquals(463, message.getData().getItemDefindex());
        assertEquals(5, message.getData().getItemQualityId());
    }
    
    @Test
    public void testJsonRoundTrip() throws Exception {
        // Create original message
        BackfillRequest request = new BackfillRequest(789, 11, null);
        BackfillKafkaMessage original = new BackfillKafkaMessage(
            request, 
            "2023-12-22T15:45:30Z", 
            "msg-456", 
            "test-source"
        );
        
        // Serialize and deserialize
        String json = objectMapper.writeValueAsString(original);
        BackfillKafkaMessage deserialized = objectMapper.readValue(json, BackfillKafkaMessage.class);
        
        // Verify all fields match
        assertEquals(original.getTimestamp(), deserialized.getTimestamp());
        assertEquals(original.getMessageId(), deserialized.getMessageId());
        assertEquals(original.getSource(), deserialized.getSource());
        assertEquals(original.getData().getItemDefindex(), deserialized.getData().getItemDefindex());
        assertEquals(original.getData().getItemQualityId(), deserialized.getData().getItemQualityId());
    }
}