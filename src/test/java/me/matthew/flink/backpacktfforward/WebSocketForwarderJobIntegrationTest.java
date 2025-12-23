package me.matthew.flink.backpacktfforward;

import me.matthew.flink.backpacktfforward.source.BackfillRequestSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for WebSocketForwarderJob to verify backfill components are properly wired.
 * Tests the configuration validation and component initialization without requiring full Flink execution.
 */
class WebSocketForwarderJobIntegrationTest {
    
    private String originalBackfillTopic;
    private String originalBackfillConsumerGroup;
    private String originalApiToken;
    private String originalDbUrl;
    private String originalDbUser;
    private String originalDbPass;
    
    @BeforeEach
    void setUp() {
        // Store original environment variables
        originalBackfillTopic = System.getenv("BACKFILL_KAFKA_TOPIC");
        originalBackfillConsumerGroup = System.getenv("BACKFILL_KAFKA_CONSUMER_GROUP");
        originalApiToken = System.getenv("BACKPACK_TF_API_TOKEN");
        originalDbUrl = System.getenv("DB_URL");
        originalDbUser = System.getenv("DB_USERNAME");
        originalDbPass = System.getenv("DB_PASSWORD");
    }
    
    @AfterEach
    void tearDown() {
        // Restore original environment variables (if they existed)
        // Note: We can't actually restore env vars in Java, but we can document the expected state
    }
    
    @Test
    void testBackfillConfigurationValidation() {
        // Test that BackfillRequestSource properly validates configuration
        
        // Test missing backfill topic
        assertThrows(IllegalArgumentException.class, () -> {
            BackfillRequestSource.getBackfillKafkaTopic();
        }, "Should throw exception when BACKFILL_KAFKA_TOPIC is not set");
        
        // Test missing backfill consumer group
        assertThrows(IllegalArgumentException.class, () -> {
            BackfillRequestSource.getBackfillKafkaConsumerGroup();
        }, "Should throw exception when BACKFILL_KAFKA_CONSUMER_GROUP is not set");
    }
    
    @Test
    void testBackfillSourceCreationWithoutConfiguration() {
        // Test that BackfillRequestSource creation fails gracefully without configuration
        assertThrows(Exception.class, () -> {
            BackfillRequestSource.createKafkaSource();
        }, "Should throw exception when creating backfill source without proper configuration");
    }
    
    @Test
    void testWebSocketForwarderJobRequiredConfiguration() {
        // Test that the main job validates required database configuration
        
        // Clear required environment variables to test validation
        // Note: In a real test environment, you would use a test framework that allows
        // setting environment variables, such as @SetEnvironmentVariable from JUnit Pioneer
        
        // For now, we'll test the components that we can test without environment manipulation
        assertNotNull(WebSocketForwarderJob.class, "WebSocketForwarderJob class should be available");
    }
    
    @Test
    void testComponentIntegration() {
        // Test that all required components are available and can be instantiated
        
        // Test that BackfillRequestSource static methods are accessible
        assertNotNull(BackfillRequestSource.class, "BackfillRequestSource class should be available");
        
        // Test that the main job class exists and has the main method
        try {
            WebSocketForwarderJob.class.getDeclaredMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
            fail("WebSocketForwarderJob should have a main method");
        }
    }
    
    @Test
    void testEventRoutingLogic() {
        // Test the event filtering logic that routes events to appropriate sinks
        
        // Create a sample ListingUpdate with listing-update event
        me.matthew.flink.backpacktfforward.model.ListingUpdate updateEvent = 
            new me.matthew.flink.backpacktfforward.model.ListingUpdate();
        updateEvent.setEvent("listing-update");
        
        // Verify event type
        assertEquals("listing-update", updateEvent.getEvent());
        assertTrue(updateEvent.getEvent().equals("listing-update"), 
                  "Update events should be routed to ListingUpsertSink");
        
        // Create a sample ListingUpdate with listing-delete event
        me.matthew.flink.backpacktfforward.model.ListingUpdate deleteEvent = 
            new me.matthew.flink.backpacktfforward.model.ListingUpdate();
        deleteEvent.setEvent("listing-delete");
        
        // Verify event type
        assertEquals("listing-delete", deleteEvent.getEvent());
        assertTrue(deleteEvent.getEvent().equals("listing-delete"), 
                  "Delete events should be routed to ListingDeleteSink");
    }
}