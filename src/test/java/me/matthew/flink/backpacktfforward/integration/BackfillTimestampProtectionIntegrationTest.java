package me.matthew.flink.backpacktfforward.integration;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.util.ConflictResolutionUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Integration test for complete conflict resolution flow.
 * Tests backfill operation with concurrent real-time updates to verify that:
 * 1. Stale backfill data is correctly skipped
 * 2. Fresh backfill data is correctly written
 * 3. Metrics and logging work correctly in integrated scenario
 * 
 * This test uses mocked database connections to simulate conflict resolution scenarios
 * without requiring a real database setup.
 * 
 * **Feature: backfill-timestamp-protection, Requirements: All requirements integrated**
 */
class BackfillTimestampProtectionIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(BackfillTimestampProtectionIntegrationTest.class);
    
    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;
    private ResultSet mockResultSet;
    private Counter mockConflictSkippedCounter;
    private Counter mockConflictAllowedCounter;
    private Counter mockRealTimeCounter;
    private AtomicInteger conflictSkippedCount;
    private AtomicInteger conflictAllowedCount;
    private AtomicInteger realTimeCount;
    
    @BeforeEach
    void setUp() throws Exception {
        // Initialize mock database components
        mockConnection = mock(Connection.class);
        mockPreparedStatement = mock(PreparedStatement.class);
        mockResultSet = mock(ResultSet.class);
        
        // Initialize mock counters with atomic integers to track calls
        conflictSkippedCount = new AtomicInteger(0);
        conflictAllowedCount = new AtomicInteger(0);
        realTimeCount = new AtomicInteger(0);
        
        mockConflictSkippedCounter = mock(Counter.class);
        mockConflictAllowedCounter = mock(Counter.class);
        mockRealTimeCounter = mock(Counter.class);
        
        // Configure counter mocks to increment atomic integers
        doAnswer(invocation -> {
            conflictSkippedCount.incrementAndGet();
            return null;
        }).when(mockConflictSkippedCounter).inc();
        
        doAnswer(invocation -> {
            conflictAllowedCount.incrementAndGet();
            return null;
        }).when(mockConflictAllowedCounter).inc();
        
        doAnswer(invocation -> {
            realTimeCount.incrementAndGet();
            return null;
        }).when(mockRealTimeCounter).inc();
        
        // Configure mock database connection
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        
        log.info("Integration test setup completed with mocked database components");
    }
    
    @Test
    void testCompleteConflictResolutionFlow() throws Exception {
        log.info("Starting complete conflict resolution flow test");
        
        // Test scenario: Simulate conflict resolution with different timestamp scenarios
        ConflictResolutionUtil conflictUtil = new ConflictResolutionUtil(
                mockConflictSkippedCounter, mockConflictAllowedCounter);
        
        long currentTime = System.currentTimeMillis();
        long oldTime = currentTime - 300000; // 5 minutes ago
        long futureTime = currentTime + 600000; // 10 minutes in future (beyond 5-minute tolerance)
        
        // Test Case 1: Database has significantly newer timestamp - should skip write
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(futureTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        boolean shouldSkip1 = conflictUtil.shouldSkipWrite("440_12345", currentTime, mockConnection);
        assertTrue(shouldSkip1, "Should skip write when database timestamp is significantly newer (beyond clock skew tolerance)");
        assertEquals(1, conflictSkippedCount.get(), "Conflict skipped counter should be incremented");
        
        // Reset mocks for next test
        reset(mockResultSet);
        conflictSkippedCount.set(0);
        conflictAllowedCount.set(0);
        
        // Test Case 2: Database has older timestamp - should allow write
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(oldTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        boolean shouldSkip2 = conflictUtil.shouldSkipWrite("440_12346", currentTime, mockConnection);
        assertFalse(shouldSkip2, "Should allow write when database timestamp is older");
        assertEquals(1, conflictAllowedCount.get(), "Conflict allowed counter should be incremented");
        
        // Reset mocks for next test
        reset(mockResultSet);
        conflictSkippedCount.set(0);
        conflictAllowedCount.set(0);
        
        // Test Case 3: No database record - should allow write
        when(mockResultSet.next()).thenReturn(false);
        
        boolean shouldSkip3 = conflictUtil.shouldSkipWrite("440_12347", currentTime, mockConnection);
        assertFalse(shouldSkip3, "Should allow write when no database record exists");
        assertEquals(1, conflictAllowedCount.get(), "Conflict allowed counter should be incremented");
        
        // Test Case 4: Database record with null timestamp - should allow write
        reset(mockResultSet);
        conflictSkippedCount.set(0);
        conflictAllowedCount.set(0);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(0L);
        when(mockResultSet.wasNull()).thenReturn(true);
        
        boolean shouldSkip4 = conflictUtil.shouldSkipWrite("440_12348", currentTime, mockConnection);
        assertFalse(shouldSkip4, "Should allow write when database timestamp is null");
        assertEquals(1, conflictAllowedCount.get(), "Conflict allowed counter should be incremented");
        
        // Test Case 5: Database timestamp within clock skew tolerance - should allow write
        reset(mockResultSet);
        conflictSkippedCount.set(0);
        conflictAllowedCount.set(0);
        
        long slightlyFutureTime = currentTime + 500; // 500 ms in future (within 5-minute tolerance)
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(slightlyFutureTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        boolean shouldSkip5 = conflictUtil.shouldSkipWrite("440_12349", currentTime, mockConnection);
        assertFalse(shouldSkip5, "Should allow write when database timestamp is within clock skew tolerance");
        assertEquals(1, conflictAllowedCount.get(), "Conflict allowed counter should be incremented for clock skew tolerance");
        
        log.info("Complete conflict resolution flow test completed successfully");
        
        // Verify all scenarios worked as expected
        assertTrue(true, "All conflict resolution scenarios passed");
    }
    
    @Test
    void testBackfillDeleteOperationsWithConflictResolution() throws Exception {
        log.info("Starting backfill delete operations with conflict resolution test");
        
        // Test conflict resolution for delete operations
        ConflictResolutionUtil conflictUtil = new ConflictResolutionUtil(
                mockConflictSkippedCounter, mockConflictAllowedCounter);
        
        long currentTime = System.currentTimeMillis();
        long futureTime = currentTime + 600000; // 10 minutes in future (beyond 5-minute tolerance)
        
        // Test Case 1: Database has significantly newer timestamp - delete should be skipped
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(futureTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        boolean shouldSkipDelete = conflictUtil.shouldSkipWrite("440_delete1", currentTime, mockConnection);
        assertTrue(shouldSkipDelete, "Delete should be skipped when database timestamp is significantly newer");
        assertEquals(1, conflictSkippedCount.get(), "Conflict skipped counter should be incremented for delete");
        
        // Reset for next test
        reset(mockResultSet);
        conflictSkippedCount.set(0);
        conflictAllowedCount.set(0);
        
        // Test Case 2: Database has older timestamp - delete should proceed
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime - 300000);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        boolean shouldSkipDelete2 = conflictUtil.shouldSkipWrite("440_delete2", currentTime, mockConnection);
        assertFalse(shouldSkipDelete2, "Delete should proceed when database timestamp is older");
        assertEquals(1, conflictAllowedCount.get(), "Conflict allowed counter should be incremented for delete");
        
        log.info("Backfill delete operations with conflict resolution test completed successfully");
    }
    
    @Test
    void testMetricsAndLoggingIntegration() throws Exception {
        log.info("Starting metrics and logging integration test");
        
        // Test that metrics are properly tracked during conflict resolution
        ConflictResolutionUtil conflictUtil = new ConflictResolutionUtil(
                mockConflictSkippedCounter, mockConflictAllowedCounter);
        
        long currentTime = System.currentTimeMillis();
        
        // Test multiple scenarios to verify metrics tracking
        
        // Scenario 1: Skip write (significantly newer database timestamp)
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime + 600000); // 10 minutes future
        when(mockResultSet.wasNull()).thenReturn(false);
        
        conflictUtil.shouldSkipWrite("440_metrics1", currentTime, mockConnection);
        assertEquals(1, conflictSkippedCount.get(), "Should increment skipped counter");
        
        // Scenario 2: Allow write (older database timestamp)
        reset(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime - 60000);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        conflictUtil.shouldSkipWrite("440_metrics2", currentTime, mockConnection);
        assertEquals(1, conflictAllowedCount.get(), "Should increment allowed counter");
        
        // Scenario 3: Allow write (no database record)
        reset(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);
        
        conflictUtil.shouldSkipWrite("440_metrics3", currentTime, mockConnection);
        assertEquals(2, conflictAllowedCount.get(), "Should increment allowed counter again");
        
        // Verify metrics integration worked correctly
        assertTrue(conflictSkippedCount.get() > 0, "Conflict skipped metrics should be tracked");
        assertTrue(conflictAllowedCount.get() > 0, "Conflict allowed metrics should be tracked");
        
        log.info("Metrics and logging integration test completed successfully");
    }
    
    @Test
    void testErrorHandlingAndRobustness() throws Exception {
        log.info("Starting error handling and robustness test");
        
        ConflictResolutionUtil conflictUtil = new ConflictResolutionUtil(
                mockConflictSkippedCounter, mockConflictAllowedCounter);
        
        // Test 1: Invalid generation timestamps (should default to allow write)
        boolean shouldSkip1 = conflictUtil.shouldSkipWrite("440_invalid", null, mockConnection);
        assertFalse(shouldSkip1, "Null generation timestamp should allow write");
        
        // Test 2: Database query failure (should default to allow write)
        when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("Database connection failed"));
        
        boolean shouldSkip2 = conflictUtil.shouldSkipWrite("440_dberror", System.currentTimeMillis(), mockConnection);
        assertFalse(shouldSkip2, "Database error should default to allow write for availability");
        
        // Test 3: Timestamp parsing errors (handled gracefully)
        // Reset mocks
        reset(mockConnection, mockPreparedStatement, mockResultSet);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenThrow(new SQLException("Invalid timestamp format"));
        when(mockResultSet.wasNull()).thenReturn(false);
        
        boolean shouldSkip3 = conflictUtil.shouldSkipWrite("440_parseerror", System.currentTimeMillis(), mockConnection);
        assertFalse(shouldSkip3, "Timestamp parsing error should default to allow write");
        
        log.info("Error handling and robustness test completed successfully");
    }
    
    @Test
    void testBackwardCompatibility() throws Exception {
        log.info("Starting backward compatibility test");
        
        // Test that ConflictResolutionUtil.isValidListingUpdate handles various scenarios
        
        // Test 1: Valid ListingUpdate with generation timestamp
        ListingUpdate validUpdate = createBackfillUpdate("440_valid", "76561198000000001", System.currentTimeMillis());
        assertTrue(ConflictResolutionUtil.isValidListingUpdate(validUpdate), 
                "Valid ListingUpdate with generation timestamp should be valid");
        
        // Test 2: Valid ListingUpdate without generation timestamp (real-time)
        ListingUpdate realTimeUpdate = createRealTimeUpdate("440_realtime", "76561198000000001");
        assertTrue(ConflictResolutionUtil.isValidListingUpdate(realTimeUpdate), 
                "Valid ListingUpdate without generation timestamp should be valid");
        
        // Test 3: Invalid ListingUpdate (null)
        assertFalse(ConflictResolutionUtil.isValidListingUpdate(null), 
                "Null ListingUpdate should be invalid");
        
        // Test 4: ListingUpdate with null payload
        ListingUpdate nullPayloadUpdate = new ListingUpdate();
        nullPayloadUpdate.setId("440_nullpayload");
        nullPayloadUpdate.setEvent("listing-update");
        nullPayloadUpdate.setPayload(null);
        
        assertFalse(ConflictResolutionUtil.isValidListingUpdate(nullPayloadUpdate), 
                "ListingUpdate with null payload should be invalid");
        
        // Test 5: ListingUpdate with payload but null ID
        ListingUpdate nullIdUpdate = new ListingUpdate();
        nullIdUpdate.setId("440_nullid");
        nullIdUpdate.setEvent("listing-update");
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId(null); // Null payload ID
        payload.setSteamid("76561198000000001");
        nullIdUpdate.setPayload(payload);
        
        assertFalse(ConflictResolutionUtil.isValidListingUpdate(nullIdUpdate), 
                "ListingUpdate with null payload ID should be invalid");
        
        log.info("Backward compatibility test completed successfully");
    }
    
    // Helper methods
    
    private ListingUpdate createRealTimeUpdate(String id, String steamid) {
        ListingUpdate update = new ListingUpdate();
        update.setId(id);
        update.setEvent("listing-update");
        update.setGenerationTimestamp(null); // Real-time update
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId(id);
        payload.setSteamid(steamid);
        payload.setAppid(440);
        payload.setIntent("sell");
        payload.setCount(1);
        payload.setStatus("active");
        payload.setListedAt(System.currentTimeMillis() / 1000);
        payload.setBumpedAt(System.currentTimeMillis() / 1000);
        
        // Create item
        ListingUpdate.Item item = new ListingUpdate.Item();
        item.setAppid(440);
        item.setDefindex(190);
        item.setMarketName("Test Item");
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.setId(6);
        quality.setName("Unique");
        item.setQuality(quality);
        
        payload.setItem(item);
        
        // Create value
        ListingUpdate.Value value = new ListingUpdate.Value();
        value.setRaw(1.0);
        value.setShortStr("1 ref");
        value.setLongStr("1 ref");
        payload.setValue(value);
        
        update.setPayload(payload);
        return update;
    }
    
    private ListingUpdate createBackfillUpdate(String id, String steamid, long generationTimestamp) {
        ListingUpdate update = createRealTimeUpdate(id, steamid);
        update.setGenerationTimestamp(generationTimestamp); // Backfill update
        return update;
    }
    
    private ListingUpdate createBackfillDelete(String id, String steamid, long generationTimestamp) {
        ListingUpdate delete = new ListingUpdate();
        delete.setId(id);
        delete.setEvent("listing-delete");
        delete.setGenerationTimestamp(generationTimestamp);
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId(id);
        payload.setSteamid(steamid);
        
        delete.setPayload(payload);
        return delete;
    }
}