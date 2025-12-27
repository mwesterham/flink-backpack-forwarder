package me.matthew.flink.backpacktfforward.util;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.metrics.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test class for enhanced ConflictResolutionUtil with listed_at and bumped_at comparison logic.
 */
class ConflictResolutionUtilEnhancedTest {

    @Mock
    private Connection mockConnection;
    @Mock
    private PreparedStatement mockStatement;
    @Mock
    private ResultSet mockResultSet;
    @Mock
    private Counter mockSkippedCounter;
    @Mock
    private Counter mockAllowedCounter;

    private ConflictResolutionUtil conflictUtil;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        conflictUtil = new ConflictResolutionUtil(mockSkippedCounter, mockAllowedCounter);
        
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(mockResultSet);
    }

    @Test
    void shouldSkipWrite_WhenIncomingListedAtIsOlder() throws SQLException {
        // Arrange
        long currentTime = System.currentTimeMillis();
        long olderTime = currentTime - 10000; // 10 seconds older
        
        ListingUpdate listingUpdate = createListingUpdate("440_12345", currentTime, olderTime, currentTime);
        ConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(listingUpdate);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime);
        when(mockResultSet.getLong("listed_at")).thenReturn(currentTime);
        when(mockResultSet.getLong("bumped_at")).thenReturn(currentTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        // Act
        boolean shouldSkip = conflictUtil.shouldSkipWrite(request, mockConnection);
        
        // Assert
        assertTrue(shouldSkip, "Should skip write when incoming listed_at is older than database listed_at");
        verify(mockSkippedCounter).inc();
    }

    @Test
    void shouldSkipWrite_WhenIncomingBumpedAtIsOlder() throws SQLException {
        // Arrange
        long currentTime = System.currentTimeMillis();
        long olderTime = currentTime - 10000; // 10 seconds older
        
        ListingUpdate listingUpdate = createListingUpdate("440_12346", currentTime, currentTime, olderTime);
        ConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(listingUpdate);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime);
        when(mockResultSet.getLong("listed_at")).thenReturn(currentTime);
        when(mockResultSet.getLong("bumped_at")).thenReturn(currentTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        // Act
        boolean shouldSkip = conflictUtil.shouldSkipWrite(request, mockConnection);
        
        // Assert
        assertTrue(shouldSkip, "Should skip write when incoming bumped_at is older than database bumped_at");
        verify(mockSkippedCounter).inc();
    }

    @Test
    void shouldAllowWrite_WhenIncomingTimestampsAreNewer() throws SQLException {
        // Arrange
        long currentTime = System.currentTimeMillis();
        long olderTime = currentTime - 10000; // 10 seconds older
        
        ListingUpdate listingUpdate = createListingUpdate("440_12347", currentTime, currentTime, currentTime);
        ConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(listingUpdate);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(olderTime);
        when(mockResultSet.getLong("listed_at")).thenReturn(olderTime);
        when(mockResultSet.getLong("bumped_at")).thenReturn(olderTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        // Act
        boolean shouldSkip = conflictUtil.shouldSkipWrite(request, mockConnection);
        
        // Assert
        assertFalse(shouldSkip, "Should allow write when incoming timestamps are newer");
        verify(mockAllowedCounter).inc();
    }

    @Test
    void shouldAllowWrite_WhenDatabaseTimestampsAreNull() throws SQLException {
        // Arrange
        long currentTime = System.currentTimeMillis();
        
        ListingUpdate listingUpdate = createListingUpdate("440_12348", currentTime, currentTime, currentTime);
        ConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(listingUpdate);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(0L);
        when(mockResultSet.getLong("listed_at")).thenReturn(0L);
        when(mockResultSet.getLong("bumped_at")).thenReturn(0L);
        when(mockResultSet.wasNull()).thenReturn(true);
        
        // Act
        boolean shouldSkip = conflictUtil.shouldSkipWrite(request, mockConnection);
        
        // Assert
        assertFalse(shouldSkip, "Should allow write when database timestamps are null");
        verify(mockAllowedCounter).inc();
    }

    @Test
    void shouldFallbackToGenerationTimestamp_WhenListedAtAndBumpedAtAreNull() throws SQLException {
        // Arrange
        long currentTime = System.currentTimeMillis();
        
        ListingUpdate listingUpdate = createListingUpdate("440_12349", currentTime, null, null);
        ConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(listingUpdate);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime + 5000); // Database is newer
        when(mockResultSet.getLong("listed_at")).thenReturn(0L);
        when(mockResultSet.getLong("bumped_at")).thenReturn(0L);
        when(mockResultSet.wasNull()).thenReturn(false, true, true); // updated_at is not null, others are null
        
        // Act
        boolean shouldSkip = conflictUtil.shouldSkipWrite(request, mockConnection);
        
        // Assert
        assertTrue(shouldSkip, "Should fallback to generation timestamp comparison when listed_at and bumped_at are null");
        verify(mockSkippedCounter).inc();
    }

    private ListingUpdate createListingUpdate(String id, Long generationTimestamp, Long listedAt, Long bumpedAt) {
        ListingUpdate listingUpdate = new ListingUpdate();
        listingUpdate.setGenerationTimestamp(generationTimestamp);
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId(id);
        if (listedAt != null) {
            payload.setListedAt(listedAt);
        }
        if (bumpedAt != null) {
            payload.setBumpedAt(bumpedAt);
        }
        
        listingUpdate.setPayload(payload);
        return listingUpdate;
    }

    @Test
    void shouldNotProcessGenerationTimestamp_WhenListedAtComparison_Skips() throws SQLException {
        // Arrange - Use an invalid generation timestamp that would cause parsing error
        long currentTime = System.currentTimeMillis();
        long olderTime = currentTime - 10000; // 10 seconds older
        long invalidGenerationTimestamp = Long.MAX_VALUE; // This would cause DateTimeException if processed
        
        ListingUpdate listingUpdate = createListingUpdate("440_optimization", invalidGenerationTimestamp, olderTime, currentTime);
        ConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(listingUpdate);
        
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getLong("updated_at")).thenReturn(currentTime);
        when(mockResultSet.getLong("listed_at")).thenReturn(currentTime);
        when(mockResultSet.getLong("bumped_at")).thenReturn(currentTime);
        when(mockResultSet.wasNull()).thenReturn(false);
        
        // Act - This should skip due to listed_at comparison without processing generation timestamp
        boolean shouldSkip = conflictUtil.shouldSkipWrite(request, mockConnection);
        
        // Assert
        assertTrue(shouldSkip, "Should skip write based on listed_at comparison without processing invalid generation timestamp");
        verify(mockSkippedCounter).inc();
    }
}