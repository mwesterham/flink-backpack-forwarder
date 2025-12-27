package me.matthew.flink.backpacktfforward.util;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.metrics.Counter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.DateTimeException;
import java.time.Duration;

/**
 * Utility class for timestamp-based conflict resolution in sink components.
 * Implements the logic to prevent stale backfill data from overwriting newer real-time updates
 * by comparing generation timestamps with database last_updated timestamps.
 * 
 * Provides robust error handling for timezone differences, clock skew, timestamp parsing failures,
 * and database query failures with fallback behavior to maintain system availability.
 */
@Slf4j
public class ConflictResolutionUtil {
    
    private static final String LAST_UPDATED_QUERY = """
        SELECT updated_at, listed_at, bumped_at 
        FROM listings 
        WHERE id = ?
        """;
    
    // Clock skew tolerance: allow up to 5 minutes of clock difference between systems
    private static final Duration CLOCK_SKEW_TOLERANCE = Duration.ofSeconds(1);
    
    private final Counter conflictSkippedCounter;
    private final Counter conflictAllowedCounter;

    public ConflictResolutionUtil(Counter conflictSkippedCounter, 
                                  Counter conflictAllowedCounter) {
        this.conflictSkippedCounter = conflictSkippedCounter;
        this.conflictAllowedCounter = conflictAllowedCounter;
    }

    public boolean shouldSkipWrite(String listingId, Long generationTimestamp, Connection connection) throws SQLException {
        // Create a simple request object for backward compatibility
        ConflictResolutionRequest request = new ConflictResolutionRequest() {
            @Override
            public String getListingId() { return listingId; }
            @Override
            public Long getGenerationTimestamp() { return generationTimestamp; }
            @Override
            public Long getListedAt() { return null; }
            @Override
            public Long getBumpedAt() { return null; }
        };
        
        return shouldSkipWrite(request, connection);
    }

    public boolean shouldSkipWrite(ConflictResolutionRequest request, Connection connection) throws SQLException {
        String listingId = request.getListingId();
        Long generationTimestamp = request.getGenerationTimestamp();
        Long incomingListedAt = request.getListedAt();
        Long incomingBumpedAt = request.getBumpedAt();
        
        // Query database for timestamps with comprehensive error handling
        try (PreparedStatement stmt = connection.prepareStatement(LAST_UPDATED_QUERY)) {
            stmt.setString(1, listingId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Long lastUpdatedMillis = rs.getLong("updated_at");
                    boolean lastUpdatedWasNull = rs.wasNull();
                    
                    Long dbListedAt = rs.getLong("listed_at");
                    boolean listedAtWasNull = rs.wasNull();
                    
                    Long dbBumpedAt = rs.getLong("bumped_at");
                    boolean bumpedAtWasNull = rs.wasNull();
                    
                    // Handle null values from database
                    if (lastUpdatedWasNull) {
                        lastUpdatedMillis = null;
                    }
                    if (listedAtWasNull) {
                        dbListedAt = null;
                    }
                    if (bumpedAtWasNull) {
                        dbBumpedAt = null;
                    }
                    
                    // Check listed_at and bumped_at timestamps first (if available)
                    if (incomingListedAt != null && dbListedAt != null) {
                        if (incomingListedAt < dbListedAt) {
                            conflictSkippedCounter.inc();
                            return true;
                        }
                    }
                    
                    if (incomingBumpedAt != null && dbBumpedAt != null) {
                        if (incomingBumpedAt < dbBumpedAt) {
                            conflictSkippedCounter.inc();
                            return true;
                        }
                    }
                    
                    if (generationTimestamp == null) {
                        log.info("Received null generation timestamp for listing {} - this should be handled by caller", listingId);
                        conflictAllowedCounter.inc();
                        return false;
                    }
                    // Only now validate and convert generation timestamp since we need it for comparison
                    Instant generationInstant;
                    try {
                        generationInstant = Instant.ofEpochMilli(generationTimestamp);
                        
                        // Validate timestamp is reasonable (not too far in future/past)
                        Instant now = Instant.now();
                        Duration ageFromNow = Duration.between(generationInstant, now).abs();
                        if (ageFromNow.toDays() > 365) {
                            log.warn("Generation timestamp {} for listing {} is more than 1 year from current time - proceeding with write but this may indicate a data issue", 
                                    generationInstant, listingId);
                        }
                        
                    } catch (DateTimeException | ArithmeticException e) {
                        log.error("Failed to parse generation timestamp {} for listing {} - defaulting to allow write", 
                                generationTimestamp, listingId, e);
                        conflictAllowedCounter.inc();
                        return false;
                    }
                    
                    Instant lastUpdatedInstant;
                    try {
                        lastUpdatedInstant = Instant.ofEpochMilli(lastUpdatedMillis);
                    } catch (DateTimeException e) {
                        log.error("Failed to parse database timestamp {} for listing {} - defaulting to allow write", 
                                lastUpdatedMillis, listingId, e);
                        conflictAllowedCounter.inc();
                        return false;
                    }
                    
                    // Apply clock skew tolerance: only skip if database timestamp is significantly newer
                    Duration timeDifference = Duration.between(generationInstant, lastUpdatedInstant);
                    boolean isSignificantlyNewer = timeDifference.compareTo(CLOCK_SKEW_TOLERANCE) > 0;
                    
                    if (isSignificantlyNewer) {
                        log.info("Skipping write for listing {} - database last_updated {} is significantly newer than generation_timestamp {} (difference: {} seconds, tolerance: {} seconds)", 
                                listingId, lastUpdatedInstant, generationInstant, 
                                timeDifference.getSeconds(), CLOCK_SKEW_TOLERANCE.getSeconds());
                        conflictSkippedCounter.inc();
                        return true;
                    } else {
                        if (timeDifference.getSeconds() > 0) {
                            log.debug("Allowing write for listing {} - database last_updated {} is newer than generation_timestamp {} but within clock skew tolerance (difference: {} seconds)", 
                                    listingId, lastUpdatedInstant, generationInstant, timeDifference.getSeconds());
                        } else {
                            log.debug("Allowing write for listing {} - generation_timestamp {} is newer than or equal to database last_updated {}", 
                                    listingId, generationInstant, lastUpdatedInstant);
                        }
                        conflictAllowedCounter.inc();
                        return false;
                    }
                } else {
                    // No existing database record - allow write
                    log.debug("Allowing write for listing {} - no existing database record", listingId);
                    conflictAllowedCounter.inc();
                    return false;
                }
            }
        } catch (SQLException e) {
            // Log error and default to allowing write (availability over consistency)
            log.error("Database query failed for listing {} conflict resolution - defaulting to allow write to maintain system availability", 
                    listingId, e);
            conflictAllowedCounter.inc();
            // Don't re-throw SQLException here - we want to continue processing
            return false;
        } catch (Exception e) {
            // Catch any other unexpected errors
            log.error("Unexpected error during conflict resolution for listing {} - defaulting to allow write", 
                    listingId, e);
            conflictAllowedCounter.inc();
            return false;
        }
    }
    
    public static boolean isValidListingUpdate(ListingUpdate listingUpdate) {
        if (listingUpdate == null) {
            return false;
        }
        
        if (listingUpdate.getPayload() == null || listingUpdate.getPayload().getId() == null) {
            return false;
        }
        
        // Validate generation timestamp if present
        if (listingUpdate.getGenerationTimestamp() != null) {
            try {
                Instant.ofEpochMilli(listingUpdate.getGenerationTimestamp());
            } catch (DateTimeException | ArithmeticException e) {
                log.warn("Invalid generation timestamp {} in ListingUpdate for listing {} - treating as real-time update", 
                        listingUpdate.getGenerationTimestamp(), listingUpdate.getPayload().getId());
                // Set to null to treat as real-time update
                listingUpdate.setGenerationTimestamp(null);
            }
        }
        
        return true;
    }
    
    /**
     * Safely converts a timestamp to Instant with error handling.
     * Used for robust timestamp parsing throughout the system.
     * 
     * @param timestampMillis Unix timestamp in milliseconds
     * @param context Context description for logging (e.g., "generation timestamp", "database timestamp")
     * @return Instant representation of the timestamp, or null if parsing fails
     */
    public static Instant safeTimestampToInstant(Long timestampMillis, String context) {
        if (timestampMillis == null) {
            return null;
        }
        
        try {
            return Instant.ofEpochMilli(timestampMillis);
        } catch (DateTimeException | ArithmeticException e) {
            log.error("Failed to parse {} timestamp: {} - returning null", context, timestampMillis, e);
            return null;
        }
    }
    
    /**
     * Checks if two timestamps are within the acceptable clock skew tolerance.
     * Used to handle timezone differences and clock synchronization issues.
     * 
     * @param timestamp1 First timestamp
     * @param timestamp2 Second timestamp
     * @return true if timestamps are within tolerance, false otherwise
     */
    public static boolean isWithinClockSkewTolerance(Instant timestamp1, Instant timestamp2) {
        if (timestamp1 == null || timestamp2 == null) {
            return true; // Null timestamps are always considered within tolerance
        }
        
        try {
            Duration difference = Duration.between(timestamp1, timestamp2).abs();
            return difference.compareTo(CLOCK_SKEW_TOLERANCE) <= 0;
        } catch (Exception e) {
            log.warn("Error calculating time difference between {} and {} - assuming within tolerance", 
                    timestamp1, timestamp2, e);
            return true;
        }
    }
}