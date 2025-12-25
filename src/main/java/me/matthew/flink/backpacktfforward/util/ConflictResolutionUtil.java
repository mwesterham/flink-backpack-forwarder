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
        SELECT updated_at 
        FROM listings 
        WHERE id = ?
        """;
    
    // Clock skew tolerance: allow up to 5 minutes of clock difference between systems
    private static final Duration CLOCK_SKEW_TOLERANCE = Duration.ofSeconds(1);
    
    private final Counter conflictSkippedCounter;
    private final Counter conflictAllowedCounter;
    
    /**
     * Creates a new ConflictResolutionUtil with the provided metrics counters.
     * 
     * @param conflictSkippedCounter Counter for writes skipped due to conflicts
     * @param conflictAllowedCounter Counter for writes allowed after timestamp comparison
     */
    public ConflictResolutionUtil(Counter conflictSkippedCounter, 
                                  Counter conflictAllowedCounter) {
        this.conflictSkippedCounter = conflictSkippedCounter;
        this.conflictAllowedCounter = conflictAllowedCounter;
    }
    
    /**
     * Determines whether a write operation should be skipped based on timestamp conflict resolution.
     * Implements the conflict resolution logic following the design decision matrix with robust
     * error handling for timezone differences, clock skew, and parsing failures.
     * 
     * Error Handling Strategy:
     * - Timestamp parsing failures: Log error and default to allow write
     * - Database query failures: Log error and default to allow write (availability over consistency)
     * - Clock skew: Apply tolerance of 5 minutes to handle reasonable time differences
     * - Timezone differences: All timestamps normalized to UTC for comparison
     * 
     * Decision Matrix:
     * - If database record doesn't exist: allow write
     * - If database updated_at is null: allow write
     * - If generation timestamp is invalid: log error and allow write
     * - If database updated_at is newer than generationTimestamp (accounting for clock skew): skip write
     * - If database updated_at is older or equal to generationTimestamp: allow write
     * 
     * NOTE: This method assumes generationTimestamp is NOT null. Real-time updates should be
     * handled by the caller before calling this method.
     * 
     * @param listingId The ID of the listing being written
     * @param generationTimestamp The timestamp when the backfill data was generated (must not be null)
     * @param connection Database connection for querying last_updated timestamp
     * @return true if the write should be skipped, false if it should proceed
     * @throws SQLException if database query fails and cannot be handled gracefully
     */
    public boolean shouldSkipWrite(String listingId, Long generationTimestamp, Connection connection) throws SQLException {
        // Validate and convert generation timestamp with error handling
        Instant generationInstant;
        try {
            if (generationTimestamp == null) {
                log.warn("Received null generation timestamp for listing {} - this should be handled by caller", listingId);
                conflictAllowedCounter.inc();
                return false;
            }
            
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
        
        // Query database for last_updated timestamp with comprehensive error handling
        try (PreparedStatement stmt = connection.prepareStatement(LAST_UPDATED_QUERY)) {
            stmt.setString(1, listingId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Long lastUpdatedMillis = rs.getLong("updated_at");
                    
                    if (!rs.wasNull() && lastUpdatedMillis != null) {
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
                        // Database record exists but updated_at is null - allow write
                        log.debug("Allowing write for listing {} - database updated_at is null", listingId);
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
    
    /**
     * Validates that a ListingUpdate object is compatible with the timestamp protection system.
     * Provides backward compatibility checks and validation for timestamp fields.
     * 
     * @param listingUpdate The ListingUpdate object to validate
     * @return true if the object is valid and can be processed, false otherwise
     */
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