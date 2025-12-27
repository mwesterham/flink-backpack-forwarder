package me.matthew.flink.backpacktfforward.util;

/**
 * Interface for conflict resolution requests containing all necessary timestamp information
 * for determining whether a write operation should be skipped to prevent stale data overwrites.
 */
public interface ConflictResolutionRequest {
    
    /**
     * Gets the listing ID for the conflict resolution check.
     * @return the listing ID
     */
    String getListingId();
    
    /**
     * Gets the generation timestamp for backfill conflict resolution.
     * This is used to determine if the update is from backfill data.
     * @return generation timestamp in milliseconds, or null for real-time updates
     */
    Long getGenerationTimestamp();
    
    /**
     * Gets the listed_at timestamp from the incoming update.
     * @return listed_at timestamp in milliseconds
     */
    Long getListedAt();
    
    /**
     * Gets the bumped_at timestamp from the incoming update.
     * @return bumped_at timestamp in milliseconds
     */
    Long getBumpedAt();
}