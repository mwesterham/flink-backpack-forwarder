package me.matthew.flink.backpacktfforward.config;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for managing BackpackTF API configuration from environment variables.
 * Handles reading and validation of BackpackTF API token and related parameters.
 * Follows existing patterns established by KafkaConfiguration and SteamApiConfiguration.
 */
@Slf4j
public class BackpackTfApiConfiguration {

    // Required environment variables
    private static final String API_TOKEN_ENV = "BACKPACK_TF_API_TOKEN";
    
    // Optional environment variables for BackpackTF API configuration
    private static final String API_TIMEOUT_ENV = "BACKPACK_TF_API_TIMEOUT_SECONDS";
    private static final String SNAPSHOT_RATE_LIMIT_ENV = "BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS";
    private static final String GET_LISTING_RATE_LIMIT_ENV = "BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS";
    
    // Default values
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_SNAPSHOT_RATE_LIMIT_SECONDS = 10; // 6 requests per minute = 10 seconds between requests
    private static final int DEFAULT_GET_LISTING_RATE_LIMIT_SECONDS = 1; // 60 requests per minute = 1 second between requests

    /**
     * Reads BackpackTF API token from environment variable.
     * @return BackpackTF API token for authentication
     * @throws IllegalArgumentException if BACKPACK_TF_API_TOKEN is not set or empty
     */
    public static String getApiToken() {
        String apiToken = System.getenv(API_TOKEN_ENV);
        if (apiToken == null || apiToken.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty. " +
                    "Please obtain a BackpackTF API token from https://backpack.tf/developer and set it as an environment variable.", 
                    API_TOKEN_ENV)
            );
        }
        return apiToken.trim();
    }

    /**
     * Reads BackpackTF API timeout configuration from environment variable.
     * @return Timeout in seconds, or default value if not configured
     * @throws IllegalArgumentException if BACKPACK_TF_API_TIMEOUT_SECONDS is set but not a valid number
     */
    public static int getApiTimeoutSeconds() {
        String timeoutStr = System.getenv(API_TIMEOUT_ENV);
        if (timeoutStr == null || timeoutStr.trim().isEmpty()) {
            return DEFAULT_TIMEOUT_SECONDS;
        }
        
        try {
            int timeout = Integer.parseInt(timeoutStr.trim());
            if (timeout <= 0) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a positive integer, got: %s", 
                        API_TIMEOUT_ENV, timeoutStr)
                );
            }
            return timeout;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s must be a valid integer, got: %s", 
                    API_TIMEOUT_ENV, timeoutStr), e
            );
        }
    }

    /**
     * Reads BackpackTF snapshot API rate limiting configuration from environment variable.
     * @return Rate limit delay in seconds for snapshot API, or default value if not configured
     * @throws IllegalArgumentException if BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS is set but not a valid number
     */
    public static int getSnapshotRateLimitSeconds() {
        String rateLimitStr = System.getenv(SNAPSHOT_RATE_LIMIT_ENV);
        if (rateLimitStr == null || rateLimitStr.trim().isEmpty()) {
            return DEFAULT_SNAPSHOT_RATE_LIMIT_SECONDS;
        }
        
        try {
            int rateLimit = Integer.parseInt(rateLimitStr.trim());
            if (rateLimit < 0) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a non-negative integer, got: %s", 
                        SNAPSHOT_RATE_LIMIT_ENV, rateLimitStr)
                );
            }
            return rateLimit;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s must be a valid integer, got: %s", 
                    SNAPSHOT_RATE_LIMIT_ENV, rateLimitStr), e
            );
        }
    }

    /**
     * Reads BackpackTF getListing API rate limiting configuration from environment variable.
     * @return Rate limit delay in seconds for getListing API, or default value if not configured
     * @throws IllegalArgumentException if BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS is set but not a valid number
     */
    public static int getGetListingRateLimitSeconds() {
        String rateLimitStr = System.getenv(GET_LISTING_RATE_LIMIT_ENV);
        if (rateLimitStr == null || rateLimitStr.trim().isEmpty()) {
            return DEFAULT_GET_LISTING_RATE_LIMIT_SECONDS;
        }
        
        try {
            int rateLimit = Integer.parseInt(rateLimitStr.trim());
            if (rateLimit < 0) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a non-negative integer, got: %s", 
                        GET_LISTING_RATE_LIMIT_ENV, rateLimitStr)
                );
            }
            return rateLimit;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s must be a valid integer, got: %s", 
                    GET_LISTING_RATE_LIMIT_ENV, rateLimitStr), e
            );
        }
    }

    /**
     * Validates that all required BackpackTF API configuration is present and valid.
     * @throws IllegalArgumentException if any required configuration is missing or invalid
     */
    public static void validateConfiguration() {
        log.info("Validating BackpackTF API configuration...");
        
        try {
            // Validate required environment variables by calling getters
            String apiToken = getApiToken();
            int timeoutSeconds = getApiTimeoutSeconds();
            int snapshotRateLimitSeconds = getSnapshotRateLimitSeconds();
            int getListingRateLimitSeconds = getGetListingRateLimitSeconds();
            
            log.info("BackpackTF API configuration validated successfully:");
            log.info("  API Token: {} (length: {} characters)", 
                apiToken.substring(0, Math.min(8, apiToken.length())) + "***", apiToken.length());
            log.info("  Timeout: {} seconds", timeoutSeconds);
            log.info("  Snapshot API Rate Limit: {} seconds between requests", snapshotRateLimitSeconds);
            log.info("  GetListing API Rate Limit: {} seconds between requests", getListingRateLimitSeconds);
            
        } catch (IllegalArgumentException e) {
            log.error("BackpackTF API configuration validation failed: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Checks if BackpackTF API configuration is present without throwing exceptions.
     * Useful for conditional initialization or feature flags.
     * @return true if BackpackTF API token is configured, false otherwise
     */
    public static boolean isConfigured() {
        try {
            getApiToken();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}