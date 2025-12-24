package me.matthew.flink.backpacktfforward.config;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for managing Steam API configuration from environment variables.
 * Handles reading and validation of Steam API key and related parameters.
 * Follows existing patterns established by KafkaConfiguration.
 */
@Slf4j
public class SteamApiConfiguration {

    // Required environment variables
    private static final String STEAM_API_KEY_ENV = "STEAM_API_KEY";
    
    // Optional environment variables for Steam API configuration
    private static final String STEAM_API_TIMEOUT_ENV = "STEAM_API_TIMEOUT_SECONDS";
    private static final String STEAM_API_RATE_LIMIT_ENV = "STEAM_API_RATE_LIMIT_SECONDS";
    
    // Default values
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_RATE_LIMIT_SECONDS = 10; // 6 requests per minute = 10 seconds between requests

    /**
     * Reads Steam API key from environment variable.
     * @return Steam API key for authentication
     * @throws IllegalArgumentException if STEAM_API_KEY is not set or empty
     */
    public static String getSteamApiKey() {
        String apiKey = System.getenv(STEAM_API_KEY_ENV);
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty. " +
                    "Please obtain a Steam API key from https://steamcommunity.com/dev/apikey and set it as an environment variable.", 
                    STEAM_API_KEY_ENV)
            );
        }
        return apiKey.trim();
    }

    /**
     * Reads Steam API timeout configuration from environment variable.
     * @return Timeout in seconds, or default value if not configured
     * @throws IllegalArgumentException if STEAM_API_TIMEOUT_SECONDS is set but not a valid number
     */
    public static int getSteamApiTimeoutSeconds() {
        String timeoutStr = System.getenv(STEAM_API_TIMEOUT_ENV);
        if (timeoutStr == null || timeoutStr.trim().isEmpty()) {
            return DEFAULT_TIMEOUT_SECONDS;
        }
        
        try {
            int timeout = Integer.parseInt(timeoutStr.trim());
            if (timeout <= 0) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a positive integer, got: %s", 
                        STEAM_API_TIMEOUT_ENV, timeoutStr)
                );
            }
            return timeout;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s must be a valid integer, got: %s", 
                    STEAM_API_TIMEOUT_ENV, timeoutStr), e
            );
        }
    }

    /**
     * Reads Steam API rate limiting configuration from environment variable.
     * @return Rate limit delay in seconds, or default value if not configured
     * @throws IllegalArgumentException if STEAM_API_RATE_LIMIT_SECONDS is set but not a valid number
     */
    public static int getSteamApiRateLimitSeconds() {
        String rateLimitStr = System.getenv(STEAM_API_RATE_LIMIT_ENV);
        if (rateLimitStr == null || rateLimitStr.trim().isEmpty()) {
            return DEFAULT_RATE_LIMIT_SECONDS;
        }
        
        try {
            int rateLimit = Integer.parseInt(rateLimitStr.trim());
            if (rateLimit < 0) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a non-negative integer, got: %s", 
                        STEAM_API_RATE_LIMIT_ENV, rateLimitStr)
                );
            }
            return rateLimit;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s must be a valid integer, got: %s", 
                    STEAM_API_RATE_LIMIT_ENV, rateLimitStr), e
            );
        }
    }

    /**
     * Validates that all required Steam API configuration is present and valid.
     * @throws IllegalArgumentException if any required configuration is missing or invalid
     */
    public static void validateConfiguration() {
        log.info("Validating Steam API configuration...");
        
        try {
            // Validate required environment variables by calling getters
            String apiKey = getSteamApiKey();
            int timeoutSeconds = getSteamApiTimeoutSeconds();
            int rateLimitSeconds = getSteamApiRateLimitSeconds();
            
            log.info("Steam API configuration validated successfully:");
            log.info("  API Key: {} (length: {} characters)", 
                apiKey.substring(0, Math.min(8, apiKey.length())) + "***", apiKey.length());
            log.info("  Timeout: {} seconds", timeoutSeconds);
            log.info("  Rate Limit: {} seconds between requests", rateLimitSeconds);
            
        } catch (IllegalArgumentException e) {
            log.error("Steam API configuration validation failed: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Checks if Steam API configuration is present without throwing exceptions.
     * Useful for conditional initialization or feature flags.
     * @return true if Steam API key is configured, false otherwise
     */
    public static boolean isConfigured() {
        try {
            getSteamApiKey();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}