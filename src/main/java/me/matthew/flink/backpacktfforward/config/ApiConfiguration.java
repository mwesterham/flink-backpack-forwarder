package me.matthew.flink.backpacktfforward.config;

import lombok.extern.slf4j.Slf4j;

/**
 * Centralized configuration validation for all API clients.
 * Provides a single entry point to validate all required API configurations.
 * Follows existing patterns established by KafkaConfiguration.
 */
@Slf4j
public class ApiConfiguration {

    /**
     * Validates all API configurations required for the backfill system.
     * This includes BackpackTF API, Steam API, Kafka, and database configurations.
     * 
     * @throws IllegalArgumentException if any required configuration is missing or invalid
     */
    public static void validateAllConfigurations() {
        log.info("Validating all API configurations for backfill system...");
        
        try {
            // Validate BackpackTF API configuration
            BackpackTfApiConfiguration.validateConfiguration();
            
            // Validate Steam API configuration
            SteamApiConfiguration.validateConfiguration();
            
            // Validate Kafka configuration
            KafkaConfiguration.validateConfiguration();
            
            log.info("All API configurations validated successfully");
            
        } catch (Exception e) {
            log.error("API configuration validation failed: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Checks if all required API configurations are present without throwing exceptions.
     * Useful for health checks or conditional initialization.
     * 
     * @return true if all API configurations are present, false otherwise
     */
    public static boolean areAllConfigurationsPresent() {
        try {
            return BackpackTfApiConfiguration.isConfigured() && 
                   SteamApiConfiguration.isConfigured();
        } catch (Exception e) {
            log.debug("Configuration check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Provides a summary of the current configuration status.
     * Useful for debugging and monitoring.
     * 
     * @return Configuration status summary
     */
    public static String getConfigurationSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append("API Configuration Status:\n");
        
        summary.append("  BackpackTF API: ")
               .append(BackpackTfApiConfiguration.isConfigured() ? "CONFIGURED" : "NOT CONFIGURED")
               .append("\n");
        
        summary.append("  Steam API: ")
               .append(SteamApiConfiguration.isConfigured() ? "CONFIGURED" : "NOT CONFIGURED")
               .append("\n");
        
        try {
            KafkaConfiguration.getKafkaBrokers();
            summary.append("  Kafka: CONFIGURED\n");
        } catch (Exception e) {
            summary.append("  Kafka: NOT CONFIGURED\n");
        }
        
        return summary.toString();
    }
}