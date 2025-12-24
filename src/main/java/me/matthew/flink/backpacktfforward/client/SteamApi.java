package me.matthew.flink.backpacktfforward.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.InventoryItem;
import me.matthew.flink.backpacktfforward.model.SteamInventoryResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP client for querying Steam Web API to retrieve user inventory data.
 * Follows existing patterns for HTTP communication and error handling.
 * Includes retry logic using Failsafe for resilient API calls.
 */
@Slf4j
public class SteamApi {
    
    private static final String STEAM_API_BASE_URL = "http://api.steampowered.com/IEconItems_{appid}/GetPlayerItems/v0001/";
    private static final int TF2_APPID = 440;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String API_KEY_ENV_VAR = "STEAM_API_KEY";
    
    private final String apiKey;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final RetryPolicy<SteamInventoryResponse> retryPolicy;
    
    /**
     * Creates a new SteamApi using API key from environment variables.
     * 
     * @throws IllegalStateException if the API key is not configured
     */
    public SteamApi() {
        this(getApiKeyFromEnvironment());
    }
    
    /**
     * Creates a new SteamApi with the specified API key.
     * 
     * @param apiKey The Steam API key for authentication
     */
    public SteamApi(String apiKey) {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Steam API key cannot be null or empty");
        }
        
        this.apiKey = apiKey;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
        this.objectMapper = new ObjectMapper();
        this.retryPolicy = createRetryPolicy();
    }
    
    /**
     * Retrieves the Steam API key from environment variables.
     * 
     * @return The Steam API key
     * @throws IllegalStateException if the key is not configured
     */
    private static String getApiKeyFromEnvironment() {
        String key = System.getenv(API_KEY_ENV_VAR);
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalStateException(
                    "Steam API key not configured. Please set the " + API_KEY_ENV_VAR + " environment variable.");
        }
        return key;
    }
    
    /**
     * Creates a retry policy for Steam API calls following existing Failsafe patterns.
     * 
     * @return RetryPolicy configured for Steam API operations
     */
    private RetryPolicy<SteamInventoryResponse> createRetryPolicy() {
        return RetryPolicy.<SteamInventoryResponse>builder()
                .handle(IOException.class)
                .handle(HttpTimeoutException.class)
                .handleIf(this::isRetryableHttpError)
                .withDelay(Duration.ofSeconds(1)) // Steam API rate limiting: 1 request per second
                .withMaxRetries(3)
                .withBackoff(Duration.ofSeconds(1), Duration.ofSeconds(10))
                .onRetry(e -> {
                    log.warn("Steam API retry (attempt {}): {}", 
                            e.getAttemptCount(), 
                            e.getLastException().getMessage());
                })
                .onRetriesExceeded(e -> 
                        log.error("Max Steam API retries exceeded", e.getException())
                )
                .build();
    }
    
    /**
     * Determines if an exception represents a retryable HTTP error.
     * 
     * @param throwable The exception to check
     * @return true if the error should be retried
     */
    private boolean isRetryableHttpError(Throwable throwable) {
        if (throwable instanceof IOException) {
            String message = throwable.getMessage();
            if (message != null) {
                // Retry on rate limiting (429), server errors (5xx), and timeouts
                return message.contains("status 429") || 
                       message.contains("status 5") ||
                       message.contains("timeout");
            }
        }
        return false;
    }
    
    /**
     * Retrieves a Steam user's inventory items for TF2.
     * Uses retry logic to handle transient failures and Steam API rate limits.
     * 
     * @param steamId The Steam ID of the user whose inventory to retrieve
     * @return SteamInventoryResponse containing the inventory data
     * @throws IOException if the HTTP request fails after all retries
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    public SteamInventoryResponse getPlayerItems(String steamId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        log.debug("Fetching Steam inventory for steamId={}", steamId);
        
        return Failsafe.with(retryPolicy).get(() -> performInventoryApiCall(steamId));
    }
    
    /**
     * Performs the actual Steam API call without retry logic.
     * 
     * @param steamId The Steam ID of the user
     * @return SteamInventoryResponse containing the inventory data
     * @throws IOException if the HTTP request fails or response cannot be parsed
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    private SteamInventoryResponse performInventoryApiCall(String steamId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        // Build the request URL with required parameters
        String url = STEAM_API_BASE_URL.replace("{appid}", String.valueOf(TF2_APPID)) +
                String.format("?key=%s&steamid=%s&format=json", apiKey, steamId);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .timeout(TIMEOUT)
                .GET()
                .build();
        
        log.debug("Making Steam API request for steamId: {}", steamId);
        
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());
        
        log.debug("Steam API response status: {}", response.statusCode());
        
        // Handle different HTTP status codes
        if (response.statusCode() == 429) {
            throw new IOException("Rate limited by Steam API (status 429)");
        } else if (response.statusCode() >= 500) {
            throw new IOException(String.format(
                    "Steam API server error (status %d): %s", 
                    response.statusCode(), response.body()));
        } else if (response.statusCode() == 401 || response.statusCode() == 403) {
            throw new IOException("Steam API authentication failed - check API key (status " + response.statusCode() + ")");
        } else if (response.statusCode() != 200) {
            throw new IOException(String.format(
                    "Steam API request failed with status %d: %s", 
                    response.statusCode(), response.body()));
        }
        
        try {
            SteamInventoryResponse inventoryResponse = objectMapper.readValue(
                    response.body(), SteamInventoryResponse.class);
            
            // Check if the API call was successful
            if (inventoryResponse.getResult() == null || inventoryResponse.getResult().getStatus() != 1) {
                throw new IOException("Steam API returned unsuccessful status: " + 
                        (inventoryResponse.getResult() != null ? inventoryResponse.getResult().getStatus() : "null"));
            }
            
            int itemCount = inventoryResponse.getResult().getItems() != null ? 
                    inventoryResponse.getResult().getItems().size() : 0;
            log.debug("Successfully parsed Steam inventory with {} items", itemCount);
            
            return inventoryResponse;
        } catch (Exception e) {
            log.error("Failed to parse Steam API response: {}", response.body(), e);
            throw new IOException("Failed to parse Steam API response", e);
        }
    }
    
    /**
     * Finds all items in the inventory that match the specified defindex and quality.
     * Uses exact matching for both defindex and quality fields.
     * 
     * @param inventory The Steam inventory response to search
     * @param targetDefindex The item definition index to match
     * @param targetQuality The item quality to match
     * @return List of matching items (empty list if no matches found)
     */
    public List<InventoryItem> findMatchingItems(SteamInventoryResponse inventory, 
            int targetDefindex, int targetQuality) {
        
        List<InventoryItem> matchingItems = new ArrayList<>();
        
        if (inventory == null || inventory.getResult() == null || inventory.getResult().getItems() == null) {
            log.debug("No inventory data to search for defindex={}, quality={}", targetDefindex, targetQuality);
            return matchingItems;
        }
        
        log.debug("Searching inventory with {} items for defindex={}, quality={}", 
                inventory.getResult().getItems().size(), targetDefindex, targetQuality);
        
        for (InventoryItem item : inventory.getResult().getItems()) {
            if (item.getDefindex() == targetDefindex && item.getQuality() == targetQuality) {
                matchingItems.add(item);
                log.debug("Found matching item: id={}, defindex={}, quality={}", 
                        item.getId(), item.getDefindex(), item.getQuality());
            }
        }
        
        log.debug("Found {} matching items for defindex={}, quality={}", 
                matchingItems.size(), targetDefindex, targetQuality);
        
        return matchingItems;
    }
}