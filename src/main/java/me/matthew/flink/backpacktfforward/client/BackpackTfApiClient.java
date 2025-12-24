package me.matthew.flink.backpacktfforward.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.BackpackTfApiResponse;
import me.matthew.flink.backpacktfforward.model.BackpackTfListingDetail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;

/**
 * HTTP client for calling the backpack.tf snapshot API.
 * Follows existing patterns for HTTP communication and error handling.
 * Includes retry logic using Failsafe for resilient API calls.
 */
@Slf4j
public class BackpackTfApiClient {
    
    private static final String API_BASE_URL = "https://backpack.tf/api/classifieds/listings/snapshot";
    private static final String GET_LISTING_BASE_URL = "https://backpack.tf/api/classifieds/listings";
    private static final String USER_AGENT = "TF2Autobot-Snapshot-Ingest";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String API_TOKEN_ENV_VAR = "BACKPACK_TF_API_TOKEN";
    
    private final String apiToken;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final RetryPolicy<BackpackTfApiResponse> retryPolicy;
    private final RetryPolicy<BackpackTfListingDetail> getListingRetryPolicy;
    
    /**
     * Creates a new BackpackTfApiClient using API token from environment variables.
     * 
     * @throws IllegalStateException if the API token is not configured
     */
    public BackpackTfApiClient() {
        this(getApiTokenFromEnvironment());
    }
    
    /**
     * Creates a new BackpackTfApiClient with the specified API token.
     * 
     * @param apiToken The API token for authentication with backpack.tf
     */
    public BackpackTfApiClient(String apiToken) {
        if (apiToken == null || apiToken.trim().isEmpty()) {
            throw new IllegalArgumentException("API token cannot be null or empty");
        }
        
        this.apiToken = apiToken;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
        this.objectMapper = new ObjectMapper();
        this.retryPolicy = createRetryPolicy();
        this.getListingRetryPolicy = createGetListingRetryPolicy();
    }
    
    /**
     * Retrieves the API token from environment variables.
     * 
     * @return The API token
     * @throws IllegalStateException if the token is not configured
     */
    private static String getApiTokenFromEnvironment() {
        String token = System.getenv(API_TOKEN_ENV_VAR);
        if (token == null || token.trim().isEmpty()) {
            throw new IllegalStateException(
                    "API token not configured. Please set the " + API_TOKEN_ENV_VAR + " environment variable.");
        }
        return token;
    }
    
    /**
     * Creates a retry policy for API calls following existing Failsafe patterns.
     * 
     * @return RetryPolicy configured for API operations
     */
    private RetryPolicy<BackpackTfApiResponse> createRetryPolicy() {
        return RetryPolicy.<BackpackTfApiResponse>builder()
                .handle(IOException.class)
                .handle(HttpTimeoutException.class)
                .handleIf(this::isRetryableHttpError)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(3)
                .withBackoff(Duration.ofMillis(500), Duration.ofSeconds(5))
                .onRetry(e -> {
                    log.warn("API retry (attempt {}): {}", 
                            e.getAttemptCount(), 
                            e.getLastException().getMessage());
                })
                .onRetriesExceeded(e -> 
                        log.error("Max API retries exceeded", e.getException())
                )
                .build();
    }
    
    /**
     * Creates a retry policy for getListing API calls following existing Failsafe patterns.
     * 
     * @return RetryPolicy configured for getListing operations
     */
    private RetryPolicy<BackpackTfListingDetail> createGetListingRetryPolicy() {
        return RetryPolicy.<BackpackTfListingDetail>builder()
                .handle(IOException.class)
                .handle(HttpTimeoutException.class)
                .handleIf(this::isRetryableHttpError)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(3)
                .withBackoff(Duration.ofMillis(500), Duration.ofSeconds(5))
                .onRetry(e -> {
                    log.warn("GetListing API retry (attempt {}): {}", 
                            e.getAttemptCount(), 
                            e.getLastException().getMessage());
                })
                .onRetriesExceeded(e -> 
                        log.error("Max getListing API retries exceeded", e.getException())
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
                // Do not retry on 404 (item not found) or 401 (authentication) errors
                return (message.contains("status 429") || 
                        message.contains("status 5") ||
                        message.contains("timeout")) &&
                       !message.contains("status 404") &&
                       !message.contains("status 401");
            }
        }
        return false;
    }
    
    /**
     * Fetches snapshot data from the backpack.tf API for a specific item.
     * Uses retry logic to handle transient failures.
     * 
     * @param sku The market name (SKU) of the item to fetch
     * @param appid The Steam application ID (typically 440 for TF2)
     * @return BackpackTfApiResponse containing the API response data
     * @throws IOException if the HTTP request fails after all retries
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    public BackpackTfApiResponse fetchSnapshot(String sku, int appid) 
            throws IOException, InterruptedException, URISyntaxException {
        
        log.debug("Fetching snapshot for sku={}, appid={}", sku, appid);
        
        return Failsafe.with(retryPolicy).get(() -> performApiCall(sku, appid));
    }
    
    /**
     * Performs the actual API call without retry logic.
     * 
     * @param sku The market name (SKU) of the item to fetch
     * @param appid The Steam application ID
     * @return BackpackTfApiResponse containing the API response data
     * @throws IOException if the HTTP request fails or response cannot be parsed
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    private BackpackTfApiResponse performApiCall(String sku, int appid) 
            throws IOException, InterruptedException, URISyntaxException {
        
        // Build the request URL with parameters - URL encode the sku parameter
        String encodedSku = URLEncoder.encode(sku, StandardCharsets.UTF_8);
        String url = String.format("%s?token=%s&sku=%s&appid=%d", 
                API_BASE_URL, apiToken, encodedSku, appid);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("User-Agent", USER_AGENT)
                .timeout(TIMEOUT)
                .GET()
                .build();
        
        log.debug("Making API request to: {}", url.replaceAll("token=[^&]*", "token=***"));
        
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());
        
        log.debug("API response status: {}", response.statusCode());
        
        // Handle different HTTP status codes
        if (response.statusCode() == 429) {
            throw new IOException("Rate limited by API (status 429)");
        } else if (response.statusCode() >= 500) {
            throw new IOException(String.format(
                    "Server error (status %d): %s", 
                    response.statusCode(), response.body()));
        } else if (response.statusCode() == 401) {
            throw new IOException("Authentication failed - check API token (status 401)");
        } else if (response.statusCode() != 200) {
            throw new IOException(String.format(
                    "API request failed with status %d: %s", 
                    response.statusCode(), response.body()));
        }
        
        try {
            BackpackTfApiResponse apiResponse = objectMapper.readValue(
                    response.body(), BackpackTfApiResponse.class);
            
            log.debug("Successfully parsed API response with {} listings", 
                    apiResponse.getListings() != null ? apiResponse.getListings().size() : 0);
            
            return apiResponse;
        } catch (Exception e) {
            log.error("Failed to parse API response: {}", response.body(), e);
            throw new IOException("Failed to parse API response", e);
        }
    }
    
    /**
     * Retrieves detailed listing information by item ID from the backpack.tf getListing API.
     * Uses retry logic to handle transient failures and follows existing authentication patterns.
     * 
     * @param itemId The Steam item ID to retrieve listing details for
     * @return BackpackTfListingDetail containing the complete listing data with actual listing ID
     * @throws IOException if the HTTP request fails after all retries
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    public BackpackTfListingDetail getListing(String itemId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        log.debug("Fetching listing detail for itemId={}", itemId);
        
        return Failsafe.with(getListingRetryPolicy).get(() -> performGetListingCall(itemId));
    }
    
    /**
     * Performs the actual getListing API call without retry logic.
     * 
     * @param itemId The Steam item ID to retrieve listing details for
     * @return BackpackTfListingDetail containing the complete listing data
     * @throws IOException if the HTTP request fails or response cannot be parsed
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    private BackpackTfListingDetail performGetListingCall(String itemId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        // Build the request URL for getListing endpoint
        String url = String.format("%s/%s", GET_LISTING_BASE_URL, itemId);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("User-Agent", USER_AGENT)
                .header("Authorization", "Bearer " + apiToken)
                .timeout(TIMEOUT)
                .GET()
                .build();
        
        log.debug("Making getListing API request to: {}", url);
        
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());
        
        log.debug("GetListing API response status: {}", response.statusCode());
        
        // Handle different HTTP status codes
        if (response.statusCode() == 429) {
            throw new IOException("Rate limited by getListing API (status 429)");
        } else if (response.statusCode() >= 500) {
            throw new IOException(String.format(
                    "Server error in getListing API (status %d): %s", 
                    response.statusCode(), response.body()));
        } else if (response.statusCode() == 401) {
            throw new IOException("Authentication failed for getListing API - check API token (status 401)");
        } else if (response.statusCode() == 404) {
            throw new IOException(String.format(
                    "Listing not found for item ID %s (status 404)", itemId));
        } else if (response.statusCode() != 200) {
            throw new IOException(String.format(
                    "GetListing API request failed with status %d: %s", 
                    response.statusCode(), response.body()));
        }
        
        try {
            BackpackTfListingDetail listingDetail = objectMapper.readValue(
                    response.body(), BackpackTfListingDetail.class);
            
            log.debug("Successfully parsed getListing API response for item ID: {}", itemId);
            
            return listingDetail;
        } catch (Exception e) {
            log.error("Failed to parse getListing API response for item ID {}: {}", 
                    itemId, response.body(), e);
            throw new IOException("Failed to parse getListing API response", e);
        }
    }
}