package me.matthew.flink.backpacktfforward.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.BackpackTfApiConfiguration;
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
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

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
    
    // Rate limiting: Different limits for different endpoints (configurable)
    // Snapshot API: default 6 requests per minute = 10 seconds between requests
    private static final Duration SNAPSHOT_RATE_LIMIT_DELAY = Duration.ofSeconds(BackpackTfApiConfiguration.getSnapshotRateLimitSeconds());
    // GetListing API: default 60 requests per minute = 1 second between requests
    private static final Duration GET_LISTING_RATE_LIMIT_DELAY = Duration.ofSeconds(BackpackTfApiConfiguration.getGetListingRateLimitSeconds());
    
    private final String apiToken;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final RetryPolicy<BackpackTfApiResponse> retryPolicy;
    private final RetryPolicy<BackpackTfListingDetail> getListingRetryPolicy;
    
    // Separate rate limiting state for each endpoint
    private static final ReentrantLock snapshotRateLimitLock = new ReentrantLock();
    private static volatile Instant lastSnapshotApiCall = Instant.EPOCH;
    
    private static final ReentrantLock getListingRateLimitLock = new ReentrantLock();
    private static volatile Instant lastGetListingApiCall = Instant.EPOCH;
    
    /**
     * Creates a new BackpackTfApiClient using API token from environment variables.
     * 
     * @throws IllegalStateException if the API token is not configured
     */
    public BackpackTfApiClient() {
        this(BackpackTfApiConfiguration.getApiToken());
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
                .connectTimeout(Duration.ofSeconds(BackpackTfApiConfiguration.getApiTimeoutSeconds()))
                .build();
        this.objectMapper = new ObjectMapper();
        this.retryPolicy = createRetryPolicy();
        this.getListingRetryPolicy = createGetListingRetryPolicy();
    }
    
    /**
     * Creates a retry policy for API calls with exponential backoff for rate limiting.
     * 
     * @return RetryPolicy configured for API operations
     */
    private RetryPolicy<BackpackTfApiResponse> createRetryPolicy() {
        return RetryPolicy.<BackpackTfApiResponse>builder()
                .handle(IOException.class)
                .handle(HttpTimeoutException.class)
                .handleIf(this::isRetryableHttpError)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(-1) // Infinite retries
                .withBackoff(Duration.ofSeconds(1), Duration.ofMinutes(2)) // Longer backoff for rate limits
                .onRetry(e -> {
                    if (e.getAttemptCount() > 10) {
                        log.warn("BackpackTF snapshot API retry attempt {} (EXCESSIVE): {}. This may indicate persistent API issues.", 
                                e.getAttemptCount(), 
                                e.getLastException().getMessage());
                    } else {
                        log.debug("BackpackTF snapshot API retry (attempt {}): {}", 
                                e.getAttemptCount(), 
                                e.getLastException().getMessage());
                    }
                })
                .build();
    }
    
    /**
     * Creates a retry policy for getListing API calls with exponential backoff for rate limiting.
     * 
     * @return RetryPolicy configured for getListing operations
     */
    private RetryPolicy<BackpackTfListingDetail> createGetListingRetryPolicy() {
        return RetryPolicy.<BackpackTfListingDetail>builder()
                .handle(IOException.class)
                .handle(HttpTimeoutException.class)
                .handleIf(this::isRetryableHttpError)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(-1) // Infinite retries
                .withBackoff(Duration.ofSeconds(1), Duration.ofMinutes(2)) // Longer backoff for rate limits
                .onRetry(e -> {
                    if (e.getAttemptCount() > 10) {
                        log.warn("BackpackTF getListing API retry attempt {} (EXCESSIVE): {}. This may indicate persistent API issues.", 
                                e.getAttemptCount(), 
                                e.getLastException().getMessage());
                    } else {
                        log.debug("BackpackTF getListing API retry (attempt {}): {}", 
                                e.getAttemptCount(), 
                                e.getLastException().getMessage());
                    }
                })
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
     * Enforces rate limiting for snapshot API calls by ensuring minimum delay between calls.
     * Uses a static lock to coordinate across all instances of the client.
     * Snapshot API: 6 requests per minute = 10 seconds between requests.
     * 
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void enforceSnapshotRateLimit() throws InterruptedException {
        snapshotRateLimitLock.lock();
        try {
            Instant now = Instant.now();
            Duration timeSinceLastCall = Duration.between(lastSnapshotApiCall, now);
            
            if (timeSinceLastCall.compareTo(SNAPSHOT_RATE_LIMIT_DELAY) < 0) {
                Duration waitTime = SNAPSHOT_RATE_LIMIT_DELAY.minus(timeSinceLastCall);
                log.debug("Snapshot API rate limiting: waiting {} ms before next call", waitTime.toMillis());
                Thread.sleep(waitTime.toMillis());
            }
            
            lastSnapshotApiCall = Instant.now();
        } finally {
            snapshotRateLimitLock.unlock();
        }
    }
    
    /**
     * Enforces rate limiting for getListing API calls by ensuring minimum delay between calls.
     * Uses a static lock to coordinate across all instances of the client.
     * GetListing API: 60 requests per minute = 1 second between requests.
     * 
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void enforceGetListingRateLimit() throws InterruptedException {
        getListingRateLimitLock.lock();
        try {
            Instant now = Instant.now();
            Duration timeSinceLastCall = Duration.between(lastGetListingApiCall, now);
            
            if (timeSinceLastCall.compareTo(GET_LISTING_RATE_LIMIT_DELAY) < 0) {
                Duration waitTime = GET_LISTING_RATE_LIMIT_DELAY.minus(timeSinceLastCall);
                log.debug("GetListing API rate limiting: waiting {} ms before next call", waitTime.toMillis());
                Thread.sleep(waitTime.toMillis());
            }
            
            lastGetListingApiCall = Instant.now();
        } finally {
            getListingRateLimitLock.unlock();
        }
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
     * Enforces snapshot API rate limiting before making the call.
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
        
        // Enforce snapshot API rate limiting before making the call (6/min = 10s delay)
        enforceSnapshotRateLimit();
        
        // Build the request URL with parameters - URL encode the sku parameter
        String encodedSku = URLEncoder.encode(sku, StandardCharsets.UTF_8);
        String url = String.format("%s?token=%s&sku=%s&appid=%d", 
                API_BASE_URL, apiToken, encodedSku, appid);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("User-Agent", USER_AGENT)
                .timeout(Duration.ofSeconds(BackpackTfApiConfiguration.getApiTimeoutSeconds()))
                .GET()
                .build();
        
        log.debug("Making BackpackTF API request to: {}", url.replaceAll("token=[^&]*", "token=***"));
        
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());
        
        log.debug("BackpackTF API response status: {}", response.statusCode());
        
        // Handle different HTTP status codes
        if (response.statusCode() == 429) {
            throw new IOException("Rate limited by BackpackTF API (status 429) - will retry with exponential backoff");
        } else if (response.statusCode() >= 500) {
            throw new IOException(String.format(
                    "BackpackTF API server error (status %d): %s", 
                    response.statusCode(), response.body()));
        } else if (response.statusCode() == 401) {
            throw new IOException("BackpackTF API authentication failed - check API token (status 401)");
        } else if (response.statusCode() != 200) {
            throw new IOException(String.format(
                    "BackpackTF API request failed with status %d: %s", 
                    response.statusCode(), response.body()));
        }
        
        try {
            BackpackTfApiResponse apiResponse = objectMapper.readValue(
                    response.body(), BackpackTfApiResponse.class);
            
            log.debug("Successfully parsed BackpackTF API response with {} listings", 
                    apiResponse.getListings() != null ? apiResponse.getListings().size() : 0);
            
            return apiResponse;
        } catch (Exception e) {
            log.error("Failed to parse BackpackTF API response: {}", response.body(), e);
            throw new IOException("Failed to parse BackpackTF API response", e);
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
     * Enforces getListing API rate limiting before making the call.
     * 
     * @param itemId The Steam item ID to retrieve listing details for
     * @return BackpackTfListingDetail containing the complete listing data
     * @throws IOException if the HTTP request fails or response cannot be parsed
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    private BackpackTfListingDetail performGetListingCall(String itemId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        // Enforce getListing API rate limiting before making the call (60/min = 1s delay)
        enforceGetListingRateLimit();
        
        // Build the request URL for getListing endpoint
        String url = String.format("%s/%s", GET_LISTING_BASE_URL, itemId);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("User-Agent", USER_AGENT)
                .header("Authorization", "Bearer " + apiToken)
                .timeout(Duration.ofSeconds(BackpackTfApiConfiguration.getApiTimeoutSeconds()))
                .GET()
                .build();
        
        log.debug("Making BackpackTF getListing API request to: {}", url);
        
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());
        
        log.debug("BackpackTF getListing API response status: {}", response.statusCode());
        
        // Handle different HTTP status codes
        if (response.statusCode() == 429) {
            throw new IOException("Rate limited by BackpackTF getListing API (status 429) - will retry with exponential backoff");
        } else if (response.statusCode() >= 500) {
            throw new IOException(String.format(
                    "BackpackTF getListing API server error (status %d): %s", 
                    response.statusCode(), response.body()));
        } else if (response.statusCode() == 401) {
            throw new IOException("BackpackTF getListing API authentication failed - check API token (status 401)");
        } else if (response.statusCode() == 404) {
            throw new IOException(String.format(
                    "Listing not found for item ID %s (status 404)", itemId));
        } else if (response.statusCode() != 200) {
            throw new IOException(String.format(
                    "BackpackTF getListing API request failed with status %d: %s", 
                    response.statusCode(), response.body()));
        }
        
        try {
            BackpackTfListingDetail listingDetail = objectMapper.readValue(
                    response.body(), BackpackTfListingDetail.class);
            
            log.debug("Successfully parsed BackpackTF getListing API response for item ID: {}", itemId);
            
            return listingDetail;
        } catch (Exception e) {
            log.error("Failed to parse BackpackTF getListing API response for item ID {}: {}", 
                    itemId, response.body(), e);
            throw new IOException("Failed to parse BackpackTF getListing API response", e);
        }
    }
}