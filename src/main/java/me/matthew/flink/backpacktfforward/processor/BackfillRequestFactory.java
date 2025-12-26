package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BackfillRequestFactory {
    private final Map<BackfillRequestType, BackfillRequestHandler> handlers;
    
    /**
     * Creates a new BackfillRequestFactory with the provided handler registry.
     * 
     * @param handlers Map of request types to their corresponding handlers
     */
    public BackfillRequestFactory(Map<BackfillRequestType, BackfillRequestHandler> handlers) {
        this.handlers = new HashMap<>(handlers);
        log.info("BackfillRequestFactory initialized with {} handlers", handlers.size());
    }
    
    /**
     * Gets the appropriate handler for the given backfill request.
     * Performs request type detection and validation before returning the handler.
     * 
     * @param request The backfill request to process
     * @return The handler capable of processing this request
     * @throws IllegalArgumentException if no suitable handler is found or validation fails
     */
    public BackfillRequestHandler getHandler(BackfillRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("BackfillRequest cannot be null");
        }
        
        BackfillRequestType requestType = determineRequestType(request);
        log.debug("Determined request type: {} for request with defindex: {}, listingId: {}", 
                    requestType, request.getItemDefindex(), request.getListingId());
        
        BackfillRequestHandler handler = handlers.get(requestType);
        if (handler == null) {
            log.error("No handler found for request type: {}", requestType);
            throw new IllegalArgumentException("No handler available for request type: " + requestType);
        }
        
        // Validate that the handler can process this specific request
        if (!handler.canHandle(request)) {
            log.error("Handler for type {} cannot process request due to validation failure. " +
                        "Request: defindex={}, listingId={}, maxInventorySize={}", 
                        requestType, request.getItemDefindex(), request.getListingId(), 
                        request.getMaxInventorySize());
            throw new IllegalArgumentException("Handler validation failed for request type: " + requestType);
        }
        
        return handler;
    }
    
    /**
     * Determines the request type from the message structure using the following priority:
     * 1. Explicit Type: If requestType field is present, use it
     * 2. Single ID: If listingId is present, use SINGLE_ID
     * 3. Inventory Filtered: If maxInventorySize is present, use INVENTORY_FILTERED
     * 4. Default: Use FULL for backward compatibility
     * 
     * @param request The backfill request to analyze
     * @return The determined BackfillRequestType
     */
    private BackfillRequestType determineRequestType(BackfillRequest request) {
        // Priority 1: Explicit request type
        if (request.getRequestType() != null) {
            log.debug("Using explicit request type: {}", request.getRequestType());
            return request.getRequestType();
        }
        
        // Priority 2: Single ID detection
        if (request.getListingId() != null && !request.getListingId().trim().isEmpty()) {
            log.debug("Detected SINGLE_ID request type from listingId: {}", request.getListingId());
            return BackfillRequestType.SINGLE_ID;
        }
        
        // Priority 4: Default to FULL for backward compatibility
        log.debug("Defaulting to FULL request type for backward compatibility");
        return BackfillRequestType.FULL;
    }
    
    /**
     * Validates that the request contains all required parameters for the determined type.
     * This method provides detailed validation beyond what individual handlers perform.
     * 
     * @param request The backfill request to validate
     * @param requestType The determined request type
     * @return true if validation passes, false otherwise
     */
    public boolean validateRequestParameters(BackfillRequest request, BackfillRequestType requestType) {
        switch (requestType) {
            case SINGLE_ID:
                if (request.getListingId() == null || request.getListingId().trim().isEmpty()) {
                    log.warn("SINGLE_ID request missing required listingId parameter");
                    return false;
                }
                break;
                
            case FULL:
            case BUY_ONLY:
            case SELL_ONLY:
                if (request.getItemDefindex() <= 0) {
                    log.warn("{} request has invalid itemDefindex: {}", requestType, request.getItemDefindex());
                    return false;
                }
                if (request.getItemQualityId() < 0) {
                    log.warn("{} request has invalid itemQualityId: {}", requestType, request.getItemQualityId());
                    return false;
                }
                break;
                
            default:
                log.warn("Unknown request type for validation: {}", requestType);
                return false;
        }
        
        return true;
    }
    
    /**
     * Gets the number of registered handlers.
     * 
     * @return The count of registered handlers
     */
    public int getHandlerCount() {
        return handlers.size();
    }
    
    /**
     * Checks if a handler is registered for the given request type.
     * 
     * @param requestType The request type to check
     * @return true if a handler is registered, false otherwise
     */
    public boolean hasHandler(BackfillRequestType requestType) {
        return handlers.containsKey(requestType);
    }
}