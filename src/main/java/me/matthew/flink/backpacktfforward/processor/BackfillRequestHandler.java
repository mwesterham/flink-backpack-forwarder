package me.matthew.flink.backpacktfforward.processor;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import org.apache.flink.util.Collector;

/**
 * Interface for processing different types of backfill requests.
 * Each implementation handles a specific BackfillRequestType with appropriate filtering and processing logic.
 * 
 * Handlers delegate to existing BackfillProcessor logic where possible to minimize code changes
 * and maintain compatibility with existing error handling, metrics, and logging patterns.
 */
public interface BackfillRequestHandler {
    
    /**
     * Processes a backfill request and emits ListingUpdate events.
     * 
     * @param request The backfill request to process
     * @param out Collector to emit ListingUpdate events
     * @throws Exception if processing fails
     */
    void process(BackfillRequest request, Collector<ListingUpdate> out) throws Exception;
    
    /**
     * Returns the request type that this handler supports.
     * 
     * @return The BackfillRequestType this handler processes
     */
    BackfillRequestType getRequestType();
    
    /**
     * Determines if this handler can process the given request.
     * Implementations should validate that all required parameters are present
     * and compatible with the handler's request type.
     * 
     * @param request The backfill request to validate
     * @return true if this handler can process the request, false otherwise
     */
    boolean canHandle(BackfillRequest request);
}