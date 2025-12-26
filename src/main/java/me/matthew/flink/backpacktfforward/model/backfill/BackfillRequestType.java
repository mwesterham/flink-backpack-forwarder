package me.matthew.flink.backpacktfforward.model.backfill;

/**
 * Enumeration defining different types of backfill operations supported by the system.
 * Each type represents a specific filtering or processing strategy for backfill requests.
 */
public enum BackfillRequestType {
    
    /**
     * Default backfill type that processes all listings (buy and sell) with complete functionality.
     * This maintains backward compatibility with existing BackfillRequest processing.
     */
    FULL,
    
    /**
     * Processes only buy listings, skipping Steam inventory scanning entirely.
     * Filters BackpackTF API responses to include only listings with intent="buy".
     */
    BUY_ONLY,
    
    /**
     * Processes only sell listings, including Steam inventory scanning for filtered listings.
     * Filters BackpackTF API responses to include only listings with intent="sell".
     */
    SELL_ONLY,
    
    /**
     * Processes a single specific listing by ID, bypassing market-wide API calls.
     * Uses getListing API directly with the provided listing ID.
     */
    SINGLE_ID
}