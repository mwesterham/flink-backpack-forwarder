package me.matthew.flink.backpacktfforward.util;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for generating BackpackTF listing IDs.
 * 
 * BackpackTF listing IDs follow specific patterns:
 * - Sell listings: {appid}_{assetid}
 * - Buy listings: {appid}_{steamid}_{md5 hash of item name}
 * 
 * This addresses the issue where listing IDs are not included in the snapshots response,
 * but can be generated using the documented patterns.
 */
@Slf4j
public class ListingIdGenerator {
    
    /**
     * Generates a listing ID for a sell listing.
     * 
     * @param appid The Steam application ID (typically 440 for TF2)
     * @param assetid The Steam asset ID of the item
     * @return The generated listing ID in format: {appid}_{assetid}
     */
    public static String generateSellListingId(int appid, String assetid) {
        if (assetid == null || assetid.trim().isEmpty()) {
            throw new IllegalArgumentException("Asset ID cannot be null or empty for sell listing");
        }
        
        String listingId = String.format("%d_%s", appid, assetid);
        log.debug("Generated sell listing ID: {} for appid={}, assetid={}", listingId, appid, assetid);
        return listingId;
    }
    
    /**
     * Generates a listing ID for a buy listing.
     * 
     * @param appid The Steam application ID (typically 440 for TF2)
     * @param steamid The Steam ID of the buyer
     * @param itemName The name of the item being bought (used for MD5 hash)
     * @return The generated listing ID in format: {appid}_{steamid}_{md5 hash of item name}
     */
    public static String generateBuyListingId(int appid, String steamid, String itemName) {
        if (steamid == null || steamid.trim().isEmpty()) {
            throw new IllegalArgumentException("Steam ID cannot be null or empty for buy listing");
        }
        if (itemName == null || itemName.trim().isEmpty()) {
            throw new IllegalArgumentException("Item name cannot be null or empty for buy listing");
        }
        
        String md5Hash = generateMd5Hash(itemName);
        String listingId = String.format("%d_%s_%s", appid, steamid, md5Hash);
        log.debug("Generated buy listing ID: {} for appid={}, steamid={}, itemName={}", 
                listingId, appid, steamid, itemName);
        return listingId;
    }
    
    /**
     * Generates a listing ID based on the intent (buy/sell) and available data.
     * 
     * @param appid The Steam application ID (typically 440 for TF2)
     * @param steamid The Steam ID associated with the listing
     * @param assetid The Steam asset ID (for sell listings)
     * @param itemName The item name (for buy listings)
     * @param intent The listing intent ("sell" or "buy")
     * @return The generated listing ID
     * @throws IllegalArgumentException if required parameters are missing for the intent
     */
    public static String generateListingId(int appid, String steamid, String assetid, String itemName, String intent) {
        if (intent == null) {
            throw new IllegalArgumentException("Intent cannot be null");
        }
        
        switch (intent.toLowerCase()) {
            case "sell":
                return generateSellListingId(appid, assetid);
            case "buy":
                return generateBuyListingId(appid, steamid, itemName);
            default:
                throw new IllegalArgumentException("Unknown intent: " + intent + ". Expected 'sell' or 'buy'");
        }
    }
    
    /**
     * Generates an MD5 hash of the given input string.
     * 
     * @param input The string to hash
     * @return The MD5 hash as a lowercase hexadecimal string
     * @throws RuntimeException if MD5 algorithm is not available (should never happen)
     */
    private static String generateMd5Hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            
            // Convert bytes to hexadecimal string
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }
}