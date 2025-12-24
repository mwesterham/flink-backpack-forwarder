package me.matthew.flink.backpacktfforward.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ListingIdGenerator utility class.
 * Verifies correct listing ID generation for both sell and buy intents.
 */
class ListingIdGeneratorTest {
    
    @Test
    void testGenerateSellListingId() {
        // Test sell listing ID generation: {appid}_{assetid}
        String result = ListingIdGenerator.generateSellListingId(440, "12345678901234567890");
        assertEquals("440_12345678901234567890", result);
    }
    
    @Test
    void testGenerateListingIdWithSellIntent() {
        String result = ListingIdGenerator.generateListingId(440, "76561198000000000", "12345678901234567890", "Strange Scattergun", "sell");
        assertEquals("440_12345678901234567890", result);
    }
    
    @Test
    void testGenerateListingIdWithBuyIntent() {
        String result1 = ListingIdGenerator.generateListingId(440, "76561199229477354", "TEST", "Mann Co. Supply Crate Key", "buy");
        assertEquals("440_76561199229477354_d9f847ff5dfcf78576a9fca04cbf6c07", result1);
        // Test buy listing ID generation: {appid}_{steamid}_{md5 hash of item name}
        String result2 = ListingIdGenerator.generateBuyListingId(440, "76561198421717954", "Strange Medi-Mask");
        assertEquals("440_76561198421717954_a947cc644014a2d0a8f0f7a9dc5def69", result2);
    
    }
    
    @Test
    void testGenerateListingIdWithInvalidIntent() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListingIdGenerator.generateListingId(440, "76561198000000000", "12345678901234567890", "Strange Scattergun", "invalid");
        });
    }
    
    @Test
    void testGenerateSellListingIdWithNullAssetId() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListingIdGenerator.generateSellListingId(440, null);
        });
    }
    
    @Test
    void testGenerateBuyListingIdWithNullSteamId() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListingIdGenerator.generateBuyListingId(440, null, "Strange Scattergun");
        });
    }
    
    @Test
    void testGenerateBuyListingIdWithNullItemName() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListingIdGenerator.generateBuyListingId(440, "76561198000000000", null);
        });
    }
    
    @Test
    void testMd5HashConsistency() {
        // Test that the same input always produces the same MD5 hash
        String result1 = ListingIdGenerator.generateBuyListingId(440, "76561198000000000", "Strange Scattergun");
        String result2 = ListingIdGenerator.generateBuyListingId(440, "76561198000000000", "Strange Scattergun");
        assertEquals(result1, result2);
    }
    
    @Test
    void testMd5HashDifference() {
        // Test that different inputs produce different MD5 hashes
        String result1 = ListingIdGenerator.generateBuyListingId(440, "76561198000000000", "Strange Scattergun");
        String result2 = ListingIdGenerator.generateBuyListingId(440, "76561198000000000", "Strange Rocket Launcher");
        assertNotEquals(result1, result2);
    }
}