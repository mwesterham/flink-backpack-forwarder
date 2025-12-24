package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BackpackTfApiResponse model classes.
 * Tests JSON deserialization and basic functionality.
 */
class BackpackTfApiResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testApiResponseDeserialization() throws Exception {
        // Test JSON deserialization with a sample API response
        String jsonResponse = """
            {
                "listings": [
                    {
                        "steamid": "76561198235929172",
                        "offers": 1,
                        "buyout": 0,
                        "details": "Paying in pure! Fast accept!",
                        "intent": "buy",
                        "timestamp": 1741545781,
                        "price": 4706.4,
                        "item": {
                            "defindex": 463,
                            "quality": 5,
                            "quantity": "1"
                        },
                        "currencies": {
                            "keys": 80
                        },
                        "bump": 1766450685,
                        "userAgent": {
                            "lastPulse": 1463831546,
                            "client": "test-client"
                        }
                    }
                ],
                "appid": 440,
                "sku": "Galactic Connection Taunt: The Schadenfreude",
                "createdAt": 1766467619
            }
            """;

        BackpackTfApiResponse response = objectMapper.readValue(jsonResponse, BackpackTfApiResponse.class);

        assertNotNull(response);
        assertEquals(440, response.getAppid());
        assertEquals("Galactic Connection Taunt: The Schadenfreude", response.getSku());
        assertEquals(1766467619L, response.getCreatedAt());
        
        assertNotNull(response.getListings());
        assertEquals(1, response.getListings().size());
        
        BackpackTfApiResponse.ApiListing listing = response.getListings().get(0);
        assertEquals("76561198235929172", listing.getSteamid());
        assertEquals(1, listing.getOffers());
        assertEquals("buy", listing.getIntent());
        assertEquals(1741545781L, listing.getTimestamp());
        assertEquals(4706.4, listing.getPrice());
        
        assertNotNull(listing.getItem());
        assertEquals(463, listing.getItem().getDefindex());
        assertEquals(5, listing.getItem().getQuality());
        assertEquals("1", listing.getItem().getQuantity());
        
        assertNotNull(listing.getCurrencies());
        assertEquals(Long.valueOf(80), listing.getCurrencies().getKeys());
        
        assertEquals(1766450685L, listing.getBump());
        
        assertNotNull(listing.getUserAgent());
        assertEquals(1463831546L, listing.getUserAgent().getLastPulse());
        assertEquals("test-client", listing.getUserAgent().getClient());
    }

    @Test
    void testApiResponseWithMinimalData() throws Exception {
        // Test with minimal required fields
        String jsonResponse = """
            {
                "listings": [],
                "appid": 440,
                "sku": "Test Item",
                "createdAt": 1234567890
            }
            """;

        BackpackTfApiResponse response = objectMapper.readValue(jsonResponse, BackpackTfApiResponse.class);

        assertNotNull(response);
        assertEquals(440, response.getAppid());
        assertEquals("Test Item", response.getSku());
        assertEquals(1234567890L, response.getCreatedAt());
        assertNotNull(response.getListings());
        assertTrue(response.getListings().isEmpty());
    }

    @Test
    void testApiResponseIgnoresUnknownFields() throws Exception {
        // Test that unknown fields are ignored
        String jsonResponse = """
            {
                "listings": [],
                "appid": 440,
                "sku": "Test Item",
                "createdAt": 1234567890,
                "unknownField": "should be ignored",
                "anotherUnknown": 12345
            }
            """;

        assertDoesNotThrow(() -> {
            BackpackTfApiResponse response = objectMapper.readValue(jsonResponse, BackpackTfApiResponse.class);
            assertNotNull(response);
            assertEquals(440, response.getAppid());
        });
    }
}