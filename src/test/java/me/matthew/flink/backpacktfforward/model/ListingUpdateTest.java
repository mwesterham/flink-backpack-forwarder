package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ListingUpdate model class focusing on JSON serialization/deserialization,
 * particularly for the strange parts field that was causing null deserialization issues.
 */
public class ListingUpdateTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testStrangePartsDeserialization() throws Exception {
        // JSON string with strange parts data (based on the actual payload that was failing)
        String json = """
            {
                "id": "test_id",
                "event": "listing-update",
                "payload": {
                    "id": "440_76561198018333134_ae238bb55b7f9979e28bef87b085622b",
                    "steamid": "76561198018333134",
                    "appid": 440,
                    "currencies": {"metal": 1.11, "keys": 11},
                    "value": {"raw": 648.24, "short": "11.02 keys", "long": "11 keys, 1.11 ref"},
                    "tradeOffersPreferred": true,
                    "details": "Test listing",
                    "listedAt": 1736692873,
                    "bumpedAt": 1767058917,
                    "intent": "buy",
                    "count": 1,
                    "status": "active",
                    "source": "user",
                    "item": {
                        "appid": 440,
                        "baseName": "Loaf Loafers",
                        "defindex": 31105,
                        "id": "",
                        "marketName": "Strange Loaf Loafers",
                        "name": "Strange Loaf Loafers",
                        "quality": {"id": 11, "name": "Strange", "color": "#CF6A32"},
                        "summary": "Level 1-100 Bread",
                        "strangeParts": [
                            {
                                "score": 0,
                                "killEater": {
                                    "id": 82,
                                    "name": "Damage Dealt",
                                    "item": {
                                        "appid": 440,
                                        "baseName": "Strange Part: Damage Dealt",
                                        "defindex": 6056,
                                        "marketName": "Strange Part: Damage Dealt",
                                        "name": "Strange Part: Damage Dealt",
                                        "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                                        "tradable": true,
                                        "craftable": true
                                    }
                                }
                            },
                            {
                                "score": 0,
                                "killEater": {
                                    "id": 28,
                                    "name": "Dominations",
                                    "item": {
                                        "appid": 440,
                                        "baseName": "Strange Part: Domination Kills",
                                        "defindex": 6016,
                                        "marketName": "Strange Part: Domination Kills",
                                        "name": "Strange Part: Domination Kills",
                                        "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                                        "tradable": true,
                                        "craftable": true
                                    }
                                }
                            },
                            {
                                "score": 0,
                                "killEater": {
                                    "id": 87,
                                    "name": "Kills",
                                    "item": {
                                        "appid": 440,
                                        "baseName": "Strange Cosmetic Part: Kills",
                                        "defindex": 6060,
                                        "marketName": "Strange Cosmetic Part: Kills",
                                        "name": "Strange Cosmetic Part: Kills",
                                        "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                                        "tradable": true,
                                        "craftable": true
                                    }
                                }
                            }
                        ],
                        "tradable": true,
                        "craftable": true
                    },
                    "userAgent": {"client": "76561198018333134", "lastPulse": 1767060671},
                    "user": {
                        "id": "76561198018333134",
                        "name": "Test User",
                        "premium": true,
                        "online": true,
                        "banned": false
                    }
                }
            }
            """;
        
        // Deserialize from JSON
        ListingUpdate listingUpdate = objectMapper.readValue(json, ListingUpdate.class);
        
        // Verify the listing update was parsed correctly
        assertNotNull(listingUpdate);
        assertNotNull(listingUpdate.getPayload());
        assertNotNull(listingUpdate.getPayload().getItem());
        
        // This is the critical test - strange parts should NOT be null
        assertNotNull(listingUpdate.getPayload().getItem().getStrangeParts(), 
                     "Strange parts should not be null when present in JSON");
        
        // Verify we have the expected number of strange parts
        assertEquals(3, listingUpdate.getPayload().getItem().getStrangeParts().size(),
                    "Should have 3 strange parts");
        
        // Verify the strange parts data is correctly parsed
        ListingUpdate.StrangePart firstPart = listingUpdate.getPayload().getItem().getStrangeParts().get(0);
        assertNotNull(firstPart);
        assertEquals(0, firstPart.getScore());
        assertNotNull(firstPart.getKillEater());
        assertEquals(82, firstPart.getKillEater().getId());
        assertEquals("Damage Dealt", firstPart.getKillEater().getName());
        
        ListingUpdate.StrangePart secondPart = listingUpdate.getPayload().getItem().getStrangeParts().get(1);
        assertNotNull(secondPart);
        assertEquals(28, secondPart.getKillEater().getId());
        assertEquals("Dominations", secondPart.getKillEater().getName());
        
        ListingUpdate.StrangePart thirdPart = listingUpdate.getPayload().getItem().getStrangeParts().get(2);
        assertNotNull(thirdPart);
        assertEquals(87, thirdPart.getKillEater().getId());
        assertEquals("Kills", thirdPart.getKillEater().getName());
    }
    
    @Test
    public void testItemWithoutStrangeParts() throws Exception {
        // JSON string without strange parts
        String json = """
            {
                "id": "test_id",
                "event": "listing-update",
                "payload": {
                    "id": "440_test_no_parts",
                    "steamid": "76561198018333134",
                    "appid": 440,
                    "listedAt": 1736692873,
                    "bumpedAt": 1767058917,
                    "intent": "sell",
                    "count": 1,
                    "status": "active",
                    "source": "user",
                    "item": {
                        "appid": 440,
                        "baseName": "Scattergun",
                        "defindex": 13,
                        "marketName": "Scattergun",
                        "name": "Scattergun",
                        "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                        "tradable": true,
                        "craftable": true
                    }
                }
            }
            """;
        
        // Deserialize from JSON
        ListingUpdate listingUpdate = objectMapper.readValue(json, ListingUpdate.class);
        
        // Verify the listing update was parsed correctly
        assertNotNull(listingUpdate);
        assertNotNull(listingUpdate.getPayload());
        assertNotNull(listingUpdate.getPayload().getItem());
        
        // Strange parts should be null when not present in JSON
        assertNull(listingUpdate.getPayload().getItem().getStrangeParts(),
                  "Strange parts should be null when not present in JSON");
    }
    
    @Test
    public void testItemWithEmptyStrangeParts() throws Exception {
        // JSON string with empty strange parts array
        String json = """
            {
                "id": "test_id",
                "event": "listing-update",
                "payload": {
                    "id": "440_test_empty_parts",
                    "steamid": "76561198018333134",
                    "appid": 440,
                    "listedAt": 1736692873,
                    "bumpedAt": 1767058917,
                    "intent": "sell",
                    "count": 1,
                    "status": "active",
                    "source": "user",
                    "item": {
                        "appid": 440,
                        "baseName": "Strange Scattergun",
                        "defindex": 200,
                        "marketName": "Strange Scattergun",
                        "name": "Strange Scattergun",
                        "quality": {"id": 11, "name": "Strange", "color": "#CF6A32"},
                        "strangeParts": [],
                        "tradable": true,
                        "craftable": true
                    }
                }
            }
            """;
        
        // Deserialize from JSON
        ListingUpdate listingUpdate = objectMapper.readValue(json, ListingUpdate.class);
        
        // Verify the listing update was parsed correctly
        assertNotNull(listingUpdate);
        assertNotNull(listingUpdate.getPayload());
        assertNotNull(listingUpdate.getPayload().getItem());
        
        // Strange parts should be an empty list, not null
        assertNotNull(listingUpdate.getPayload().getItem().getStrangeParts(),
                     "Strange parts should not be null even when empty array");
        assertTrue(listingUpdate.getPayload().getItem().getStrangeParts().isEmpty(),
                  "Strange parts should be empty when empty array in JSON");
    }
    
    @Test
    public void testExactPayloadFromIssue() throws Exception {
        // This is the EXACT JSON payload that was causing the issue
        String json = """
            {
                "id": "440_76561198018333134_ae238bb55b7f9979e28bef87b085622b",
                "steamid": "76561198018333134",
                "appid": 440,
                "currencies": {"metal": 1.11, "keys": 11},
                "value": {"raw": 648.24, "short": "11.02 keys", "long": "11 keys, 1.11 ref"},
                "tradeOffersPreferred": true,
                "details": "Interested in parted ones [⇄]. Send an offer to negotiate. Also open to spelled ones for different price. Can beat others buyers!",
                "listedAt": 1736692873,
                "bumpedAt": 1767058917,
                "intent": "buy",
                "count": 1,
                "status": "active",
                "source": "user",
                "item": {
                    "appid": 440,
                    "baseName": "Loaf Loafers",
                    "defindex": 31105,
                    "id": "",
                    "imageUrl": "https://steamcdn-a.akamaihd.net/apps/440/icons/sum20_loaf_loafers_style1.0a522dd7743d33937e4dda909ef8574e59e6a280.png",
                    "marketName": "Strange Loaf Loafers",
                    "name": "Strange Loaf Loafers",
                    "origin": null,
                    "originalId": "",
                    "quality": {"id": 11, "name": "Strange", "color": "#CF6A32"},
                    "summary": "Level 1-100 Bread",
                    "price": {
                        "steam": {"currency": "usd", "short": "$13.99", "long": "508.73 ref, 8.65 keys", "raw": 391.9198571428571, "value": 1399},
                        "community": {"value": 11, "valueHigh": 14.3, "currency": "keys", "raw": 744.1995, "short": "11–14.3 keys", "long": "744.20 ref, $20.47", "usd": 21.800191875000003, "updatedAt": 1762448873, "difference": 295.186375},
                        "suggested": {"raw": 852.887925, "short": "14.5 keys", "long": "852.89 ref, $23.45", "usd": 23.4544179375}
                    },
                    "strangeParts": [
                        {
                            "score": 0,
                            "killEater": {
                                "id": 82,
                                "name": "Damage Dealt",
                                "item": {
                                    "appid": 440,
                                    "baseName": "Strange Part: Damage Dealt",
                                    "defindex": 6056,
                                    "id": "",
                                    "imageUrl": "https://steamcdn-a.akamaihd.net/apps/440/icons/strange_part_damage_dealt.54d3ff8e5e70497ec6348c6fd332c1777429aabc.png",
                                    "marketName": "Strange Part: Damage Dealt",
                                    "name": "Strange Part: Damage Dealt",
                                    "origin": null,
                                    "originalId": "",
                                    "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                                    "summary": "Level 1 Strange Part",
                                    "price": {
                                        "steam": {"currency": "usd", "short": "$15.60", "long": "567.27 ref, 9.64 keys", "raw": 437.0228571428571, "value": 1560},
                                        "community": {"value": 5.55, "valueHigh": 6.35, "currency": "keys", "raw": 350.03849999999994, "short": "5.55–6.35 keys", "long": "350.04 ref, $9.63", "usd": 9.949623749999999, "updatedAt": 1752708381, "difference": -90.67275000000001},
                                        "suggested": {"raw": 350.03849999999994, "short": "5.95 keys", "long": "350.04 ref, $9.63", "usd": 9.626058749999999}
                                    },
                                    "tradable": true,
                                    "craftable": true
                                }
                            }
                        },
                        {
                            "score": 0,
                            "killEater": {
                                "id": 28,
                                "name": "Dominations",
                                "item": {
                                    "appid": 440,
                                    "baseName": "Strange Part: Domination Kills",
                                    "defindex": 6016,
                                    "id": "",
                                    "imageUrl": "https://steamcdn-a.akamaihd.net/apps/440/icons/strange_part_killstartdomination.3e24971eebd1bb5c4bf2e7c786ca89c1def99824.png",
                                    "marketName": "Strange Part: Domination Kills",
                                    "name": "Strange Part: Domination Kills",
                                    "origin": null,
                                    "originalId": "",
                                    "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                                    "summary": "Level 1 Strange Part",
                                    "price": {
                                        "steam": {"currency": "usd", "short": "$12.49", "long": "454.18 ref, 7.72 keys", "raw": 349.89842857142855, "value": 1249},
                                        "community": {"value": 5.5, "valueHigh": 6.6, "currency": "keys", "raw": 355.9215, "short": "5.5–6.6 keys", "long": "355.92 ref, $9.79", "usd": 10.232743124999999, "updatedAt": 1762987856, "difference": -46.608375000000024},
                                        "suggested": {"raw": 355.9215, "short": "6.05 keys", "long": "355.92 ref, $9.79", "usd": 9.78784125}
                                    },
                                    "tradable": true,
                                    "craftable": true
                                }
                            }
                        },
                        {
                            "score": 0,
                            "killEater": {
                                "id": 87,
                                "name": "Kills",
                                "item": {
                                    "appid": 440,
                                    "baseName": "Strange Cosmetic Part: Kills",
                                    "defindex": 6060,
                                    "id": "",
                                    "imageUrl": "https://steamcdn-a.akamaihd.net/apps/440/icons/strange_cosmetic_part_kills.16438424758c2ef452a78d9b886940884d1603c3.png",
                                    "marketName": "Strange Cosmetic Part: Kills",
                                    "name": "Strange Cosmetic Part: Kills",
                                    "origin": null,
                                    "originalId": "",
                                    "quality": {"id": 6, "name": "Unique", "color": "#FFD700"},
                                    "summary": "Level 1 Strange Part",
                                    "price": {
                                        "steam": {"currency": "usd", "short": "$13.11", "long": "476.73 ref, 8.10 keys", "raw": 367.26728571428566, "value": 1311},
                                        "community": {"value": 6.05, "valueHigh": 6.9, "currency": "keys", "raw": 380.92425, "short": "6.05–6.9 keys", "long": "380.92 ref, $10.48", "usd": 10.8192046875, "updatedAt": 1762201415, "difference": 16.948500000000024},
                                        "suggested": {"raw": 380.92425, "short": "6.48 keys", "long": "380.92 ref, $10.48", "usd": 10.475416874999999}
                                    },
                                    "tradable": true,
                                    "craftable": true
                                }
                            }
                        }
                    ],
                    "slot": "misc",
                    "tradable": true,
                    "craftable": true
                },
                "userAgent": {"client": "76561198018333134", "lastPulse": 1767060671},
                "user": {
                    "id": "76561198018333134",
                    "name": "Philibert | Buy Bp's/Spells",
                    "avatar": "https://avatars.steamstatic.com/b13bde7437ef9f4c77e1039ee32d21d5be3b155c_medium.jpg",
                    "avatarFull": "https://avatars.steamstatic.com/b13bde7437ef9f4c77e1039ee32d21d5be3b155c_full.jpg",
                    "premium": true,
                    "online": true,
                    "banned": false,
                    "customNameStyle": "xmas",
                    "class": "xmas",
                    "style": "",
                    "role": null,
                    "tradeOfferUrl": "https://steamcommunity.com/tradeoffer/new/?partner=58067406&token=H-mVqjA9",
                    "bans": []
                }
            }
            """;
        
        // Test as a payload directly (not wrapped in ListingUpdate)
        ListingUpdate.Payload payload = objectMapper.readValue(json, ListingUpdate.Payload.class);
        
        // This should NOT be null if deserialization is working
        assertNotNull(payload.getItem(), "Item should not be null");
        assertNotNull(payload.getItem().getStrangeParts(), "Strange parts should not be null for this exact payload");
        assertEquals(3, payload.getItem().getStrangeParts().size(), "Should have exactly 3 strange parts");
        
        // Verify the specific strange part IDs that should be extracted
        assertEquals(82, payload.getItem().getStrangeParts().get(0).getKillEater().getId());
        assertEquals(28, payload.getItem().getStrangeParts().get(1).getKillEater().getId());
        assertEquals(87, payload.getItem().getStrangeParts().get(2).getKillEater().getId());
    }
    
    @Test
    public void testJsonRoundTrip() throws Exception {
        // Create a ListingUpdate with strange parts
        ListingUpdate original = new ListingUpdate();
        original.setId("test_id");
        original.setEvent("listing-update");
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.setId("440_test_roundtrip");
        payload.setSteamid("76561198018333134");
        payload.setAppid(440);
        payload.setListedAt(1736692873L);
        payload.setBumpedAt(1767058917L);
        payload.setIntent("sell");
        payload.setCount(1);
        payload.setStatus("active");
        payload.setSource("user");
        
        ListingUpdate.Item item = new ListingUpdate.Item();
        item.setAppid(440);
        item.setBaseName("Strange Rocket Launcher");
        item.setDefindex(205);
        item.setMarketName("Strange Rocket Launcher");
        item.setName("Strange Rocket Launcher");
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.setId(11);
        quality.setName("Strange");
        quality.setColor("#CF6A32");
        item.setQuality(quality);
        
        // Create strange parts
        java.util.List<ListingUpdate.StrangePart> strangeParts = new java.util.ArrayList<>();
        
        ListingUpdate.StrangePart part = new ListingUpdate.StrangePart();
        part.setScore(42);
        
        ListingUpdate.KillEater killEater = new ListingUpdate.KillEater();
        killEater.setId(0);
        killEater.setName("Kills");
        part.setKillEater(killEater);
        
        strangeParts.add(part);
        item.setStrangeParts(strangeParts);
        
        item.setTradable(true);
        item.setCraftable(true);
        
        payload.setItem(item);
        original.setPayload(payload);
        
        // Serialize and deserialize
        String json = objectMapper.writeValueAsString(original);
        ListingUpdate deserialized = objectMapper.readValue(json, ListingUpdate.class);
        
        // Verify strange parts survived the round trip
        assertNotNull(deserialized.getPayload().getItem().getStrangeParts());
        assertEquals(1, deserialized.getPayload().getItem().getStrangeParts().size());
        assertEquals(42, deserialized.getPayload().getItem().getStrangeParts().get(0).getScore());
        assertEquals(0, deserialized.getPayload().getItem().getStrangeParts().get(0).getKillEater().getId());
        assertEquals("Kills", deserialized.getPayload().getItem().getStrangeParts().get(0).getKillEater().getName());
    }
}