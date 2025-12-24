# Listing ID Generation Examples

This document provides examples of how BackpackTF listing IDs are generated for different types of listings.

## Background

BackpackTF listing IDs are not included in the snapshots API response, but they can be generated using documented patterns:

- **Sell listings**: `{appid}_{assetid}`
- **Buy listings**: `{appid}_{steamid}_{md5 hash of item name}`

## Examples

### Sell Listing

For a sell listing where:
- App ID: 440 (Team Fortress 2)
- Asset ID: 12345678901234567890

**Generated ID**: `440_12345678901234567890`

```java
String listingId = ListingIdGenerator.generateSellListingId(440, "12345678901234567890");
// Result: "440_12345678901234567890"
```

### Buy Listing

For a buy listing where:
- App ID: 440 (Team Fortress 2)
- Steam ID: 76561198000000000
- Item Name: "Strange Scattergun"

**Generated ID**: `440_76561198000000000_{md5_hash_of_strange_scattergun}`

```java
String listingId = ListingIdGenerator.generateBuyListingId(440, "76561198000000000", "Strange Scattergun");
// Result: "440_76561198000000000_8b9c8c8e8f8a8d8e8c8b8a8d8e8f8c8b" (example MD5)
```

### Using the Generic Method

```java
// For sell listings
String sellId = ListingIdGenerator.generateListingId(440, "76561198000000000", "12345678901234567890", "Strange Scattergun", "sell");
// Result: "440_12345678901234567890"

// For buy listings  
String buyId = ListingIdGenerator.generateListingId(440, "76561198000000000", "12345678901234567890", "Strange Scattergun", "buy");
// Result: "440_76561198000000000_{md5_hash}"
```

## Usage in BackfillProcessor

The BackfillProcessor now correctly generates listing IDs based on the intent:

```java
// Generate the correct listing ID based on intent
String listingId;
if ("sell".equalsIgnoreCase(apiListing.getIntent())) {
    // For sell listings: {appid}_{assetid}
    listingId = ListingIdGenerator.generateSellListingId(440, String.valueOf(matchingItem.getId()));
} else if ("buy".equalsIgnoreCase(apiListing.getIntent())) {
    // For buy listings: {appid}_{steamid}_{md5 hash of item name}
    listingId = ListingIdGenerator.generateBuyListingId(440, apiListing.getSteamid(), marketName);
} else {
    log.warn("Unknown intent '{}' for steamid={}. Skipping this listing.", 
            apiListing.getIntent(), apiListing.getSteamid());
    continue;
}

BackpackTfListingDetail listingDetail = apiClient.getListing(listingId);
```

## Key Changes Made

1. **Fixed format string**: Changed from `String.format("{}_{}", 440, matchingItem.getId())` to proper format strings
2. **Added intent-based logic**: Different ID generation for sell vs buy listings
3. **Added MD5 hashing**: Proper MD5 hash generation for buy listing item names
4. **Added validation**: Proper error handling for missing required fields
5. **Added comprehensive tests**: Unit tests covering all scenarios and edge cases

This resolves the issue where listing IDs were being generated incorrectly, causing getListing API calls to fail.