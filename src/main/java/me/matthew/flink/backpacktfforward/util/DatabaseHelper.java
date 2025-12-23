package me.matthew.flink.backpacktfforward.util;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Database helper class for backfill operations.
 * Provides methods for querying market names and existing listings
 * following existing database connection and retry patterns.
 */
@Slf4j
public class DatabaseHelper {
    
    private static final String MARKET_NAME_QUERY = """
        SELECT DISTINCT market_name 
        FROM listings 
        WHERE item_defindex = ? AND item_quality_id = ? 
        AND market_name IS NOT NULL 
        ORDER BY updated_at DESC 
        LIMIT 1
        """;
    
    private static final String EXISTING_LISTINGS_QUERY = """
        SELECT id, steamid 
        FROM listings 
        WHERE item_defindex = ? AND item_quality_id = ? AND is_deleted = false
        """;
    
    private static final String LISTING_ID_BY_MARKET_STEAMID_QUERY = """
        SELECT id 
        FROM listings 
        WHERE market_name = ? AND steamid = ? AND is_deleted = false
        ORDER BY updated_at DESC 
        LIMIT 1
        """;
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final RetryPolicy<Object> retryPolicy;
    
    /**
     * Creates a new DatabaseHelper with connection parameters and retry policy.
     * 
     * @param jdbcUrl Database JDBC URL
     * @param username Database username  
     * @param password Database password
     * @param retryPolicy Retry policy for handling database errors
     */
    public DatabaseHelper(String jdbcUrl, String username, String password, RetryPolicy<Object> retryPolicy) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.retryPolicy = retryPolicy;
    }
    
    /**
     * Queries the database for the most recent market_name associated with the given item identifiers.
     * Uses existing database connection patterns and retry logic.
     * 
     * @param itemDefindex Item definition index
     * @param itemQualityId Item quality ID
     * @return Market name if found, null if no market_name exists for the item combination
     * @throws SQLException if database query fails after retries
     */
    public String getMarketName(int itemDefindex, int itemQualityId) throws SQLException {
        log.debug("Querying market_name for item_defindex={}, item_quality_id={}", itemDefindex, itemQualityId);
        
        return Failsafe.with(retryPolicy).get(() -> {
            try (Connection connection = createConnection();
                 PreparedStatement stmt = connection.prepareStatement(MARKET_NAME_QUERY)) {
                
                stmt.setInt(1, itemDefindex);
                stmt.setInt(2, itemQualityId);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String marketName = rs.getString("market_name");
                        log.debug("Found market_name: {} for item_defindex={}, item_quality_id={}", 
                                marketName, itemDefindex, itemQualityId);
                        return marketName;
                    } else {
                        log.debug("No market_name found for item_defindex={}, item_quality_id={}", 
                                itemDefindex, itemQualityId);
                        return null;
                    }
                }
            }
        });
    }
    
    /**
     * Represents an existing listing in the database.
     */
    public static class ExistingListing {
        private final String id;
        private final String steamid;
        
        public ExistingListing(String id, String steamid) {
            this.id = id;
            this.steamid = steamid;
        }
        
        public String getId() {
            return id;
        }
        
        public String getSteamid() {
            return steamid;
        }
    }
    
    /**
     * Queries the database for existing listings with the same item identifiers.
     * Returns id and steamid for comparison with API response.
     * Filters out already deleted listings (is_deleted=false).
     * Uses existing database connection and retry patterns.
     * 
     * @param itemDefindex Item definition index
     * @param itemQualityId Item quality ID
     * @return List of existing listings with id and steamid
     * @throws SQLException if database query fails after retries
     */
    public List<ExistingListing> getExistingListings(int itemDefindex, int itemQualityId) throws SQLException {
        log.debug("Querying existing listings for item_defindex={}, item_quality_id={}", itemDefindex, itemQualityId);
        
        return Failsafe.with(retryPolicy).get(() -> {
            List<ExistingListing> existingListings = new ArrayList<>();
            
            try (Connection connection = createConnection();
                 PreparedStatement stmt = connection.prepareStatement(EXISTING_LISTINGS_QUERY)) {
                
                stmt.setInt(1, itemDefindex);
                stmt.setInt(2, itemQualityId);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String id = rs.getString("id");
                        String steamid = rs.getString("steamid");
                        existingListings.add(new ExistingListing(id, steamid));
                    }
                }
            }
            
            log.debug("Found {} existing listings for item_defindex={}, item_quality_id={}", 
                    existingListings.size(), itemDefindex, itemQualityId);
            return existingListings;
        });
    }
    
    /**
     * Queries the database for existing listing ID based on market_name and steamid combination.
     * Returns the most recent listing ID if found, null if no matching listing exists.
     * 
     * @param marketName Market name of the item
     * @param steamId Steam ID of the user
     * @return Listing ID if found, null if no matching listing exists
     * @throws SQLException if database query fails after retries
     */
    public String getExistingListingId(String marketName, String steamId) throws SQLException {
        log.debug("Querying existing listing ID for market_name={}, steamid={}", marketName, steamId);
        
        return Failsafe.with(retryPolicy).get(() -> {
            try (Connection connection = createConnection();
                 PreparedStatement stmt = connection.prepareStatement(LISTING_ID_BY_MARKET_STEAMID_QUERY)) {
                
                stmt.setString(1, marketName);
                stmt.setString(2, steamId);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String listingId = rs.getString("id");
                        log.debug("Found existing listing ID: {} for market_name={}, steamid={}", 
                                listingId, marketName, steamId);
                        return listingId;
                    } else {
                        log.debug("No existing listing ID found for market_name={}, steamid={}", 
                                marketName, steamId);
                        return null;
                    }
                }
            }
        });
    }
    
    /**
     * Creates a new database connection following existing patterns from sinks.
     * 
     * @return Database connection
     * @throws SQLException if connection fails
     */
    private Connection createConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL driver not found", e);
        }
    }
}