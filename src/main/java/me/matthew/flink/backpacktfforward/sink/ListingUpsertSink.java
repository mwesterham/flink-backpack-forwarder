package me.matthew.flink.backpacktfforward.sink;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.*;

@Slf4j
public class ListingUpsertSink extends RichSinkFunction<ListingUpdate> {

    private static final String UPSERT_SQL = """
        INSERT INTO listings (
            id, steamid, item_defindex, item_quality_id, intent, appid, metal, keys,
            raw_value, short_value, long_value, details, listed_at,
            market_name, status, user_agent_client, user_name, user_premium, user_online,
            user_banned, user_trade_offer_url, item_tradable, item_craftable,
            item_quality_color, item_particle_name, item_particle_type, bumped_at, is_deleted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false)
        ON CONFLICT (id) DO UPDATE SET
            steamid = EXCLUDED.steamid,
            item_defindex = EXCLUDED.item_defindex,
            item_quality_id = EXCLUDED.item_quality_id,
            intent = EXCLUDED.intent,
            appid = EXCLUDED.appid,
            metal = EXCLUDED.metal,
            keys = EXCLUDED.keys,
            raw_value = EXCLUDED.raw_value,
            short_value = EXCLUDED.short_value,
            long_value = EXCLUDED.long_value,
            details = EXCLUDED.details,
            listed_at = EXCLUDED.listed_at,
            market_name = EXCLUDED.market_name,
            status = EXCLUDED.status,
            user_agent_client = EXCLUDED.user_agent_client,
            user_name = EXCLUDED.user_name,
            user_premium = EXCLUDED.user_premium,
            user_online = EXCLUDED.user_online,
            user_banned = EXCLUDED.user_banned,
            user_trade_offer_url = EXCLUDED.user_trade_offer_url,
            item_tradable = EXCLUDED.item_tradable,
            item_craftable = EXCLUDED.item_craftable,
            item_quality_color = EXCLUDED.item_quality_color,
            item_particle_name = EXCLUDED.item_particle_name,
            item_particle_type = EXCLUDED.item_particle_type,
            bumped_at = EXCLUDED.bumped_at,
            is_deleted = false,
            updated_at = now();
        """;

    private Connection connection;
    private PreparedStatement stmt;
    private transient Counter upsertCounter;

    private final List<ListingUpdate> batch = new ArrayList<>();
    private final int batchSize;
    private final long batchIntervalMs;
    private long lastFlushTime;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public ListingUpsertSink(String jdbcUrl, String username, String password, int batchSize, long batchIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
        stmt = connection.prepareStatement(UPSERT_SQL);

        upsertCounter = getRuntimeContext().getMetricGroup().counter("listing_upserts");
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(ListingUpdate lu, Context context) throws Exception {
        batch.add(lu);

        long now = System.currentTimeMillis();
        if (batch.size() >= batchSize || now - lastFlushTime >= batchIntervalMs) {
            flushBatch();
            lastFlushTime = now;
        }
    }

    /**
     * Flush with ordering + retry on deadlock (40P01)
     */
    private void flushBatch() throws SQLException {
        if (batch.isEmpty()) return;

        // 1. Order deterministically by primary key to avoid deadlocks
        batch.sort(Comparator.comparing(l -> l.getPayload().getId()));

        int maxRetries = 5;
        int attempt = 0;

        while (true) {
            try {
                stmt.clearBatch();

                for (ListingUpdate lu : batch) {
                    var payload = lu.getPayload();

                    stmt.setString(1, payload.getId());
                    stmt.setString(2, payload.getSteamid());
                    stmt.setInt(3, payload.getItem().getDefindex());
                    stmt.setObject(4, payload.getItem().getQuality() != null ? payload.getItem().getQuality().getId() : null, Types.INTEGER);
                    stmt.setString(5, payload.getIntent());
                    stmt.setInt(6, payload.getAppid());

                    if (payload.getCurrencies() != null && payload.getCurrencies().getMetal() != null)
                        stmt.setDouble(7, payload.getCurrencies().getMetal());
                    else stmt.setNull(7, Types.DOUBLE);

                    if (payload.getCurrencies() != null && payload.getCurrencies().getKeys() != null)
                        stmt.setInt(8, payload.getCurrencies().getKeys());
                    else stmt.setNull(8, Types.INTEGER);

                    if (payload.getValue() != null)
                        stmt.setDouble(9, payload.getValue().getRaw());
                    else stmt.setNull(9, Types.DOUBLE);

                    stmt.setString(10, payload.getValue() != null ? payload.getValue().getShortStr() : null);
                    stmt.setString(11, payload.getValue() != null ? payload.getValue().getLongStr() : null);

                    stmt.setString(12, payload.getDetails());
                    stmt.setTimestamp(13, new Timestamp(payload.getListedAt() * 1000));
                    stmt.setString(14, payload.getItem() != null ? payload.getItem().getMarketName() : null);
                    stmt.setString(15, payload.getStatus());
                    stmt.setString(16, payload.getUserAgent() != null ? payload.getUserAgent().getClient() : null);
                    stmt.setString(17, payload.getUser() != null ? payload.getUser().getName() : null);

                    stmt.setObject(18, payload.getUser() != null ? payload.getUser().getPremium() : null, Types.BOOLEAN);
                    stmt.setObject(19, payload.getUser() != null ? payload.getUser().getOnline() : null, Types.BOOLEAN);
                    stmt.setObject(20, payload.getUser() != null ? payload.getUser().getBanned() : null, Types.BOOLEAN);
                    stmt.setString(21, payload.getUser() != null ? payload.getUser().getTradeOfferUrl() : null);

                    stmt.setObject(22, payload.getItem() != null ? payload.getItem().getTradable() : null, Types.BOOLEAN);
                    stmt.setObject(23, payload.getItem() != null ? payload.getItem().getCraftable() : null, Types.BOOLEAN);
                    stmt.setString(24, payload.getItem() != null && payload.getItem().getQuality() != null ? payload.getItem().getQuality().getColor() : null);
                    stmt.setString(25, payload.getItem() != null && payload.getItem().getParticle() != null ? payload.getItem().getParticle().getName() : null);
                    stmt.setString(26, payload.getItem() != null && payload.getItem().getParticle() != null ? payload.getItem().getParticle().getType() : null);
                    stmt.setTimestamp(27, new Timestamp(payload.getBumpedAt() * 1000));

                    stmt.addBatch();
                    upsertCounter.inc();
                }

                stmt.executeBatch();
                connection.commit();

                batch.clear();
                return; // success
            }
            catch (SQLException e) {
                // Only retry on deadlock
                if ("40P01".equals(e.getSQLState())) {
                    attempt++;

                    log.warn("Deadlock detected on batch (attempt {}/{}) - retrying", attempt, maxRetries);

                    connection.rollback();

                    if (attempt > maxRetries) {
                        log.error("Max deadlock retries exceeded. Failing batch.");
                        throw e;
                    }

                    // Exponential jitter backoff
                    try {
                        Thread.sleep(50L * attempt + (long)(Math.random() * 30));
                    } catch (InterruptedException ignored) {}

                    continue; // retry
                }

                connection.rollback();
                throw e; // non-deadlock errors fail immediately
            }
        }
    }

    @Override
    public void close() throws Exception {
        flushBatch();
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
        super.close();
    }
}
