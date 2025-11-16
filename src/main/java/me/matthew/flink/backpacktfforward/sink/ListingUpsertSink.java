package me.matthew.flink.backpacktfforward.sink;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
    private final int batchSize = 10;
    private final long batchIntervalMs = 200;
    private long lastFlushTime;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public ListingUpsertSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize JDBC connection
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false); // use batching
        stmt = connection.prepareStatement(UPSERT_SQL);

        // Metrics
        upsertCounter = getRuntimeContext()
                .getMetricGroup().counter("listing_upserts");

        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(ListingUpdate lu, Context context) throws Exception {
        batch.add(lu);
        var payload = lu.getPayload();


        log.info("Preparing to upsert listing: id={} steamid={}, defindex={}, quality={}, intent={}",
                payload.getId(),
                payload.getSteamid(),
                payload.getItem().getDefindex(),
                payload.getItem().getQuality().getId(),
                payload.getIntent());

        long now = System.currentTimeMillis();

        if (batch.size() >= batchSize || now - lastFlushTime >= batchIntervalMs) {
            log.info("Preparing to commit batch upserts currentBatchCount={} lastFlushTime={}", batch.size(), lastFlushTime);
            flushBatch();
            lastFlushTime = now;
        }
    }

    private void flushBatch() throws SQLException {
        if (batch.isEmpty()) return;

        for (ListingUpdate lu : batch) {
            var payload = lu.getPayload();
            // Set parameters (like in your original JdbcSink lambda)
            stmt.setString(1, payload.getId());
            stmt.setString(2, payload.getSteamid());
            stmt.setInt(3, payload.getItem().getDefindex());
            stmt.setInt(4, payload.getItem().getQuality().getId());
            stmt.setString(5, payload.getIntent());
            stmt.setInt(6, payload.getAppid());

            // Currencies
            if (payload.getCurrencies() != null && payload.getCurrencies().getMetal() != null)
                stmt.setDouble(7, payload.getCurrencies().getMetal());
            else stmt.setNull(7, Types.DOUBLE);

            if (payload.getCurrencies() != null && payload.getCurrencies().getKeys() != null)
                stmt.setInt(8, payload.getCurrencies().getKeys());
            else stmt.setNull(8, Types.INTEGER);

            // Value
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
    }

    @Override
    public void close() throws Exception {
        flushBatch();
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
        super.close();
    }
}
