package me.matthew.flink.backpacktfforward.sink;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.LISTING_UPSERTS;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.LISTING_UPSERT_RETRIES;

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
            updated_at = now()
        WHERE EXCLUDED.bumped_at >= listings.updated_at;
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
    private RetryPolicy<Object> retryPolicy;

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

        upsertCounter = getRuntimeContext().getMetricGroup().counter(LISTING_UPSERTS);
        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(),
                LISTING_UPSERT_RETRIES
        );
        lastFlushTime = System.currentTimeMillis();

        retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);
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
     * Flush with deterministic ordering + Failsafe retry
     */
    private void flushBatch() throws SQLException {
        if (batch.isEmpty())
            return;

        // Sort rows by PK to guarantee consistent lock order
        batch.sort(Comparator.comparing(l -> l.getPayload().getId()));

        Failsafe.with(retryPolicy).run(() -> {
            try {
                stmt.clearBatch();

                for (ListingUpdate lu : batch) {
                    var p = lu.getPayload();

                    stmt.setString(1, p.getId());
                    stmt.setString(2, p.getSteamid());
                    stmt.setInt(3, p.getItem().getDefindex());
                    stmt.setObject(4, p.getItem().getQuality() != null ? p.getItem().getQuality().getId() : null, Types.INTEGER);
                    stmt.setString(5, p.getIntent());
                    stmt.setInt(6, p.getAppid());

                    if (p.getCurrencies() != null && p.getCurrencies().getMetal() != null)
                        stmt.setDouble(7, p.getCurrencies().getMetal());
                    else stmt.setNull(7, Types.DOUBLE);

                    if (p.getCurrencies() != null && p.getCurrencies().getKeys() != null)
                        stmt.setInt(8, p.getCurrencies().getKeys());
                    else stmt.setNull(8, Types.INTEGER);

                    if (p.getValue() != null)
                        stmt.setDouble(9, p.getValue().getRaw());
                    else stmt.setNull(9, Types.DOUBLE);

                    stmt.setString(10, p.getValue() != null ? p.getValue().getShortStr() : null);
                    stmt.setString(11, p.getValue() != null ? p.getValue().getLongStr() : null);
                    stmt.setString(12, p.getDetails());
                    stmt.setTimestamp(13, new Timestamp(p.getListedAt() * 1000));
                    stmt.setString(14, p.getItem() != null ? p.getItem().getMarketName() : null);
                    stmt.setString(15, p.getStatus());
                    stmt.setString(16, p.getUserAgent() != null ? p.getUserAgent().getClient() : null);
                    stmt.setString(17, p.getUser() != null ? p.getUser().getName() : null);

                    stmt.setObject(18, p.getUser() != null ? p.getUser().getPremium() : null, Types.BOOLEAN);
                    stmt.setObject(19, p.getUser() != null ? p.getUser().getOnline() : null, Types.BOOLEAN);
                    stmt.setObject(20, p.getUser() != null ? p.getUser().getBanned() : null, Types.BOOLEAN);
                    stmt.setString(21, p.getUser() != null ? p.getUser().getTradeOfferUrl() : null);

                    stmt.setObject(22, p.getItem() != null ? p.getItem().getTradable() : null, Types.BOOLEAN);
                    stmt.setObject(23, p.getItem() != null ? p.getItem().getCraftable() : null, Types.BOOLEAN);
                    stmt.setString(24, p.getItem() != null && p.getItem().getQuality() != null ? p.getItem().getQuality().getColor() : null);
                    stmt.setString(25, p.getItem() != null && p.getItem().getParticle() != null ? p.getItem().getParticle().getName() : null);
                    stmt.setString(26, p.getItem() != null && p.getItem().getParticle() != null ? p.getItem().getParticle().getType() : null);
                    stmt.setTimestamp(27, new Timestamp(p.getBumpedAt() * 1000));

                    stmt.addBatch();
                    upsertCounter.inc();
                }

                stmt.executeBatch();
                connection.commit();
            }
            catch (SQLException ex) {
                connection.rollback();
                throw ex;
            }
        });

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
