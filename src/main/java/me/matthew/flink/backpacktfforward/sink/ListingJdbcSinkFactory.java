package me.matthew.flink.backpacktfforward.sink;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;
import java.sql.Types;

@Slf4j
public class ListingJdbcSinkFactory {

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

    private static final String MARK_DELETED_SQL = """
        UPDATE listings
        SET is_deleted = true, updated_at = now()
        WHERE id = ?;
        """;

    private static final JdbcExecutionOptions EXEC_OPTS = JdbcExecutionOptions.builder()
            .withBatchSize(1000)
            .withBatchIntervalMs(200)
            .withMaxRetries(5)
            .build();

    private static final JdbcConnectionOptions CONNECTION_OPTS = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:postgresql://localhost:5432/testdb")
            .withDriverName("org.postgresql.Driver")
            .withUsername("testuser")
            .withPassword("testpass")
            .build();

    /**
     * Creates the sink for handling "listing-update" events.
     */
    public static SinkFunction<ListingUpdate> upsertSink() {
        return JdbcSink.sink(
                UPSERT_SQL,
                (statement, lu) -> {
                    var payload = lu.getPayload();

                    log.info("Preparing to upsert listing: id={} steamid={}, defindex={}, quality={}, intent={}, payload={}",
                            payload.getId(),
                            payload.getSteamid(),
                            payload.getItem().getDefindex(),
                            payload.getItem().getQuality().getId(),
                            payload.getIntent(),
                            payload);


                    statement.setString(1, payload.getId());
                    statement.setString(2, payload.getSteamid());
                    statement.setInt(3, payload.getItem().getDefindex());
                    statement.setInt(4, payload.getItem().getQuality().getId());
                    statement.setString(5, payload.getIntent());
                    statement.setInt(6, payload.getAppid());

                    // Currencies
                    if (payload.getCurrencies() != null && payload.getCurrencies().getMetal() != null)
                        statement.setDouble(7, payload.getCurrencies().getMetal());
                    else
                        statement.setNull(7, Types.DOUBLE);

                    if (payload.getCurrencies() != null && payload.getCurrencies().getKeys() != null)
                        statement.setInt(8, payload.getCurrencies().getKeys());
                    else
                        statement.setNull(8, Types.INTEGER);

                    // Value
                    if (payload.getValue() != null)
                        statement.setDouble(9, payload.getValue().getRaw());
                    else
                        statement.setNull(9, Types.DOUBLE);

                    if (payload.getValue() != null && payload.getValue().getShortStr() != null)
                        statement.setString(10, payload.getValue().getShortStr());
                    else
                        statement.setNull(10, Types.VARCHAR);

                    if (payload.getValue() != null && payload.getValue().getLongStr() != null)
                        statement.setString(11, payload.getValue().getLongStr());
                    else
                        statement.setNull(11, Types.VARCHAR);

                    // Details
                    if (payload.getDetails() != null)
                        statement.setString(12, payload.getDetails());
                    else
                        statement.setNull(12, Types.VARCHAR);

                    // Listed at
                    statement.setTimestamp(13, new Timestamp(payload.getListedAt() * 1000));

                    // Market name
                    if (payload.getItem() != null && payload.getItem().getMarketName() != null)
                        statement.setString(14, payload.getItem().getMarketName());
                    else
                        statement.setNull(14, Types.VARCHAR);

                    // Status
                    if (payload.getStatus() != null)
                        statement.setString(15, payload.getStatus());
                    else
                        statement.setNull(15, Types.VARCHAR);

                    // Status
                    if (payload.getUserAgent() != null && payload.getUserAgent().getClient() != null)
                        statement.setString(16, payload.getUserAgent().getClient());
                    else
                        statement.setNull(16, Types.VARCHAR);

                    if (payload.getUser() != null && payload.getUser().getName() != null)
                        statement.setString(17, payload.getUser().getName());
                    else
                        statement.setNull(17, Types.VARCHAR);

                    if (payload.getUser() != null && payload.getUser().getPremium() != null)
                        statement.setBoolean(18, payload.getUser().getPremium());
                    else
                        statement.setNull(18, Types.BOOLEAN);

                    if (payload.getUser() != null && payload.getUser().getOnline() != null)
                        statement.setBoolean(19, payload.getUser().getOnline());
                    else
                        statement.setNull(19, Types.BOOLEAN);

                    if (payload.getUser() != null && payload.getUser().getBanned() != null)
                        statement.setBoolean(20, payload.getUser().getBanned());
                    else
                        statement.setNull(20, Types.BOOLEAN);

                    if (payload.getUser() != null && payload.getUser().getTradeOfferUrl() != null)
                        statement.setString(21, payload.getUser().getTradeOfferUrl());
                    else
                        statement.setNull(21, Types.VARCHAR);

                    if (payload.getItem() != null && payload.getItem().getTradable() != null)
                        statement.setBoolean(22, payload.getItem().getTradable());
                    else
                        statement.setNull(22, Types.BOOLEAN);

                    if (payload.getItem() != null && payload.getItem().getCraftable() != null)
                        statement.setBoolean(23, payload.getItem().getCraftable());
                    else
                        statement.setNull(23, Types.BOOLEAN);

                    if (payload.getItem() != null && payload.getItem().getQuality() != null && payload.getItem().getQuality().getColor() != null)
                        statement.setString(24, payload.getItem().getQuality().getColor());
                    else
                        statement.setNull(24, Types.VARCHAR);

                    if (payload.getItem() != null && payload.getItem().getParticle() != null && payload.getItem().getParticle().getName() != null)
                        statement.setString(25, payload.getItem().getParticle().getName());
                    else
                        statement.setNull(25, Types.VARCHAR);

                    if (payload.getItem() != null && payload.getItem().getParticle() != null && payload.getItem().getParticle().getType() != null)
                        statement.setString(26, payload.getItem().getParticle().getType());
                    else
                        statement.setNull(26, Types.VARCHAR);

                    statement.setTimestamp(27, new Timestamp(payload.getBumpedAt() * 1000));
                },
                EXEC_OPTS,
                CONNECTION_OPTS
        );
    }

    /**
     * Creates the sink for handling "listing-delete" events.
     */
    public static SinkFunction<ListingUpdate> deleteSink() {
        return JdbcSink.sink(
                MARK_DELETED_SQL,
                (statement, lu) -> {
                    var payload = lu.getPayload();
                    var item = payload.getItem();

                    log.info("Preparing to soft delete listing: id={} steamid={}, defindex={}, quality={}, intent={}, payload={}",
                            payload.getId(),
                            payload.getSteamid(),
                            item.getDefindex(),
                            item.getQuality().getId(),
                            payload.getIntent(),
                            payload);

                    statement.setString(1, payload.getId());
                },
                EXEC_OPTS,
                CONNECTION_OPTS
        );
    }
}
