package me.matthew.flink.backpacktfforward;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.List;

@Slf4j
public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        log.info("Beginning WebSocketForwarderJob.");
        ObjectMapper mapper = new ObjectMapper();
        String sourceUrl = "ws://192.168.1.18:30331/forwarded";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new WebSocketSource(sourceUrl));
        source.flatMap((String event, Collector<ListingUpdate> out) -> {
                    try {
                        // Deserialize as a list
                        List<ListingUpdate> updates = mapper.readValue(event, new TypeReference<List<ListingUpdate>>() {});
                        for (ListingUpdate update : updates) {
                            out.collect(update);
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse WebSocket payload", e);
                    }
                }).returns(ListingUpdate.class)
                .addSink(
                        JdbcSink.sink(
                                new StringBuilder()
                                        .append("INSERT INTO listings (")
                                        .append("id, steamid, appid, metal, keys, raw_value, short_value, long_value, details, listed_at")
                                        .append(") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ")
                                        .append("ON CONFLICT (steamid, id) DO UPDATE SET ")
                                        .append("event = EXCLUDED.event, ")
                                        .append("appid = EXCLUDED.appid, ")
                                        .append("metal = EXCLUDED.metal, ")
                                        .append("keys = EXCLUDED.keys, ")
                                        .append("raw_value = EXCLUDED.raw_value, ")
                                        .append("short_value = EXCLUDED.short_value, ")
                                        .append("long_value = EXCLUDED.long_value, ")
                                        .append("details = EXCLUDED.details, ")
                                        .append("listed_at = EXCLUDED.listed_at, ")
                                        .append("updated_at = now()")
                                        .toString(),
                                (statement, lu) -> {
                                    String sku = lu.getPayload().getItem().getDefindex() + ";" + lu.getPayload().getItem().getQuality().getId();
                                    try {
                                        log.info("Preparing to upsert listing: id={}, steamid={}", sku, lu.getPayload().getSteamid());

                                        statement.setString(1, sku);
                                        statement.setString(2, lu.getPayload().getSteamid());
                                        statement.setInt(3, lu.getPayload().getAppid());

                                        // Nullable currencies
                                        Double metal = lu.getPayload().getCurrencies() != null ? lu.getPayload().getCurrencies().getMetal() : null;
                                        Integer keys = lu.getPayload().getCurrencies() != null ? lu.getPayload().getCurrencies().getKeys() : null;
                                        if (metal != null) {
                                            statement.setDouble(4, metal);
                                        } else {
                                            statement.setNull(4, java.sql.Types.DOUBLE);
                                            log.debug("Metal is null for listing id={}", sku);
                                        }
                                        if (keys != null) {
                                            statement.setInt(5, keys);
                                        } else {
                                            statement.setNull(5, java.sql.Types.INTEGER);
                                            log.debug("Keys is null for listing id={}", sku);
                                        }

                                        // Nullable value
                                        Double raw = lu.getPayload().getValue() != null ? lu.getPayload().getValue().getRaw() : null;
                                        String shortValue = lu.getPayload().getValue() != null ? lu.getPayload().getValue().getShortStr() : null;
                                        String longValue = lu.getPayload().getValue() != null ? lu.getPayload().getValue().getLongStr() : null;
                                        if (raw != null) statement.setDouble(6, raw); else statement.setNull(6, java.sql.Types.DOUBLE);
                                        if (shortValue != null) statement.setString(7, shortValue); else statement.setNull(7, java.sql.Types.VARCHAR);
                                        if (longValue != null) statement.setString(8, longValue); else statement.setNull(8, java.sql.Types.VARCHAR);

                                        // Details
                                        String details = lu.getPayload().getDetails();
                                        if (details != null) statement.setString(9, details); else statement.setNull(9, java.sql.Types.VARCHAR);

                                        // listed_at
                                        statement.setLong(10, lu.getPayload().getListedAt());

                                        log.info("Listing prepared for upsert: id={}, steamid={}", sku, lu.getPayload().getSteamid());

                                    } catch (Exception e) {
                                        log.error("Error preparing statement for listing id={}", sku, e);
                                        throw e; // rethrow so Flink knows this record failed
                                    }
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:postgresql://localhost:5432/testdb")
                                        .withDriverName("org.postgresql.Driver")
                                        .withUsername("testuser")
                                        .withPassword("testpass")
                                        .build()
                        )
                );

        env.execute("BackpackTF WebSocket Forwarder");
    }
}
