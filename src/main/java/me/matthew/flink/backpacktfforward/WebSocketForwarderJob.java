package me.matthew.flink.backpacktfforward;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.source.WebSocketSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

@Slf4j
public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        log.info("Starting BackpackTF WebSocketForwarderJob...");

        ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read env vars safely
        String sourceUrl = System.getenv("SOURCE_URL");
        String dbUrl = System.getenv("DB_URL");
        String dbUser = System.getenv("DB_USERNAME");
        String dbPass = System.getenv("DB_PASSWORD");
        int upsertBatchSize = Integer.parseInt(System.getenv().getOrDefault("UPSERT_BATCH_SIZE", "10"));
        long upsertBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("UPSERT_BATCH_INTERVAL_MS", "200"));
        int deleteBatchSize = Integer.parseInt(System.getenv().getOrDefault("DELETE_BATCH_SIZE", "10"));
        long deleteBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("DELETE_BATCH_INTERVAL_MS", "1000"));

        log.info("Upsert batch size: {}", upsertBatchSize);
        log.info("Upsert batch interval (ms): {}", upsertBatchIntervalMs);
        log.info("Delete batch size: {}", deleteBatchSize);
        log.info("Delete batch interval (ms): {}", deleteBatchIntervalMs);

        if (sourceUrl == null) throw new IllegalArgumentException("SOURCE_URL is not set");
        if (dbUrl == null || dbUser == null || dbPass == null)
            throw new IllegalArgumentException("Database env vars missing");

        // Always single-threaded WebSocket
        DataStreamSource<String> source =
                env.addSource(new WebSocketSource(sourceUrl))
                        .setParallelism(1);

        var parsed = source
                .name("BackpackTFWebSocketSource")
                .flatMap((String event, Collector<ListingUpdate> out) -> {
                    try {
                        List<ListingUpdate> updates =
                                mapper.readValue(event, new TypeReference<List<ListingUpdate>>() {});

                        updates.forEach(out::collect);

                    } catch (MismatchedInputException e) {
                        log.error("Failed to parse JSON. Path = {}", e.getPathReference());
                        log.error("Raw message = {}", event);
                    } catch (Exception e) {
                        log.error("Failed to parse WebSocket payload: {}", event, e);
                    }
                })
                .returns(ListingUpdate.class)
                .name("BackpackTFWebSocketPayloadParser");

        parsed.map(new RichMapFunction<ListingUpdate, ListingUpdate>() {

            private Counter incomingWsEvents;

            @Override
            public ListingUpdate map(ListingUpdate listingUpdate) throws Exception {
                incomingWsEvents.inc();
                return listingUpdate;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                incomingWsEvents =
                        getRuntimeContext()
                                .getMetricGroup()
                                .counter("incoming_ws_events");
            }
        });

        // Route events
        parsed.filter(lu -> "listing-update".equals(lu.getEvent()))
                .name("BackpackTFListingUpdateFilter")
                .addSink(new ListingUpsertSink(dbUrl, dbUser, dbPass, upsertBatchSize, upsertBatchIntervalMs))
                .name("BackpackTFListingUpsertSink");

        parsed.filter(lu -> "listing-delete".equals(lu.getEvent()))
                .name("BackpackTFListingUpdateFilter")
                .addSink(new ListingDeleteSink(dbUrl, dbUser, dbPass, deleteBatchSize, deleteBatchIntervalMs))
                .name("BackpackTFListingDeleteSink");

        env.execute("BackpackTF WebSocket Forwarder");
    }
}
