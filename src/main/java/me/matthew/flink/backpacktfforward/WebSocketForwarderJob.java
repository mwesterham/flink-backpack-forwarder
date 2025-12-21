package me.matthew.flink.backpacktfforward;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.parser.KafkaMessageParser;
import me.matthew.flink.backpacktfforward.source.KafkaMessageSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.KafkaException;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.INCOMING_WS_EVENTS;

@Slf4j
public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        log.info("Starting BackpackTF Kafka Forwarder Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read env vars safely
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

        if (dbUrl == null || dbUser == null || dbPass == null)
            throw new IllegalArgumentException("Database env vars missing");

        // Create Kafka source with error handling
        KafkaSource<String> kafkaSource;
        try {
            kafkaSource = KafkaMessageSource.createSource();
        } catch (KafkaException e) {
            log.error("Failed to create Kafka source. Please check Kafka configuration and connectivity.", e);
            throw new IllegalStateException("Kafka source creation failed: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            log.error("Invalid Kafka configuration. Please check environment variables.", e);
            throw e;
        }

        DataStreamSource<String> source = env.fromSource(kafkaSource, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
                "BackpackTFKafkaSource");

        var parsed = source
                .name("BackpackTFKafkaSource")
                .flatMap(new KafkaMessageParser())
                .returns(ListingUpdate.class)
                .name("BackpackTFKafkaMessageParser");

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
                                .counter(INCOMING_WS_EVENTS);
            }
        });

        // Route events
        parsed.filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-update"))
                .name("BackpackTFListingUpdateFilter")
                .addSink(new ListingUpsertSink(dbUrl, dbUser, dbPass, upsertBatchSize, upsertBatchIntervalMs))
                .name("BackpackTFListingUpsertSink");

        parsed.filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-delete"))
                .name("BackpackTFListingUpdateFilter")
                .addSink(new ListingDeleteSink(dbUrl, dbUser, dbPass, deleteBatchSize, deleteBatchIntervalMs))
                .name("BackpackTFListingDeleteSink");

        log.info("Starting Flink job execution...");
        env.execute("BackpackTF Kafka Forwarder");
    }
}
