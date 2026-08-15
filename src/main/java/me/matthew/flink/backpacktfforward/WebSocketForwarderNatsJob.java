package me.matthew.flink.backpacktfforward;

import io.synadia.flink.source.JetStreamSource;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.parser.NatsMessageParser;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.source.NatsListingSource;
import me.matthew.flink.backpacktfforward.source.NatsSourceWithMetrics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.INCOMING_EVENTS;

/**
 * Phase 3 of the Kafka -> NATS migration: the same WebSocketForwarderJob
 * pipeline (parse -> filter -> upsert/delete sinks), but sourced from NATS
 * JetStream instead of Kafka. Deployed as a separate FlinkDeployment
 * (tf2-ingest-flink-job-nats-verify) running side-by-side with the
 * Kafka-sourced production job, both writing to the same Postgres — safe
 * because ListingUpsertSink/ListingDeleteSink are idempotent upserts/deletes.
 *
 * Operator names are suffixed "Nats" (not shared with WebSocketForwarderJob's
 * "...Kafka..." names) so this verification job's metrics don't get folded
 * into the existing Grafana panels/alerts that key off the Kafka job's
 * operator names during the side-by-side comparison window.
 *
 * Checkpointing is not enabled in this deployment (same as the Kafka job
 * today — see the migration notes), so the NATS source acks explicitly per
 * message via AckingUtf8StringSourceConverter rather than relying on
 * checkpoint-gated acking, which would never fire.
 */
@Slf4j
public class WebSocketForwarderNatsJob {

    public static void main(String[] args) throws Exception {
        log.info("Starting BackpackTF NATS Forwarder Job (Phase 3 verification)...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String dbUrl = System.getenv("DB_URL");
        String dbUser = System.getenv("DB_USERNAME");
        String dbPass = System.getenv("DB_PASSWORD");
        int upsertBatchSize = Integer.parseInt(System.getenv().getOrDefault("UPSERT_BATCH_SIZE", "10"));
        long upsertBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("UPSERT_BATCH_INTERVAL_MS", "200"));
        int deleteBatchSize = Integer.parseInt(System.getenv().getOrDefault("DELETE_BATCH_SIZE", "10"));
        long deleteBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("DELETE_BATCH_INTERVAL_MS", "1000"));

        if (dbUrl == null || dbUser == null || dbPass == null)
            throw new IllegalArgumentException("Database env vars missing");

        JetStreamSource<String> natsSource = NatsListingSource.createSource();

        DataStreamSource<String> source = env.fromSource(natsSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "BackpackTFNatsSource");

        var sourceWithMetrics = NatsSourceWithMetrics.addMetrics(source)
                .name("BackpackTFNatsSourceWithMetrics");

        var parsed = sourceWithMetrics
                .flatMap(new NatsMessageParser())
                .returns(ListingUpdate.class)
                .name("BackpackTFNatsMessageParser");

        parsed.map(new RichMapFunction<ListingUpdate, ListingUpdate>() {

            private Counter incomingEvents;

            @Override
            public ListingUpdate map(ListingUpdate listingUpdate) throws Exception {
                incomingEvents.inc();
                return listingUpdate;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                incomingEvents = getRuntimeContext().getMetricGroup().counter(INCOMING_EVENTS);
            }
        });

        parsed.filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-update"))
                .name("BackpackTFNatsListingUpdateFilter")
                .addSink(new ListingUpsertSink(dbUrl, dbUser, dbPass, upsertBatchSize, upsertBatchIntervalMs))
                .name("BackpackTFNatsListingUpsertSink");

        parsed.filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-delete"))
                .name("BackpackTFNatsListingDeleteFilter")
                .addSink(new ListingDeleteSink(dbUrl, dbUser, dbPass, deleteBatchSize, deleteBatchIntervalMs))
                .name("BackpackTFNatsListingDeleteSink");

        log.info("Starting Flink job execution...");
        env.execute("BackpackTF NATS Forwarder (verification)");
    }
}
