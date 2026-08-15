package me.matthew.flink.backpacktfforward.source;

import io.synadia.flink.source.AckBehavior;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.NatsSourceConfiguration;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Properties;

/**
 * Factory for the Phase 5 (Kafka -> NATS migration) backfill request source:
 * a JetStreamSource reading the BACKFILL stream via the flink-backfill-processor
 * durable consumer (nats-streams.yaml — ackWait 35m, exceeding BackfillJob's
 * 30-minute AsyncDataStream.unorderedWait timeout so a slow backpack.tf/Steam
 * API call doesn't trigger a redelivery mid-flight).
 *
 * Mirrors NatsListingSource's structure; kept as a separate class rather than
 * a shared one because the two jobs are separate FlinkDeployments with their
 * own env vars (NATS_STREAM/NATS_SUBJECT/NATS_CONSUMER_NAME/NATS_ACK_WAIT_MS
 * point at BACKFILL/flink-backfill-processor here, LISTINGS/
 * flink-listings-nats-source there) — same convention BackfillRequestSource
 * used for the old Kafka source.
 */
@Slf4j
public class BackfillRequestNatsSource {

    private BackfillRequestNatsSource() {
    }

    public static JetStreamSource<String> createSource() {
        NatsSourceConfiguration.validateConfiguration();

        String url = NatsSourceConfiguration.getNatsUrl();
        String stream = NatsSourceConfiguration.getStream();
        String subject = NatsSourceConfiguration.getSubject();
        String consumerName = NatsSourceConfiguration.getConsumerName();
        long ackWaitMillis = NatsSourceConfiguration.getAckWaitMillis();

        log.info("Configuring NATS JetStream backfill source with url: {}, stream: {}, subject: {}, consumer: {}",
                url, stream, subject, consumerName);

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("io.nats.client.url", url);
        // See NatsListingSource for why this matters: without it, a NATS-side
        // blip permanently kills the source instead of reconnecting.
        connectionProperties.setProperty("io.nats.client.reconnect.max", "-1");

        JetStreamSubjectConfiguration subjectConfiguration = JetStreamSubjectConfiguration.builder()
                .streamName(stream)
                .subject(subject)
                .durableName(consumerName)
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(Duration.ofMillis(ackWaitMillis))
                // Only takes effect if this durable doesn't already exist —
                // JetStream ignores startTime/deliverPolicy when binding to
                // an existing durable by name (see flink-backfill-processor
                // in nats-streams.yaml, created back in Phase 1: this source
                // will inherit whatever backlog it accumulated while unbound
                // and needs a one-time consumer reset on first deploy, same
                // as pricer-listings did in Phase 4).
                .startTime(ZonedDateTime.now())
                .build();

        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
                .connectionProperties(connectionProperties)
                .sourceConverter(new AckingUtf8StringSourceConverter())
                .addSubjectConfigurations(subjectConfiguration)
                .build();

        log.info("NATS JetStream backfill source created successfully");
        return source;
    }
}
