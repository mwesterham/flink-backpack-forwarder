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
 * a JetStreamSource reading the BACKFILL stream via a durable consumer this
 * connector owns and creates itself (NATS_CONSUMER_NAME=flink-backfill-nats-source
 * — ackWait 35m, exceeding BackfillJob's 30-minute AsyncDataStream.unorderedWait
 * timeout so a slow backpack.tf/Steam API call doesn't trigger a redelivery
 * mid-flight).
 *
 * Deliberately does NOT bind to flink-backfill-processor, the durable nack
 * already created in Phase 1 (nats-streams.yaml): the flink-connector-nats
 * library always issues a create-or-update call for the consumer it's given,
 * and JetStream rejects changing deliverPolicy on an existing consumer
 * ("deliver policy can not be updated" [10012]) — confirmed by hitting that
 * exact error reusing flink-backfill-processor. Using a fresh, connector-owned
 * name sidesteps it entirely, same fix NatsListingSource already applied for
 * the listings stream in Phase 3.
 *
 * Mirrors NatsListingSource's structure; kept as a separate class rather than
 * a shared one because the two jobs are separate FlinkDeployments with their
 * own env vars (NATS_STREAM/NATS_SUBJECT/NATS_CONSUMER_NAME/NATS_ACK_WAIT_MS
 * point at BACKFILL/flink-backfill-nats-source here, LISTINGS/
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
                // Fresh durable consumer, so nothing to bind to yet: approximate
                // the old Kafka source's OffsetsInitializer.latest() by starting
                // from now rather than replaying this stream's whole retention
                // window (72h — see nats-streams.yaml).
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
