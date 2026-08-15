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
 * Factory for the Phase 3 (Kafka -> NATS migration) verification source:
 * a JetStreamSource reading the listings stream via a durable consumer
 * that this connector owns and creates itself.
 *
 * Deliberately does NOT bind to the durable consumers nack already created
 * in Phase 1 (flink-listings-forwarder, etc.) — this connector's consumer
 * creation path only sends ackPolicy + filterSubject (+ ackWait/
 * inactiveThreshold if set) when a durable name is given, never deliverPolicy,
 * so reusing a name whose consumer NATS already knows a DeliverPolicy for is a
 * config-mismatch risk that isn't worth taking without live testing. Using a
 * fresh, connector-owned durable name sidesteps that risk entirely; the
 * unused nack-managed consumer gets cleaned up in the eventual cutover.
 */
@Slf4j
public class NatsListingSource {

    private NatsListingSource() {
    }

    public static JetStreamSource<String> createSource() {
        NatsSourceConfiguration.validateConfiguration();

        String url = NatsSourceConfiguration.getNatsUrl();
        String stream = NatsSourceConfiguration.getStream();
        String subject = NatsSourceConfiguration.getSubject();
        String consumerName = NatsSourceConfiguration.getConsumerName();
        long ackWaitMillis = NatsSourceConfiguration.getAckWaitMillis();

        log.info("Configuring NATS JetStream source with url: {}, stream: {}, subject: {}, consumer: {}",
                url, stream, subject, consumerName);

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("io.nats.client.url", url);
        // jnats defaults to 60 reconnect attempts at ~2s apart (~2 minutes)
        // before permanently closing the connection. During the 2026-08-15
        // NATS StatefulSet restart/leader-election incident, this source hit
        // that ceiling, gave up for good, and sat silently dead for over an
        // hour while Flink kept reporting the job RUNNING/STABLE — the same
        // root cause already fixed in tf2-data-bridge's NatsPublisher. Retry
        // indefinitely so a future NATS-side blip is self-healing.
        connectionProperties.setProperty("io.nats.client.reconnect.max", "-1");

        JetStreamSubjectConfiguration subjectConfiguration = JetStreamSubjectConfiguration.builder()
                .streamName(stream)
                .subject(subject)
                .durableName(consumerName)
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(Duration.ofMillis(ackWaitMillis))
                // Fresh durable consumer, so nothing to bind to yet: approximate
                // Kafka's cold-start "latest" default by starting from now
                // rather than replaying this stream's whole retention window.
                .startTime(ZonedDateTime.now())
                .build();

        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
                .connectionProperties(connectionProperties)
                .sourceConverter(new AckingUtf8StringSourceConverter())
                .addSubjectConfigurations(subjectConfiguration)
                .build();

        log.info("NATS JetStream source created successfully");
        return source;
    }
}
