package me.matthew.flink.backpacktfforward.source;

import io.nats.client.Message;
import io.synadia.flink.message.AbstractStringSourceConverter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * A UTF-8 string source converter that acks each message immediately as it's
 * read, rather than tying acks to a Flink checkpoint (this job does not have
 * checkpointing enabled — see the migration notes on WebSocketForwarderNatsJob).
 * This mirrors today's real Kafka behavior: auto-commit on a timer, decoupled
 * from whether the message was actually fully processed downstream.
 * Must be paired with AckBehavior.ExplicitButDoNotAck on the subject
 * configuration — that's what leaves the ack up to this converter instead of
 * having the source (uselessly, since checkpoints never fire) try to do it.
 */
@Slf4j
public class AckingUtf8StringSourceConverter extends AbstractStringSourceConverter {

    public AckingUtf8StringSourceConverter() {
        super(StandardCharsets.UTF_8);
    }

    @Override
    public String convert(Message message) {
        String payload = super.convert(message);
        try {
            message.ack();
        } catch (Exception e) {
            // Not fatal: if the ack doesn't land, the message is simply
            // redelivered after ackWait — the same at-least-once shape as
            // today's Kafka auto-commit missing a beat.
            log.warn("Failed to ack NATS message on subject {}: {}", message.getSubject(), e.getMessage());
        }
        return payload;
    }
}
