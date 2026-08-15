package me.matthew.flink.backpacktfforward.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.KafkaMessageWrapper;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import java.util.List;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.NATS_MESSAGES_CONSUMED;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.NATS_MESSAGES_PARSED_FAILED;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.NATS_MESSAGES_PARSED_SUCCESS;

/**
 * Parses NATS messages containing WebSocket data and extracts ListingUpdate objects.
 * Same envelope format and parsing logic as KafkaMessageParser (the bridge
 * publishes the identical {data, timestamp, messageId, source} envelope to
 * both Kafka and NATS during the Phase 2 dual-publish) — only the metric
 * names differ, to keep NATS-sourced traffic distinguishable from Kafka's
 * during the Phase 3 side-by-side verification.
 */
@Slf4j
public class NatsMessageParser extends RichFlatMapFunction<String, ListingUpdate> {

    private transient ObjectMapper objectMapper;
    private transient Counter successfulParses;
    private transient Counter failedParses;
    private transient Counter messagesConsumed;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.objectMapper = new ObjectMapper();

        this.successfulParses = getRuntimeContext()
                .getMetricGroup()
                .counter(NATS_MESSAGES_PARSED_SUCCESS);

        this.failedParses = getRuntimeContext()
                .getMetricGroup()
                .counter(NATS_MESSAGES_PARSED_FAILED);

        this.messagesConsumed = getRuntimeContext()
                .getMetricGroup()
                .counter(NATS_MESSAGES_CONSUMED);
    }

    @Override
    public void flatMap(String natsMessageValue, Collector<ListingUpdate> out) throws Exception {
        messagesConsumed.inc();

        try {
            KafkaMessageWrapper wrapper = objectMapper.readValue(natsMessageValue, KafkaMessageWrapper.class);

            if (wrapper == null) {
                log.error("Parsed NATS message wrapper is null. Raw message = {}", natsMessageValue);
                failedParses.inc();
                return;
            }

            if (wrapper.getData() == null) {
                log.error("NATS message wrapper data field is null. Raw message = {}", natsMessageValue);
                failedParses.inc();
                return;
            }

            String dataJson = objectMapper.writeValueAsString(wrapper.getData());
            List<ListingUpdate> updates = objectMapper.readValue(dataJson, new TypeReference<List<ListingUpdate>>() {});

            if (updates == null) {
                log.error("Parsed WebSocket data returned null list. Wrapper data = {}, Raw message = {}",
                        wrapper.getData(), natsMessageValue);
                failedParses.inc();
                return;
            }

            for (ListingUpdate update : updates) {
                if (update == null) {
                    log.error("Parsed WebSocket data contains null element. Wrapper data = {}, Raw message = {}",
                            wrapper.getData(), natsMessageValue);
                } else {
                    out.collect(update);
                }
            }

            successfulParses.inc();

        } catch (Exception e) {
            log.error("Failed to parse NATS message. Error = {}, Raw message = {}", e.getMessage(), natsMessageValue, e);
            failedParses.inc();
        }
    }
}
