package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.NATS_MESSAGES_CONSUMED;

/**
 * Wrapper for the NATS source that adds message consumption metrics.
 * Mirrors KafkaSourceWithMetrics for the Phase 3 NATS-sourced job.
 */
@Slf4j
public class NatsSourceWithMetrics {

    public static SingleOutputStreamOperator<String> addMetrics(DataStream<String> sourceStream) {
        return sourceStream.map(new NatsMetricsCollector())
                .name("NatsMetricsCollector");
    }

    public static class NatsMetricsCollector extends RichMapFunction<String, String> {

        private transient Counter messagesConsumed;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            this.messagesConsumed = getRuntimeContext()
                    .getMetricGroup()
                    .counter(NATS_MESSAGES_CONSUMED);

            log.info("NATS message counter initialized");
        }

        @Override
        public String map(String message) throws Exception {
            messagesConsumed.inc();
            return message;
        }
    }
}
