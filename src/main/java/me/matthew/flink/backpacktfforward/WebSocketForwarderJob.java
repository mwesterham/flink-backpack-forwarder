package me.matthew.flink.backpacktfforward;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        log.info("Beginning WebSocketForwarderJob.");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String sourceUrl = "wss://ws.backpack.tf/events";
        String targetUrl = "ws://your-target-websocket-service:8080/forwarded";

        DataStreamSource<String> source = env.addSource(new WebSocketSource(sourceUrl));

        // Log events instead of print
        source.map(event -> {
            log.info("Received event: {}", event);
            return event;
        }).addSink(new WebSocketSink(targetUrl));

        env.execute("BackpackTF WebSocket Forwarder");
    }
}
