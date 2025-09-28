package me.matthew.flink.backpacktfforward;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;

public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: Replace with your actual WebSocket source implementation
        // For now, we'll simulate a stream.
        DataStreamSource<String> source = env.fromElements(
                "Event 1", "Event 2", "Event 3"
        );

        // Example: print to stdout (later we’ll forward to another WebSocket)
        source.print();

        env.execute("BackpackTF → WebSocket Forwarder");
    }
}
