package me.matthew.flink.backpacktfforward;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.source.WebSocketSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

@Slf4j
public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        log.info("Starting BackpackTF WebSocketForwarderJob...");

        ObjectMapper mapper = new ObjectMapper();
        String sourceUrl = "ws://laputa.local:30331/forwarded";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new WebSocketSource(sourceUrl));

        var parsed = source.flatMap((String event, Collector<ListingUpdate> out) -> {
            try {
                List<ListingUpdate> updates = mapper.readValue(event, new TypeReference<>() {});
                for (ListingUpdate update : updates) out.collect(update);
            } catch (Exception e) {
                log.error("Failed to parse WebSocket payload", e);
            }
        }).returns(ListingUpdate.class);

        // Branch by event type
        final String dbUrl = "jdbc:postgresql://localhost:5432/testdb";
        final String dbUName = "testuser";
        final String dbPass = "testpass";
        parsed.filter(lu -> "listing-update".equals(lu.getEvent()))
                .addSink(new ListingUpsertSink(dbUrl, dbUName, dbPass));

        parsed.filter(lu -> "listing-delete".equals(lu.getEvent()))
                .addSink(new ListingDeleteSink(dbUrl, dbUName, dbPass));

        env.execute("BackpackTF WebSocket Forwarder");
    }
}
