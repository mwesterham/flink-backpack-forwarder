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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read env vars safely
        String sourceUrl = System.getenv("SOURCE_URL");
        String dbUrl = System.getenv("DB_URL");
        String dbUser = System.getenv("DB_USERNAME");
        String dbPass = System.getenv("DB_PASSWORD");

        if (sourceUrl == null) throw new IllegalArgumentException("SOURCE_URL is not set");
        if (dbUrl == null || dbUser == null || dbPass == null)
            throw new IllegalArgumentException("Database env vars missing");

        // Always single-threaded WebSocket
        DataStreamSource<String> source =
                env.addSource(new WebSocketSource(sourceUrl))
                        .setParallelism(1);

        var parsed = source
                .flatMap((String event, Collector<ListingUpdate> out) -> {
                    try {
                        List<ListingUpdate> updates =
                                mapper.readValue(event, new TypeReference<List<ListingUpdate>>() {
                                });
                        updates.forEach(out::collect);
                    } catch (Exception e) {
                        log.error("Failed to parse WebSocket payload: {}", event, e);
                    }
                })
                .returns(ListingUpdate.class);

        // Route events
        parsed.filter(lu -> "listing-update".equals(lu.getEvent()))
                .addSink(new ListingUpsertSink(dbUrl, dbUser, dbPass));

        parsed.filter(lu -> "listing-delete".equals(lu.getEvent()))
                .addSink(new ListingDeleteSink(dbUrl, dbUser, dbPass));

        env.execute("BackpackTF WebSocket Forwarder");
    }
}
