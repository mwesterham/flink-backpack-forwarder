package me.matthew.flink.backpacktfforward;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;

@Slf4j
public class WebSocketSink implements SinkFunction<String> {

    private final String targetUrl;
    private transient WebSocket ws;

    public WebSocketSink(String targetUrl) {
        this.targetUrl = targetUrl;
    }

    @Override
    public void invoke(String value, Context context) {
        try {
            if (ws == null) {
                HttpClient client = HttpClient.newHttpClient();
                ws = client.newWebSocketBuilder()
                        .buildAsync(URI.create(targetUrl), new WebSocket.Listener() {

                            @Override
                            public void onOpen(WebSocket webSocket) {
                                log.info("Connected to target WebSocket: {}", targetUrl);
                                webSocket.request(1); // Request first message
                            }

                            @Override
                            public void onError(WebSocket webSocket, Throwable error) {
                                log.error("Sink WebSocket error: {}", error.getMessage(), error);
                            }
                        }).join();
            }

            // Send text asynchronously and log failures
            ws.sendText(value, true)
                    .exceptionally(ex -> {
                        log.error("Failed to send message to target WebSocket: {}", ex.getMessage(), ex);
                        return null;
                    });
        } catch (Exception e) {
            log.error("Error in WebSocketSink invoke: {}", e.getMessage(), e);
        }
    }
}
