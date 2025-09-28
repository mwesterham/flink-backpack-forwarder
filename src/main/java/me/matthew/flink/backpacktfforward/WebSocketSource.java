package me.matthew.flink.backpacktfforward;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class WebSocketSource implements SourceFunction<String> {

    private final String websocketUrl;
    private volatile boolean running = true;
    private transient WebSocket ws;

    public WebSocketSource(String websocketUrl) {
        this.websocketUrl = websocketUrl;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                messageQueue.offer(data.toString());
                webSocket.request(1); // Request next message
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                log.error("WebSocket error: {}", error.getMessage(), error);
            }

            @Override
            public void onOpen(WebSocket webSocket) {
                log.info("Connected to WebSocket: {}", websocketUrl);
                webSocket.request(1); // Start requesting messages
            }
        };

        ws = client.newWebSocketBuilder()
                .buildAsync(URI.create(websocketUrl), listener)
                .join();

        // Main loop: collect messages from the queue and emit to Flink
        while (running) {
            String msg = messageQueue.take(); // Blocks until a message is available
            ctx.collect(msg);
        }

        if (ws != null) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "Job stopped").join();
            log.info("WebSocket closed normally.");
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (ws != null) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "Job canceled").join();
        }
    }
}
