package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class WebSocketSource extends RichSourceFunction<String> {

    private final String websocketUrl;
    private volatile boolean running = true;

    private transient WebSocket ws;
    private transient HttpClient client;

    // Metrics
    private transient Counter messagesReceived;
    private transient Counter connectionsOpened;
    private transient Counter connectionsClosed;
    private transient Counter connectFailures;
    private transient Counter reconnectAttempts;
    private transient Counter heartbeatFailures;

    private volatile long lastBackoffMs = 0;

    // Reconnect config
    private final long initialBackoffMs = 1000;   // 1s
    private final long maxBackoffMs = 30000;      // 30s
    private final Duration heartbeatInterval = Duration.ofSeconds(20);

    public WebSocketSource(String websocketUrl) {
        this.websocketUrl = websocketUrl;
    }

    @Override
    public void open(Configuration parameters) {
        var metrics = getRuntimeContext().getMetricGroup();

        messagesReceived = metrics.counter("websocket_source_messages_received");
        connectionsOpened = metrics.counter("websocket_source_connections_opened");
        connectionsClosed = metrics.counter("websocket_source_connections_closed");
        connectFailures = metrics.counter("websocket_source_connection_failures");
        reconnectAttempts = metrics.counter("websocket_source_reconnect_attempts");
        heartbeatFailures = metrics.counter("websocket_source_heartbeat_failures");

        metrics.gauge("last_reconnect_backoff_ms", (Gauge<Long>) () -> lastBackoffMs);
        // queue size gauge will be registered inside run() once queue is created
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        client = HttpClient.newHttpClient();
        LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

        // Register gauge after queue creation
        getRuntimeContext()
                .getMetricGroup()
                .gauge("queue_size", (Gauge<Integer>) messageQueue::size);

        long backoff = initialBackoffMs;

        while (running) {
            try {
                connect(messageQueue);
                backoff = initialBackoffMs; // reset after success

                // Emit messages until connection fails
                while (running && ws != null) {
                    String msg = messageQueue.take();
                    messagesReceived.inc();
                    ctx.collect(msg);
                }

            } catch (Exception ex) {
                connectFailures.inc();
                log.error("WebSocket connection failed: {}", ex.getMessage());
            }

            if (!running) break;

            // Reconnect
            reconnectAttempts.inc();
            lastBackoffMs = backoff;

            log.warn("Reconnecting in {} ms...", backoff);
            Thread.sleep(backoff);

            backoff = Math.min(backoff * 2, maxBackoffMs); // exponential backoff
        }

        closeWs();
    }

    /** Create the WebSocket and listener */
    private void connect(LinkedBlockingQueue<String> queue) {
        log.info("Connecting to WebSocket {}", websocketUrl);

        try {
            ws = client.newWebSocketBuilder()
                    .buildAsync(URI.create(websocketUrl), new Listener(queue))
                    .join();
        } catch (Exception e) {
            connectFailures.inc();
            throw e;
        }

        // Heartbeat thread
        Thread heartbeat = new Thread(() -> {
            while (running && ws != null) {
                try {
                    Thread.sleep(heartbeatInterval.toMillis());
                    ws.sendPing(java.nio.ByteBuffer.wrap(new byte[]{}));
                } catch (Exception ex) {
                    heartbeatFailures.inc();
                }
            }
        }, "ws-heartbeat");

        heartbeat.setDaemon(true);
        heartbeat.start();
    }

    private void closeWs() {
        try {
            if (ws != null) {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "Job stopped").join();
                connectionsClosed.inc();
                log.info("WebSocket closed normally.");
            }
        } catch (Exception ignored) {}
    }

    @Override
    public void cancel() {
        running = false;
        closeWs();
    }

    // -----------------------------
    // WebSocket Listener
    // -----------------------------
    private class Listener implements WebSocket.Listener {

        private final LinkedBlockingQueue<String> queue;
        private final StringBuilder buffer = new StringBuilder();

        Listener(LinkedBlockingQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            connectionsOpened.inc();
            log.info("WebSocket connected: {}", websocketUrl);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);

            if (last) {
                queue.offer(buffer.toString());
                buffer.setLength(0);
            }

            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            connectionsClosed.inc();
            log.warn("WebSocket closed: code={} reason={}", statusCode, reason);
            ws = null; // trigger reconnect
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            connectFailures.inc();
            log.error("WebSocket error: {}", error.getMessage(), error);
            ws = null; // trigger reconnect
        }
    }
}
