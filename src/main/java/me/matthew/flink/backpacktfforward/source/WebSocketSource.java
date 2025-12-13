package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.*;

@Slf4j
public class WebSocketSource extends RichSourceFunction<String> {

    private final String websocketUrl;
    private volatile boolean running = true;

    // Configurable parameters (tweak when constructing)
    private final int queueCapacity;
    private final Duration heartbeatInterval;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final Duration connectTimeout;

    // Transient runtime objects
    private transient HttpClient client;
    private transient AtomicReference<WebSocket> wsRef;
    private transient ScheduledExecutorService heartbeatScheduler;
    private transient ExecutorService connectExecutor;

    // Queue used to buffer messages between WebSocket threads and the Flink source thread
    private transient BlockingQueue<String> messageQueue;

    // Metrics
    private transient Counter messagesReceived;
    private transient Counter messagesDropped;
    private transient Counter connectionsOpened;
    private transient Counter connectionsClosed;
    private transient Counter connectFailures;
    private transient Counter reconnectAttempts;
    private transient Counter heartbeatFailures;
    private transient AtomicLong lastBackoffMs;

    // internal control to signal connection closed (used for reconnect detection)
    private transient CompletableFuture<Void> closeSignal;

    public WebSocketSource(String websocketUrl) {
        // defaults
        this(websocketUrl, 5_000, Duration.ofSeconds(20), 1_000L, 30_000L, Duration.ofSeconds(10));
    }

    public WebSocketSource(String websocketUrl,
                           int queueCapacity,
                           Duration heartbeatInterval,
                           long initialBackoffMs,
                           long maxBackoffMs,
                           Duration connectTimeout) {
        this.websocketUrl = Objects.requireNonNull(websocketUrl);
        this.queueCapacity = queueCapacity;
        this.heartbeatInterval = heartbeatInterval;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.connectTimeout = connectTimeout;
    }

    @Override
    public void open(Configuration parameters) {
        var metrics = getRuntimeContext().getMetricGroup();

        messagesReceived = metrics.counter(WS_MESSAGES_RECEIVED);
        messagesDropped = metrics.counter(WS_MESSAGES_DROPPED);
        connectionsOpened = metrics.counter(WS_CONNECTIONS_OPENED);
        connectionsClosed = metrics.counter(WS_CONNECTIONS_CLOSED);
        connectFailures = metrics.counter(WS_CONNECTION_FAILURES);
        reconnectAttempts = metrics.counter(WS_RECONNECT_ATTEMPTS);
        heartbeatFailures = metrics.counter(WS_HEARTBEAT_FAILURES);

        lastBackoffMs = new AtomicLong(0L);
        metrics.gauge(LAST_RECONNECT_BACKOFF_MS, (Gauge<Long>) lastBackoffMs::get);
        // queue gauge will be registered in run() after queue is created

        wsRef = new AtomicReference<>();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        client = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .build();

        // Bounded queue prevents unbounded memory growth and makes the source drop on overload
        messageQueue = new ArrayBlockingQueue<>(queueCapacity);

        // Register gauge for queue size
        getRuntimeContext().getMetricGroup().gauge("queue_size", (Gauge<Integer>) messageQueue::size);

        // single-thread ScheduledExecutor for heartbeat (clean lifecycle)
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-heartbeat-scheduler");
            t.setDaemon(true);
            return t;
        });

        // single thread for connecting/joining web socket futures (prevents blocking Flink thread on .join)
        connectExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ws-connect-thread");
            t.setDaemon(true);
            return t;
        });

        long backoff = initialBackoffMs;

        try {
            while (running) {
                try {
                    // Attempt to connect (blocks until connected or throws)
                    connect();
                    backoff = initialBackoffMs; // reset on success

                    // Emit until connection is lost or cancel() called
                    while (running && wsRef.get() != null) {
                        // Poll with timeout so we can check running/wsRef periodically
                        String msg = messageQueue.poll(1, TimeUnit.SECONDS);
                        if (msg != null) {
                            // Emit under checkpoint lock per Flink requirements
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collect(msg);
                            }
                            messagesReceived.inc();
                        }
                    }
                } catch (Exception connectEx) {
                    connectFailures.inc();
                    log.error("WebSocket connection failed: {}", connectEx.getMessage(), connectEx);
                }

                if (!running) break;

                // reconnect backoff
                reconnectAttempts.inc();
                lastBackoffMs.set(backoff);
                log.warn("WebSocket disconnected — reconnecting in {} ms...", backoff);
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, maxBackoffMs);
            }
        } finally {
            // clean shutdown
            shutdownHeartbeat();
            closeWebSocket();
            shutdownExecutors();
        }
    }

    /**
     * Create/establish the WebSocket and set up listener.
     * Blocks until a WebSocket is established or throws exception.
     */
    private void connect() throws Exception {
        log.info("Connecting to WebSocket {}", websocketUrl);

        // reset signal for this connection
        closeSignal = new CompletableFuture<>();

        try {
            // build async and wait on a separate thread to avoid blocking the Flink main thread on join()
            CompletableFuture<WebSocket> wsFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return client.newWebSocketBuilder()
                            .buildAsync(URI.create(websocketUrl), new WsListener(messageQueue));
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, connectExecutor).thenCompose(f -> f);

            WebSocket ws = wsFuture.get(30, TimeUnit.SECONDS); // wait for open
            wsRef.set(ws);

            // start heartbeat task (will be canceled in shutdownHeartbeat)
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                WebSocket w = wsRef.get();
                if (w == null) return;
                try {
                    // send ping (empty payload)
                    w.sendPing(ByteBuffer.allocate(0));
                } catch (Throwable t) {
                    heartbeatFailures.inc();
                    log.warn("Heartbeat ping failed: {}", t.getMessage());
                }
            }, heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);

            log.info("WebSocket connected: {}", websocketUrl);
            connectionsOpened.inc();
        } catch (Exception e) {
            connectFailures.inc();
            log.error("Failed to establish WebSocket connection: {}", e.getMessage());
            throw e;
        }
    }

    private void closeWebSocket() {
        try {
            WebSocket w = wsRef.getAndSet(null);
            if (w != null) {
                try {
                    w.sendClose(WebSocket.NORMAL_CLOSURE, "Shutting down").join();
                } catch (Exception ignored) {
                }
                connectionsClosed.inc();
                log.info("WebSocket closed normally.");
            }
            if (closeSignal != null && !closeSignal.isDone()) {
                closeSignal.complete(null);
            }
        } catch (Exception ex) {
            log.warn("Exception while closing WebSocket: {}", ex.getMessage(), ex);
        }
    }

    private void shutdownHeartbeat() {
        if (heartbeatScheduler != null) {
            try {
                heartbeatScheduler.shutdownNow();
                heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                heartbeatScheduler = null;
            }
        }
    }

    private void shutdownExecutors() {
        if (connectExecutor != null) {
            try {
                connectExecutor.shutdownNow();
            } catch (Exception ignored) {
            }
            connectExecutor = null;
        }
    }

    @Override
    public void cancel() {
        running = false;
        // make sure to close the websocket and stop heartbeat
        closeWebSocket();
        shutdownHeartbeat();
        shutdownExecutors();
        // clear queue to free memory
        if (messageQueue != null) {
            messageQueue.clear();
        }
    }

    // -----------------------------
    // WebSocket Listener
    // -----------------------------
    private class WsListener implements WebSocket.Listener {
        private final BlockingQueue<String> queue;
        // Use per-message buffer; create new buffer after each completed message to let
        // the old backing char[] be GC eligible.
        private StringBuilder buffer = new StringBuilder();

        WsListener(BlockingQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            log.debug("Listener.onOpen");
            webSocket.request(1); // request first item
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                // try to offer without blocking — drop if full
                boolean accepted = queue.offer(message);
                if (!accepted) {
                    messagesDropped.inc();
                    log.warn("Dropping WebSocket message because queue is full (capacity={})", queueCapacity);
                }
                // create a fresh buffer to free previous backing array
                buffer = new StringBuilder();
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.warn("WebSocket closed: code={} reason={}", statusCode, reason);
            connectionsClosed.inc();
            // mark wsRef null to trigger reconnect
            wsRef.set(null);
            if (closeSignal != null) closeSignal.complete(null);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            connectFailures.inc();
            log.error("WebSocket error: {}", error.getMessage(), error);
            // clear reference so run() will reconnect
            wsRef.set(null);
            if (closeSignal != null) closeSignal.completeExceptionally(error);
        }
    }
}
