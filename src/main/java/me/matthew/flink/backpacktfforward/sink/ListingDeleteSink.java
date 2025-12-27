package me.matthew.flink.backpacktfforward.sink;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.util.ConflictResolutionUtil;
import me.matthew.flink.backpacktfforward.util.ListingUpdateConflictResolutionRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.CONFLICT_WRITES_ALLOWED;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.CONFLICT_WRITES_SKIPPED;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.DELETED_LISTINGS;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.DELETED_LISTINGS_RETRIES;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.REALTIME_WRITES_PROCESSED;

@Slf4j
public class ListingDeleteSink extends RichSinkFunction<ListingUpdate> {

    private static final String MARK_DELETED_SQL = """
            UPDATE listings
            SET is_deleted = true
            WHERE id = ?;
            """;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int batchSize;
    private final long batchIntervalMs;

    private transient Connection connection;
    private transient PreparedStatement markDeletedStmt;
    private transient Counter deleteCounter;
    private transient RetryPolicy<Object> retryPolicy;
    private transient ConflictResolutionUtil conflictResolutionUtil;
    private transient Counter conflictSkippedCounter;
    private transient Counter conflictAllowedCounter;
    private transient Counter realTimeWritesCounter;

    private final List<ListingUpdate> batch = new ArrayList<>();
    private long lastFlushTime;

    public ListingDeleteSink(String jdbcUrl, String username, String password, int batchSize, long batchIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);

        markDeletedStmt = connection.prepareStatement(MARK_DELETED_SQL);

        deleteCounter = getRuntimeContext()
                .getMetricGroup()
                .counter(DELETED_LISTINGS);

        conflictSkippedCounter = getRuntimeContext().getMetricGroup().counter(CONFLICT_WRITES_SKIPPED);
        conflictAllowedCounter = getRuntimeContext().getMetricGroup().counter(CONFLICT_WRITES_ALLOWED);
        realTimeWritesCounter = getRuntimeContext().getMetricGroup().counter(REALTIME_WRITES_PROCESSED);
        
        // Initialize conflict resolution utility with metrics counters
        conflictResolutionUtil = new ConflictResolutionUtil(
                conflictSkippedCounter,
                conflictAllowedCounter
        );

        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(),
                DELETED_LISTINGS_RETRIES
        );
        retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);

        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(ListingUpdate lu, Context context) throws Exception {
        if (!ConflictResolutionUtil.isValidListingUpdate(lu)) {
            log.warn("Received invalid ListingUpdate - skipping processing");
            return;
        }
        
        ListingUpdateConflictResolutionRequest request = new ListingUpdateConflictResolutionRequest(lu);
        boolean shouldSkip = conflictResolutionUtil.shouldSkipWrite(request, connection);
        
        if (!shouldSkip) {
            // Track real-time vs backfill writes separately for monitoring
            if (lu.getGenerationTimestamp() == null) {
                realTimeWritesCounter.inc();
            }
            batch.add(lu);
        }

        long now = System.currentTimeMillis();
        if (batch.size() >= batchSize || now - lastFlushTime >= batchIntervalMs) {
            flushBatch();
            lastFlushTime = now;
        }
    }

    private void flushBatch() throws SQLException {
        if (batch.isEmpty()) return;

        // Sort by ID to prevent deadlocks
        batch.sort(Comparator.comparing(l -> l.getPayload().getId()));

        Failsafe.with(retryPolicy).run(() -> {
            try {
                markDeletedStmt.clearBatch();

                for (ListingUpdate lu : batch) {
                    markDeletedStmt.setString(1, lu.getPayload().getId());
                    markDeletedStmt.addBatch();
                    deleteCounter.inc();
                }

                markDeletedStmt.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        });

        batch.clear();
    }

    @Override
    public void close() throws Exception {
        flushBatch();

        if (markDeletedStmt != null) markDeletedStmt.close();
        if (connection != null) connection.close();

        super.close();
    }
}
