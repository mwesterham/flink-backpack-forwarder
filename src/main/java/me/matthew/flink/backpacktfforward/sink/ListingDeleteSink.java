package me.matthew.flink.backpacktfforward.sink;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.DELETED_LISTINGS;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.DELETED_LISTINGS_RETRIES;

@Slf4j
public class ListingDeleteSink extends RichSinkFunction<ListingUpdate> {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement markDeletedStmt;
    private transient Counter deleteCounter;

    private transient RetryPolicy<Object> retryPolicy;

    private static final String MARK_DELETED_SQL = """
        UPDATE listings
        SET is_deleted = true, updated_at = now()
        WHERE id = ?;
        """;

    // Flush controls
    private final int batchSize;
    private final long batchIntervalMs;
    private int currentBatchCount = 0;
    private long lastFlushTime = 0;

    public ListingDeleteSink(
            String jdbcUrl,
            String username,
            String password,
            int batchSize,
            long batchIntervalMs
    ) {
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
        connection.setAutoCommit(false); // use batching
        markDeletedStmt = connection.prepareStatement(MARK_DELETED_SQL);

        deleteCounter = getRuntimeContext()
                .getMetricGroup()
                .counter(DELETED_LISTINGS);

        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(),
                DELETED_LISTINGS_RETRIES
        );
        retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);

        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(ListingUpdate value, Context context) throws Exception {
        var payload = value.getPayload();

        markDeletedStmt.setString(1, payload.getId());
        markDeletedStmt.addBatch();

        currentBatchCount++;
        deleteCounter.inc();

        long now = System.currentTimeMillis();
        if (currentBatchCount >= batchSize || (now - lastFlushTime) >= batchIntervalMs) {
            flushBatch();
            lastFlushTime = now;
        }
    }

    private void flushBatch() throws SQLException {
        if (currentBatchCount == 0) {
            return;
        }

        Failsafe.with(retryPolicy).run(() -> {
            try {
                markDeletedStmt.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        });

        currentBatchCount = 0;
    }

    @Override
    public void close() throws Exception {
        flushBatch();

        if (markDeletedStmt != null) {
            markDeletedStmt.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
