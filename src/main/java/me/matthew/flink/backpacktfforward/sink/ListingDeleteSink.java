package me.matthew.flink.backpacktfforward.sink;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@Slf4j
public class ListingDeleteSink extends RichSinkFunction<ListingUpdate> {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement markDeletedStmt;
    private transient Counter deleteCounter;

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
        connection.setAutoCommit(false); // use batching
        markDeletedStmt = connection.prepareStatement(MARK_DELETED_SQL);

        deleteCounter = getRuntimeContext().getMetricGroup().counter("deleted_listings");
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(ListingUpdate value, Context context) throws Exception {
        var payload = value.getPayload();

        log.info("Preparing to soft delete listing: id={} steamid={}, defindex={}, quality={}, intent={}",
                payload.getId(),
                payload.getSteamid(),
                payload.getItem().getDefindex(),
                payload.getItem().getQuality().getId(),
                payload.getIntent());

        markDeletedStmt.setString(1, payload.getId());
        markDeletedStmt.addBatch();

        currentBatchCount++;
        deleteCounter.inc();

        long now = System.currentTimeMillis();
        if (currentBatchCount >= batchSize || (now - lastFlushTime) >= batchIntervalMs) {
            log.info("Preparing to commit batch soft deletes currentBatchCount={} lastFlushTime={}", currentBatchCount, lastFlushTime);
            markDeletedStmt.executeBatch();
            connection.commit();
            currentBatchCount = 0;
            lastFlushTime = now;
        }
    }

    @Override
    public void close() throws Exception {
        if (markDeletedStmt != null) {
            markDeletedStmt.executeBatch();
            markDeletedStmt.close();
        }
        if (connection != null) {
            connection.commit();
            connection.close();
        }
        super.close();
    }
}
