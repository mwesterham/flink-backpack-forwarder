package me.matthew.flink.backpacktfforward.metrics;

import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public final class SqlRetryMetrics {

    private final MetricGroup metricGroup;
    private final Map<String, Counter> countersBySqlState = new HashMap<>();
    private final String metricName;

    private static final String DEADLOCK_SQL_STATE = "40P01";

    public SqlRetryMetrics(MetricGroup metricGroup, String metricName) {
        this.metricGroup = metricGroup;
        this.metricName = metricName;
    }

    public RetryPolicy<Object> deadlockRetryPolicy(int maxRetries) {
        return RetryPolicy.builder()
                .handle(SQLException.class)
                .handleIf(e -> DEADLOCK_SQL_STATE.equals(((SQLException) e).getSQLState()))
                .withDelay(Duration.ofMillis(50))
                .withMaxRetries(maxRetries)
                .onRetry(e -> {
                    SQLException ex = (SQLException) e.getLastException();
                    String sqlState = ex.getSQLState();

                    Counter counter = countersBySqlState.computeIfAbsent(
                            sqlState,
                            s -> metricGroup
                                    .addGroup("sql_state", s)
                                    .counter(metricName)
                    );

                    counter.inc();

                    log.warn(
                            "SQL retry (attempt {}, sqlState={}): {}",
                            e.getAttemptCount(),
                            sqlState,
                            ex.getMessage()
                    );
                })
                .onRetriesExceeded(e ->
                        log.error("Max SQL retries exceeded", e.getException())
                )
                .build();
    }
}