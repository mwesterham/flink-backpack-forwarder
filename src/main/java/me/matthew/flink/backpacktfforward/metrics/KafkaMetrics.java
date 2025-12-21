package me.matthew.flink.backpacktfforward.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka-specific metrics collector for monitoring consumer performance and health.
 * Provides counters for connection events, rebalancing, and offset management,
 * as well as gauges for consumer lag monitoring.
 */
@Slf4j
public class KafkaMetrics {
    
    private final Counter connectionFailures;
    private final Counter reconnectAttempts;
    private final Counter topicValidationFailures;
    private final Counter consumerRebalances;
    private final Counter offsetCommitsSuccess;
    private final Counter offsetCommitsFailed;
    
    private final AtomicLong consumerLag = new AtomicLong(0);
    private final Gauge<Long> consumerLagGauge;
    
    /**
     * Creates a new KafkaMetrics instance with all Kafka-specific metrics registered
     * to the provided metric group.
     * 
     * @param metricGroup The Flink metric group to register metrics with
     */
    public KafkaMetrics(MetricGroup metricGroup) {
        // Register counters for connection and error handling
        this.connectionFailures = metricGroup.counter(Metrics.KAFKA_CONNECTION_FAILURES);
        this.reconnectAttempts = metricGroup.counter(Metrics.KAFKA_RECONNECT_ATTEMPTS);
        this.topicValidationFailures = metricGroup.counter(Metrics.KAFKA_TOPIC_VALIDATION_FAILURES);
        
        // Register counters for consumer group coordination
        this.consumerRebalances = metricGroup.counter(Metrics.KAFKA_CONSUMER_REBALANCES);
        this.offsetCommitsSuccess = metricGroup.counter(Metrics.KAFKA_OFFSET_COMMITS_SUCCESS);
        this.offsetCommitsFailed = metricGroup.counter(Metrics.KAFKA_OFFSET_COMMITS_FAILED);
        
        // Register gauge for consumer lag monitoring
        this.consumerLagGauge = metricGroup.gauge(Metrics.KAFKA_CONSUMER_LAG, () -> consumerLag.get());
        
        log.info("Kafka metrics initialized successfully");
    }
    
    /**
     * Records a Kafka connection failure event.
     * Should be called when the consumer fails to connect to Kafka brokers.
     */
    public void recordConnectionFailure() {
        connectionFailures.inc();
        log.debug("Recorded Kafka connection failure. Total failures: {}", connectionFailures.getCount());
    }
    
    /**
     * Records a Kafka reconnection attempt.
     * Should be called when the consumer attempts to reconnect to Kafka brokers.
     */
    public void recordReconnectAttempt() {
        reconnectAttempts.inc();
        log.debug("Recorded Kafka reconnect attempt. Total attempts: {}", reconnectAttempts.getCount());
    }
    
    /**
     * Records a topic validation failure.
     * Should be called when topic existence validation fails.
     */
    public void recordTopicValidationFailure() {
        topicValidationFailures.inc();
        log.debug("Recorded Kafka topic validation failure. Total failures: {}", topicValidationFailures.getCount());
    }
    
    /**
     * Records a consumer group rebalancing event.
     * Should be called when the consumer group undergoes rebalancing.
     */
    public void recordConsumerRebalance() {
        consumerRebalances.inc();
        log.debug("Recorded Kafka consumer rebalance. Total rebalances: {}", consumerRebalances.getCount());
    }
    
    /**
     * Records a successful offset commit.
     * Should be called when offsets are successfully committed to Kafka.
     */
    public void recordOffsetCommitSuccess() {
        offsetCommitsSuccess.inc();
        log.debug("Recorded successful Kafka offset commit. Total successful commits: {}", offsetCommitsSuccess.getCount());
    }
    
    /**
     * Records a failed offset commit.
     * Should be called when offset commit to Kafka fails.
     */
    public void recordOffsetCommitFailure() {
        offsetCommitsFailed.inc();
        log.debug("Recorded failed Kafka offset commit. Total failed commits: {}", offsetCommitsFailed.getCount());
    }
    
    /**
     * Updates the consumer lag metric.
     * Should be called periodically with the current consumer lag value.
     * 
     * @param lag Current consumer lag in number of messages
     */
    public void updateConsumerLag(long lag) {
        consumerLag.set(lag);
        log.trace("Updated Kafka consumer lag to: {}", lag);
    }
    
    /**
     * Gets the current consumer lag value.
     * 
     * @return Current consumer lag in number of messages
     */
    public long getCurrentConsumerLag() {
        return consumerLag.get();
    }
    
    /**
     * Gets the total number of connection failures recorded.
     * 
     * @return Total connection failures
     */
    public long getConnectionFailures() {
        return connectionFailures.getCount();
    }
    
    /**
     * Gets the total number of reconnect attempts recorded.
     * 
     * @return Total reconnect attempts
     */
    public long getReconnectAttempts() {
        return reconnectAttempts.getCount();
    }
    
    /**
     * Gets the total number of consumer rebalances recorded.
     * 
     * @return Total consumer rebalances
     */
    public long getConsumerRebalances() {
        return consumerRebalances.getCount();
    }
    
    /**
     * Gets the total number of successful offset commits recorded.
     * 
     * @return Total successful offset commits
     */
    public long getOffsetCommitsSuccess() {
        return offsetCommitsSuccess.getCount();
    }
    
    /**
     * Gets the total number of failed offset commits recorded.
     * 
     * @return Total failed offset commits
     */
    public long getOffsetCommitsFailed() {
        return offsetCommitsFailed.getCount();
    }
}