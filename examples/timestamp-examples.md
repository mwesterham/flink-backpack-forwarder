# Kafka Consumer Timestamp Examples

This document shows different ways to start your Kafka consumer from a specific timestamp.

## Method 1: Environment Variable (Recommended for Production)

Set the `KAFKA_START_TIMESTAMP` environment variable with a Unix timestamp in milliseconds:

```bash
# Start from a specific timestamp (December 20, 2024 10:30:00 UTC)
export KAFKA_START_TIMESTAMP=1703936200000

# Start from 1 hour ago
export KAFKA_START_TIMESTAMP=$(date -d '1 hour ago' +%s)000

# Start from beginning of today
export KAFKA_START_TIMESTAMP=$(date -d 'today 00:00:00' +%s)000
```

Then run your application normally - it will automatically use the timestamp.

## Method 2: Programmatic Usage

### Using Unix Timestamp

```java
// Start from specific timestamp (milliseconds)
long timestamp = 1703936200000L; // December 20, 2024 10:30:00 UTC
KafkaSource<String> source = KafkaMessageSource.createSourceFromTimestamp(timestamp);
```

### Using Instant

```java
// Start from 1 hour ago
Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
KafkaSource<String> source = KafkaMessageSource.createSourceFromTimestamp(oneHourAgo);
```

### Using TimestampUtil Helper

```java
// Start from 2 hours ago
long timestamp = TimestampUtil.getTimestampHoursAgo(2);
KafkaSource<String> source = KafkaMessageSource.createSourceFromTimestamp(timestamp);

// Start from specific date-time
long timestamp = TimestampUtil.parseIsoDateTime("2024-12-20T10:30:00");
KafkaSource<String> source = KafkaMessageSource.createSourceFromTimestamp(timestamp);
```

## Method 3: Docker Environment

When running in Docker, pass the timestamp as an environment variable:

```bash
docker run -e KAFKA_START_TIMESTAMP=1703936200000 your-app:latest
```

Or in docker-compose.yml:

```yaml
services:
  kafka-consumer:
    image: your-app:latest
    environment:
      - KAFKA_START_TIMESTAMP=1703936200000
      # ... other env vars
```

## Getting Timestamps

### Current timestamp
```bash
date +%s000  # Current timestamp in milliseconds
```

### Specific date
```bash
# December 20, 2024 10:30:00 UTC
date -d '2024-12-20 10:30:00 UTC' +%s000
```

### Relative timestamps
```bash
# 1 hour ago
date -d '1 hour ago' +%s000

# Beginning of today
date -d 'today 00:00:00' +%s000

# Yesterday at midnight
date -d 'yesterday 00:00:00' +%s000
```

## Important Notes

1. **Timestamp Resolution**: Kafka uses message timestamps, not offset timestamps. The consumer will start from the first message at or after your specified timestamp.

2. **Message Timestamps**: Make sure your Kafka messages have proper timestamps. By default, Kafka uses the producer timestamp when the message was sent.

3. **Partition Behavior**: The timestamp applies to all partitions. Each partition will start from the first message at or after the timestamp.

4. **No Messages**: If no messages exist at or after the timestamp, the consumer will wait for new messages.

5. **Consumer Group**: If using a consumer group that already has committed offsets, you may need to reset the consumer group or use a new group ID to start from the timestamp.

## Troubleshooting

### Consumer not starting from timestamp
- Check that `KAFKA_START_TIMESTAMP` is set correctly
- Verify the timestamp is in milliseconds (not seconds)
- Ensure the consumer group is new or reset

### No messages received
- Verify messages exist at or after the timestamp
- Check message timestamps in your Kafka topic
- Confirm topic and partition configuration