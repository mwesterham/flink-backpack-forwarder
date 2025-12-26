# Development Guide

## Local Development Setup

### Prerequisites

- Java 17 or higher
- Maven 3.6+
- Docker and Docker Compose
- Apache Flink 1.20.2

### Flink Setup

**Download and Install Flink:**
```bash
wget https://dlcdn.apache.org/flink/flink-1.20.2/flink-1.20.2-bin-scala_2.12.tgz
tar -xzf flink-1.20.2-bin-scala_2.12.tgz
```

**Add to PATH:**
```bash
vi ~/.bashrc
export FLINK_HOME=/home/username/flink-1.20.2
export PATH=$FLINK_HOME/bin:$PATH
source ~/.bashrc
```

**Verify Installation:**
```bash
flink -v
```

### Flink Configuration

**Configure Metrics (add to `$FLINK_HOME/conf/config.yaml`):**
```yaml
metrics:
  reporters: prom
  reporter:
    prom:
      factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
      port: 9249-9259
      filter.includes: "*"
```

**Adjust Task Manager Resources:**
```bash
code $FLINK_HOME/conf/config.yaml
```

## Building and Running

### Build Commands

**One-liner (Build, Docker, Push):**
```bash
mvn clean package && \
docker build -t tf2-ingest-flink-job:1.0 . && \
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest && \
docker push mwesterham/tf2-ingest-flink-job:latest
```

**Separate Steps:**
```bash
# Build the application
mvn clean package

# Build Docker image
docker build -t tf2-ingest-flink-job:1.0 .

# Tag for registry
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest

# Push to registry
docker push mwesterham/tf2-ingest-flink-job:latest
```

### Local Cluster Execution

**Start Flink Cluster:**
```bash
start-cluster.sh
```

**Run Application (Basic):**
```bash
mvn clean package && \
KAFKA_BROKERS="localhost:9092" \
KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic" \
KAFKA_CONSUMER_GROUP="flink-backpack-tf-test-consumer" \
DB_URL="jdbc:postgresql://localhost:5432/testdb" \
DB_USERNAME="testuser" \
DB_PASSWORD="testpass" \
flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

**Run Application (With Backfill):**
```bash
mvn clean package && \
KAFKA_BROKERS="localhost:9092" \
KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic" \
KAFKA_CONSUMER_GROUP="flink-backpack-tf-test-consumer" \
BACKFILL_KAFKA_TOPIC="backpack-tf-backfill-requests" \
BACKFILL_KAFKA_CONSUMER_GROUP="flink-backfill-test-consumer" \
BACKPACK_TF_API_TOKEN="your-api-token" \
STEAM_API_KEY="your-steam-key" \
DB_URL="jdbc:postgresql://localhost:5432/testdb" \
DB_USERNAME="testuser" \
DB_PASSWORD="testpass" \
flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

**Start from Specific Timestamp:**
```bash
# Add KAFKA_START_TIMESTAMP to any of the above commands
KAFKA_START_TIMESTAMP="0" \  # Start from beginning
# or
KAFKA_START_TIMESTAMP="$(date -d '1 hour ago' +%s)000" \  # Start from 1 hour ago
```

### Job Management

**List Running Jobs:**
```bash
flink list
```

**Cancel Job:**
```bash
flink cancel <job_id>
```

**Stop Cluster:**
```bash
stop-cluster.sh
```

## Testing

### Unit Tests

**Run All Tests:**
```bash
mvn test
```

**Run Specific Test Class:**
```bash
mvn test -Dtest="BackfillProcessorTest"
```

**Run Integration Tests:**
```bash
mvn test -Dtest="*IntegrationTest"
```

**Run Backfill Handler Tests:**
```bash
mvn test -Dtest="*BackfillHandler*IntegrationTest"
```

### Integration Testing

The project includes comprehensive integration tests for all backfill handlers:

- `BuyOnlyBackfillHandlerIntegrationTest`
- `SellOnlyBackfillHandlerIntegrationTest`
- `FullBackfillHandlerIntegrationTest`
- `SingleIdBackfillHandlerIntegrationTest`

These tests use Mockito to simulate API responses and verify the complete data flow.

### Manual Testing

**Database Queries:**
```bash
# Connect to test database
docker exec -it flink-postgres psql -U testuser -d testdb

# Check listings
SELECT * FROM listings LIMIT 10;

# Check specific item listings
SELECT * FROM listings 
WHERE item_defindex = 190 AND item_quality_id = 11 
ORDER BY raw_value;

# Check recent updates
SELECT COUNT(*) as recent_updates
FROM listings 
WHERE updated_at > (EXTRACT(EPOCH FROM now() - interval '5 minutes') * 1000);
```

## Debugging

### IDE Setup

**IntelliJ IDEA:**
1. Right-click `pom.xml`
2. Select `Maven -> Sync Project`
3. Ensure Java 17 is configured as project SDK

**VS Code:**
1. Install Java Extension Pack
2. Open project folder
3. Ensure Java 17 is configured

### Common Issues

**Flink Highlighted RED in IDE:**
- Right-click `pom.xml`
- Select `Maven -> Sync Project`

**Build Failures:**
```bash
# Clean and rebuild
mvn clean compile

# Skip tests if needed
mvn clean package -DskipTests
```

**Runtime Issues:**
```bash
# Check Flink logs
# Go to http://localhost:8081 -> Task Managers -> Select task manager -> Logs

# Check application logs
tail -f $FLINK_HOME/log/flink-*.log
```

### Debugging Backfill Issues

**Enable Debug Logging:**
Add to `$FLINK_HOME/conf/log4j2.properties`:
```properties
logger.backfill.name = me.matthew.flink.backpacktfforward.processor
logger.backfill.level = DEBUG
```

**Test API Connections:**
```bash
# Test BackpackTF API
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "https://backpack.tf/api/IGetMarketPrices/v1?appid=440&market_name=Strange%20Bat"

# Test Steam API
curl "https://api.steampowered.com/IEconItems_440/GetPlayerItems/v1/?key=YOUR_KEY&steamid=76561199574661225"
```

**Monitor Metrics During Development:**
```bash
# Watch metrics in real-time
watch -n 2 "curl -s http://localhost:9250/metrics | grep backfill"

# Check specific handler metrics
curl http://localhost:9250/metrics | grep backfill_full_requests
```

## Code Structure

### Key Packages

- `processor/`: Backfill handlers and main processing logic
- `client/`: API clients for BackpackTF and Steam
- `model/`: Data models and DTOs
- `util/`: Utility classes (database helpers, mappers, etc.)
- `config/`: Configuration classes
- `sink/`: Database sink implementations
- `source/`: Kafka source implementations

### Adding New Backfill Handlers

1. **Create Handler Class:**
   ```java
   public class CustomBackfillHandler implements BackfillRequestHandler {
       @Override
       public void process(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
           // Implementation
       }
       
       @Override
       public BackfillRequestType getRequestType() {
           return BackfillRequestType.CUSTOM;
       }
       
       @Override
       public boolean canHandle(BackfillRequest request) {
           // Validation logic
       }
   }
   ```

2. **Add Request Type:**
   ```java
   public enum BackfillRequestType {
       FULL, BUY_ONLY, SELL_ONLY, SINGLE_ID, CUSTOM
   }
   ```

3. **Update Factory:**
   ```java
   // Add to BackfillRequestFactory
   case CUSTOM:
       return new CustomBackfillHandler(databaseHelper, apiClient, steamApi);
   ```

4. **Add Tests:**
   ```java
   @ExtendWith(MockitoExtension.class)
   class CustomBackfillHandlerIntegrationTest {
       // Test implementation
   }
   ```

### Code Style Guidelines

- Use Lombok annotations for boilerplate code
- Follow existing naming conventions
- Add comprehensive logging with appropriate levels
- Include JavaDoc for public methods
- Use meaningful variable and method names
- Handle exceptions gracefully with proper logging

### Performance Considerations

- Use appropriate backfill handlers for the use case
- Implement proper rate limiting for API calls
- Use connection pooling for database operations
- Monitor memory usage with large datasets
- Consider async processing for I/O operations

## Deployment

### Docker Deployment

**Build Production Image:**
```bash
mvn clean package
docker build -t flink-backpack-tf-forwarder:latest .
```

**Environment Variables for Production:**
```bash
# Core configuration
KAFKA_BROKERS=prod-kafka:9092
KAFKA_TOPIC=backpack-tf-relay-egress-queue-topic
KAFKA_CONSUMER_GROUP=flink-backpack-tf-prod-consumer
DB_URL=jdbc:postgresql://prod-db:5432/listings
DB_USERNAME=prod_user
DB_PASSWORD=secure_password

# Backfill configuration
BACKFILL_KAFKA_TOPIC=backpack-tf-backfill-requests
BACKFILL_KAFKA_CONSUMER_GROUP=flink-backfill-prod-consumer
BACKPACK_TF_API_TOKEN=production_api_token
STEAM_API_KEY=production_steam_key

# Production tuning
BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS=15
STEAM_API_RATE_LIMIT_SECONDS=12
```

### Kubernetes Deployment

**Example Deployment YAML:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-backpack-tf-forwarder
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink-backpack-tf-forwarder
  template:
    metadata:
      labels:
        app: flink-backpack-tf-forwarder
    spec:
      containers:
      - name: flink-job
        image: flink-backpack-tf-forwarder:latest
        env:
        - name: KAFKA_BROKERS
          value: "kafka-service:9092"
        # Add other environment variables
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
```