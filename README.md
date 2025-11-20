### Setup flink

- Download
```
wget https://dlcdn.apache.org/flink/flink-1.20.2/flink-1.20.2-bin-scala_2.12.tgz
tar -xzf flink-1.20.2-bin-scala_2.12.tgz
```

- Add to path
```
vi .bashrc
export FLINK_HOME=/home/mwesterham/flink-1.20.2
export PATH=$FLINK_HOME/bin:$PATH
```

```
flink -v
```

- Setup windows cluster
Put this in `$FLINK_HOME/conf/flink-conf.yaml`
```
vi $FLINK_HOME/conf/flink-conf.yaml
```
```
# REST
rest.port: 8081
rest.address: localhost
rest.bind-address: 0.0.0.0

# JobManager memory
jobmanager.memory.process.size: 1024m

# TaskManager memory
taskmanager.memory.process.size: 1024m
taskmanager.numberOfTaskSlots: 2

# Default parallelism
parallelism.default: 2

```

### Building the docker file

```
docker build -t tf2-ingest-flink-job:1.0 .
```

```
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest
```

```
docker push mwesterham/tf2-ingest-flink-job:latest
```

### Running locally via cluster

- Setup the test sink

```
docker run --name flink-postgres -e POSTGRES_USER=testuser -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=testdb -p 5432:5432 -d postgres:16
```

```
docker exec -it flink-postgres psql -U testuser -d testdb

DROP TABLE IF EXISTS listings;
CREATE TABLE listings (
    id TEXT PRIMARY KEY,
    steamid TEXT NOT NULL,
    item_defindex INT NOT NULL,
    item_quality_id INT NOT NULL,
    intent TEXT NOT NULL,
    appid INT,
    metal DOUBLE PRECISION,
    keys INT,
    raw_value DOUBLE PRECISION,
    short_value TEXT,
    long_value TEXT,
    details TEXT,
    listed_at TIMESTAMP,
    market_name TEXT,
    status TEXT,
    user_agent_client TEXT,
    user_name TEXT,
    user_premium BOOLEAN,
    user_online BOOLEAN,
    user_banned BOOLEAN,
    user_trade_offer_url TEXT,
    item_tradable BOOLEAN,
    item_craftable BOOLEAN,
    item_quality_color TEXT,
    item_particle_name TEXT,
    item_particle_type TEXT,
    bumped_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

- Build the package

```
mvn clean package
```

- Startup the cluster, this is located in $FLINK_HOME
```
start-cluster.sh
```

- Startup the job
```
mvn clean package && \
SOURCE_URL="ws://laputa.local:30331/forwarded" \
DB_URL="jdbc:postgresql://localhost:5432/testdb" \
DB_USERNAME="testuser" \
DB_PASSWORD="testpass" \
flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

- Observe the job

Go to http://localhost:8081 and to `Task Managers -> pick your task manager -> Logs`

- View/cancel running jobs

```
flink list
flink cancel <job_id>
```

- Observe the sink

```
docker exec -it flink-postgres psql -U testuser -d testdb

SELECT * FROM listings;

SELECT * FROM listings WHERE item_defindex = '5021' AND item_quality_id = '6' AND intent = 'sell' AND is_deleted = false ORDER BY raw_value;

SELECT * FROM listings WHERE id = '440_16358814163';
```

- Stop the cluster

```
stop-cluster.sh
```

### Adjustting task manager ram / prometheus ports / etc...

```
code /home/mwesterham/flink-1.20.2/conf/config.yaml
```

### Checking metrics

```
curl http://localhost:9249/metrics
```

```
curl http://localhost:9250/metrics | grep listing_upsert
```

### Debugging

#### Flink is highlighted RED in code?

- Right-click `pom.xml`
- `Maven -> Sync Project`