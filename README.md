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

### Running locally via cluster

- Setup the test sink

```
docker run --name flink-postgres -e POSTGRES_USER=testuser -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=testdb -p 5432:5432 -d postgres:16
```

```
docker exec -it flink-postgres psql -U testuser -d testdb

DROP TABLE IF EXISTS listings;
CREATE TABLE listings (
    id TEXT,
    event TEXT,
    steamid TEXT,
    appid INT,
    metal DOUBLE PRECISION,
    keys INT,
    raw_value DOUBLE PRECISION,
    short_value TEXT,
    long_value TEXT,
    details TEXT,
    listed_at BIGINT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (steamid, id)
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
```

- Stop the cluster

```
stop-cluster.sh
```

### Debugging

#### Flink is highlighted RED in code?

- Right-click `pom.xml`
- `Maven -> Sync Project`