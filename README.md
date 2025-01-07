# Sync

Synchronize Production NOSQL and SQL data to Standalone instances for Data scientists or other purposes. A **Go-based** tool to synchronize MongoDB or SQL data from a **MongoDB replica set** or **sharded cluster** or production SQL instance to a **standalone instance**, supports initial and incremental synchronization with change stream monitoring.

> [!NOTE]
> Sync is now supporting MongoDB, MySQL, PostgreSQL, MariaDB, and Redis. Next, `Sync` will support Elasticsearch.

## What is the problem
Let’s assume you have a mid to big-size SaaS platform or service with multiple tech teams and stakeholders. Different teams have different requirements for analyzing the production data independently. However, the tech team doesn’t want to allow all these stakeholders direct access to the production databases due to security and stability issues.

## A simple one-way solution
Create standalone databases outside of your production database servers with the same name as production and sync the production data of the specific tables or collections to the standalone database. **Sync** will do this for you.

## Supported Databases

- MongoDB (Sharded clusters, Replica sets)
- MySQL
- MariaDB 
- PostgreSQL (PostgreSQL version 10+ with logical replication enabled)
- Redis (Standalone, Sentinel; does not support cluster mode)

## High Level Design Diagram

### MongoDB sync (usual)
![image](https://github.com/user-attachments/assets/f600c3ae-a6bf-4d64-9a7b-6715456a146b)

### MongoDB sync (shard or replica set)

![image](https://github.com/user-attachments/assets/82cd3811-44bf-4d44-8ac8-9f32aace7a83)

### MySQL or MariaDB

![image](https://github.com/user-attachments/assets/65b23a4c-56db-4833-89a1-0f802af878bd)

### Grafana Integration (temporary transition phase)
![image](https://github.com/user-attachments/assets/cdc8e57b-8aa4-4386-8aa8-de5028698fd0)


## Features

- **Initial Sync**:
  - MongoDB: Bulk synchronization of data from the MongoDB cluster or MongoDB replica set to the standalone MongoDB instance.
  - MySQL/MariaDB: Initial synchronization using batch inserts (default batch size: 100 rows) from the source to the target if the target table is empty.
  - PostgreSQL: Initial synchronization using batch inserts (default batch size: 100 rows) from the source to the target using logical replication slots and the pgoutput plugin.
  - Redis: Supports full data synchronization for standalone Redis and Sentinel setups using Redis Streams and Keyspace Notifications.
- **Change Stream & Incremental Updates**:
  - MongoDB: Watches for real-time changes (insert, update, replace, delete) in the cluster's collections and reflects them in the standalone instance.
  - MySQL/MariaDB: Uses binlog replication events to capture and apply incremental changes to the target.
  - PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to capture and apply incremental changes to the target.
  - Redis: Uses Redis Streams and Keyspace Notifications to capture and sync incremental changes in real-time.
- **Batch Processing & Concurrency**:  
  Handles synchronization in batches for optimized performance and supports parallel synchronization for multiple collections/tables.
- **Restart Resilience**: 
  Stores MongoDB resume tokens, MySQL binlog positions, PostgreSQL replication positions, and Redis stream offsets in configurable state files, allowing the tool to resume synchronization from the last known position after a restart.
  - **Note for Redis**: Redis does not support resuming from the last state after a sync interruption. If `Sync` is interrupted or crashes, it will restart the synchronization process by executing the initial sync method to retrieve all keys and sync them to the target database. This is due to limitations in Redis Streams and Keyspace Notifications, which do not provide a built-in mechanism to persist and resume stream offsets across restarts. As a result, the tool cannot accurately determine the last synced state and must perform a full resync to ensure data consistency.
- **Grafana Integration**:
  - For data visualization, this tool integrates with **Grafana** using data from **GCP Logging** and **GCP BigQuery**.
  - When **`enable_table_row_count_monitoring`** is enabled, the tool records data changes, including table row counts, in **GCP Logging**.
  - These logs are then forwarded via **Log Router** to **GCP BigQuery**.<br>Finally, **Grafana** is used to visualize this data, providing users with insights into the synchronization process.
  - This integration is part of a temporary transition phase, and future development will focus on using a more flexible database solution for direct display and synchronization.

## Prerequisites
- For MongoDB sources:
  - A source MongoDB cluster (replica set or sharded cluster) with MongoDB version >= 4.0.
  - A target standalone MongoDB instance with write permissions.
- For MySQL/MariaDB sources:
  - A MySQL or MariaDB instance with binlog enabled (ROW or MIXED format recommended) and a user with replication privileges.
  - A target MySQL or MariaDB instance with write permissions.
- For PostgreSQL sources:
  - A PostgreSQL instance with logical replication enabled and a replication slot created.
  - A target PostgreSQL instance with write permissions.
- For Redis sources:
  - Redis standalone or Sentinel setup with Redis version >= 5.0.
  - Redis Streams and Keyspace Notifications enabled.
  - A target Redis instance with write permissions.

## Quick start

This is a demo for macOS on amd64. For other operating systems and architectures, you need to replace the download link with the appropriate binary URL for your system.
```
curl -L -o sync.tar.gz https://github.com/retail-ai-inc/sync/releases/download/v2.0.1/sync_2.0.1_darwin_amd64.tar.gz
tar -xzf sync.tar.gz
# Edit configs/config.yaml to replace the placeholders with your instance details.
./sync
```

## Installation(For development)

```
# 1. Clone the repository:
git clone https://github.com/retail-ai-inc/sync.git
cd sync

# 2. Install dependencies
go mod tidy

# 3. Build the binary
# Edit configs/config.yaml to replace the placeholders with your instance details.
go build -o sync cmd/sync/main.go

# 4. Build the Docker image
docker build -t sync .
docker run -v $(pwd)/configs/config.yaml:/app/configs/config.yaml sync
```

### Configuration File: config.yaml

The `config.yaml` defines multiple sync tasks. Each sync task specifies:
- **Multi Mappings Config**: Sync supports multiple mappings per source, allowing you to replicate data from multiple databases or schemas to one or more target databases/schemas simultaneously.
- **Type of Source** (`mongodb`, `mysql`, `mariadb`, `postgresql`).
- **Source and Target Connection Strings**.
- **Database/Table or Collection Mappings**.
- **enable_table_row_count_monitoring**:  
  Enables tracking of row counts for tables, providing insights into data growth and consistency.
- **State File Paths for Resume Tokens or Binlog Positions**:  
  Supports **resume functionality**, allowing the sync to continue from the last state after interruptions, ensuring data consistency.
  - **MongoDB**: `mongodb_resume_token_path` specifies the file path where the MongoDB resume token is stored.
  - **MySQL/MariaDB**: `mysql_position_path` specifies the file path where the MySQL/MariaDB binlog position is stored.
  - **PostgreSQL**: `pg_replication_slot` and `pg_plugin` specify the replication slot and plugin used for capturing WAL changes.

#### Example `config.yaml`

```yaml
enable_table_row_count_monitoring: true
log_level: "info"  # Optional values: "debug", "info", "warn", "error", "fatal", "panic"

sync_configs:
  - type: "mongodb"
    enable: true
    source_connection: "mongodb://localhost:27017"
    target_connection: "mongodb://localhost:27017"
    mongodb_resume_token_path: "/tmp/state/mongodb_resume_token"
    mappings:
      - source_database: "source_db"
        target_database: "target_db"
        tables:
          - source_table: "users"
            target_table: "users"

  - type: "mysql"
    enable: true
    source_connection: "root:root@tcp(localhost:3306)/source_db"
    target_connection: "root:root@tcp(localhost:3306)/target_db"
    mysql_position_path: "/tmp/state/mysql_position"
    mappings:
      - source_database: "source_db"
        target_database: "target_db"
        tables:
          - source_table: "users"
            target_table: "users"

  - type: "mariadb"
    enable: true
    source_connection: "root:root@tcp(localhost:3307)/source_db"
    target_connection: "root:root@tcp(localhost:3307)/target_db"
    mysql_position_path: "/tmp/state/mariadb_position"
    mappings:
      - source_database: "source_db"
        target_database: "target_db"
        tables:
          - source_table: "users"
            target_table: "users"

  - type: "postgresql"
    enable: true
    source_connection: "postgres://root:root@localhost:5432/source_db?sslmode=disable"
    target_connection: "postgres://root:root@localhost:5432/target_db?sslmode=disable"
    pg_position_path: "/tmp/state/pg_position"
    pg_replication_slot: "sync_slot"
    pg_plugin: "pgoutput"
    pg_publication_names: "mypub"
    mappings:
      - source_database: "source_db"
        source_schema: "public"
        target_database: "target_db"
        target_schema: "public"
        tables:
          - source_table: "users"
            target_table: "users"

  - type: "redis"
    enable: true
    source_connection: "redis://localhost:6379/0"
    target_connection: "redis://localhost:6379/1"
    redis_position_path: "/tmp/state/redis_position"
    mappings:
      - source_database: "db0"
        target_database: "db1"
        tables:
          - source_table: "source_stream"  # Redis Stream Name
            target_table: "" 
```

## Real-Time Synchronization

- MongoDB: Uses Change Streams from replica sets or sharded clusters for incremental updates.
- MySQL/MariaDB: Uses binlog replication to apply incremental changes to the target.
- PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to apply incremental changes to the target.
- Redis: Uses Redis Streams and Keyspace Notifications to sync changes in real-time.
  - **Note for Redis**: If `Sync` is interrupted, Redis will restart the synchronization process with an initial sync of all keys to the target. This ensures data consistency but may increase synchronization time after interruptions.

On the restart, the tool resumes from the stored state (resume token for MongoDB, binlog position for MySQL/MariaDB, replication slot for PostgreSQL).

## Availability  

- MongoDB: MongoDB Change Streams require a replica set or sharded cluster. See [Convert Standalone to Replica Set](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).
- MySQL/MariaDB: MySQL/MariaDB binlog-based incremental sync requires ROW or MIXED binlog format for proper event capturing.
- PostgreSQL: PostgreSQL incremental sync requires logical replication enabled with a replication slot.
- Redis: Redis sync supports standalone and Sentinel setups but does not support Redis Cluster mode. Redis does not support resuming from the last synced state after a crash or interruption.

## Contributing

We encourage all contributions to this repository! Please fork the repository or open an issue, make changes, and submit a pull request.
Note: All interactions here should conform to the [Code of Conduct](https://github.com/retail-ai-inc/sync/blob/main/CODE_OF_CONDUCT.md).

## Give a Star! ⭐

If you like or are using this project, please give it a **star**. Thanks!
