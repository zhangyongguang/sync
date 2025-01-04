# Sync

Synchronize Production NOSQL and SQL data to Standalone instances for Data scientists or other purposes. A **Go-based** tool to synchronize MongoDB or SQL data from a **MongoDB replica set** or **sharded cluster** or production SQL instance to a **standalone instance**, supports initial and incremental synchronization with change stream monitoring.

> [!NOTE]
> Sync is now supporting MySQl, PostgreSQL and MariaDB. Next `Sync` will support Redis and Elasticsearch.

## Supported Databases

- MongoDB (Sharded clusters, Replica sets)
- MySQL
- MariaDB 
- PostgreSQL (PostgreSQL version 10+、Enable logical replication)

## High Level Design Diagram

### MongoDB sync (usual)
![image](https://github.com/user-attachments/assets/f600c3ae-a6bf-4d64-9a7b-6715456a146b)

### MongoDB sync (shard or replica set)

![image](https://github.com/user-attachments/assets/82cd3811-44bf-4d44-8ac8-9f32aace7a83)

### MySQL or MariaDB

![image](https://github.com/user-attachments/assets/65b23a4c-56db-4833-89a1-0f802af878bd)

### Grafana Integration(temporary transition phase)
![image](https://github.com/user-attachments/assets/cdc8e57b-8aa4-4386-8aa8-de5028698fd0)


## Features

- **Initial Sync**:
  - MongoDB: Bulk synchronization of data from the MongoDB cluster or MongoDB replica set to the standalone MongoDB instance.
  - MySQL/MariaDB: Initial synchronization using batch inserts (default batch size: 100 rows) from the source to the target if the target table is empty.
  - PostgreSQL: Initial synchronization using batch inserts (default batch size: 100 rows) from the source to the target using logical replication slots and the pgoutput plugin.
- **Change Stream & Binlog Monitoring**:
  - MongoDB: Watches for real-time changes (insert, update, replace, delete) in the cluster's collections and reflects them in the standalone instance.
  - MySQL/MariaDB: Uses binlog replication events to capture and apply incremental changes to the target.
  - PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to capture and apply incremental changes to the target.
- **Batch Processing & Concurrency**:  
  Handles synchronization in batches for optimized performance and supports parallel synchronization for multiple collections/tables.
- **Restart Resilience**: 
  Stores MongoDB resume tokens, MySQL binlog positions, and PostgreSQL replication positions in configurable state files, allowing the tool to resume synchronization from the last known position after a restart.
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
```

## Real-Time Synchronization

- MongoDB: Uses Change Streams from replica sets or sharded clusters for incremental updates.
- MySQL/MariaDB: Uses binlog replication to apply incremental changes to the target.
- PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to apply incremental changes to the target.

On the restart, the tool resumes from the stored state (resume token for MongoDB, binlog position for MySQL/MariaDB, replication slot for PostgreSQL).

## Availability  

- MongoDB: MongoDB Change Streams require a replica set or sharded cluster. See [Convert Standalone to Replica Set](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).
- MySQL/MariaDB: MySQL/MariaDB binlog-based incremental sync requires ROW or MIXED binlog format for proper event capturing.
- PostgreSQL incremental sync requires logical replication enabled with a replication slot.

## Contributing

We encourage all contributions to this repository! Please fork the repository or open an issue, make changes, and submit a pull request.
Note: All interactions here should conform to the [Code of Conduct](https://github.com/retail-ai-inc/sync/blob/main/CODE_OF_CONDUCT.md).

## Give a Star! ⭐

If you like or are using this project, please give it a **star**. Thanks!