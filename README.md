# Sync

Synchronize Production NOSQL and SQL data to Standalone instances for Data scientists or other purposes. A **Go-based** tool to synchronize MongoDB or SQL data from a **MongoDB replica set** or **sharded cluster** or production SQL instance to a **standalone instance**, supports initial and incremental synchronization with change stream monitoring.

> [!NOTE]
> We are experimenting with SQL options and plan to release the feature in **sync** version 2.0.

## Supported Databases

- MongoDB (Sharded clusters, Replica sets)
- MySQL (Supports incremental synchronization using binlog, with position tracking for restart resilience.)
- MariaDB (TBD)
- PgSQL (TBD)

## High Level Architecture

### MongoDB sync
![image](https://github.com/user-attachments/assets/f600c3ae-a6bf-4d64-9a7b-6715456a146b)

### Shard mongodb sync

![image](https://github.com/user-attachments/assets/82cd3811-44bf-4d44-8ac8-9f32aace7a83)

### MySQL
![image](https://github.com/user-attachments/assets/4b8f77ec-8072-41a1-ba7c-000bf91282cc)



## Features

- **Initial Sync**:
  - MongoDB: Bulk synchronization of data from the MongoDB cluster or MongoDB replica set to the standalone MongoDB instance.
  - MySQL: Initial dump (if required) or start from a specified binlog position.
- **Incremental Sync**:
  - MongoDB: Synchronizes newly updated or inserted data since the last sync using timestamps.
  - MySQL: Continuously applies changes read from the MySQL binlog, maintaining a current position for restart.
- **Change Stream & Binlog Monitoring**:
  - MongoDB: Watches for real-time changes (insert, update, replace, delete) in the cluster's collections and reflects them in the standalone instance.
  - MySQL: Uses binlog replication events to capture incremental changes.
- **Batch Processing & Concurrency**:  
  Handles synchronization in batches for optimized performance, and supports parallel synchronization for multiple collections/tables.
- **Restart Resilience**:  
  Stores MongoDB resume tokens and MySQL binlog positions in configurable state files, allowing the tool to resume synchronization from the last known position after a restart.

## Prerequisites
- For MongoDB sources:
  - A source MongoDB cluster (replica set or sharded cluster) with MongoDB version >= 4.0.
  - A target standalone MongoDB instance with write permissions.
- For MySQL sources:
  - A MySQL instance with binlog enabled (ROW or MIXED format recommended) and a user with replication privileges.
  - A target MySQL instance with write permissions.

## Installation

```
# 1.Clone the repository:
git clone https://github.com/retail-ai-inc/sync.git
cd sync

# 2.Install dependencies
go mod tidy

# 3.Build the binary
cp configs/config.sample.yaml configs/config.yaml
# Edit config.yaml to replace the placeholders with your instance details.
go build -o sync cmd/sync/main.go

# 4.Build the Docker image
docker build -t sync .
docker run -v $(pwd)/configs/config.yaml:/app/configs/config.yaml sync
```

### Configuration File: `config.yaml`

The config.yaml defines multiple sync tasks. Each sync task specifies:  
- The type of source (mongodb or mysql).
- Source and target connection strings.
- Database/table or collection mappings.
- State file paths for resume tokens or binlog positions.
  - MongoDB: The mongodb_resume_token_path is a configuration option used to specify the file path where the MongoDB resume token is stored.
  - MySQL: The mysql_position_path is a configuration option that specifies the file path where the MySQL binlog position is stored.

#### Example `config.yaml`

```yaml
sync_configs:
  - type: "mongodb"
    enable: true
    source_connection: "mongodb://user:pass@host1:27017"
    target_connection: "mongodb://user:pass@host2:27017"
    mongodb_resume_token_path: "/data/state/mongodb_resume_token.json"
    mappings:
      - source_database: "source_db"
        target_database: "target_db"
        tables:
          - source_table: "A1"
            target_table: "A1"
          - source_table: "A2"
            target_table: "A2"

  - type: "mysql"
    enable: true
    source_connection: "user:pass@tcp(source-host:3306)/source_db"
    target_connection: "user:pass@tcp(target-host:3306)/target_db"
    mysql_position_path: "/data/state/mysql_position.json"
    mappings:
      - source_database: "source_db"
        target_database: "target_db"
        tables:
          - source_table: "A1"
            target_table: "A1"
          - source_table: "A2"
            target_table: "A2"
    dump_execution_path: "" # optional
```

## Real-Time Synchronization

- MongoDB: Uses Change Streams from replica sets or sharded clusters for incremental updates.
- MySQL: Uses binlog replication to apply incremental changes to the target.

On restart, the tool resumes from the stored state (resume token for MongoDB, binlog position for MySQL).

## Availability  

- MongoDB: MongoDB Change Streams require a replica set or sharded cluster. See [Convert Standalone to Replica Set](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).
- MySQL: MySQL binlog-based incremental sync requires ROW or MIXED binlog format for proper event capturing.

## Contributing
Contributions are welcome! Please fork the repository, make changes, and submit a pull request.
