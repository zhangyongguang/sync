package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer/mariadb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mongodb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mysql"
	"github.com/retail-ai-inc/sync/pkg/syncer/postgresql"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Added function: get the full "schema.table" or "database.table"
func getQualifiedTableName(dbmap config.DatabaseMapping, useSource bool, tblmap config.TableMapping) string {
	if useSource {
		if dbmap.SourceSchema != "" {
			return fmt.Sprintf("%s.%s", dbmap.SourceSchema, tblmap.SourceTable)
		}
		return fmt.Sprintf("%s.%s", dbmap.SourceDatabase, tblmap.SourceTable)
	}
	if dbmap.TargetSchema != "" {
		return fmt.Sprintf("%s.%s", dbmap.TargetSchema, tblmap.TargetTable)
	}
	return fmt.Sprintf("%s.%s", dbmap.TargetDatabase, tblmap.TargetTable)
}

// Modified getNextSQLID: pass in "fullTableName" instead of "database, table"
func getNextSQLID(t *testing.T, db *sql.DB, fullTableName string) int64 {
	var maxID sql.NullInt64
	query := fmt.Sprintf("SELECT COALESCE(MAX(id),0) FROM %s", fullTableName)
	err := db.QueryRow(query).Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to get max ID from %s: %v", fullTableName, err)
	}
	return maxID.Int64 + 1
}

// Detect DB type to decide placeholders and possible TRUNCATE approach
func isPostgresDBType(dbType string) bool {
	return dbType == "postgresql"
}

// TestFullSync is a comprehensive integration test example.
func TestFullSync(t *testing.T) {
	// Prepare environment
	ctx, cancel := prepareTestEnvironment(t)
	defer cancel()

	// Connect all databases
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB :=
		connectAllDatabases(t)

	// Extract all mappings
	cfg := config.NewConfig()
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping := extractAllMappings(cfg)

	// Start all syncers
	startAllSyncers(ctx, cfg, logger.InitLogger(logrus.DebugLevel.String()))
	t.Log("Syncers started, waiting initial sync...")
	time.Sleep(5 * time.Second)

	// Insert initial data
	const initialInsertCount = 3
	insertInitialData(t,
		mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled,
		mongoSourceClient, mysqlSourceDB, mariaDBSourceDB, pgSourceDB,
		mongoMapping, mysqlMapping, mariadbMapping, pgMapping,
		initialInsertCount,
	)

	// Verify initial data synchronization
	verifyInitialDataConsistency(t,
		mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB,
		mongoMapping, mysqlMapping, mariadbMapping, pgMapping,
	)

	// Create/Update/Delete tests
	performCRUDOperations(t,
		mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB,
		mongoMapping, mysqlMapping, mariadbMapping, pgMapping,
	)

	time.Sleep(5 * time.Second)
	t.Log("Full synchronization test completed successfully.")
}

// 1. Read config, initialize Logger, and set environment
func prepareTestEnvironment(t *testing.T) (context.Context, context.CancelFunc) {
	projectRoot := "../../"
	configPath := filepath.Join(projectRoot, "configs/config.yaml")
	os.Setenv("CONFIG_PATH", configPath)

	ctx, cancel := context.WithCancel(context.Background())

	// Cleanup after test
	t.Cleanup(func() {
		os.Unsetenv("CONFIG_PATH")
	})

	return ctx, cancel
}

// 2. Connect all databases and determine which are enabled
func connectAllDatabases(t *testing.T) (
	bool, bool, bool, bool,
	*mongo.Client, *mongo.Client,
	*sql.DB, *sql.DB,
	*sql.DB, *sql.DB,
	*sql.DB, *sql.DB,
) {
	cfg := config.NewConfig()
	var (
		mongoEnabled    = false
		mysqlEnabled    = false
		mariaDBEnabled  = false
		postgresEnabled = false
	)

	for _, sc := range cfg.SyncConfigs {
		switch {
		case sc.Type == "mongodb" && sc.Enable:
			mongoEnabled = true
		case sc.Type == "mysql" && sc.Enable:
			mysqlEnabled = true
		case sc.Type == "mariadb" && sc.Enable:
			mariaDBEnabled = true
		case sc.Type == "postgresql" && sc.Enable:
			postgresEnabled = true
		}
	}

	if !mongoEnabled && !mysqlEnabled && !mariaDBEnabled && !postgresEnabled {
		t.Skip("No enabled MongoDB, MySQL, MariaDB or PostgreSQL sync config found in config.yaml, skipping test.")
	}

	// Initialize return objects
	var (
		mongoSourceClient *mongo.Client
		mongoTargetClient *mongo.Client
		mysqlSourceDB     *sql.DB
		mysqlTargetDB     *sql.DB
		mariaDBSourceDB   *sql.DB
		mariaDBTargetDB   *sql.DB
		pgSourceDB        *sql.DB
		pgTargetDB        *sql.DB
		err               error
	)

	// MongoDB
	if mongoEnabled {
		mongoSourceClient, mongoTargetClient, err = connectMongoDB(cfg)
		if err != nil {
			t.Fatalf("Failed to connect MongoDB: %v", err)
		}
		t.Log("MongoDB source/target connected successfully.")
		t.Cleanup(func() {
			_ = mongoSourceClient.Disconnect(context.Background())
			_ = mongoTargetClient.Disconnect(context.Background())
		})
	}

	// MySQL
	if mysqlEnabled {
		mysqlSourceDB, mysqlTargetDB, err = connectSQLDB(cfg, "mysql")
		if err != nil {
			t.Fatalf("Failed to connect MySQL: %v", err)
		}
		t.Log("MySQL source/target connected successfully.")
		t.Cleanup(func() {
			mysqlSourceDB.Close()
			mysqlTargetDB.Close()
		})
	}

	// MariaDB
	if mariaDBEnabled {
		mariaDBSourceDB, mariaDBTargetDB, err = connectSQLDB(cfg, "mariadb")
		if err != nil {
			t.Fatalf("Failed to connect MariaDB: %v", err)
		}
		t.Log("MariaDB source/target connected successfully.")
		t.Cleanup(func() {
			mariaDBSourceDB.Close()
			mariaDBTargetDB.Close()
		})
	}

	// PostgreSQL
	if postgresEnabled {
		pgSourceDB, pgTargetDB, err = connectPGDB(cfg)
		if err != nil {
			t.Fatalf("Failed to connect PostgreSQL: %v", err)
		}
		t.Log("PostgreSQL source/target connected successfully.")
		t.Cleanup(func() {
			pgSourceDB.Close()
			pgTargetDB.Close()
		})
	}

	return mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB
}

// 3. Extract all database mappings
func extractAllMappings(cfg *config.Config) ([]config.DatabaseMapping, []config.DatabaseMapping, []config.DatabaseMapping, []config.DatabaseMapping) {
	var mongoMapping, mysqlMapping, mariadbMapping, pgMapping []config.DatabaseMapping
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "mongodb" && sc.Enable {
			mongoMapping = sc.Mappings
		}
		if sc.Type == "mysql" && sc.Enable {
			mysqlMapping = sc.Mappings
		}
		if sc.Type == "mariadb" && sc.Enable {
			mariadbMapping = sc.Mappings
		}
		if sc.Type == "postgresql" && sc.Enable {
			pgMapping = sc.Mappings
		}
	}
	return mongoMapping, mysqlMapping, mariadbMapping, pgMapping
}

// 4. Start all Syncers
func startAllSyncers(ctx context.Context, cfg *config.Config, log *logrus.Logger) {
	for _, sc := range cfg.SyncConfigs {
		if !sc.Enable {
			continue
		}
		switch sc.Type {
		case "mongodb":
			syncer := mongodb.NewMongoDBSyncer(sc, log)
			go syncer.Start(ctx)
		case "mysql":
			syncer := mysql.NewMySQLSyncer(sc, log)
			go syncer.Start(ctx)
		case "mariadb":
			syncer := mariadb.NewMariaDBSyncer(sc, log)
			go syncer.Start(ctx)
		case "postgresql":
			syncer := postgresql.NewPostgreSQLSyncer(sc, log)
			go syncer.Start(ctx)
		}
	}
}

// 5. Insert initial data
func insertInitialData(
	t *testing.T,
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled bool,
	mongoSourceClient *mongo.Client,
	mysqlSourceDB, mariaDBSourceDB, pgSourceDB *sql.DB,
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping []config.DatabaseMapping,
	initialInsertCount int,
) {
	if mongoEnabled && mongoSourceClient != nil {
		prepareInitialData(t, mongoSourceClient, mongoMapping, "initial_mongo_doc", initialInsertCount, "mongodb")
		t.Logf("Inserted %d initial documents into MongoDB source.", initialInsertCount)
	}
	if mysqlEnabled && mysqlSourceDB != nil {
		prepareInitialData(t, mysqlSourceDB, mysqlMapping, "initial_mysql_doc", initialInsertCount, "mysql")
		t.Logf("Inserted %d initial rows into MySQL source.", initialInsertCount)
	}
	if mariaDBEnabled && mariaDBSourceDB != nil {
		prepareInitialData(t, mariaDBSourceDB, mariadbMapping, "initial_mariadb_doc", initialInsertCount, "mariadb")
		t.Logf("Inserted %d initial rows into MariaDB source.", initialInsertCount)
	}
	if postgresEnabled && pgSourceDB != nil {
		prepareInitialData(t, pgSourceDB, pgMapping, "initial_postgres_doc", initialInsertCount, "postgresql")
		t.Logf("Inserted %d initial rows into PostgreSQL source.", initialInsertCount)
	}
}

// 6. Verify initial data synchronization
func verifyInitialDataConsistency(
	t *testing.T,
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled bool,
	mongoSourceClient, mongoTargetClient *mongo.Client,
	mysqlSourceDB, mysqlTargetDB, mariaDBSourceDB, mariaDBTargetDB, pgSourceDB, pgTargetDB *sql.DB,
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping []config.DatabaseMapping,
) {
	if mongoEnabled && mongoSourceClient != nil && mongoTargetClient != nil {
		verifyDataConsistency(t, mongoSourceClient, mongoTargetClient, mongoMapping, "initial_mongo_sync")
		t.Log("Verified MongoDB initial sync data consistency.")
	}
	if mysqlEnabled && mysqlSourceDB != nil && mysqlTargetDB != nil {
		verifyDataConsistency(t, mysqlSourceDB, mysqlTargetDB, mysqlMapping, "initial_mysql_sync")
		t.Log("Verified MySQL initial sync data consistency.")
	}
	if mariaDBEnabled && mariaDBSourceDB != nil && mariaDBTargetDB != nil {
		verifyDataConsistency(t, mariaDBSourceDB, mariaDBTargetDB, mariadbMapping, "initial_mariadb_sync")
		t.Log("Verified MariaDB initial sync data consistency.")
	}
	if postgresEnabled && pgSourceDB != nil && pgTargetDB != nil {
		verifyDataConsistency(t, pgSourceDB, pgTargetDB, pgMapping, "initial_postgres_sync")
		t.Log("Verified PostgreSQL initial sync data consistency.")
	}
}

// 7. Create/Update/Delete operation tests
func performCRUDOperations(
	t *testing.T,
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled bool,
	mongoSourceClient, mongoTargetClient *mongo.Client,
	mysqlSourceDB, mysqlTargetDB, mariaDBSourceDB, mariaDBTargetDB, pgSourceDB, pgTargetDB *sql.DB,
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping []config.DatabaseMapping,
) {
	if mongoEnabled && mongoSourceClient != nil && mongoTargetClient != nil {
		performDataOperations(t, mongoSourceClient, mongoTargetClient, mongoMapping, "mongodb")
		t.Log("MongoDB increment/update/delete operations tested successfully.")
	}
	if mysqlEnabled && mysqlSourceDB != nil && mysqlTargetDB != nil {
		performDataOperations(t, mysqlSourceDB, mysqlTargetDB, mysqlMapping, "mysql")
		t.Log("MySQL increment/update/delete operations tested successfully.")
	}
	if mariaDBEnabled && mariaDBSourceDB != nil && mariaDBTargetDB != nil {
		performDataOperations(t, mariaDBSourceDB, mariaDBTargetDB, mariadbMapping, "mariadb")
		t.Log("MariaDB increment/update/delete operations tested successfully.")
	}
	if postgresEnabled && pgSourceDB != nil && pgTargetDB != nil {
		performDataOperations(t, pgSourceDB, pgTargetDB, pgMapping, "postgresql")
		t.Log("PostgreSQL increment/update/delete operations tested successfully.")
	}
}

// Connect to MongoDB
func connectMongoDB(cfg *config.Config) (*mongo.Client, *mongo.Client, error) {
	var mongoSourceURI, mongoTargetURI string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "mongodb" && sc.Enable {
			mongoSourceURI = sc.SourceConnection
			mongoTargetURI = sc.TargetConnection
			break
		}
	}
	if mongoSourceURI == "" || mongoTargetURI == "" {
		return nil, nil, fmt.Errorf("no enabled MongoDB sync config found in config.yaml")
	}

	sourceClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoSourceURI))
	if err != nil {
		return nil, nil, err
	}
	if err := sourceClient.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, nil, err
	}

	targetClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoTargetURI))
	if err != nil {
		return nil, nil, err
	}
	if err := targetClient.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, nil, err
	}

	return sourceClient, targetClient, nil
}

func connectSQLDB(cfg *config.Config, dbType string) (*sql.DB, *sql.DB, error) {
	var sourceDSN, targetDSN string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == dbType && sc.Enable {
			sourceDSN = sc.SourceConnection
			targetDSN = sc.TargetConnection
			break
		}
	}
	if sourceDSN == "" || targetDSN == "" {
		return nil, nil, fmt.Errorf("no enabled %s sync config found in config.yaml", dbType)
	}

	srcDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := srcDB.Ping(); err != nil {
		return nil, nil, err
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := tgtDB.Ping(); err != nil {
		return nil, nil, err
	}

	return srcDB, tgtDB, nil
}

func connectPGDB(cfg *config.Config) (*sql.DB, *sql.DB, error) {
	var pgSourceDSN, pgTargetDSN string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "postgresql" && sc.Enable {
			pgSourceDSN = sc.SourceConnection
			pgTargetDSN = sc.TargetConnection
			break
		}
	}
	if pgSourceDSN == "" || pgTargetDSN == "" {
		return nil, nil, fmt.Errorf("no enabled postgresql sync config found in config.yaml")
	}

	srcDB, err := sql.Open("postgres", pgSourceDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := srcDB.Ping(); err != nil {
		return nil, nil, err
	}

	tgtDB, err := sql.Open("postgres", pgTargetDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := tgtDB.Ping(); err != nil {
		return nil, nil, err
	}

	return srcDB, tgtDB, nil
}

// Insert initial data
func prepareInitialData(t *testing.T, src interface{}, mappings []config.DatabaseMapping, docName string, count int, dbType string) {
	switch s := src.(type) {
	case *mongo.Client:
		// MongoDB insertion logic
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				srcColl := s.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)
				var docs []interface{}
				for i := 0; i < count; i++ {
					docs = append(docs, bson.M{
						"name":    fmt.Sprintf("%s_%s", docName, uuid.New().String()),
						"content": fmt.Sprintf("RandomContent_%d_%s", i, uuid.New().String()),
					})
				}
				_, err := srcColl.InsertMany(context.Background(), docs)
				if err != nil {
					t.Fatalf("Failed to insert initial docs into MongoDB %s.%s: %v",
						dbmap.SourceDatabase, tblmap.SourceTable, err)
				}
			}
		}

	case *sql.DB:
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				// Build the source table name (like public.a1 or sync.a1)
				fullTableName := getQualifiedTableName(dbmap, true, tblmap)

				// If it is PostgreSQL, we can optionally TRUNCATE before insert to avoid duplicate keys
				// if isPostgresDBType(dbType) {
				//     truncSQL := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", fullTableName)
				//     if _, err := s.Exec(truncSQL); err != nil {
				//         t.Fatalf("Failed to TRUNCATE table %s: %v", fullTableName, err)
				//     }
				// }

				// Choose placeholders based on dbType
				var insertSQL string
				if isPostgresDBType(dbType) {
					// PostgreSQL uses $1, $2, $3
					insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, content) VALUES ($1, $2, $3)", fullTableName)
				} else {
					// MySQL / MariaDB still uses ?
					insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, content) VALUES (?, ?, ?)", fullTableName)
				}

				for i := 0; i < count; i++ {
					nextID := getNextSQLID(t, s, fullTableName)
					name := fmt.Sprintf("%s_%s", docName, uuid.New().String())
					content := fmt.Sprintf("RandomContent_%d_%s", i, uuid.New().String())

					// Execute insert
					if _, err := s.Exec(insertSQL, nextID, name, content); err != nil {
						t.Fatalf("Failed to insert row into %s: %v", fullTableName, err)
					}
				}
			}
		}
	}
}

// Verify data consistency
func verifyDataConsistency(t *testing.T, src interface{}, tgt interface{}, mappings []config.DatabaseMapping, stage string) {
	time.Sleep(10 * time.Second)
	switch s := src.(type) {
	case *mongo.Client:
		// Additional wait for Mongo sync
		time.Sleep(30 * time.Second)
		tc := tgt.(*mongo.Client)
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				srcColl := s.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)
				tgtColl := tc.Database(dbmap.TargetDatabase).Collection(tblmap.TargetTable)

				// Fetch source data
				srcCursor, err := srcColl.Find(context.Background(), bson.M{})
				if err != nil {
					t.Fatalf("Failed to fetch documents from MongoDB source %s.%s: %v",
						dbmap.SourceDatabase, tblmap.SourceTable, err)
				}
				defer srcCursor.Close(context.Background())
				var srcDocs []bson.M
				if err := srcCursor.All(context.Background(), &srcDocs); err != nil {
					t.Fatalf("Failed to decode documents from MongoDB source %s.%s: %v",
						dbmap.SourceDatabase, tblmap.SourceTable, err)
				}

				// Fetch target data
				tgtCursor, err := tgtColl.Find(context.Background(), bson.M{})
				if err != nil {
					t.Fatalf("Failed to fetch documents from MongoDB target %s.%s: %v",
						dbmap.TargetDatabase, tblmap.TargetTable, err)
				}
				defer tgtCursor.Close(context.Background())
				var tgtDocs []bson.M
				if err := tgtCursor.All(context.Background(), &tgtDocs); err != nil {
					t.Fatalf("Failed to decode documents from MongoDB target %s.%s: %v",
						dbmap.TargetDatabase, tblmap.TargetTable, err)
				}

				// Compare counts
				if len(srcDocs) != len(tgtDocs) {
					t.Fatalf("MongoDB data mismatch at %s stage for %s.%s -> %s.%s: sourceCount=%d, targetCount=%d",
						stage, dbmap.SourceDatabase, tblmap.SourceTable,
						dbmap.TargetDatabase, tblmap.TargetTable, len(srcDocs), len(tgtDocs))
				}

				// Compare contents
				srcMap := make(map[string]bson.M)
				for _, doc := range srcDocs {
					id, ok := doc["_id"].(primitive.ObjectID)
					if !ok {
						t.Fatalf("MongoDB document missing _id or invalid type in %s.%s",
							dbmap.SourceDatabase, tblmap.SourceTable)
					}
					srcMap[id.Hex()] = doc
				}

				for _, doc := range tgtDocs {
					id, ok := doc["_id"].(primitive.ObjectID)
					if !ok {
						t.Fatalf("MongoDB target document missing _id or invalid type in %s.%s",
							dbmap.TargetDatabase, tblmap.TargetTable)
					}
					srcDoc, exists := srcMap[id.Hex()]
					if !exists {
						t.Fatalf("MongoDB target has extra document with _id=%s in %s.%s",
							id.Hex(), dbmap.TargetDatabase, tblmap.TargetTable)
					}
					// Compare specific fields
					for key, value := range srcDoc {
						if tgtVal, exists := doc[key]; !exists || tgtVal != value {
							t.Fatalf("MongoDB data mismatch for _id=%s in field '%s': source='%v', target='%v'",
								id.Hex(), key, value, tgtVal)
						}
					}
				}
			}
		}

	case *sql.DB:
		tc := tgt.(*sql.DB)
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				fullSrcTable := getQualifiedTableName(dbmap, true, tblmap)
				fullTgtTable := getQualifiedTableName(dbmap, false, tblmap)

				// Fetch source data
				srcQuery := fmt.Sprintf("SELECT id, name, content FROM %s ORDER BY id", fullSrcTable)
				srcRows, err := s.Query(srcQuery)
				if err != nil {
					t.Fatalf("Failed to fetch rows from source %s at %s stage: %v", fullSrcTable, stage, err)
				}
				defer srcRows.Close()
				var srcRowsData []map[string]interface{}
				for srcRows.Next() {
					var id int64
					var name, content string
					if err := srcRows.Scan(&id, &name, &content); err != nil {
						t.Fatalf("Failed to scan row from source %s: %v", fullSrcTable, err)
					}
					srcRowsData = append(srcRowsData, map[string]interface{}{
						"id":      id,
						"name":    name,
						"content": content,
					})
				}

				// Fetch target data
				tgtQuery := fmt.Sprintf("SELECT id, name, content FROM %s ORDER BY id", fullTgtTable)
				tgtRows, err := tc.Query(tgtQuery)
				if err != nil {
					t.Fatalf("Failed to fetch rows from target %s at %s stage: %v", fullTgtTable, stage, err)
				}
				defer tgtRows.Close()
				var tgtRowsData []map[string]interface{}
				for tgtRows.Next() {
					var id int64
					var name, content string
					if err := tgtRows.Scan(&id, &name, &content); err != nil {
						t.Fatalf("Failed to scan row from target %s: %v", fullTgtTable, err)
					}
					tgtRowsData = append(tgtRowsData, map[string]interface{}{
						"id":      id,
						"name":    name,
						"content": content,
					})
				}

				// Compare counts
				if len(srcRowsData) != len(tgtRowsData) {
					t.Fatalf("%s data mismatch at %s stage for %s -> %s: sourceCount=%d, targetCount=%d",
						dbmap.SourceDatabase, stage, fullSrcTable, fullTgtTable,
						len(srcRowsData), len(tgtRowsData))
				}

				// Compare contents
				srcMap := make(map[int64]map[string]interface{})
				for _, row := range srcRowsData {
					srcMap[row["id"].(int64)] = row
				}

				for _, row := range tgtRowsData {
					srcRow, exists := srcMap[row["id"].(int64)]
					if !exists {
						t.Fatalf("Target has extra row with id=%d in %s",
							row["id"].(int64), fullTgtTable)
					}
					// Compare specific fields
					for key, value := range srcRow {
						if tgtVal, exists := row[key]; !exists || tgtVal != value {
							t.Fatalf("Data mismatch for id=%d in field '%s': source='%v', target='%v'",
								row["id"].(int64), key, value, tgtVal)
						}
					}
				}
			}
		}
	}
}

// Perform create/update/delete operations during synchronization
func performDataOperations(t *testing.T, src interface{}, tgt interface{}, mappings []config.DatabaseMapping, dbType string) {
	switch dbType {
	case "mongodb":
		performMongoOperations(t, src.(*mongo.Client), tgt.(*mongo.Client), mappings)
	case "mysql", "mariadb", "postgresql":
		performSQLOperations(t, src.(*sql.DB), tgt.(*sql.DB), mappings, dbType)
	default:
		t.Fatalf("Unknown dbType: %s", dbType)
	}
}

func performMongoOperations(t *testing.T, sClient, tClient *mongo.Client, mappings []config.DatabaseMapping) {
	for _, dbmap := range mappings {
		for _, tblmap := range dbmap.Tables {
			srcColl := sClient.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)

			// Insert
			insertCount := 3
			var docs []interface{}
			for i := 0; i < insertCount; i++ {
				docs = append(docs, bson.M{
					"name":    "test_insert_" + uuid.New().String(),
					"content": "RandomContent_" + strconv.Itoa(rand.Intn(1000)),
				})
			}
			_, err := srcColl.InsertMany(context.Background(), docs)
			if err != nil {
				t.Fatalf("MongoDB insert failed: %v", err)
			}

			t.Log("MongoDB insert operation successful.")
			verifyDataConsistency(t, sClient, tClient, []config.DatabaseMapping{dbmap}, "mongo_insert")

			// Update
			updateFilter := bson.M{"name": bson.M{"$regex": "^test_insert_"}}
			update := bson.M{"$set": bson.M{"name": "test_updated_" + uuid.New().String()}}
			_, err = srcColl.UpdateMany(context.Background(), updateFilter, update)
			if err != nil {
				t.Fatalf("MongoDB update failed: %v", err)
			}
			t.Log("MongoDB update operation successful.")
			verifyDataConsistency(t, sClient, tClient, []config.DatabaseMapping{dbmap}, "mongo_update")

			// Delete
			deleteFilter := bson.M{"name": bson.M{"$regex": "^test_updated_"}}
			_, err = srcColl.DeleteMany(context.Background(), deleteFilter)
			if err != nil {
				t.Fatalf("MongoDB delete failed: %v", err)
			}
			t.Log("MongoDB delete operation successful.")
			verifyDataConsistency(t, sClient, tClient, []config.DatabaseMapping{dbmap}, "mongo_delete")
		}
	}
}

func performSQLOperations(t *testing.T, sDB, tDB *sql.DB, mappings []config.DatabaseMapping, dbType string) {
	for _, dbmap := range mappings {
		for _, tblmap := range dbmap.Tables {
			fullSrcTable := getQualifiedTableName(dbmap, true, tblmap)

			// Insert
			insertCount := 3
			var insertSQL string
			if isPostgresDBType(dbType) {
				insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, content) VALUES ($1, $2, $3)", fullSrcTable)
			} else {
				insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, content) VALUES (?, ?, ?)", fullSrcTable)
			}
			for i := 0; i < insertCount; i++ {
				insertID := getNextSQLID(t, sDB, fullSrcTable)
				name := "test_insert_" + uuid.New().String()
				content := "RandomContent_" + strconv.Itoa(rand.Intn(1000))

				if _, err := sDB.Exec(insertSQL, insertID, name, content); err != nil {
					t.Fatalf("%s insert failed: %v", dbType, err)
				}
			}
			t.Logf("%s insert operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_insert")

			// Update
			// PostgreSQL does not have CONCAT or UUID() by default, handle differently
			updateQuery := fmt.Sprintf("UPDATE %s SET name=CONCAT('test_updated_', UUID()) WHERE name LIKE 'test_insert_%%'", fullSrcTable)
			if isPostgresDBType(dbType) {
				// change to 'test_updated_' || substring(md5(random()::text),1,8)
				updateQuery = fmt.Sprintf("UPDATE %s SET name='test_updated_' || substring(md5(random()::text),1,8) WHERE name LIKE 'test_insert_%%'", fullSrcTable)
			}
			if _, err := sDB.Exec(updateQuery); err != nil {
				t.Fatalf("%s update failed: %v", dbType, err)
			}
			t.Logf("%s update operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_update")

			// Delete
			deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE name LIKE 'test_updated_%%'", fullSrcTable)
			if _, err := sDB.Exec(deleteQuery); err != nil {
				t.Fatalf("%s delete failed: %v", dbType, err)
			}
			t.Logf("%s delete operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_delete")
		}
	}
}
