package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer/mariadb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mongodb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mysql"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	_ "github.com/go-sql-driver/mysql"
)

// Global counter for generating unique primary key IDs (MongoDB does not depend on this ID)
var testIDCounter int64 = 0

// This function may only be used in MongoDB (if needed)
// For MySQL, we no longer use this global counter, but instead get the next ID by querying the database
func getUniqueID() int64 {
	return atomic.AddInt64(&testIDCounter, 1)
}

// Get the next available auto-increment ID value from a SQL table: max(id) + 1
func getNextSQLID(t *testing.T, db *sql.DB, database, table string) int64 {
	var maxID sql.NullInt64
	query := fmt.Sprintf("SELECT IFNULL(MAX(id), 0) FROM %s.%s", database, table)
	err := db.QueryRow(query).Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to get max ID from %s.%s: %v", database, table, err)
	}
	return maxID.Int64 + 1
}

// TestFullSync is a comprehensive integration test example. This test will:
// 1. Read data source and mapping configurations from config.yaml
// 2. Start synchronization logic
// 3. Check initial data synchronization
// 4. Simulate create/update/delete operations and check synchronization results
func TestFullSync(t *testing.T) {
	// Set the CONFIG_PATH environment variable to point to the config file in the project root directory
	projectRoot := "../../" // from pkg/syncer to the project root directory
	configPath := filepath.Join(projectRoot, "configs/config.yaml")
	os.Setenv("CONFIG_PATH", configPath)
	defer os.Unsetenv("CONFIG_PATH")

	cfg := config.NewConfig()
	log := logger.InitLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Check if MongoDB, MySQL, and MariaDB are enabled
	mongoEnabled := false
	mysqlEnabled := false
	mariaDBEnabled := false
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "mongodb" && sc.Enable {
			mongoEnabled = true
		}
		if sc.Type == "mysql" && sc.Enable {
			mysqlEnabled = true
		}
		if sc.Type == "mariadb" && sc.Enable {
			mariaDBEnabled = true
		}
	}

	if !mongoEnabled && !mysqlEnabled && !mariaDBEnabled {
		t.Skip("No enabled MongoDB, MySQL or MariaDB sync config found in config.yaml, skipping test.")
	}

	t.Log("Starting FullSync test...")

	// Connect to databases
	var (
		mongoSourceClient *mongo.Client
		mongoTargetClient *mongo.Client
		mysqlSourceDB     *sql.DB
		mysqlTargetDB     *sql.DB
		mariaDBSourceDB   *sql.DB
		mariaDBTargetDB   *sql.DB
		err               error
	)

	if mongoEnabled {
		mongoSourceClient, mongoTargetClient, err = connectMongoDB(cfg)
		if err != nil {
			t.Fatalf("Failed to connect MongoDB: %v", err)
		}
		defer mongoSourceClient.Disconnect(context.Background())
		defer mongoTargetClient.Disconnect(context.Background())
		t.Log("MongoDB source/target connected successfully.")
	}

	if mysqlEnabled {
		mysqlSourceDB, mysqlTargetDB, err = connectSQLDB(cfg, "mysql")
		if err != nil {
			t.Fatalf("Failed to connect MySQL: %v", err)
		}
		defer mysqlSourceDB.Close()
		defer mysqlTargetDB.Close()
		t.Log("MySQL source/target connected successfully.")
	}

	if mariaDBEnabled {
		mariaDBSourceDB, mariaDBTargetDB, err = connectSQLDB(cfg, "mariadb")
		if err != nil {
			t.Fatalf("Failed to connect MariaDB: %v", err)
		}
		defer mariaDBSourceDB.Close()
		defer mariaDBTargetDB.Close()
		t.Log("MariaDB source/target connected successfully.")
	}

	mongoMappings, mysqlMappings, mariadbMappings := extractAllMappings(cfg)

	// Clean up target data
	// if mongoEnabled && mongoTargetClient != nil {
	//     cleanupMongoSourceAndTargetData(t, mongoSourceClient, mongoTargetClient, mongoMappings)
	//     t.Log("Cleaned up MongoDB target data.")
	// }
	// if mysqlEnabled && mysqlTargetDB != nil {
	//     cleanupMySQLSourceAndTargetData(t, mysqlSourceDB, mysqlTargetDB, mysqlMappings)
	//     t.Log("Cleaned up MySQL target data.")
	// }

	// Start syncers
	startAllSyncers(ctx, cfg, log)
	t.Log("Syncers started, waiting initial sync...")
	time.Sleep(5 * time.Second)

	// Insert initial data
	initialInsertCount := 3
	if mongoEnabled && mongoSourceClient != nil {
		prepareInitialData(t, mongoSourceClient, mongoMappings, "initial_mongo_doc", initialInsertCount)
		t.Logf("Inserted %d initial documents into MongoDB source.", initialInsertCount)
	}
	if mysqlEnabled && mysqlSourceDB != nil {
		prepareInitialData(t, mysqlSourceDB, mysqlMappings, "initial_mysql_doc", initialInsertCount)
		t.Logf("Inserted %d initial rows into MySQL source.", initialInsertCount)
	}
	if mariaDBEnabled && mariaDBSourceDB != nil {
		prepareInitialData(t, mariaDBSourceDB, mariadbMappings, "initial_mariadb_doc", initialInsertCount)
		t.Logf("Inserted %d initial rows into MariaDB source.", initialInsertCount)
	}

	// Verify initial data synchronization
	if mongoEnabled && mongoSourceClient != nil && mongoTargetClient != nil {
		verifyDataConsistency(t, mongoSourceClient, mongoTargetClient, mongoMappings, "initial_mongo_sync")
		t.Log("Verified MongoDB initial sync data consistency.")
	}

	if mysqlEnabled && mysqlSourceDB != nil && mysqlTargetDB != nil {
		verifyDataConsistency(t, mysqlSourceDB, mysqlTargetDB, mysqlMappings, "initial_mysql_sync")
		t.Log("Verified MySQL initial sync data consistency.")
	}

	if mariaDBEnabled && mariaDBSourceDB != nil && mariaDBTargetDB != nil {
		verifyDataConsistency(t, mariaDBSourceDB, mariaDBTargetDB, mariadbMappings, "initial_mariadb_sync")
		t.Log("Verified MariaDB initial sync data consistency.")
	}

	// Perform create/update/delete operations
	if mongoEnabled && mongoSourceClient != nil && mongoTargetClient != nil {
		performDataOperations(t, mongoSourceClient, mongoTargetClient, mongoMappings, "mongodb")
		t.Log("MongoDB increment/update/delete operations tested successfully.")
	}
	if mysqlEnabled && mysqlSourceDB != nil && mysqlTargetDB != nil {
		performDataOperations(t, mysqlSourceDB, mysqlTargetDB, mysqlMappings, "mysql")
		t.Log("MySQL increment/update/delete operations tested successfully.")
	}
	if mariaDBEnabled && mariaDBSourceDB != nil && mariaDBTargetDB != nil {
		performDataOperations(t, mariaDBSourceDB, mariaDBTargetDB, mariadbMappings, "mariadb")
		t.Log("MariaDB increment/update/delete operations tested successfully.")
	}

	time.Sleep(5 * time.Second)
	t.Log("Full synchronization test completed successfully.")
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

// Clean up MongoDB source and target data
func cleanupMongoSourceAndTargetData(t *testing.T, mongoSourceClient, mongoTargetClient *mongo.Client, mongoMapping []config.DatabaseMapping) {
	for _, m := range mongoMapping {
		// Clean up target database data
		for _, tbl := range m.Tables {
			targetColl := mongoTargetClient.Database(m.TargetDatabase).Collection(tbl.TargetTable)
			if _, err := targetColl.DeleteMany(context.Background(), bson.M{}); err != nil {
				t.Fatalf("Failed to cleanup Mongo target collection %s.%s: %v", m.TargetDatabase, tbl.TargetTable, err)
			}
		}

		// Clean up source database data
		for _, tbl := range m.Tables {
			sourceColl := mongoSourceClient.Database(m.SourceDatabase).Collection(tbl.SourceTable)
			if _, err := sourceColl.DeleteMany(context.Background(), bson.M{}); err != nil {
				t.Fatalf("Failed to cleanup Mongo source collection %s.%s: %v", m.SourceDatabase, tbl.SourceTable, err)
			}
		}
	}
}

// Clean up MySQL source and target data
func cleanupMySQLSourceAndTargetData(t *testing.T, mysqlSourceDB, mysqlTargetDB *sql.DB, mysqlMapping []config.DatabaseMapping) {

	for _, m := range mysqlMapping {
		for _, tbl := range m.Tables {
			sourceQuery := fmt.Sprintf("DELETE FROM %s.%s", m.SourceDatabase, tbl.SourceTable)
			t.Log("MySQL source query: " + sourceQuery + ".")
			if _, err := mysqlSourceDB.Exec(sourceQuery); err != nil {
				t.Fatalf("Failed to cleanup MySQL source table %s.%s: %v", m.SourceDatabase, tbl.SourceTable, err)
			}

			targetQuery := fmt.Sprintf("DELETE FROM %s.%s", m.TargetDatabase, tbl.TargetTable)
			t.Log("MySQL target query: " + targetQuery + ".")
			if _, err := mysqlTargetDB.Exec(targetQuery); err != nil {
				t.Fatalf("Failed to cleanup MySQL target table %s.%s: %v", m.TargetDatabase, tbl.TargetTable, err)
			}
		}
	}
}

// Extract mappings
func extractAllMappings(cfg *config.Config) ([]config.DatabaseMapping, []config.DatabaseMapping, []config.DatabaseMapping) {
	var mongoMapping, mysqlMapping, mariadbMapping []config.DatabaseMapping
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
	}
	return mongoMapping, mysqlMapping, mariadbMapping
}

// Start all syncers
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
		}
	}
}

// Insert initial data
func prepareInitialData(t *testing.T, src interface{}, mappings []config.DatabaseMapping, docName string, count int) {
	switch s := src.(type) {
	case *mongo.Client:
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
				for i := 0; i < count; i++ {
					nextID := getNextSQLID(t, s, dbmap.SourceDatabase, tblmap.SourceTable)
					query := fmt.Sprintf("INSERT INTO %s.%s (id, name, content) VALUES (?, ?, ?)", dbmap.SourceDatabase, tblmap.SourceTable)
					name := fmt.Sprintf("%s_%s", docName, uuid.New().String())
					content := fmt.Sprintf("RandomContent_%d_%s", i, uuid.New().String())
					_, err := s.Exec(query, nextID, name, content)
					if err != nil {
						t.Fatalf("Failed to insert initial row into %s %s.%s: %v",
							dbmap.SourceDatabase, dbmap.SourceDatabase, tblmap.SourceTable, err)
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
		time.Sleep(30 * time.Second)
		tc := tgt.(*mongo.Client)
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				srcColl := s.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)
				tgtColl := tc.Database(dbmap.TargetDatabase).Collection(tblmap.TargetTable)

				// Fetch source data
				srcCursor, err := srcColl.Find(context.Background(), bson.M{})
				if err != nil {
					t.Fatalf("Failed to fetch documents from MongoDB source %s.%s: %v", dbmap.SourceDatabase, tblmap.SourceTable, err)
				}
				defer srcCursor.Close(context.Background())
				var srcDocs []bson.M
				if err := srcCursor.All(context.Background(), &srcDocs); err != nil {
					t.Fatalf("Failed to decode documents from MongoDB source %s.%s: %v", dbmap.SourceDatabase, tblmap.SourceTable, err)
				}

				// Fetch target data
				tgtCursor, err := tgtColl.Find(context.Background(), bson.M{})
				if err != nil {
					t.Fatalf("Failed to fetch documents from MongoDB target %s.%s: %v", dbmap.TargetDatabase, tblmap.TargetTable, err)
				}
				defer tgtCursor.Close(context.Background())
				var tgtDocs []bson.M
				if err := tgtCursor.All(context.Background(), &tgtDocs); err != nil {
					t.Fatalf("Failed to decode documents from MongoDB target %s.%s: %v", dbmap.TargetDatabase, tblmap.TargetTable, err)
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
						t.Fatalf("MongoDB document missing _id or invalid type in %s.%s", dbmap.SourceDatabase, tblmap.SourceTable)
					}
					srcMap[id.Hex()] = doc
				}

				for _, doc := range tgtDocs {
					id, ok := doc["_id"].(primitive.ObjectID)
					if !ok {
						t.Fatalf("MongoDB target document missing _id or invalid type in %s.%s", dbmap.TargetDatabase, tblmap.TargetTable)
					}
					srcDoc, exists := srcMap[id.Hex()]
					if !exists {
						t.Fatalf("MongoDB target has extra document with _id=%s in %s.%s", id.Hex(), dbmap.TargetDatabase, tblmap.TargetTable)
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
				// Fetch source data
				srcQuery := fmt.Sprintf("SELECT id, name, content FROM %s.%s ORDER BY id", dbmap.SourceDatabase, tblmap.SourceTable)
				srcRows, err := s.Query(srcQuery)
				if err != nil {
					t.Fatalf("Failed to fetch rows from source %s.%s: %v", dbmap.SourceDatabase, tblmap.SourceTable, err)
				}
				defer srcRows.Close()
				var srcRowsData []map[string]interface{}
				for srcRows.Next() {
					var id int64
					var name, content string
					if err := srcRows.Scan(&id, &name, &content); err != nil {
						t.Fatalf("Failed to scan row from source %s.%s: %v", dbmap.SourceDatabase, tblmap.SourceTable, err)
					}
					srcRowsData = append(srcRowsData, map[string]interface{}{
						"id":      id,
						"name":    name,
						"content": content,
					})
				}

				// Fetch target data
				tgtQuery := fmt.Sprintf("SELECT id, name, content FROM %s.%s ORDER BY id", dbmap.TargetDatabase, tblmap.TargetTable)
				tgtRows, err := tc.Query(tgtQuery)
				if err != nil {
					t.Fatalf("Failed to fetch rows from target %s.%s: %v", dbmap.TargetDatabase, tblmap.TargetTable, err)
				}
				defer tgtRows.Close()
				var tgtRowsData []map[string]interface{}
				for tgtRows.Next() {
					var id int64
					var name, content string
					if err := tgtRows.Scan(&id, &name, &content); err != nil {
						t.Fatalf("Failed to scan row from target %s.%s: %v", dbmap.TargetDatabase, tblmap.TargetTable, err)
					}
					tgtRowsData = append(tgtRowsData, map[string]interface{}{
						"id":      id,
						"name":    name,
						"content": content,
					})
				}

				// Compare counts
				if len(srcRowsData) != len(tgtRowsData) {
					t.Fatalf("%s data mismatch at %s stage for %s.%s -> %s.%s: sourceCount=%d, targetCount=%d",
						dbmap.SourceDatabase, stage, dbmap.SourceDatabase, tblmap.SourceTable,
						dbmap.TargetDatabase, tblmap.TargetTable, len(srcRowsData), len(tgtRowsData))
				}

				// Compare contents
				srcMap := make(map[int64]map[string]interface{})
				for _, row := range srcRowsData {
					srcMap[row["id"].(int64)] = row
				}

				for _, row := range tgtRowsData {
					srcRow, exists := srcMap[row["id"].(int64)]
					if !exists {
						t.Fatalf("Target has extra row with id=%d in %s.%s", row["id"].(int64), dbmap.TargetDatabase, tblmap.TargetTable)
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
	case "mysql", "mariadb":
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
			// Insert
			insertCount := 3
			for i := 0; i < insertCount; i++ {
				insertID := getNextSQLID(t, sDB, dbmap.SourceDatabase, tblmap.SourceTable)
				insertQuery := fmt.Sprintf("INSERT INTO %s.%s (id, name, content) VALUES (?, ?, ?)", dbmap.SourceDatabase, tblmap.SourceTable)
				name := "test_insert_" + uuid.New().String()
				content := "RandomContent_" + strconv.Itoa(rand.Intn(1000))
				if _, err := sDB.Exec(insertQuery, insertID, name, content); err != nil {
					t.Fatalf("%s insert failed: %v", dbType, err)
				}
			}
			t.Logf("%s insert operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_insert")

			// Update
			updateQuery := fmt.Sprintf("UPDATE %s.%s SET name=CONCAT('test_updated_', UUID()) WHERE name LIKE 'test_insert_%%'", dbmap.SourceDatabase, tblmap.SourceTable)
			if _, err := sDB.Exec(updateQuery); err != nil {
				t.Fatalf("%s update failed: %v", dbType, err)
			}
			t.Logf("%s update operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_update")

			// Delete
			deleteQuery := fmt.Sprintf("DELETE FROM %s.%s WHERE name LIKE 'test_updated_%%'", dbmap.SourceDatabase, tblmap.SourceTable)
			if _, err := sDB.Exec(deleteQuery); err != nil {
				t.Fatalf("%s delete failed: %v", dbType, err)
			}
			t.Logf("%s delete operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_delete")
		}
	}
}
