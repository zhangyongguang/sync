package mongodb

import (
    "context"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "sync"
    "time"

    "github.com/sirupsen/logrus"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "sync/pkg/config"
)

type MongoDBSyncer struct {
    sourceClient   *mongo.Client
    targetClient   *mongo.Client
    syncConfig     config.SyncConfig
    logger         *logrus.Logger
    lastSyncedAt   time.Time
    resumeToken    bson.Raw // ADD: Store resume token after reading from file
}

func NewMongoDBSyncer(syncCfg config.SyncConfig, logger *logrus.Logger) *MongoDBSyncer {
    // Create MongoDB clients
    sourceClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(syncCfg.SourceConnection))
    if err != nil {
        logger.Fatalf("Failed to connect to MongoDB source: %v", err)
    }

    targetClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(syncCfg.TargetConnection))
    if err != nil {
        logger.Fatalf("Failed to connect to MongoDB target: %v", err)
    }

    // ADD: Load resume token if configured
    var resumeToken bson.Raw
    if syncCfg.MongoDBResumeTokenPath != "" {
        resumeToken = loadMongoDBResumeToken(syncCfg.MongoDBResumeTokenPath, logger)
    }

    return &MongoDBSyncer{
        sourceClient: sourceClient,
        targetClient: targetClient,
        syncConfig:   syncCfg,
        logger:       logger,
        lastSyncedAt: time.Now().Add(-10 * time.Minute),
        resumeToken:  resumeToken,
    }
}

// ADD: helper function to load resume token
func loadMongoDBResumeToken(path string, logger *logrus.Logger) bson.Raw {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        logger.Infof("No previous MongoDB resume token found at %s: %v", path, err)
        return nil
    }
    var token bson.Raw
    if err := json.Unmarshal(data, &token); err != nil {
        logger.Errorf("Failed to unmarshal MongoDB resume token: %v", err)
        return nil
    }
    logger.Infof("Loaded MongoDB resume token from %s", path)
    return token
}

// ADD: helper function to save resume token
func (s *MongoDBSyncer) saveMongoDBResumeToken(token bson.Raw) {
    if s.syncConfig.MongoDBResumeTokenPath == "" {
        return
    }
    data, err := json.Marshal(token)
    if err != nil {
        s.logger.Errorf("Failed to marshal MongoDB resume token: %v", err)
        return
    }
    if err := ioutil.WriteFile(s.syncConfig.MongoDBResumeTokenPath, data, 0644); err != nil {
        s.logger.Errorf("Failed to write MongoDB resume token to file %s: %v", s.syncConfig.MongoDBResumeTokenPath, err)
    } else {
        s.logger.Infof("Saved MongoDB resume token to %s", s.syncConfig.MongoDBResumeTokenPath)
    }
}

func (s *MongoDBSyncer) Start(ctx context.Context) {
    var wg sync.WaitGroup

    for _, mapping := range s.syncConfig.Mappings {
        wg.Add(1)
        go func(mapping config.DatabaseMapping) {
            defer wg.Done()
            s.syncDatabase(ctx, mapping)
        }(mapping)
    }

    wg.Wait()
    s.logger.Info("MongoDB synchronization completed.")
}
func (s *MongoDBSyncer) syncDatabase(ctx context.Context, mapping config.DatabaseMapping) {
    sourceDB := s.sourceClient.Database(mapping.SourceDatabase)
    targetDB := s.targetClient.Database(mapping.TargetDatabase)
    s.logger.Infof("Processing MongoDB database mapping: %+v", mapping)

    for _, tableMap := range mapping.Tables {
        sourceColl := sourceDB.Collection(tableMap.SourceTable)
        targetColl := targetDB.Collection(tableMap.TargetTable)
        s.logger.Infof("Processing collection mapping: %+v", tableMap)

        // Initial sync
        err := s.initialSync(ctx, sourceColl, targetColl, mapping.SourceDatabase, mapping.TargetDatabase)
        if err != nil {
            s.logger.Errorf("Initial sync failed: %v", err)
            continue
        }

        // Watch for changes
        go s.watchChangesForCollection(ctx, sourceColl, targetColl, mapping.SourceDatabase, mapping.TargetDatabase)
    }
}

// Initial synchronization of collections
func (s *MongoDBSyncer) initialSync(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) error {
    count, err := targetColl.EstimatedDocumentCount(ctx)
    if err != nil {
        return fmt.Errorf("Failed to check target collection %s.%s document count: %v", targetDB, targetColl.Name(), err)
    }

    if count > 0 {
        s.logger.Infof("Skipping initial sync for %s.%s to %s.%s as target collection already contains data",
            sourceDB, sourceColl.Name(), targetDB, targetColl.Name())
        return nil
    }

    s.logger.Infof("Starting initial sync for collection %s.%s to %s.%s", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())

    cursor, err := sourceColl.Find(ctx, bson.M{})
    if err != nil {
        return fmt.Errorf("Failed to query source collection %s.%s for initial sync: %v", sourceDB, sourceColl.Name(), err)
    }
    defer cursor.Close(ctx)

    batchSize := 200
    var batch []interface{}

    for cursor.Next(ctx) {
        var doc bson.M
        if err := cursor.Decode(&doc); err != nil {
            return fmt.Errorf("Failed to decode document during initial sync: %v", err)
        }
        batch = append(batch, doc)

        if len(batch) >= batchSize {
            _, err := targetColl.InsertMany(ctx, batch)
            if err != nil {
                return fmt.Errorf("Failed to insert documents into target collection %s.%s during initial sync: %v", targetDB, targetColl.Name(), err)
            }
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        _, err := targetColl.InsertMany(ctx, batch)
        if err != nil {
            return fmt.Errorf("Failed to insert remaining documents into target collection %s.%s during initial sync: %v", targetDB, targetColl.Name(), err)
        }
    }

    s.logger.Infof("Initial sync of collection %s.%s to %s.%s completed", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())
    return nil
}

// Watch for changes in the source collection
func (s *MongoDBSyncer) watchChangesForCollection(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) {
    // Set up Change Stream pipeline, filtering operation types
    pipeline := mongo.Pipeline{
        {{Key: "$match", Value: bson.D{
            {Key: "ns.db", Value: sourceDB},
            {Key: "ns.coll", Value: sourceColl.Name()},
            {Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
        }}},
    }
    opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
    // ADD: If we have a resume token loaded, set it
    if s.resumeToken != nil {
        opts.SetResumeAfter(s.resumeToken)
        s.logger.Infof("Resuming change stream for %s.%s from saved resume token", sourceDB, sourceColl.Name())
    }

    cs, err := sourceColl.Watch(ctx, pipeline, opts)
    if err != nil {
        s.logger.Errorf("Failed to watch Change Stream for collection %s.%s: %v", sourceDB, sourceColl.Name(), err)
        return
    }
    defer cs.Close(ctx)

    s.logger.Infof("Started watching changes in collection %s.%s", sourceDB, sourceColl.Name())

    // Introduce batch processing
    var buffer []mongo.WriteModel
    const batchSize = 200
    flushInterval := time.Second * 1
    timer := time.NewTimer(flushInterval)
    defer timer.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        default:
            if cs.Next(ctx) {
                var changeEvent bson.M
                if err := cs.Decode(&changeEvent); err != nil {
                    s.logger.Errorf("Failed to decode change event for %s.%s: %v", sourceDB, sourceColl.Name(), err)
                    continue
                }

                // ADD: Save new resume token
                token := cs.ResumeToken()
                if token != nil {
                    s.saveMongoDBResumeToken(token)
                }

                writeModel := s.prepareWriteModel(changeEvent)
                if writeModel != nil {
                    buffer = append(buffer, writeModel)
                }

                // Perform batch write when batch size is reached
                if len(buffer) >= batchSize {
                    s.flushBuffer(ctx, targetColl, &buffer, targetDB)
                    timer.Reset(flushInterval)
                }
            } else {
                // Check for errors
                if err := cs.Err(); err != nil {
                    s.logger.Errorf("Change Stream error for collection %s.%s: %v", sourceDB, sourceColl.Name(), err)
                    return
                }
            }

            select {
            case <-timer.C:
                if len(buffer) > 0 {
                    s.flushBuffer(ctx, targetColl, &buffer, targetDB)
                }
                timer.Reset(flushInterval)
            default:
                // Continue watching for changes
            }
        }
    }
}

func (s *MongoDBSyncer) prepareWriteModel(changeEvent bson.M) mongo.WriteModel {
    operationType, _ := changeEvent["operationType"].(string)
    fullDocument, _ := changeEvent["fullDocument"].(bson.M)
    documentKey, _ := changeEvent["documentKey"].(bson.M)
    filter := documentKey

    switch operationType {
    case "insert":
        if fullDocument != nil {
            return mongo.NewInsertOneModel().SetDocument(fullDocument)
        }
    case "update", "replace":
        if fullDocument != nil {
            return mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(fullDocument).SetUpsert(true)
        }
    case "delete":
        return mongo.NewDeleteOneModel().SetFilter(filter)
    default:
        s.logger.Warnf("Unhandled operation type: %s", operationType)
    }
    return nil
}

func (s *MongoDBSyncer) flushBuffer(ctx context.Context, targetColl *mongo.Collection, buffer *[]mongo.WriteModel, targetDB string) {
    if len(*buffer) == 0 {
        return
    }

    opts := options.BulkWrite().SetOrdered(false)
    result, err := targetColl.BulkWrite(ctx, *buffer, opts)
    if err != nil {
        s.logger.Errorf("Bulk write failed for collection %s.%s: %v", targetDB, targetColl.Name(), err)
    } else {
        s.logger.Infof("Bulk write result for collection %s.%s - Inserted: %d, Matched: %d, Modified: %d, Upserted: %d, Deleted: %d",
            targetDB, targetColl.Name(), 
            result.InsertedCount, 
            result.MatchedCount, 
            result.ModifiedCount, 
            result.UpsertedCount, 
            result.DeletedCount)
    }

    // Clear buffer
    *buffer = (*buffer)[:0]
}