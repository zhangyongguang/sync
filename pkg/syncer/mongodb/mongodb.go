package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBSyncer struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	syncConfig    config.SyncConfig
	logger        *logrus.Logger
	lastSyncedAt  time.Time
	resumeTokens  map[string]bson.Raw // Key: collection name
	resumeTokensM sync.RWMutex        // Mutex for resumeTokens map
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

	// Initialize resumeTokens map
	resumeTokens := make(map[string]bson.Raw)
	if syncCfg.MongoDBResumeTokenPath != "" {
		// Assume MongoDBResumeTokenPath is a directory
		err := os.MkdirAll(syncCfg.MongoDBResumeTokenPath, os.ModePerm)
		if err != nil {
			logger.Fatalf("Failed to create resume token directory %s: %v", syncCfg.MongoDBResumeTokenPath, err)
		}
	}

	return &MongoDBSyncer{
		sourceClient: sourceClient,
		targetClient: targetClient,
		syncConfig:   syncCfg,
		logger:       logger,
		lastSyncedAt: time.Now().Add(-10 * time.Minute),
		resumeTokens: resumeTokens,
	}
}

// getResumeTokenPath constructs the resume token file path for a given collection
func (s *MongoDBSyncer) getResumeTokenPath(collection string) string {
	// If MongoDBResumeTokenPath is a directory, store tokens as separate files
	// Otherwise, append the collection name to the file path
	info, err := os.Stat(s.syncConfig.MongoDBResumeTokenPath)
	if err != nil || !info.IsDir() {
		// Not a directory, append collection name to the file
		return fmt.Sprintf("%s_%s.json", s.syncConfig.MongoDBResumeTokenPath, collection)
	}
	// It's a directory, use separate files
	return filepath.Join(s.syncConfig.MongoDBResumeTokenPath, fmt.Sprintf("%s_resume_token.json", collection))
}

// loadMongoDBResumeToken loads the resume token for a specific collection
func (s *MongoDBSyncer) loadMongoDBResumeToken(collection string) bson.Raw {
	path := s.getResumeTokenPath(collection)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		s.logger.Infof("No previous MongoDB resume token found for collection %s at %s: %v", collection, path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Infof("MongoDB resume token file for collection %s is empty: %s", collection, path)
		return nil
	}
	var token bson.Raw
	if err := json.Unmarshal(data, &token); err != nil {
		s.logger.Errorf("Failed to unmarshal MongoDB resume token for collection %s: %v", collection, err)
		return nil
	}
	s.logger.Infof("Loaded MongoDB resume token for collection %s from %s", collection, path)
	return token
}

// saveMongoDBResumeToken saves the resume token for a specific collection
func (s *MongoDBSyncer) saveMongoDBResumeToken(collection string, token bson.Raw) {
	if s.syncConfig.MongoDBResumeTokenPath == "" {
		return
	}
	path := s.getResumeTokenPath(collection)
	data, err := json.Marshal(token)
	if err != nil {
		s.logger.Errorf("Failed to marshal MongoDB resume token for collection %s: %v", collection, err)
		return
	}
	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		s.logger.Errorf("Failed to write MongoDB resume token for collection %s to file %s: %v", collection, path, err)
	} else {
		s.logger.Infof("Saved MongoDB resume token for collection %s to %s", collection, path)
	}
}

// removeMongoDBResumeToken removes the resume token file for a specific collection
func (s *MongoDBSyncer) removeMongoDBResumeToken(collection string) {
	if s.syncConfig.MongoDBResumeTokenPath == "" {
		return
	}
	path := s.getResumeTokenPath(collection)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		s.logger.Errorf("Failed to remove invalid MongoDB resume token file %s for collection %s: %v", path, collection, err)
	} else {
		s.logger.Infof("Removed invalid MongoDB resume token file %s for collection %s", path, collection)
	}
	s.resumeTokensM.Lock()
	delete(s.resumeTokens, collection)
	s.resumeTokensM.Unlock()
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

		err := s.initialSync(ctx, sourceColl, targetColl, mapping.SourceDatabase, mapping.TargetDatabase)
		if err != nil {
			s.logger.Errorf("Initial sync failed for collection %s.%s: %v", mapping.SourceDatabase, tableMap.SourceTable, err)
			continue
		}

		go s.watchChangesForCollection(ctx, sourceColl, targetColl, mapping.SourceDatabase, mapping.TargetDatabase)
	}
}

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

	batchSize := 100
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

func (s *MongoDBSyncer) watchChangesForCollection(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) {
	collectionName := sourceColl.Name()
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "ns.db", Value: sourceDB},
			{Key: "ns.coll", Value: collectionName},
			{Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
		}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// Load resume token for this collection
	resumeToken := s.loadMongoDBResumeToken(collectionName)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
		s.logger.Infof("Resuming change stream for collection %s from saved resume token", collectionName)
	}

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil && resumeToken != nil {
		// If resume token is invalid, remove it and retry without it
		s.logger.Errorf("Failed to resume change stream for collection %s with token: %v, retrying without resume token", collectionName, err)
		s.removeMongoDBResumeToken(collectionName)
		opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		cs, err = sourceColl.Watch(ctx, pipeline, opts)
	}

	if err != nil {
		s.logger.Errorf("Failed to watch Change Stream for collection %s.%s: %v", sourceDB, collectionName, err)
		return
	}
	defer cs.Close(ctx)

	s.logger.Infof("Started watching changes in collection %s.%s", sourceDB, collectionName)

	var buffer []mongo.WriteModel
	const batchSize = 200
	flushInterval := time.Second * 1
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	var bufferMutex sync.Mutex

	// Goroutine to handle periodic flushing
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				bufferMutex.Lock()
				if len(buffer) > 0 {
					s.logger.Infof("Timer triggered, flushing buffer for %s.%s, buffer size: %d", sourceDB, collectionName, len(buffer))
					s.flushBuffer(ctx, targetColl, &buffer, targetDB)
				}
				bufferMutex.Unlock()
				timer.Reset(flushInterval)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			bufferMutex.Lock()
			if len(buffer) > 0 {
				s.logger.Infof("Context done, flushing remaining buffer for %s.%s: %d write models", sourceDB, collectionName, len(buffer))
				s.flushBuffer(ctx, targetColl, &buffer, targetDB)
			}
			bufferMutex.Unlock()
			return
		default:
			if cs.Next(ctx) {
				var changeEvent bson.M
				if err := cs.Decode(&changeEvent); err != nil {
					s.logger.Errorf("Failed to decode change event for %s.%s: %v", sourceDB, collectionName, err)
					continue
				}

				s.logger.Infof("Change Event Detected: %+v", changeEvent)
				// Save new resume token
				token := cs.ResumeToken()
				if token != nil {
					s.saveMongoDBResumeToken(collectionName, token)
				}

				writeModel := s.prepareWriteModel(changeEvent)
				if writeModel != nil {
					bufferMutex.Lock()
					buffer = append(buffer, writeModel)
					bufferSize := len(buffer)
					bufferMutex.Unlock()

					s.logger.Infof("Added WriteModel to buffer for %s.%s, buffer size: %d", sourceDB, collectionName, bufferSize)

					if bufferSize >= batchSize {
						bufferMutex.Lock()
						s.logger.Infof("Buffer size reached %d for %s.%s, flushing buffer", batchSize, sourceDB, collectionName)
						s.flushBuffer(ctx, targetColl, &buffer, targetDB)
						bufferMutex.Unlock()
						timer.Reset(flushInterval)
					}
				}
			} else {
				if err := cs.Err(); err != nil {
					s.logger.Errorf("Change Stream error for collection %s.%s: %v", sourceDB, collectionName, err)
					return
				}
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

	// Log buffer contents before flush
	for i, wm := range *buffer {
		switch m := wm.(type) {
		case *mongo.InsertOneModel:
			s.logger.Infof("WriteModel %d: InsertOneModel - Document: %+v", i+1, m.Document)
		case *mongo.ReplaceOneModel:
			s.logger.Infof("WriteModel %d: ReplaceOneModel - Filter: %+v, Replacement: %+v", i+1, m.Filter, m.Replacement)
		case *mongo.DeleteOneModel:
			s.logger.Infof("WriteModel %d: DeleteOneModel - Filter: %+v", i+1, m.Filter)
		default:
			s.logger.Infof("WriteModel %d: Unknown type", i+1)
		}
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
	s.logger.Infof("Buffer cleared for %s.%s", targetDB, targetColl.Name())
}
