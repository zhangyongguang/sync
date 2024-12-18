package syncer

import (
	// "context"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer/mongodb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mysql"
	"github.com/sirupsen/logrus"
)

func NewMongoDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mongodb.MongoDBSyncer {
	return mongodb.NewMongoDBSyncer(cfg, logger)
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mysql.MySQLSyncer {
	return mysql.NewMySQLSyncer(cfg, logger)
}
