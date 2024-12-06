package syncer

import (
    // "context"

    "github.com/sirupsen/logrus"
    "sync/pkg/config"
    "sync/pkg/syncer/mongodb"
    "sync/pkg/syncer/mysql"
)

func NewMongoDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mongodb.MongoDBSyncer {
    return mongodb.NewMongoDBSyncer(cfg, logger)
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mysql.MySQLSyncer {
    return mysql.NewMySQLSyncer(cfg, logger)
}