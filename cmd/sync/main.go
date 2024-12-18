package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer"
)

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture system interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Log.Info("Received interrupt signal, exiting...")
		cancel()
	}()

	// Load configuration
	cfg := config.NewConfig()
	log := logger.InitLogger()

	// Start syncer
	var wg sync.WaitGroup
	for _, syncCfg := range cfg.SyncConfigs {
		if !syncCfg.Enable {
			continue
		}
		wg.Add(1)
		switch syncCfg.Type {
		case "mongodb":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMongoDBSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "mysql":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMySQLSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		default:
			log.Errorf("Unknown sync type: %s", syncCfg.Type)
			wg.Done()
		}
	}

	// Wait for all syncers to finish
	wg.Wait()
	log.Info("All synchronization tasks have completed.")

	// Wait for program to end
	<-ctx.Done()
	log.Info("Program has exited")
}
