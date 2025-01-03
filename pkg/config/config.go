package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type TableMapping struct {
	SourceTable string `yaml:"source_table"`
	TargetTable string `yaml:"target_table"`
}

type DatabaseMapping struct {
	SourceDatabase string         `yaml:"source_database"`
	TargetDatabase string         `yaml:"target_database"`
	Tables         []TableMapping `yaml:"tables"`
}

type SyncConfig struct {
	Type                   string            `yaml:"type"`
	Enable                 bool              `yaml:"enable"`
	SourceConnection       string            `yaml:"source_connection"`
	TargetConnection       string            `yaml:"target_connection"`
	Mappings               []DatabaseMapping `yaml:"mappings"`
	DumpExecutionPath      string            `yaml:"dump_execution_path,omitempty"`
	MySQLPositionPath      string            `yaml:"mysql_position_path,omitempty"`
	MongoDBResumeTokenPath string            `yaml:"mongodb_resume_token_path,omitempty"`
}

type Config struct {
	SyncConfigs []SyncConfig   `yaml:"sync_configs"`
	Logger      *logrus.Logger `yaml:"-"`
}

func NewConfig() *Config {
	// Get the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}
	log.Printf("Current working directory: %s", cwd)

	// Prefer using the CONFIG_PATH environment variable to specify the configuration file path
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		// If the environment variable is not set, use the path relative to the project root directory
		configPath = filepath.Join(cwd, "configs/config.yaml")
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read configuration file: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse configuration file: %v", err)
	}

	cfg.Logger = logrus.New()
	return &cfg
}
