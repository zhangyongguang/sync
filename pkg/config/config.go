package config

import (
    "io/ioutil"
    "log"
    "os"

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
    Type                    string             `yaml:"type"`
    Enable                  bool               `yaml:"enable"`
    SourceConnection        string             `yaml:"source_connection"`
    TargetConnection        string             `yaml:"target_connection"`
    Mappings                []DatabaseMapping  `yaml:"mappings"`
    DumpExecutionPath       string             `yaml:"dump_execution_path,omitempty"`
    MySQLPositionPath       string             `yaml:"mysql_position_path,omitempty"`
    MongoDBResumeTokenPath  string             `yaml:"mongodb_resume_token_path,omitempty"`
}

type Config struct {
    SyncConfigs []SyncConfig   `yaml:"sync_configs"`
    Logger      *logrus.Logger `yaml:"-"`
}

func NewConfig() *Config {
    configPath := os.Getenv("CONFIG_PATH")
    if configPath == "" {
        configPath = "configs/config.yaml"
    }

    data, err := ioutil.ReadFile(configPath)
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