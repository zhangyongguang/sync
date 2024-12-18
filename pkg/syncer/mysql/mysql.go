package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLSyncer struct {
	cfg    config.SyncConfig
	logger *logrus.Logger
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MySQLSyncer {
	return &MySQLSyncer{
		cfg:    cfg,
		logger: logger,
	}
}

func (s *MySQLSyncer) Start(ctx context.Context) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = s.parseAddr(s.cfg.SourceConnection)
	cfg.User, cfg.Password = s.parseUserPassword(s.cfg.SourceConnection)
	cfg.Dump.ExecutionPath = s.cfg.DumpExecutionPath

	includeTables := []string{}
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			includeTables = append(includeTables, fmt.Sprintf("%s\\.%s", mapping.SourceDatabase, table.SourceTable))
		}
	}
	cfg.IncludeTableRegex = includeTables

	// Create canal instance
	c, err := canal.NewCanal(cfg)
	if err != nil {
		s.logger.Fatalf("Failed to create canal: %v", err)
	}

	// Initialize target database connection
	targetDB, err := sql.Open("mysql", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Fatalf("Failed to connect to target MySQL database: %v", err)
	}

	h := &MyEventHandler{
		targetDB:          targetDB,
		mappings:          s.cfg.Mappings,
		logger:            s.logger,
		positionSaverPath: s.cfg.MySQLPositionPath,
		canal:             c, // Save canal instance to get position in the ticker
	}
	c.SetEventHandler(h)

	// Ensure the directory for the binlog position file exists
	if s.cfg.MySQLPositionPath != "" {
		positionDir := filepath.Dir(s.cfg.MySQLPositionPath)
		if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
			s.logger.Fatalf("Failed to create directory for MySQL position file %s: %v", s.cfg.MySQLPositionPath, err)
		}
	}

	// Load last position from file
	var startPos *mysql.Position
	if s.cfg.MySQLPositionPath != "" {
		startPos = s.loadBinlogPosition(s.cfg.MySQLPositionPath)
		if startPos != nil {
			s.logger.Infof("Starting MySQL canal from saved position: %v", *startPos)
		}
	}

	// Start a goroutine with a ticker to periodically write the current position to file every 3 seconds
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pos := c.SyncedPosition()
				data, err := json.Marshal(pos)
				if err != nil {
					s.logger.Errorf("Failed to marshal binlog position: %v", err)
					continue
				}
				if h.positionSaverPath != "" {
					// Ensure the directory exists
					positionDir := filepath.Dir(h.positionSaverPath)
					if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
						s.logger.Errorf("Failed to create directory for MySQL position file %s: %v", h.positionSaverPath, err)
						continue
					}
					if err := ioutil.WriteFile(h.positionSaverPath, data, 0644); err != nil {
						s.logger.Errorf("Failed to write binlog position to %s: %v", h.positionSaverPath, err)
					} else {
						s.logger.Infof("Periodically saved binlog position to %s: %v", h.positionSaverPath, pos)
					}
				}
			}
		}
	}()

	go func() {
		if startPos != nil {
			// Start from the specified position
			err = c.RunFrom(*startPos)
		} else {
			// Start from the current latest position
			err = c.Run()
		}
		if err != nil {
			s.logger.Fatalf("Failed to run canal: %v", err)
		}
	}()

	<-ctx.Done()
	s.logger.Info("MySQL synchronization stopped.")
}

func (s *MySQLSyncer) loadBinlogPosition(path string) *mysql.Position {
	// Ensure the directory exists
	positionDir := filepath.Dir(path)
	if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
		s.logger.Errorf("Failed to create directory for MySQL position file %s: %v", path, err)
		return nil
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		s.logger.Infof("No previous binlog position file at %s: %v", path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Infof("Binlog position file for %s is empty", path)
		return nil
	}
	var pos mysql.Position
	if err := json.Unmarshal(data, &pos); err != nil {
		s.logger.Errorf("Failed to unmarshal binlog position from %s: %v", path, err)
		return nil
	}
	return &pos
}

func (s *MySQLSyncer) parseAddr(dsn string) string {
	parts := strings.Split(dsn, "@tcp(")
	if len(parts) < 2 {
		s.logger.Fatalf("Invalid DSN format: %s", dsn)
	}
	addr := strings.Split(parts[1], ")")[0]
	return addr
}

func (s *MySQLSyncer) parseUserPassword(dsn string) (string, string) {
	parts := strings.Split(dsn, "@")
	if len(parts) < 2 {
		s.logger.Fatalf("Invalid DSN format: %s", dsn)
	}
	userInfo := parts[0]
	userParts := strings.Split(userInfo, ":")
	if len(userParts) < 2 {
		s.logger.Fatalf("Invalid DSN user info: %s", userInfo)
	}
	return userParts[0], userParts[1]
}

// Custom event handler
type MyEventHandler struct {
	canal.DummyEventHandler
	targetDB          *sql.DB
	mappings          []config.DatabaseMapping
	logger            *logrus.Logger
	positionSaverPath string
	canal             *canal.Canal
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	table := e.Table
	sourceDB := table.Schema
	tableName := table.Name

	var targetDBName, targetTableName string

	// Find corresponding target database and table
	found := false
	for _, mapping := range h.mappings {
		if mapping.SourceDatabase == sourceDB {
			for _, tableMap := range mapping.Tables {
				if tableMap.SourceTable == tableName {
					targetDBName = mapping.TargetDatabase
					targetTableName = tableMap.TargetTable
					found = true
					break
				}
			}
		}
		if found {
			break
		}
	}

	if !found {
		h.logger.Warnf("No mapping found for source table %s.%s", sourceDB, tableName)
		return nil
	}

	columnNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columnNames[i] = col.Name
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			h.handleInsert(targetDBName, targetTableName, columnNames, row)
		}
	case canal.UpdateAction:
		for i := 0; i < len(e.Rows); i += 2 {
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]
			h.handleUpdate(targetDBName, targetTableName, columnNames, table, oldRow, newRow)
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			h.handleDelete(targetDBName, targetTableName, columnNames, table, row)
		}
	}
	return nil
}

func (h *MyEventHandler) handleInsert(targetDBName, targetTableName string, columnNames []string, row []interface{}) {
	// Dynamically build INSERT
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", targetDBName, targetTableName, strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))
	h.logger.Infof("Inserted: %s %v", query, row)
	_, err := h.targetDB.Exec(query, row...)
	if err != nil {
		h.logger.Errorf("Failed to insert into target database: %v", err)
	} else {
		h.logger.Infof("Inserted row into target database: %s.%s %v", targetDBName, targetTableName, row)
	}
}

func (h *MyEventHandler) handleUpdate(targetDBName, targetTableName string, columnNames []string, table *schema.Table, oldRow, newRow []interface{}) {
	// Build UPDATE statement
	setClauses := make([]string, len(columnNames))
	for i, col := range columnNames {
		setClauses[i] = fmt.Sprintf("%s = ?", col)
	}
	// Build WHERE clause
	whereClauses := []string{}
	whereValues := []interface{}{}
	for _, pkIndex := range table.PKColumns {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", columnNames[pkIndex]))
		whereValues = append(whereValues, oldRow[pkIndex])
	}
	if len(whereClauses) == 0 {
		h.logger.Warnf("No primary key defined on table %s.%s, cannot perform update", targetDBName, targetTableName)
		return
	}
	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", targetDBName, targetTableName, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
	args := append(newRow, whereValues...)
	_, err := h.targetDB.Exec(query, args...)
	if err != nil {
		h.logger.Errorf("Failed to update target database: %v", err)
	} else {
		h.logger.Infof("Updated row in target database: %s.%s oldData=%v, newData=%v", targetDBName, targetTableName, oldRow, newRow)
	}
}

func (h *MyEventHandler) handleDelete(targetDBName, targetTableName string, columnNames []string, table *schema.Table, row []interface{}) {
	// Build DELETE statement
	whereClauses := []string{}
	whereValues := []interface{}{}
	for _, pkIndex := range table.PKColumns {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", columnNames[pkIndex]))
		whereValues = append(whereValues, row[pkIndex])
	}
	if len(whereClauses) == 0 {
		h.logger.Warnf("No primary key defined on table %s.%s, cannot perform delete", targetDBName, targetTableName)
		return
	}
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", targetDBName, targetTableName, strings.Join(whereClauses, " AND "))
	_, err := h.targetDB.Exec(query, whereValues...)
	if err != nil {
		h.logger.Errorf("Failed to delete from target database: %v", err)
	} else {
		h.logger.Infof("Deleted row from target database: %s.%s %v", targetDBName, targetTableName, row)
	}
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

// Even if force is always false in OnPosSynced, we do not rely on it to write the position. Instead, we use a ticker to periodically write the position.
// If you still want to debug in OnPosSynced, you can print logs here to observe the invocation.
func (h *MyEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gs mysql.GTIDSet, force bool) error {
	return nil
}
