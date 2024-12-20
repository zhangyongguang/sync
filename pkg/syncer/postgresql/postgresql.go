package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"
)

type PostgreSQLSyncer struct {
	cfg              config.SyncConfig
	logger           *logrus.Logger
	sourceConnNormal *pgx.Conn      // 普通连接，用于初始全量同步SQL查询
	sourceConnRepl   *pgconn.PgConn // 复制模式连接，用于IdentifySystem和StartReplication等
	targetDB         *sql.DB
	repSlot          string
	outputPlugin     string
	currentLsn       pglogrepl.LSN
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *PostgreSQLSyncer {
	return &PostgreSQLSyncer{
		cfg:    cfg,
		logger: logger,
	}
}

func (s *PostgreSQLSyncer) Start(ctx context.Context) {
	var err error

	// 建立普通模式连接，用于初始同步查询
	s.sourceConnNormal, err = pgx.Connect(ctx, s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("Failed to connect to PostgreSQL source (normal): %v", err)
	}
	defer s.sourceConnNormal.Close(ctx)

	// 为复制模式连接构造 DSN
	replDSN, err := s.buildReplicationDSN(s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("Failed to build replication DSN: %v", err)
	}

	s.sourceConnRepl, err = pgconn.Connect(ctx, replDSN)
	if err != nil {
		s.logger.Fatalf("Failed to connect to PostgreSQL source (replication): %v", err)
	}
	defer s.sourceConnRepl.Close(ctx)

	// 建立目标数据库连接
	s.targetDB, err = sql.Open("postgres", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Fatalf("Failed to connect to PostgreSQL target: %v", err)
	}
	defer s.targetDB.Close()

	s.repSlot = s.cfg.PGReplicationSlot()
	s.outputPlugin = s.cfg.PGPlugin()
	if s.repSlot == "" || s.outputPlugin == "" {
		s.logger.Fatalf("PostgreSQL sync config must specify pg_replication_slot and pg_plugin")
	}

	err = s.ensureReplicationSlot(ctx)
	if err != nil {
		s.logger.Fatalf("Failed to ensure replication slot: %v", err)
	}

	err = s.initialSync(ctx)
	if err != nil {
		s.logger.Errorf("Initial sync failed: %v", err)
	}

	err = s.startLogicalReplication(ctx)
	if err != nil {
		s.logger.Errorf("Logical replication failed: %v", err)
		return
	}

	s.logger.Info("All synchronization tasks have completed.")
}

func (s *PostgreSQLSyncer) buildReplicationDSN(normalDSN string) (string, error) {
	u, err := url.Parse(normalDSN)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (s *PostgreSQLSyncer) ensureReplicationSlot(ctx context.Context) error {
	identifyResp, err := pglogrepl.IdentifySystem(ctx, s.sourceConnRepl)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed: %v", err)
	}
	systemID := identifyResp.SystemID
	timeline := identifyResp.Timeline
	xLogPos := identifyResp.XLogPos
	s.logger.Infof("IdentifySystem: systemID=%s timeline=%d xLogPos=%X", systemID, timeline, xLogPos)

	slotRes, err := pglogrepl.CreateReplicationSlot(ctx, s.sourceConnRepl, s.repSlot, s.outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("CreateReplicationSlot failed: %v", err)
		} else {
			s.logger.Infof("Replication slot %s already exists, use IdentifySystem XLogPos as start.", s.repSlot)
			s.currentLsn = xLogPos
			return nil
		}
	}

	lsn, err := pglogrepl.ParseLSN(slotRes.ConsistentPoint)
	if err != nil {
		return fmt.Errorf("Failed to parse ConsistentPoint %s: %v", slotRes.ConsistentPoint, err)
	}
	s.logger.Infof("Created replication slot %s at LSN %X", s.repSlot, lsn)
	s.currentLsn = lsn
	return nil
}

// initialSync现在使用 source_database, source_schema, source_table 来构造查询
func (s *PostgreSQLSyncer) initialSync(ctx context.Context) error {
	s.logger.Info("Starting initial full sync")

	for _, dbmap := range s.cfg.Mappings {
		sourceDB := dbmap.SourceDatabase
		targetDB := dbmap.TargetDatabase
		sourceSchema := dbmap.SourceSchema
		targetSchema := dbmap.TargetSchema
		if sourceSchema == "" {
			sourceSchema = "public"
		}
		if targetSchema == "" {
			targetSchema = "public"
		}

		for _, tblmap := range dbmap.Tables {
			sourceTableFull := fmt.Sprintf("%s.%s.%s", sourceDB, sourceSchema, tblmap.SourceTable)
			query := fmt.Sprintf("SELECT * FROM %s.%s", sourceSchema, tblmap.SourceTable)

			rows, err := s.sourceConnNormal.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("Failed to query source table %s: %v", sourceTableFull, err)
			}

			cols := rows.FieldDescriptions()
			colNames := make([]string, len(cols))
			for i, col := range cols {
				colNames[i] = string(col.Name)
			}

			insertCols := strings.Join(colNames, ", ")
			placeholders := make([]string, len(colNames))
			for i := range placeholders {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
			}

			insertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
				targetSchema, tblmap.TargetTable,
				insertCols, strings.Join(placeholders, ", "))

			tx, err := s.targetDB.Begin()
			if err != nil {
				rows.Close()
				return fmt.Errorf("Failed to begin tx for initial sync: %v", err)
			}

			count := 0
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					tx.Rollback()
					rows.Close()
					return fmt.Errorf("Failed to get row values from %s: %v", sourceTableFull, err)
				}
				_, err = tx.Exec(insertSQL, vals...)
				if err != nil {
					tx.Rollback()
					rows.Close()
					return fmt.Errorf("Failed to insert row into %s.%s.%s: %v",
						targetDB, targetSchema, tblmap.TargetTable, err)
				}
				count++
			}
			rows.Close()

			if err := rows.Err(); err != nil {
				tx.Rollback()
				return fmt.Errorf("Rows error from %s: %v", sourceTableFull, err)
			}

			if err = tx.Commit(); err != nil {
				return fmt.Errorf("Failed to commit tx for initial sync on %s.%s.%s: %v",
					targetDB, targetSchema, tblmap.TargetTable, err)
			}

			s.logger.Infof("Initial sync of %s to %s.%s.%s completed, inserted %d rows.",
				sourceTableFull, targetDB, targetSchema, tblmap.TargetTable, count)
		}
	}
	s.logger.Info("Initial full sync completed")
	return nil
}

func (s *PostgreSQLSyncer) startLogicalReplication(ctx context.Context) error {
	s.logger.Infof("Starting logical replication from slot: %s with plugin: %s", s.repSlot, s.outputPlugin)

	err := pglogrepl.StartReplication(ctx, s.sourceConnRepl, s.repSlot, s.currentLsn, pglogrepl.StartReplicationOptions{})
	if err != nil {
		return fmt.Errorf("StartReplication failed: %v", err)
	}

	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeatTicker.C:
			err = pglogrepl.SendStandbyStatusUpdate(ctx, s.sourceConnRepl, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: s.currentLsn,
				ReplyRequested:   false,
			})
			if err != nil {
				s.logger.Errorf("SendStandbyStatusUpdate failed: %v", err)
			}
		default:
			ctxReceive, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
			rawMsg, err := s.sourceConnRepl.ReceiveMessage(ctxReceive)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				return fmt.Errorf("ReceiveMessage failed: %v", err)
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				// 非复制数据消息，忽略
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %v", err)
				}
				s.logger.Infof("PrimaryKeepaliveMessage ServerWALEnd=%s ServerTime=%s ReplyRequested=%t",
					pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)
				if pkm.ServerWALEnd > s.currentLsn {
					s.currentLsn = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					err = pglogrepl.SendStandbyStatusUpdate(ctx, s.sourceConnRepl, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: s.currentLsn,
						ReplyRequested:   false,
					})
					if err != nil {
						s.logger.Errorf("SendStandbyStatusUpdate failed: %v", err)
					}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParseXLogData failed: %v", err)
				}
				if xld.WALStart > s.currentLsn {
					s.currentLsn = xld.WALStart
				}
				err = s.handleWalData(ctx, xld.WALData)
				if err != nil {
					s.logger.Errorf("handleWalData error: %v", err)
				}

			default:
				s.logger.Warnf("Unknown replication message type: %v", msg.Data[0])
			}
		}
	}
}

// handleWalData根据事件中的schema和table从mappings中匹配database, schema, table
func (s *PostgreSQLSyncer) handleWalData(ctx context.Context, walData []byte) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(walData, &payload); err != nil {
		return fmt.Errorf("Failed to unmarshal wal2json output: %v", err)
	}

	changeList, ok := payload["change"].([]interface{})
	if !ok {
		// No changes
		return nil
	}

	// 不再使用 getSourceDBFromConn()作为匹配，直接从mappings中寻找对应schema和table进行匹配
	for _, c := range changeList {
		entry, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		kind, _ := entry["kind"].(string)
		table, _ := entry["table"].(string)
		schema, _ := entry["schema"].(string)

		var mappedDB, mappedSchema, mappedTable string
		found := false
		// 在mappings中匹配
		for _, m := range s.cfg.Mappings {
			if m.SourceDatabase != "" && m.SourceSchema != "" {
				// 若配置有明确的source_schema, source_table
				for _, tblMap := range m.Tables {
					if m.SourceSchema == schema && tblMap.SourceTable == table {
						mappedDB = m.TargetDatabase
						mappedSchema = m.TargetSchema
						if mappedSchema == "" {
							mappedSchema = "public"
						}
						mappedTable = tblMap.TargetTable
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
			s.logger.Warnf("No mapping found for source table %s.%s", schema, table)
			continue
		}

		colNames, _ := entry["columnnames"].([]interface{})
		colValues, _ := entry["columnvalues"].([]interface{})

		switch kind {
		case "insert":
			s.handleInsert(mappedDB, mappedSchema, mappedTable, colNames, colValues)
		case "update":
			oldKeys, _ := entry["oldkeys"].(map[string]interface{})
			s.handleUpdate(mappedDB, mappedSchema, mappedTable, colNames, colValues, oldKeys)
		case "delete":
			oldKeys, _ := entry["oldkeys"].(map[string]interface{})
			s.handleDelete(mappedDB, mappedSchema, mappedTable, oldKeys)
		}
	}

	return nil
}

func (s *PostgreSQLSyncer) handleInsert(dbName, schemaName, tableName string, colNames []interface{}, colValues []interface{}) {
	s.insertOrUpdate(dbName, schemaName, tableName, colNames, colValues, false, nil)
}

func (s *PostgreSQLSyncer) handleUpdate(dbName, schemaName, tableName string, colNames []interface{}, colValues []interface{}, oldKeys map[string]interface{}) {
	s.insertOrUpdate(dbName, schemaName, tableName, colNames, colValues, true, oldKeys)
}

func (s *PostgreSQLSyncer) handleDelete(dbName, schemaName, tableName string, oldKeys map[string]interface{}) {
	keyNames, _ := oldKeys["keynames"].([]interface{})
	keyValues, _ := oldKeys["keyvalues"].([]interface{})

	whereClauses := []string{}
	args := []interface{}{}
	for i, kn := range keyNames {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", kn.(string), i+1))
		args = append(args, keyValues[i])
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", schemaName, tableName, strings.Join(whereClauses, " AND "))
	_, err := s.targetDB.Exec(query, args...)
	if err != nil {
		s.logger.Errorf("PostgreSQL delete failed: %v", err)
	}
}

func (s *PostgreSQLSyncer) insertOrUpdate(dbName, schemaName, tableName string, colNames, colValues []interface{}, isUpdate bool, oldKeys map[string]interface{}) {
	cols := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	args := make([]interface{}, len(colValues))
	for i := range colNames {
		cols[i] = colNames[i].(string)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = colValues[i]
	}
	if !isUpdate {
		// insert
		query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", schemaName, tableName, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		_, err := s.targetDB.Exec(query, args...)
		if err != nil {
			s.logger.Errorf("PostgreSQL insert failed: %v", err)
		}
	} else {
		// update
		setClauses := []string{}
		for i, c := range cols {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", c, i+1))
		}

		keyNames, _ := oldKeys["keynames"].([]interface{})
		keyValues, _ := oldKeys["keyvalues"].([]interface{})
		whereClauses := []string{}
		for i, kn := range keyNames {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", kn.(string), len(args)+i+1))
			args = append(args, keyValues[i])
		}

		query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", schemaName, tableName, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
		_, err := s.targetDB.Exec(query, args...)
		if err != nil {
			s.logger.Errorf("PostgreSQL update failed: %v", err)
		}
	}
}
