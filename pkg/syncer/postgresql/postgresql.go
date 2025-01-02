package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/lib/pq"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"
)

type PostgreSQLSyncer struct {
	cfg              config.SyncConfig
	logger           *logrus.Logger
	sourceConnNormal *pgx.Conn
	sourceConnRepl   *pgconn.PgConn
	targetDB         *sql.DB
	repSlot          string
	outputPlugin     string
	currentLsn       pglogrepl.LSN

	positionSaverTicker *time.Ticker
	cancelPositionSaver context.CancelFunc
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *PostgreSQLSyncer {
	return &PostgreSQLSyncer{
		cfg:    cfg,
		logger: logger,
	}
}

func (s *PostgreSQLSyncer) Start(ctx context.Context) {
	var err error

	s.sourceConnNormal, err = pgx.Connect(ctx, s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to connect to source (normal): %v", err)
	}
	defer s.sourceConnNormal.Close(ctx)

	replDSN, err := s.buildReplicationDSN(s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to build replication DSN: %v", err)
	}

	s.sourceConnRepl, err = pgconn.Connect(ctx, replDSN)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to connect to source (replication): %v", err)
	}
	defer s.sourceConnRepl.Close(ctx)

	s.targetDB, err = sql.Open("postgres", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to connect to target: %v", err)
	}
	defer s.targetDB.Close()

	s.repSlot = s.cfg.PGReplicationSlot()
	s.outputPlugin = s.cfg.PGPlugin()
	if s.repSlot == "" || s.outputPlugin == "" {
		s.logger.Fatalf("[PostgreSQL] Must specify pg_replication_slot and pg_plugin")
	}

	err = s.ensureReplicationSlot(ctx)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to ensure replication slot: %v", err)
	}

	if s.cfg.PGPositionPath != "" {
		lsnFromFile, err := s.loadPosition(s.cfg.PGPositionPath)
		if err == nil && lsnFromFile > 0 {
			s.logger.Infof("[PostgreSQL] Loaded last LSN from file: %X", lsnFromFile)
			s.currentLsn = lsnFromFile
		}
	}

	err = s.initialSync(ctx)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Initial sync failed: %v", err)
	}

	ctxPos, cancelPos := context.WithCancel(ctx)
	s.cancelPositionSaver = cancelPos
	s.positionSaverTicker = time.NewTicker(3 * time.Second)
	go s.runPositionSaver(ctxPos, s.cfg.PGPositionPath)

	err = s.startLogicalReplication(ctx)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Logical replication failed: %v", err)
		s.stopPositionSaver()
		return
	}

	s.stopPositionSaver()
	s.logger.Info("[PostgreSQL] Synchronization tasks completed.")
}

func (s *PostgreSQLSyncer) stopPositionSaver() {
	if s.positionSaverTicker != nil {
		s.positionSaverTicker.Stop()
	}
	if s.cancelPositionSaver != nil {
		s.cancelPositionSaver()
	}
}

func (s *PostgreSQLSyncer) runPositionSaver(ctx context.Context, path string) {
	if path == "" {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.positionSaverTicker.C:
			err := s.savePosition(path, s.currentLsn)
			if err != nil {
				s.logger.Errorf("[PostgreSQL] Failed to save position: %v", err)
			}
		}
	}
}

func (s *PostgreSQLSyncer) savePosition(path string, lsn pglogrepl.LSN) error {
	positionDir := filepath.Dir(path)
	if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
		return fmt.Errorf("[PostgreSQL] failed to create directory %s: %v", path, err)
	}
	data, err := json.Marshal(lsn.String())
	if err != nil {
		return fmt.Errorf("[PostgreSQL] failed to marshal LSN: %v", err)
	}
	return ioutil.WriteFile(path, data, 0644)
}

func (s *PostgreSQLSyncer) loadPosition(path string) (pglogrepl.LSN, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) <= 1 {
		return 0, fmt.Errorf("empty position file")
	}
	var lsnStr string
	err = json.Unmarshal(data, &lsnStr)
	if err != nil {
		return 0, err
	}
	lsn, err := parseLSNFromString(lsnStr)
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

func parseLSNFromString(lsnStr string) (pglogrepl.LSN, error) {
	parts := strings.Split(lsnStr, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %s", lsnStr)
	}
	hiVal, err := hexStrToUint32(parts[0])
	if err != nil {
		return 0, err
	}
	loVal, err := hexStrToUint32(parts[1])
	if err != nil {
		return 0, err
	}
	return pglogrepl.LSN(uint64(hiVal)<<32 + uint64(loVal)), nil
}

func hexStrToUint32(s string) (uint32, error) {
	val, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(val), nil
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
	s.logger.Infof("[PostgreSQL] IdentifySystem: systemID=%s timeline=%d xLogPos=%X", systemID, timeline, xLogPos)

	slotRes, err := pglogrepl.CreateReplicationSlot(ctx, s.sourceConnRepl, s.repSlot, s.outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("CreateReplicationSlot failed: %v", err)
		} else {
			s.logger.Infof("[PostgreSQL] Replication slot %s already exists. Using XLogPos as start if not loaded.", s.repSlot)
			if s.currentLsn == 0 {
				s.currentLsn = xLogPos
			}
			return nil
		}
	}

	lsn, err := pglogrepl.ParseLSN(slotRes.ConsistentPoint)
	if err != nil {
		return fmt.Errorf("Failed to parse ConsistentPoint %s: %v", slotRes.ConsistentPoint, err)
	}
	s.logger.Infof("[PostgreSQL] Created replication slot %s at LSN %X", s.repSlot, lsn)
	if s.currentLsn == 0 {
		s.currentLsn = lsn
	}
	return nil
}

func (s *PostgreSQLSyncer) initialSync(ctx context.Context) error {
	s.logger.Info("[PostgreSQL] Starting initial full sync")

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
				return fmt.Errorf("[PostgreSQL] Failed to query source table %s: %v", sourceTableFull, err)
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
				return fmt.Errorf("[PostgreSQL] Failed to begin tx for initial sync: %v", err)
			}

			count := 0
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					tx.Rollback()
					rows.Close()
					return fmt.Errorf("[PostgreSQL] Failed to get row values from %s: %v", sourceTableFull, err)
				}
				_, err = tx.Exec(insertSQL, vals...)
				if err != nil {
					tx.Rollback()
					rows.Close()
					return fmt.Errorf("[PostgreSQL] Insert row into %s.%s.%s error: %v",
						targetDB, targetSchema, tblmap.TargetTable, err)
				}
				count++
			}
			rows.Close()

			if err := rows.Err(); err != nil {
				tx.Rollback()
				return fmt.Errorf("[PostgreSQL] Rows error from %s: %v", sourceTableFull, err)
			}

			if err = tx.Commit(); err != nil {
				return fmt.Errorf("[PostgreSQL] Failed to commit tx for %s.%s.%s: %v",
					targetDB, targetSchema, tblmap.TargetTable, err)
			}

			s.logger.Infof("[PostgreSQL] Initial sync of %s to %s.%s.%s inserted %d rows",
				sourceTableFull, targetDB, targetSchema, tblmap.TargetTable, count)
		}
	}
	s.logger.Info("[PostgreSQL] Initial full sync completed")
	return nil
}

func (s *PostgreSQLSyncer) startLogicalReplication(ctx context.Context) error {
	s.logger.Infof("[PostgreSQL] Starting logical replication from slot: %s, plugin: %s", s.repSlot, s.outputPlugin)

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
				s.logger.Errorf("[PostgreSQL] SendStandbyStatusUpdate failed: %v", err)
			}
		default:
			ctxReceive, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
			rawMsg, err := s.sourceConnRepl.ReceiveMessage(ctxReceive)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				return fmt.Errorf("[PostgreSQL] ReceiveMessage failed: %v", err)
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("[PostgreSQL] WAL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("[PostgreSQL] ParsePrimaryKeepaliveMessage failed: %v", err)
				}
				if pkm.ServerWALEnd > s.currentLsn {
					s.currentLsn = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					err = pglogrepl.SendStandbyStatusUpdate(ctx, s.sourceConnRepl, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: s.currentLsn,
						ReplyRequested:   false,
					})
					if err != nil {
						s.logger.Errorf("[PostgreSQL] SendStandbyStatusUpdate failed: %v", err)
					}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("[PostgreSQL] ParseXLogData failed: %v", err)
				}
				if xld.WALStart > s.currentLsn {
					s.currentLsn = xld.WALStart
				}
				err = s.handleWalData(ctx, xld.WALData)
				if err != nil {
					s.logger.Errorf("[PostgreSQL] handleWalData error: %v", err)
				}
				if s.cfg.PGPositionPath != "" {
					saveErr := s.savePosition(s.cfg.PGPositionPath, s.currentLsn)
					if saveErr != nil {
						s.logger.Errorf("[PostgreSQL] Failed to save position: %v", saveErr)
					}
				}

			default:
				s.logger.Warnf("[PostgreSQL] Unknown replication message type: %v", msg.Data[0])
			}
		}
	}
}

func (s *PostgreSQLSyncer) handleWalData(ctx context.Context, walData []byte) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(walData, &payload); err != nil {
		return fmt.Errorf("Failed to unmarshal wal2json: %v", err)
	}

	changeList, ok := payload["change"].([]interface{})
	if !ok {
		return nil
	}

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

		for _, m := range s.cfg.Mappings {
			if m.SourceSchema == schema {
				for _, tblMap := range m.Tables {
					if tblMap.SourceTable == table {
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
			s.logger.Warnf("[PostgreSQL] No mapping found for %s.%s", schema, table)
			continue
		}

		colNames, _ := entry["columnnames"].([]interface{})
		colValues, _ := entry["columnvalues"].([]interface{})

		switch kind {
		case "insert":
			s.handleInsert(schema, table, mappedDB, mappedSchema, mappedTable, colNames, colValues)
		case "update":
			oldKeys, _ := entry["oldkeys"].(map[string]interface{})
			s.handleUpdate(schema, table, mappedDB, mappedSchema, mappedTable, colNames, colValues, oldKeys)
		case "delete":
			oldKeys, _ := entry["oldkeys"].(map[string]interface{})
			s.handleDelete(schema, table, mappedDB, mappedSchema, mappedTable, oldKeys)
		}
	}

	return nil
}

func (s *PostgreSQLSyncer) handleInsert(
	srcSchema, srcTable, dstDB, dstSchema, dstTable string,
	colNames []interface{},
	colValues []interface{},
) {
	s.insertOrUpdate(srcSchema, srcTable, dstDB, dstSchema, dstTable, colNames, colValues, false, nil)
}

func (s *PostgreSQLSyncer) handleUpdate(
	srcSchema, srcTable, dstDB, dstSchema, dstTable string,
	colNames []interface{},
	colValues []interface{},
	oldKeys map[string]interface{},
) {
	s.insertOrUpdate(srcSchema, srcTable, dstDB, dstSchema, dstTable, colNames, colValues, true, oldKeys)
}

func (s *PostgreSQLSyncer) handleDelete(
	srcSchema, srcTable, dstDB, dstSchema, dstTable string,
	oldKeys map[string]interface{},
) {
	keyNames, _ := oldKeys["keynames"].([]interface{})
	keyValues, _ := oldKeys["keyvalues"].([]interface{})

	whereClauses := []string{}
	args := []interface{}{}
	for i, kn := range keyNames {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", kn.(string), i+1))
		args = append(args, keyValues[i])
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", dstSchema, dstTable, strings.Join(whereClauses, " AND "))
	_, err := s.targetDB.Exec(query, args...)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] [DELETE] {src_schema: %s, src_table: %s} => {dst_db: %s, dst_schema: %s, dst_table: %s} Error: %v", srcSchema, srcTable, dstDB, dstSchema, dstTable, err)
	} else {
		s.logger.Infof("[PostgreSQL] [DELETE] {src_schema: %s, src_table: %s} => {dst_db: %s, dst_schema: %s, dst_table: %s} Key: %+v", srcSchema, srcTable, dstDB, dstSchema, dstTable, keyValues)
	}
}

func (s *PostgreSQLSyncer) insertOrUpdate(
	srcSchema, srcTable, dstDB, dstSchema, dstTable string,
	colNames, colValues []interface{},
	isUpdate bool,
	oldKeys map[string]interface{},
) {
	cols := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	args := make([]interface{}, len(colValues))
	for i := range colNames {
		cols[i] = colNames[i].(string)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = colValues[i]
	}

	if !isUpdate {
		query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", dstSchema, dstTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		_, err := s.targetDB.Exec(query, args...)
		if err != nil {
			s.logger.Errorf("[PostgreSQL] [INSERT] {src_schema: %s, src_table: %s} => {dst_db: %s, dst_schema: %s, dst_table: %s} Error: %v", srcSchema, srcTable, dstDB, dstSchema, dstTable, err)
		} else {
			s.logger.Infof("[PostgreSQL] [INSERT] {src_schema: %s, src_table: %s} => {dst_db: %s, dst_schema: %s, dst_table: %s} Values: %+v", srcSchema, srcTable, dstDB, dstSchema, dstTable, colValues)
		}
	} else {
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

		query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", dstSchema, dstTable, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
		_, err := s.targetDB.Exec(query, args...)
		if err != nil {
			s.logger.Errorf("[PostgreSQL] [UPDATE] {src_schema: %s, src_table: %s} => {dst_db: %s, dst_schema: %s, dst_table: %s} Error: %v", srcSchema, srcTable, dstDB, dstSchema, dstTable, err)
		} else {
			s.logger.Infof("[PostgreSQL] [UPDATE] {src_schema: %s, src_table: %s} => {dst_db: %s, dst_schema: %s, dst_table: %s} Old Keys: %+v, New Values: %+v", srcSchema, srcTable, dstDB, dstSchema, dstTable, keyValues, colValues)
		}
	}
}
