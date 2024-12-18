package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// GetMySQLDB returns a MySQL database connection
func GetMySQLDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}
