package mariadb

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// GetMariaDBDB returns a MariaDB database connection (MySQL compatible DSN)
func GetMariaDBDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn) // MariaDB is MySQL compatible
	if err != nil {
		return nil, err
	}
	return db, nil
}
