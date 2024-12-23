package postgresql

import (
	"database/sql"

	_ "github.com/lib/pq"
)

// GetPostgreSQLDB returns a PostgreSQL database connection
func GetPostgreSQLDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}
