package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

const (
	host     = "postgresql"
	port     = 5432
	user     = "okteto"
	password = "okteto"
	dbname   = "rentals"
)

func Open() (*sql.DB, error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	return sql.Open("postgres", psqlconn)
}

func LoadData(db *sql.DB) error {
	// Create rentals table
	dropRentalsStmt := `DROP TABLE IF EXISTS rentals`
	if _, err := db.Exec(dropRentalsStmt); err != nil {
		return err
	}

	createRentalsStmt := `CREATE TABLE IF NOT EXISTS rentals (id VARCHAR(255) NOT NULL UNIQUE, price VARCHAR(255) NOT NULL, namespace VARCHAR(255) NOT NULL DEFAULT '')`
	if _, err := db.Exec(createRentalsStmt); err != nil {
		return err
	}

	return nil
}

func Ping(db *sql.DB) {
	log.Println("Waiting for postgresql...")
	for {
		if err := db.Ping(); err == nil {
			log.Println("Postgresql connected!")
			return
		}

		time.Sleep(1 * time.Second)
	}
}

// createOrUpdateRental creates or updates a rental entry in the database,
// stamping it with the namespace of the worker that processed it — this is
// what lets the UI show which personal deployment actually handled a rental.
// title and requestID are log-only correlation metadata (see kafka.go) — they
// aren't persisted.
func CreateOrUpdateRental(db *sql.DB, rentalID string, rentalPrice string, namespace string, title string, requestID string) error {
	log.Printf("[req=%s] Received internal request to create/update rental for %q (id=%s)", requestID, title, rentalID)

	insertDynStmt := `insert into "rentals"("id", "price", "namespace") values($1, $2, $3) on conflict(id) do update set price = $2, namespace = $3`
	if _, err := db.Exec(insertDynStmt, rentalID, rentalPrice, namespace); err != nil {
		return err
	}

	return nil
}

// deleteRental deletes a rental entry from the database. title and requestID
// are log-only correlation metadata (see kafka.go) — they aren't persisted.
func DeleteRental(db *sql.DB, rentalID string, title string, requestID string) error {
	log.Printf("[req=%s] Received internal request to delete rental: %q (ID=%s)\n", requestID, title, rentalID)
	deleteStmt := `DELETE FROM rentals WHERE id = $1`
	if _, err := db.Exec(deleteStmt, rentalID); err != nil {
		return err
	}

	return nil
}
