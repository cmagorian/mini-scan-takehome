package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/censys/scan-takehome/pkg/repository"
	_ "github.com/mattn/go-sqlite3"
)

type Repository struct {
	db     *sql.DB
	logger *log.Logger
}

func NewSQLiteRepository(dbPath string, logger *log.Logger) (repository.Repository, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Performance tuning
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA busy_timeout = 5000;
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("set pragmas: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS scans (
			ip TEXT NOT NULL,
			port INTEGER NOT NULL,
			service TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			response TEXT,
			PRIMARY KEY (ip, port, service)
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}

	return &Repository{db: db, logger: logger}, nil
}

func (r *Repository) UpsertScan(ctx context.Context, record *repository.ScanRecord) error {
	r.logger.Printf("Repository: Upserting record for %s:%d (%s)", record.IP, record.Port, record.Service)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO scans (ip, port, service, timestamp, response)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(ip, port, service) DO UPDATE SET
			response = excluded.response,
			timestamp = excluded.timestamp
		WHERE excluded.timestamp > scans.timestamp
	`, record.IP, record.Port, record.Service, record.Timestamp, record.Response)

	if err != nil {
		return fmt.Errorf("upsert scan: %w", err)
	}

	return nil
}

func (r *Repository) UpsertScans(ctx context.Context, records []*repository.ScanRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO scans (ip, port, service, timestamp, response)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(ip, port, service) DO UPDATE SET
			response = excluded.response,
			timestamp = excluded.timestamp
		WHERE excluded.timestamp > scans.timestamp
	`)
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		r.logger.Printf("Repository: Upserting record for %s:%d (%s)", record.IP, record.Port, record.Service)
		_, err := stmt.ExecContext(ctx, record.IP, record.Port, record.Service, record.Timestamp, record.Response)
		if err != nil {
			return fmt.Errorf("exec upsert: %w", err)
		}
	}

	return tx.Commit()
}

func (r *Repository) GetScan(ctx context.Context, ip string, port uint32, service string) (*repository.ScanRecord, error) {
	row := r.db.QueryRowContext(ctx, "SELECT ip, port, service, timestamp, response FROM scans WHERE ip = ? AND port = ? AND service = ?", ip, port, service)

	var record repository.ScanRecord
	err := row.Scan(&record.IP, &record.Port, &record.Service, &record.Timestamp, &record.Response)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}
