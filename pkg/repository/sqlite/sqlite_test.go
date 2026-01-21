// Package sqlite_test provides unit tests for the SQLite repository implementation.
package sqlite

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/cmagorian/scan-takehome/pkg/repository"
	"github.com/cmagorian/scan-takehome/pkg/scanning"
)

// TestSQLiteRepository_UpsertScan verifies the core idempotency and out-of-order
// handling logic of the repository.
func TestSQLiteRepository_UpsertScan(t *testing.T) {
	// Use an in-memory database for fast, isolated unit testing.
	logger := log.New(io.Discard, "", 0)
	repo, err := NewSQLiteRepository(":memory:", logger)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}

	ctx := context.Background()
	ip := "1.2.3.4"
	port := uint32(80)
	service := "HTTP"

	// 1. Initial Insert: verify that a new record can be saved.
	t1 := time.Now().Truncate(time.Second)
	rec1 := &repository.ScanRecord{
		IP:          ip,
		Port:        port,
		Service:     service,
		Timestamp:   t1,
		DataVersion: int(scanning.V2),
		RawData:     &scanning.V2Data{ResponseStr: "first"},
		Response:    "first",
	}

	if err := repo.UpsertScan(ctx, rec1); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err := repo.GetScan(ctx, ip, port, service)
	if err != nil {
		t.Fatalf("GetScan failed: %v", err)
	}
	if got.Response != "first" {
		t.Errorf("Expected first, got %s", got.Response)
	}

	// 2. Update with newer timestamp: verify that existing records are updated
	// when newer information arrives.
	t2 := t1.Add(time.Hour)
	rec2 := &repository.ScanRecord{
		IP:          ip,
		Port:        port,
		Service:     service,
		Timestamp:   t2,
		DataVersion: int(scanning.V2),
		RawData:     &scanning.V2Data{ResponseStr: "second"},
		Response:    "second",
	}

	if err := repo.UpsertScan(ctx, rec2); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err = repo.GetScan(ctx, ip, port, service)
	if err != nil {
		t.Fatalf("GetScan failed: %v", err)
	}
	if got.Response != "second" {
		t.Errorf("Expected second, got %s", got.Response)
	}

	// 3. Try to update with older timestamp: verify that late-arriving (out-of-order)
	// data does NOT overwrite newer data.
	t3 := t1.Add(-time.Hour)
	rec3 := &repository.ScanRecord{
		IP:          ip,
		Port:        port,
		Service:     service,
		Timestamp:   t3,
		DataVersion: int(scanning.V2),
		RawData:     &scanning.V2Data{ResponseStr: "third (old)"},
		Response:    "third (old)",
	}

	if err := repo.UpsertScan(ctx, rec3); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err = repo.GetScan(ctx, ip, port, service)
	if err != nil {
		t.Fatalf("GetScan failed: %v", err)
	}
	if got.Response != "second" {
		t.Errorf("Expected second to remain, got %s", got.Response)
	}
	if !got.Timestamp.Equal(t2) {
		t.Errorf("Expected timestamp %v, got %v", t2, got.Timestamp)
	}

	// 4. Extreme out-of-order (24 hours old): explicitly test the case mentioned
	// in the problem description.
	t4 := t2.Add(-24 * time.Hour)
	rec4 := &repository.ScanRecord{
		IP:          ip,
		Port:        port,
		Service:     service,
		Timestamp:   t4,
		DataVersion: int(scanning.V2),
		RawData:     &scanning.V2Data{ResponseStr: "extreme old"},
		Response:    "extreme old",
	}

	if err := repo.UpsertScan(ctx, rec4); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err = repo.GetScan(ctx, ip, port, service)
	if err != nil {
		t.Fatalf("GetScan failed: %v", err)
	}
	if got.Response != "second" {
		t.Errorf("Expected second to remain after 24h old scan, got %s", got.Response)
	}
	if !got.Timestamp.Equal(t2) {
		t.Errorf("Expected timestamp %v to remain, got %v", t2, got.Timestamp)
	}
}

// TestSQLiteRepository_ErrorHandling ensures that errors (like a closed DB)
// are correctly propagated to the caller.
func TestSQLiteRepository_ErrorHandling(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	repo, err := NewSQLiteRepository(":memory:", logger)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}

	ctx := context.Background()

	// Intentionally close the database to trigger errors.
	sqliteRepo := repo.(*Repository)
	sqliteRepo.db.Close()

	rec := &repository.ScanRecord{
		IP:          "1.1.1.1",
		Port:        80,
		Service:     "HTTP",
		Timestamp:   time.Now(),
		DataVersion: int(scanning.V2),
		RawData:     &scanning.V2Data{ResponseStr: "test"},
	}

	// Test single upsert error handling.
	err = repo.UpsertScan(ctx, rec)
	if err == nil {
		t.Error("Expected error when database is closed, got nil")
	}

	// Test batch upsert error handling.
	err = repo.UpsertScans(ctx, []*repository.ScanRecord{rec})
	if err == nil {
		t.Error("Expected error in batch upsert when database is closed, got nil")
	}
}

// TestSQLiteRepository_Constraints verifies that database-level constraints
// (like NOT NULL) are enforced.
func TestSQLiteRepository_Constraints(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	repo, err := NewSQLiteRepository(":memory:", logger)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	ctx := context.Background()

	// Test NOT NULL constraint on Ip.
	// Since we use the sql.DB directly here, we can force a NULL value that
	// the Go struct wouldn't normally allow.
	sqliteRepo := repo.(*Repository)
	_, err = sqliteRepo.db.ExecContext(ctx, "INSERT INTO scans (ip, port, service, timestamp, response) VALUES (NULL, 80, 'HTTP', '2023-01-01', 'test')")
	if err == nil {
		t.Error("Expected error when inserting NULL Ip, got nil")
	}
}

// TestSQLiteRepository_UpsertScans verifies that batch processing works
// correctly for multiple records.
func TestSQLiteRepository_UpsertScans(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	repo, err := NewSQLiteRepository(":memory:", logger)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}

	ctx := context.Background()

	recs := []*repository.ScanRecord{
		{
			IP:          "1.1.1.1",
			Port:        80,
			Service:     "HTTP",
			Timestamp:   time.Now(),
			DataVersion: int(scanning.V2),
			RawData:     &scanning.V2Data{ResponseStr: "one"},
			Response:    "one",
		},
		{
			IP:          "1.1.1.2",
			Port:        443,
			Service:     "HTTPS",
			Timestamp:   time.Now(),
			DataVersion: int(scanning.V2),
			RawData:     &scanning.V2Data{ResponseStr: "two"},
			Response:    "two",
		},
	}

	if err := repo.UpsertScans(ctx, recs); err != nil {
		t.Fatalf("UpsertScans failed: %v", err)
	}

	for _, wantRecord := range recs {
		got, err := repo.GetScan(ctx, wantRecord.IP, wantRecord.Port, wantRecord.Service)
		if err != nil {
			t.Fatalf("GetScan failed for %s: %v", wantRecord.IP, err)
		}
		wantResponse := wantRecord.RawData.(*scanning.V2Data).ResponseStr
		if got == nil || got.Response != wantResponse {
			t.Errorf("Expected response %s for %s, got %v", wantResponse, wantRecord.IP, got)
		}
	}
}

// TestSQLiteRepository_Versions ensures that the repository correctly handles
// both V1 (base64) and V2 (plain text) data formats.
func TestSQLiteRepository_Versions(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	repo, err := NewSQLiteRepository(":memory:", logger)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	ctx := context.Background()

	tests := []struct {
		name     string
		record   *repository.ScanRecord
		expected string
	}{
		{
			name: "V1 base64: 'hello world'",
			record: &repository.ScanRecord{
				IP:          "1.1.1.1",
				Port:        80,
				Service:     "HTTP",
				Timestamp:   time.Now(),
				DataVersion: int(scanning.V1),
				RawData:     &scanning.V1Data{ResponseBytesUtf8: []byte("hello world")},
				Response:    "hello world",
			},
			expected: "hello world",
		},
		{
			name: "V2 plain text: 'hello world'",
			record: &repository.ScanRecord{
				IP:          "1.1.1.2",
				Port:        80,
				Service:     "HTTP",
				Timestamp:   time.Now(),
				DataVersion: int(scanning.V2),
				RawData:     &scanning.V2Data{ResponseStr: "hello world"},
				Response:    "hello world",
			},
			expected: "hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := repo.UpsertScan(ctx, tt.record); err != nil {
				t.Fatalf("UpsertScan failed: %v", err)
			}
			got, err := repo.GetScan(ctx, tt.record.IP, tt.record.Port, tt.record.Service)
			if err != nil {
				t.Fatalf("GetScan failed: %v", err)
			}
			if got.Response != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, got.Response)
			}
		})
	}
}
