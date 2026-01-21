package service

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/censys/scan-takehome/pkg/repository"
	"github.com/censys/scan-takehome/pkg/scanning"
)

type mockRepository struct {
	upsertFunc func(ctx context.Context, record *repository.ScanRecord) error
}

func (m *mockRepository) UpsertScan(ctx context.Context, record *repository.ScanRecord) error {
	return m.upsertFunc(ctx, record)
}

func (m *mockRepository) UpsertScans(ctx context.Context, records []*repository.ScanRecord) error {
	for _, r := range records {
		if err := m.upsertFunc(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockRepository) GetScan(ctx context.Context, ip string, port uint32, service string) (*repository.ScanRecord, error) {
	return nil, nil
}

func TestProcessScan(t *testing.T) {
	ctx := context.Background()
	var capturedRecord *repository.ScanRecord

	repo := &mockRepository{
		upsertFunc: func(ctx context.Context, record *repository.ScanRecord) error {
			capturedRecord = record
			return nil
		},
	}

	logger := log.New(io.Discard, "", 0)
	svc := NewScanService(repo, logger)

	scan := &scanning.Scan{
		Ip:          "1.2.3.4",
		Port:        80,
		Service:     "HTTP",
		Timestamp:   1234567890,
		DataVersion: scanning.V2,
		Data:        &scanning.V2Data{ResponseStr: "hello"},
	}

	err := svc.ProcessScan(ctx, scan)
	if err != nil {
		t.Fatalf("ProcessScan failed: %v", err)
	}

	if capturedRecord == nil {
		t.Fatal("Expected record to be captured by mock repository")
	}

	if capturedRecord.IP != "1.2.3.4" {
		t.Errorf("Expected Ip 1.2.3.4, got %s", capturedRecord.IP)
	}

	if capturedRecord.Timestamp.Unix() != 1234567890 {
		t.Errorf("Expected timestamp 1234567890, got %d", capturedRecord.Timestamp.Unix())
	}

	if capturedRecord.DataVersion != int(scanning.V2) {
		t.Errorf("Expected Version V2, got %d", capturedRecord.DataVersion)
	}

	if v2, ok := capturedRecord.RawData.(*scanning.V2Data); !ok || v2.ResponseStr != "hello" {
		t.Errorf("Expected RawData to be V2 with 'hello', got %v", capturedRecord.RawData)
	}

	if capturedRecord.Response != "hello" {
		t.Errorf("Expected Response 'hello', got %s", capturedRecord.Response)
	}
}

func TestProcessScan_V1(t *testing.T) {
	ctx := context.Background()
	var capturedRecord *repository.ScanRecord

	repo := &mockRepository{
		upsertFunc: func(ctx context.Context, record *repository.ScanRecord) error {
			capturedRecord = record
			return nil
		},
	}

	logger := log.New(io.Discard, "", 0)
	svc := NewScanService(repo, logger)

	scan := &scanning.Scan{
		Ip:          "1.2.3.4",
		Port:        80,
		Service:     "HTTP",
		Timestamp:   1234567890,
		DataVersion: scanning.V1,
		Data:        &scanning.V1Data{ResponseBytesUtf8: []byte("hello v1")},
	}

	err := svc.ProcessScan(ctx, scan)
	if err != nil {
		t.Fatalf("ProcessScan failed: %v", err)
	}

	if capturedRecord.Response != "hello v1" {
		t.Errorf("Expected Response 'hello v1', got %s", capturedRecord.Response)
	}
}
