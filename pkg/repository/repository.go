package repository

import (
	"context"
	"time"
)

type ScanRecord struct {
	IP          string
	Port        uint32
	Service     string
	Timestamp   time.Time
	DataVersion int
	RawData     interface{}
	Response    string
}

type Repository interface {
	UpsertScan(ctx context.Context, record *ScanRecord) error
	UpsertScans(ctx context.Context, records []*ScanRecord) error
	GetScan(ctx context.Context, ip string, port uint32, service string) (*ScanRecord, error)
}
