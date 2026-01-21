package service

import (
	"context"
	"log"
	"time"

	"github.com/cmagorian/scan-takehome/pkg/repository"
	"github.com/cmagorian/scan-takehome/pkg/scanning"
)

type scanService struct {
	repo   repository.Repository
	logger *log.Logger
}

func NewScanService(repo repository.Repository, logger *log.Logger) scanning.Service {
	return &scanService{repo: repo, logger: logger}
}

func (s *scanService) ProcessScan(ctx context.Context, scan *scanning.Scan) error {
	return s.repo.UpsertScan(ctx, s.toRecord(scan))
}

func (s *scanService) ProcessScans(ctx context.Context, scans []*scanning.Scan) error {
	records := make([]*repository.ScanRecord, len(scans))
	for i, scan := range scans {
		records[i] = s.toRecord(scan)
	}
	return s.repo.UpsertScans(ctx, records)
}

func (s *scanService) toRecord(scan *scanning.Scan) *repository.ScanRecord {
	var resp string
	switch scan.DataVersion {
	case scanning.V1:
		if v1, ok := scan.Data.(*scanning.V1Data); ok {
			resp = string(v1.ResponseBytesUtf8)
		}
	case scanning.V2:
		if v2, ok := scan.Data.(*scanning.V2Data); ok {
			resp = v2.ResponseStr
		}
	}

	return &repository.ScanRecord{
		IP:          scan.Ip,
		Port:        scan.Port,
		Service:     scan.Service,
		Timestamp:   time.Unix(scan.Timestamp, 0),
		DataVersion: int(scan.DataVersion),
		RawData:     scan.Data,
		Response:    resp,
	}
}
