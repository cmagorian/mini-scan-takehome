package scanning

import (
	"context"
)

type Service interface {
	ProcessScan(ctx context.Context, scan *Scan) error
	ProcessScans(ctx context.Context, scans []*Scan) error
}
