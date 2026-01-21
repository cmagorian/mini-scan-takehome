// Package main provides the entry point for the scan processor application.
// The processor consumes scan results from a Google Pub/Sub subscription and
// persists them to a database using a high-throughput, concurrent architecture.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/cmagorian/scan-takehome/pkg/repository/sqlite"
	"github.com/cmagorian/scan-takehome/pkg/scanning"
	"github.com/cmagorian/scan-takehome/pkg/service"
)

func Run(ctx context.Context, projectID, subscriptionID, dbPath string, logger *log.Logger) error {
	repo, err := sqlite.NewSQLiteRepository(dbPath, logger)
	if err != nil {
		return err
	}

	svc := service.NewScanService(repo, logger)

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	sub := client.Subscription(subscriptionID)
	sub.ReceiveSettings.NumGoroutines = 64
	sub.ReceiveSettings.MaxOutstandingMessages = 1000

	batchSize := 100
	batchTimeout := 500 * time.Millisecond
	workerPoolSize := 8

	msgChan := make(chan *pubsub.Message, 1000)
	batchChan := make(chan []*pubsub.Message, workerPoolSize)

	var wg sync.WaitGroup

	for i := 0; i < workerPoolSize; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for batch := range batchChan {
				logger.Printf("Worker %d: Processing batch of %d", id, len(batch))
				var scans []*scanning.Scan
				for _, msg := range batch {
					var scan scanning.Scan
					if err := json.Unmarshal(msg.Data, &scan); err != nil {
						logger.Printf("Worker %d: unmarshal failed: %v", id, err)
						msg.Ack()
						continue
					}
					scans = append(scans, &scan)
				}

				if len(scans) > 0 {
					for _, s := range scans {
						logger.Printf("Worker %d: Writing record for %s:%d (%s)", id, s.Ip, s.Port, s.Service)
					}
					if err := svc.ProcessScans(ctx, scans); err != nil {
						logger.Printf("Worker %d: process failed: %v", id, err)
						for _, m := range batch {
							m.Nack()
						}
					} else {
						for _, m := range batch {
							m.Ack()
						}
					}
				}
			}
		}(i)
	}

	go func() {
		defer close(batchChan)
		for {
			var batch []*pubsub.Message
			timeout := time.After(batchTimeout)
			finished := false

			for len(batch) < batchSize && !finished {
				select {
				case msg, ok := <-msgChan:
					if !ok {
						finished = true
						break
					}
					batch = append(batch, msg)
				case <-timeout:
					finished = true
				case <-ctx.Done():
					finished = true
				}
			}

			if len(batch) > 0 {
				select {
				case batchChan <- batch:
				case <-ctx.Done():
					for _, m := range batch {
						m.Nack()
					}
					return
				}
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			msg.Nack()
		}
	})

	close(msgChan)
	wg.Wait()

	if err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func main() {
	projectID := flag.String("project", "test-project", "GCP Project ID")
	subscriptionID := flag.String("subscription", "scan-sub", "GCP PubSub Subscription ID")
	dbPath := flag.String("db", "scans.db", "Path to SQLite database")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Printf("Starting processor for project %s, subscription %s", *projectID, *subscriptionID)

	if err := Run(ctx, *projectID, *subscriptionID, *dbPath, logger); err != nil {
		logger.Fatalf("Processor failed: %v", err)
	}
	logger.Println("Processor shut down gracefully")
}
