// Package e2e provides end-to-end tests for the entire mini-scan system.
// These tests verify the full data flow: from Pub/Sub message publishing to
// database persistence, ensuring all components (processor, service, repository)
// work together correctly.
package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/repository/sqlite"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/service"
	_ "github.com/mattn/go-sqlite3"
)

// testCase defines the parameters for a single E2E test run.
type testCase struct {
	name        string        // Description of the test
	numMessages int           // Number of scan results to simulate
	timeout     time.Duration // Maximum time to wait for processing to complete
}

// TestEndToEnd executes a series of table-driven end-to-end tests.
func TestEndToEnd(t *testing.T) {
	projectID := "test-project"
	topicID := "scan-topic-e2e"
	subscriptionID := "scan-sub-e2e"

	// Ensure PubSub emulator is used. The emulator is expected to be running
	// (usually started by the Makefile).
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("Failed to create pubsub client: %v", err)
	}
	defer client.Close()

	// Setup Topic: create it if it doesn't exist.
	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		t.Fatalf("Failed to check topic: %v", err)
	}
	if !exists {
		topic, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}
	}

	// Setup Subscription: create it if it doesn't exist.
	sub := client.Subscription(subscriptionID)
	exists, err = sub.Exists(ctx)
	if err != nil {
		t.Fatalf("Failed to check subscription: %v", err)
	}
	if !exists {
		_, err = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
			Topic: topic,
		})
		if err != nil {
			t.Fatalf("Failed to create subscription: %v", err)
		}
	}

	testCases := []testCase{
		{
			name:        "Process 10 messages",
			numMessages: 10,
			timeout:     10 * time.Second,
		},
		{
			name:        "Process 50 messages",
			numMessages: 50,
			timeout:     10 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary database file for each test case to ensure isolation.
			tmpDB := filepath.Join(t.TempDir(), "test.db")

			// Start the Processor logic in a background goroutine.
			procCtx, procCancel := context.WithCancel(ctx)
			defer procCancel()

			go func() {
				// We use a simplified version of the processor logic for testing.
				runProcessor(procCtx, projectID, subscriptionID, tmpDB)
			}()

			// Give the processor a moment to initialize and start its receive loop.
			time.Sleep(1 * time.Second)

			// 2. Publish messages (simulating the 'scanner' application).
			var sentCount int
			for i := 0; i < tc.numMessages; i++ {
				scan := &scanning.Scan{
					Ip:          fmt.Sprintf("192.168.1.%d", i),
					Port:        uint32(8080 + i),
					Service:     "HTTP",
					Timestamp:   time.Now().Unix(),
					DataVersion: scanning.V2,
					Data:        &scanning.V2Data{ResponseStr: fmt.Sprintf("resp-%d", i)},
				}
				data, _ := json.Marshal(scan)
				res := topic.Publish(ctx, &pubsub.Message{Data: data})
				_, err := res.Get(ctx)
				if err == nil {
					sentCount++
				}
			}
			t.Logf("Sent %d messages", sentCount)

			// 3. Wait and verify: poll the database until all messages are stored or timeout occurs.
			db, err := sql.Open("sqlite3", tmpDB)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			deadline := time.Now().Add(tc.timeout)
			var count int
			for time.Now().Before(deadline) {
				// Query the database to count the number of successfully persisted scans.
				_ = db.QueryRow("SELECT COUNT(*) FROM scans").Scan(&count)
				if count >= tc.numMessages {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			// Final verification: did we store exactly as many as we sent?
			if count != sentCount {
				t.Errorf("Expected %d records (sent), got %d (stored)", sentCount, count)
			} else {
				t.Logf("Successfully processed %d messages and verified in DB", count)
			}
		})
	}
}

// runProcessor is a helper function that spins up a minimal version of the
// processor application for testing purposes. It enables end-to-end verification
// without needing to run a separate binary.
func runProcessor(ctx context.Context, projectID, subscriptionID, dbPath string) {
	// Initialize real dependencies.
	logger := log.New(io.Discard, "", 0)
	repo, _ := sqlite.NewSQLiteRepository(dbPath, logger)
	svc := service.NewScanService(repo, logger)
	client, _ := pubsub.NewClient(ctx, projectID)
	defer client.Close()

	// Simplified receive loop for E2E testing.
	sub := client.Subscription(subscriptionID)
	_ = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var scan scanning.Scan
		if err := json.Unmarshal(msg.Data, &scan); err == nil {
			// Process and then Ack to maintain at-least-once semantics.
			if err := svc.ProcessScan(ctx, &scan); err == nil {
				msg.Ack()
			} else {
				msg.Nack()
			}
		} else {
			msg.Ack() // Ack malformed JSON to avoid loops.
		}
	})
}
