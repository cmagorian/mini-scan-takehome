# Mini-Scan

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

## Solution Documentation

The solution implements a data processor that consumes scan results from Google Pub/Sub and stores them in a SQLite database.

### Components

- **Processor**: A Go application that subscribes to `scan-sub`, unmarshals scan results (handling V1 and V2 formats), and persists them.
- **Repository**: An abstraction for data storage, implemented using SQLite. It handles out-of-order messages by only updating records if the incoming scan has a newer timestamp.
- **Docker Integration**: The processor is integrated into the `docker-compose.yml` environment.

### Key Features

- **At-least-once semantics**: Messages are acknowledged only after successful persistence in the database.
- **Out-of-order handling**: Uses SQL `ON CONFLICT` with a `WHERE` clause to ensure only the latest scan data is kept for each unique `(ip, port, service)`.
- **Horizontal Scaling**: The processor can be scaled by running multiple instances against the same subscription (Pub/Sub handles distribution) and a shared or partitioned database.
- **High Throughput Optimizations**:
    - **Concurrency**: Uses multiple goroutines for Pub/Sub message consumption and a fixed worker pool for batch processing.
    - **Batching**: A dedicated dispatcher groups messages into batches (up to 100 messages or every 500ms) before passing them to the worker pool, significantly reducing I/O overhead.
    - **Database Tuning**: SQLite is configured in WAL (Write-Ahead Logging) mode with `synchronous = NORMAL` for optimal write performance while maintaining safety.
    - **Transactional Upserts**: Each batch is processed within a single database transaction.

### Testing Instructions

#### Makefile
A `Makefile` is provided for common tasks:
- `make build`: Build the `processor` and `scanner` binaries.
- `make test`: Run all unit tests.
- `make docker-up`: Start the full environment.
- `make clean`: Clean up binaries and the database.

#### Automated Tests
You can run all tests using the Makefile:
```bash
make test
```

To run the end-to-end tests that leverage `docker-compose`:
```bash
make test-e2e
```

Or run the repository tests directly (requires Go and `gcc` for `go-sqlite3`):
```bash
go test ./pkg/repository/...
```

#### Manual Verification
1. Start the environment:
   ```bash
   make docker-up
   ```
   (Alternatively: `docker compose up --build`)
2. The `processor` service will start consuming messages from the `scanner` service.
3. You can inspect the results by querying the `scans.db` file created in the project root:
   ```bash
   sqlite3 scans.db "SELECT * FROM scans LIMIT 10;"
   ```

---

## Original Requirements

Your job is to build the data processing side. It should:

1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
> The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
>
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go would be preferred.

You may use any data store of your choosing, with `sqlite` being one example. Like our own code, we expect the code structure to make it easy to switch data stores.

Please note that Google Pub/Sub is best effort ordering and we want to keep the latest scan. While the example scanner does not publish scans at a rate where this would be an issue, we expect the application to be able to handle extreme out of orderness. Consider what would happen if the application received a scan that is 24 hours old.

cmd/scanner/main.go should not be modified

---

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account. This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don’t hesitate to ask any follow-up questions for clarification.
