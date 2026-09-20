# raf

`raf` is a local HTTP bridge for testing Kafka services that use Protobuf. Send a message as ProtoJSON from Postman, or inspect recent messages as JSON. Kafka values contain raw Protobuf bytes, without a Schema Registry prefix.

## Try it with Docker Compose

From the repository directory, start raf and a separate local Kafka broker:

```sh
docker compose up --build -d
curl http://localhost:18080/types
curl -X POST 'http://localhost:18080/topics/example.events/messages?key=example-1' \
  -H 'Content-Type: application/json' \
  -d '{"id":"example-1","status":"CREATED"}'
curl 'http://localhost:18080/topics/example.events/messages?limit=10'
```

Compose waits for Kafka and creates `example.events` with three partitions before starting raf. The example contract and its topic mapping are already configured. The broker uses the official [Apache Kafka 4.1.2 image](https://kafka.apache.org/41/getting-started/docker/).

The HTTP API is available at `localhost:18080`; Kafka is available to host applications at `localhost:19092` and to Compose services at `kafka:9092`. These ports are bound to the loopback interface. Override `RAF_HTTP_PORT` and `RAF_KAFKA_PORT` in your shell or copy [.env.example](.env.example) to `.env`. To debug raf in GoLand with this broker, start only the broker and topic initializer with `docker compose up -d kafka-init`, then use `RAF_KAFKA_BROKERS=localhost:19092` in GoLand.

Use `docker compose logs -f raf` to view logs and `docker compose stop` to stop the example while keeping its data. `docker compose down` removes the containers and their Kafka data; this example has no persistent data volume.

## Run locally

Set the broker address and the `.proto` files to load. Go 1.24 or newer is required; CI and the Docker build use Go 1.26.

```sh
export RAF_KAFKA_BROKERS=localhost:9092
export RAF_PROTO_IMPORT_PATHS=./examples/proto
export RAF_PROTO_FILES=event.proto
go run ./cmd/raf
```

`RAF_HTTP_ADDR` defaults to `:8080`. Multiple brokers and source files can be separated by commas. For a real contract, point the import paths at the directories containing its `.proto` files and their imports, then list the root files whose message types you want to use.

In GoLand, create a Go Build configuration for the **package** `./cmd/raf`, use the repository as the working directory, and set the same environment variables there. Run it with Debug. The application does not load `.env` files itself.

## Configure contracts and topic types

| Variable | Meaning | Default |
| --- | --- | --- |
| `RAF_KAFKA_BROKERS` | Comma-separated broker addresses | Required |
| `RAF_PROTO_FILES` | Comma-separated root `.proto` file names | Required |
| `RAF_PROTO_IMPORT_PATHS` | Directories used to find root files and imports | `.` |
| `RAF_TOPIC_TYPES` | JSON object mapping topic names to Protobuf message names | No mappings |
| `RAF_HTTP_ADDR` | HTTP listen address | `:8080` |

File names in `RAF_PROTO_FILES` are relative to an import directory. `RAF_PROTO_IMPORT_PATHS` uses the operating system's path separator: `:` on macOS/Linux, `;` on Windows. For example, given contracts in two repositories:

```text
/path/to/events/api/event.proto
/path/to/projects/api/project.proto
```

Load both roots on macOS/Linux with:

```sh
export RAF_PROTO_IMPORT_PATHS=/path/to/events/api:/path/to/projects/api
export RAF_PROTO_FILES=event.proto,project.proto
export RAF_TOPIC_TYPES='{"example.events":"example.Event","projects.changed":"projects.ProjectChanged"}'
```

Keep both directories in `RAF_PROTO_IMPORT_PATHS` when both files are listed. Compilation errors report the requested files and the absolute import directories; invalid directories are rejected at startup. Imported contracts are loaded automatically, including standard Google Protobuf types. Restart raf after changing contracts or configuration.

To discover the exact message names, request:

```sh
curl http://localhost:8080/types
# {"types":["example.Event"]}
```

The list is sorted and includes nested and imported message types, excluding synthetic map-entry types. Use the full name without a trailing dot. Mappings in `RAF_TOPIC_TYPES` are validated at startup. For both POST and GET, a nonempty `type` query parameter overrides the configured topic mapping; without either, the API returns HTTP 400.

## Publish

Use the fully qualified Protobuf message name in `type`. The HTTP body follows the [ProtoJSON mapping](https://protobuf.dev/programming-guides/json/), including its rules for timestamps, enums, and 64-bit integers. Unknown fields and invalid values return HTTP 400.

```sh
curl -X POST 'http://localhost:8080/topics/example.events/messages?type=example.Event&key=example-1' \
  -H 'Content-Type: application/json' \
  -H 'X-Raf-Headers: {"id":"manual-test-1"}' \
  -d '{"id":"example-1","status":"CREATED"}'
```

`key` and `X-Raf-Headers` are optional. The header contains a JSON object with string values; its names are written as Kafka header names. The JSON body is limited to 1 MiB. A successful response is HTTP 201 and includes the topic, partition, and offset. Each POST is sent immediately without waiting to fill a producer batch.

The Kafka topic must already exist; `raf` does not create topics automatically.

## Inspect recent messages

```sh
curl 'http://localhost:8080/topics/example.events/messages?type=example.Event&limit=10'
```

`limit` defaults to 10 and accepts 1–100. It caps the total response across all partitions. The endpoint reads without joining a consumer group or committing offsets, so it does not advance the tested service's position.

For each partition, raf captures the available offset range, then reads up to `limit` of the last retained records by offset. It searches older offsets when compaction leaves gaps. These candidates are combined, sorted by record timestamp descending, and trimmed to the total `limit`. Equal timestamps are ordered by partition ascending, then offset descending. This is a view of partition tails: a much older record with a later timestamp is not guaranteed to appear, and Kafka has no single order across partitions.

Reads use batches and at most four partitions are read concurrently. Raf does not wait for new messages or include records beyond each partition's captured end offset. Empty partitions return no records. Each partition captures its range independently; this is not an atomic snapshot of the whole topic. Retention or compaction during the request can remove records before they are read. The request returns the records still available, or an error if reading fails; it does not return a silently truncated partial result after a timeout.

Each record contains `partition`, `offset`, `time`, `key`, `headers`, and its decoded `value`:

- A Kafka tombstone has `"tombstone": true` and `"value": null`. A zero-byte Protobuf message is decoded normally, usually as `{}`.
- A missing key or header value is `null`; a present empty value is `""`. Binary keys that are not valid UTF-8 use `key_base64` with `key: null`. Binary header values use `value_base64` with `value: null` in the header entry.
- If a message value cannot be decoded, that record contains `decode_error` and `value_base64`, while other records are still returned.

Raw Protobuf bytes do not identify their message type. Choose the type that the consumer expects for the topic: a wrong but wire-compatible type can decode successfully with misleading or missing fields.

## Timeouts and errors

An HTTP request waits at most 15 seconds for Kafka; cancellation stops that wait. Individual Kafka network operations have shorter deadlines. A broker timeout returns HTTP 504; other broker failures return HTTP 502. The producer may continue delivering a queued message after the HTTP request times out or is cancelled, so neither outcome establishes whether Kafka accepted the record. Inspect the topic before retrying if duplicates matter.

The HTTP server allows 5 seconds for request headers, 15 seconds for reading a request, and 20 seconds for writing a response. On interruption or SIGTERM, active HTTP requests and Kafka shutdown share a 10-second grace period. If that expires, raf cancels request contexts, stops waiting for the producer, cancels background broker discovery, closes idle connections, and exits with an error. Active Kafka network requests retain their own deadlines; they do not keep the process from exiting.

`GET /healthz` returns HTTP 204 while the HTTP server is running. It is a liveness endpoint and does not verify Kafka availability.

The [Postman collection](postman/raf.postman_collection.json) includes `/types`, publish and read requests, and variants using configured topic types. Set `base_url` to `http://localhost:18080` for Compose or `http://localhost:8080` for the default local launch.

## Docker

```sh
docker build -t raf .
docker run --rm -p 127.0.0.1:8080:8080 \
  -v "$PWD/examples/proto:/protos:ro" \
  -e RAF_KAFKA_BROKERS=host.docker.internal:9092 \
  -e RAF_PROTO_IMPORT_PATHS=/protos \
  -e RAF_PROTO_FILES=event.proto \
  raf
```

The broker's advertised address must be reachable from the container. Mount your own `.proto` directories when testing real topics. `raf` currently supports plaintext Kafka connections and raw Protobuf values; Schema Registry framing and broker authentication are not configured yet.

## Development checks

The [architecture guide](docs/architecture.md) describes the package structure, request flow, and application lifecycle in Russian.

```sh
go test -race -count=1 ./...
go vet ./...
go build ./cmd/raf
```

Kafka integration tests are skipped unless a dedicated test broker is configured. To run them against the Compose broker:

```sh
docker compose up -d --wait --wait-timeout 180 kafka
RAF_TEST_KAFKA_BROKERS=localhost:19092 go test -race -count=1 ./...
```

The integration tests create uniquely named `raf-test-...` topics and delete them afterward. [CI](.github/workflows/ci.yml) starts its own Kafka broker, runs the tests with the race detector and `go vet`, and builds the Go binary and Docker image.
