# raf

`raf` is a local HTTP bridge for testing Kafka services that use Protobuf. Send a message as ProtoJSON from Postman, or inspect recent messages as JSON. Kafka values contain raw Protobuf bytes, without a Schema Registry prefix.

## Run locally

Set the broker address and the `.proto` files to load. File names in `RAF_PROTO_FILES` are relative to one of the directories in `RAF_PROTO_IMPORT_PATHS`; imported `.proto` files are resolved from the same directories. The import path list uses the operating system's path separator (`:` on macOS and Linux).

```sh
export RAF_KAFKA_BROKERS=localhost:9092
export RAF_PROTO_IMPORT_PATHS=./examples/proto
export RAF_PROTO_FILES=event.proto
go run ./cmd/raf
```

`RAF_HTTP_ADDR` defaults to `:8080`. Multiple brokers and source files can be separated by commas. For a real contract, point the import paths at the directories containing its `.proto` files and their imports, then list the root files whose message types you want to use.

## Publish

Use the fully qualified Protobuf message name in `type`. The HTTP body follows the [ProtoJSON mapping](https://protobuf.dev/programming-guides/json/), including its rules for timestamps, enums, and 64-bit integers. Unknown fields and invalid values return HTTP 400.

```sh
curl -X POST 'http://localhost:8080/topics/example.events/messages?type=example.Event&key=example-1' \
  -H 'Content-Type: application/json' \
  -H 'X-Raf-Headers: {"id":"manual-test-1"}' \
  -d '{"id":"example-1","status":"CREATED"}'
```

`key` and `X-Raf-Headers` are optional. The header contains a JSON object with string values; its names are written as Kafka header names. A successful response includes the topic, partition, and offset.

The Kafka topic must already exist; `raf` does not create topics automatically.

## Inspect recent messages

```sh
curl 'http://localhost:8080/topics/example.events/messages?type=example.Event&limit=10'
```

`limit` defaults to 10 and accepts 1–100. It applies across all partitions. The endpoint reads without joining a consumer group or committing offsets, so it does not advance the tested service's position. Results are sorted by record timestamp for display; Kafka does not provide a single order across partitions. Each record includes its partition and offset. If a value cannot be decoded as the selected message type, the response includes `decode_error` and `value_base64` for that record. Raw Protobuf messages do not identify their type, so choose the type that the consumer expects for this topic.

The [Postman collection](postman/raf.postman_collection.json) contains both requests and editable variables.

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
