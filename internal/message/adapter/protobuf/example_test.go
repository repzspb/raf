package protobuf_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/repzspb/raf/internal/message/adapter/protobuf"
)

func TestExampleContainsContractFields(t *testing.T) {
	codec := newTestCodec(t)
	body, err := codec.Example(t.Context(), "example.Record")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode("example.Record", body); err != nil {
		t.Fatalf("example is not publishable: %s: %v", body, err)
	}
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"id", "usage", "at", "state", "raw", "note", "optionalCount", "meta", "child", "imported"} {
		if _, ok := value[field]; !ok {
			t.Errorf("missing field %s: %s", field, body)
		}
	}
	if _, ok := value["count"]; ok {
		t.Fatal("both oneof alternatives were generated")
	}
	if value["usage"] != "1" || value["state"] != "UNKNOWN" || value["at"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("incorrect ProtoJSON representations: %s", body)
	}
}

func TestExamplesSupportRecursionAndWellKnownTypes(t *testing.T) {
	dir := t.TempDir()
	writeProto(t, dir, "example.proto", `syntax = "proto3";
package samples;
import "google/protobuf/any.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/field_mask.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";
message Node {
  Node child = 1;
  repeated Node children = 2;
  map<string, Node> indexed = 3;
  repeated int64 numbers = 4;
  map<bool, uint64> counters = 5;
  oneof choice { Node recursive = 6; string text = 7; }
  google.protobuf.Any payload = 8;
  google.protobuf.Value value = 9;
  optional bool enabled = 10;
}`)
	codec, err := protobuf.New(t.Context(), []string{"example.proto"}, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range codec.MessageTypes() {
		t.Run(name, func(t *testing.T) {
			body, err := codec.Example(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := codec.Encode(name, body); err != nil {
				t.Fatalf("example is not publishable: %s: %v", body, err)
			}
			if name == "samples.Node" {
				assertJSONEqual(t, []byte(`{"children":[],"indexed":{},"numbers":["1"],"counters":{"true":"1"},"text":"example","payload":{},"value":"example","enabled":true}`), body)
			}
		})
	}
}

func TestExampleProto2AndGenerationLimits(t *testing.T) {
	dir := t.TempDir()
	writeProto(t, dir, "legacy.proto", `syntax = "proto2"; package legacy;
message Valid { required int32 id = 1; optional string note = 2; }
message Recursive { required Recursive child = 1; }`)
	codec, err := protobuf.New(t.Context(), []string{"legacy.proto"}, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	body, err := codec.Example(t.Context(), "legacy.Valid")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode("legacy.Valid", body); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Example(t.Context(), "legacy.Recursive"); err == nil {
		t.Fatal("required recursion must produce an error")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := codec.Example(ctx, "legacy.Valid"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if _, err := codec.Example(t.Context(), "missing.Type"); err == nil {
		t.Fatal("unknown type accepted")
	}
	var schema strings.Builder
	schema.WriteString(`syntax = "proto3"; message Wide {`)
	for i := 1; i <= 2050; i++ {
		fmt.Fprintf(&schema, "string field%d = %d;\n", i, i)
	}
	schema.WriteString("}")
	writeProto(t, dir, "wide.proto", schema.String())
	codec, err = protobuf.New(t.Context(), []string{"wide.proto"}, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Example(t.Context(), "Wide"); err == nil {
		t.Fatal("field budget ignored")
	}
}
