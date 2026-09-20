package protobuf_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/repzspb/raf/internal/message/adapter/protobuf"
)

const recordProto = `syntax = "proto3";
package example;

import "google/protobuf/timestamp.proto";
import "common.proto";

message Record {
  enum State { UNKNOWN = 0; CREATED = 1; }
  message Child { string value = 1; }
  string id = 1;
  int64 usage = 2;
  google.protobuf.Timestamp at = 3;
  State state = 4;
  bytes raw = 5;
  oneof detail {
    string note = 6;
    int64 count = 7;
  }
  optional int64 optional_count = 8;
  map<string, string> meta = 9;
  Child child = 10;
  common.Imported imported = 11;
}
`

func newTestCodec(t *testing.T) *protobuf.Codec {
	t.Helper()
	root := t.TempDir()
	imports := t.TempDir()
	writeProto(t, root, "record.proto", recordProto)
	writeProto(t, imports, "common.proto", `syntax = "proto3"; package common; message Imported { string name = 1; }`)
	codec, err := protobuf.New(context.Background(), []string{"record.proto"}, []string{root, imports})
	if err != nil {
		t.Fatalf("load contracts from multiple import paths: %v", err)
	}
	return codec
}

func writeProto(
	t *testing.T,
	dir string,
	name string,
	source string,
) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	codec := newTestCodec(t)
	for _, test := range []struct {
		name string
		body string
	}{
		{
			name: "ProtoJSON field representations and explicit defaults",
			body: `{
				"id":"test-1",
				"usage":"9223372036854775807",
				"at":"2026-09-19T12:34:56.123Z",
				"state":"CREATED",
				"raw":"AP+A",
				"note":"",
				"optionalCount":"0",
				"meta":{"source":"test"},
				"child":{"value":"nested"},
				"imported":{"name":"imported contract"}
			}`,
		},
		{
			name: "numeric oneof preserves zero",
			body: `{"count":"0"}`,
		},
		{
			name: "absent optional fields stay absent",
			body: `{"id":"test-2"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := codec.Encode("example.Record", []byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := codec.Decode("example.Record", encoded)
			if err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, []byte(test.body), decoded)
		})
	}
}

func TestEncodeEmptyMessageIsNotTombstone(t *testing.T) {
	codec := newTestCodec(t)
	encoded, err := codec.Encode("example.Record", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if encoded == nil || len(encoded) != 0 {
		t.Fatalf("empty message must be non-nil zero bytes, got %#v", encoded)
	}
	decoded, err := codec.Decode("example.Record", encoded)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, []byte(`{}`), decoded)
}

func TestMessageTypesIncludesImportsAndNestedMessages(t *testing.T) {
	codec := newTestCodec(t)
	want := []string{
		"common.Imported",
		"example.Record",
		"example.Record.Child",
		"google.protobuf.Timestamp",
	}
	if got := codec.MessageTypes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("message names must be sorted and exclude enum/map entries: got %v, want %v", got, want)
	}
	for _, name := range want {
		if err := codec.ValidateType(name); err != nil {
			t.Errorf("advertised message %q is not usable: %v", name, err)
		}
	}
}

func TestCodecRejectsInvalidInput(t *testing.T) {
	codec := newTestCodec(t)
	for _, test := range []struct {
		name     string
		typeName string
		body     string
	}{
		{
			name:     "unknown type",
			typeName: "example.Missing",
			body:     `{}`,
		},
		{
			name:     "enum is not a message",
			typeName: "example.Record.State",
			body:     `{}`,
		},
		{
			name:     "malformed JSON",
			typeName: "example.Record",
			body:     `{`,
		},
		{
			name:     "unknown field",
			typeName: "example.Record",
			body:     `{"status":"CREATED"}`,
		},
		{
			name:     "two oneof alternatives",
			typeName: "example.Record",
			body:     `{"note":"test","count":"1"}`,
		},
		{
			name:     "invalid enum",
			typeName: "example.Record",
			body:     `{"state":"CREATED_WRONG"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := codec.Encode(test.typeName, []byte(test.body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := codec.Decode("example.Record", []byte{0xff}); err == nil {
		t.Fatal("expected error for malformed wire payload")
	}
}

func TestMissingProtoDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name       string
		rootSource string
		missing    string
	}{
		{
			name:    "missing root",
			missing: "root.proto",
		},
		{
			name:       "missing import",
			rootSource: `syntax = "proto3"; import "absent.proto"; message Root {}`,
			missing:    "absent.proto",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			imports := t.TempDir()
			if test.rootSource != "" {
				writeProto(t, root, "root.proto", test.rootSource)
			}
			_, err := protobuf.New(context.Background(), []string{"root.proto"}, []string{root, imports})
			if err == nil {
				t.Fatal("expected missing file error")
			}
			for _, detail := range []string{"root.proto", test.missing, root, imports} {
				if !strings.Contains(err.Error(), detail) {
					t.Errorf("error %q does not identify %q", err, detail)
				}
			}
		})
	}
}

func assertJSONEqual(
	t *testing.T,
	want []byte,
	got []byte,
) {
	t.Helper()
	var wantValue, gotValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantValue, gotValue) {
		t.Fatalf("JSON differs: got %s, want %s", got, want)
	}
}
