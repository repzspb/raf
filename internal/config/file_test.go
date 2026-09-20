package config_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/repzspb/raf/internal/config"
	"github.com/repzspb/raf/internal/message/adapter/protobuf"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"RAF_CONFIG_FILE", "RAF_HTTP_ADDR", "RAF_KAFKA_BROKERS",
		"RAF_PROTO_FILES", "RAF_PROTO_IMPORT_PATHS", "RAF_TOPIC_TYPES",
	} {
		t.Setenv(name, "")
	}
}

func writeConfig(
	t *testing.T,
	body string,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raf.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFileLoadsMultipleContractsRelativeToItsDirectory(t *testing.T) {
	clearConfigEnv(t)
	second := t.TempDir()
	path := writeConfig(t, fmt.Sprintf(`
http:
  address: "127.0.0.1:18080"
kafka:
  brokers: [" localhost:19092 "]
protobuf:
  import_paths: ["contracts one", %q]
  files: ["one.proto", "two.proto"]
topics:
  example.one:
    type: sample.One
  example.two:
    type: sample.Two
`, second))
	first := filepath.Join(filepath.Dir(path), "contracts one")
	if err := os.Mkdir(first, 0700); err != nil {
		t.Fatal(err)
	}
	for file, body := range map[string]string{
		filepath.Join(first, "one.proto"):  `syntax = "proto3"; package sample; message One { string id = 1; }`,
		filepath.Join(second, "two.proto"): `syntax = "proto3"; package sample; import "one.proto"; message Two { One child = 1; }`,
	} {
		if err := os.WriteFile(file, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RAF_CONFIG_FILE", path)
	// Рабочий каталог не должен влиять на пути, заданные в файле.
	t.Chdir(t.TempDir())
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:18080" || !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:19092"}) ||
		!reflect.DeepEqual(cfg.ProtoImportPaths, []string{first, second}) {
		t.Fatalf("config=%+v", cfg)
	}
	codec, err := protobuf.New(t.Context(), cfg.ProtoFiles, cfg.ProtoImportPaths)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(codec.MessageTypes(), []string{"sample.One", "sample.Two"}) {
		t.Fatalf("types=%v", codec.MessageTypes())
	}
	for _, name := range cfg.TopicTypes {
		if err := codec.ValidateType(name); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnvironmentReplacesFileValues(t *testing.T) {
	clearConfigEnv(t)
	path := writeConfig(t, `
kafka:
  brokers: ["file:9092"]
protobuf:
  files: ["file.proto"]
topics:
  old:
    type: sample.Old
`)
	t.Setenv("RAF_CONFIG_FILE", path)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || !reflect.DeepEqual(cfg.ProtoImportPaths, []string{filepath.Dir(path)}) {
		t.Fatalf("file defaults=%+v", cfg)
	}
	t.Setenv("RAF_HTTP_ADDR", ":18081")
	t.Setenv("RAF_KAFKA_BROKERS", "env:9092,env:9093")
	t.Setenv("RAF_PROTO_FILES", "env.proto")
	t.Setenv("RAF_PROTO_IMPORT_PATHS", "./env-protos")
	t.Setenv("RAF_TOPIC_TYPES", `{"new":"sample.New"}`)
	cfg, err = config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":18081" ||
		!reflect.DeepEqual(cfg.KafkaBrokers, []string{"env:9092", "env:9093"}) ||
		!reflect.DeepEqual(cfg.ProtoFiles, []string{"env.proto"}) ||
		!reflect.DeepEqual(cfg.ProtoImportPaths, []string{"./env-protos"}) ||
		!reflect.DeepEqual(cfg.TopicTypes, map[string]string{"new": "sample.New"}) {
		t.Fatalf("overrides=%+v", cfg)
	}
	t.Setenv("RAF_TOPIC_TYPES", "{}")
	cfg, err = config.Load()
	if err != nil || len(cfg.TopicTypes) != 0 {
		t.Fatalf("clear mapping=%+v error=%v", cfg, err)
	}
}

func TestInvalidConfigFilesFailAtStartup(t *testing.T) {
	for name, body := range map[string]string{
		"empty":                       "",
		"null":                        "null",
		"sequence":                    "[]",
		"unknown field":               "unknown: value",
		"nested typo":                 "http: {adress: ':8080'}",
		"duplicate section":           "topics: {}\ntopics: {}",
		"duplicate topic":             "topics: {events: {type: A}, events: {type: B}}",
		"duplicate property":          "topics: {events: {type: A, type: B}}",
		"multiple documents":          "{}\n---\n{}",
		"malformed trailing document": "{}\n---\n[",
		"null section":                "http: null",
		"null property":               "http: {address: null}",
		"number":                      "topics: {events: {type: 42}}",
		"null topic":                  "topics: {events: null}",
		"empty path":                  "protobuf: {import_paths: [' ']}",
		"empty paths":                 "protobuf: {import_paths: []}",
	} {
		t.Run(name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("RAF_CONFIG_FILE", writeConfig(t, body))
			if _, err := config.Load(); err == nil || !strings.Contains(err.Error(), "RAF_CONFIG_FILE") {
				t.Fatalf("expected file diagnostic, got %v", err)
			}
		})
	}
	for name, body := range map[string]string{
		"empty address":      "http: {address: ''}",
		"empty broker":       "kafka: {brokers: ['']}",
		"empty file name":    "protobuf: {files: ['']}",
		"empty topic type":   "topics: {events: {type: ''}}",
		"missing topic type": "topics: {events: {}}",
		"empty topic name":   "topics: {'': {type: sample.Event}}",
	} {
		t.Run(name, func(t *testing.T) {
			setRequiredEnv(t)
			if name == "empty broker" {
				t.Setenv("RAF_KAFKA_BROKERS", "")
			}
			if name == "empty file name" {
				t.Setenv("RAF_PROTO_FILES", "")
			}
			t.Setenv("RAF_CONFIG_FILE", writeConfig(t, body))
			if _, err := config.Load(); err == nil {
				t.Fatal("invalid settings accepted")
			}
		})
	}
	t.Run("missing file", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("RAF_CONFIG_FILE", filepath.Join(t.TempDir(), "missing.yaml"))
		if _, err := config.Load(); err == nil || !strings.Contains(err.Error(), "RAF_CONFIG_FILE") {
			t.Fatalf("missing file diagnostic: %v", err)
		}
	})
}

func TestBundledConfiguration(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("RAF_CONFIG_FILE", "../../examples/raf.yaml")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	codec, err := protobuf.New(t.Context(), cfg.ProtoFiles, cfg.ProtoImportPaths)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.TopicTypes) != 2 {
		t.Fatalf("mappings=%v", cfg.TopicTypes)
	}
	for _, name := range cfg.TopicTypes {
		body, err := codec.Example(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := codec.Encode(name, body); err != nil {
			t.Fatal(err)
		}
	}
}
