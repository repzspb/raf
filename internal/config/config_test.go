package config_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/repzspb/raf/internal/config"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RAF_CONFIG_FILE", "")
	t.Setenv("RAF_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("RAF_PROTO_FILES", "events.proto")
	t.Setenv("RAF_HTTP_ADDR", "")
	t.Setenv("RAF_PROTO_IMPORT_PATHS", "")
	t.Setenv("RAF_TOPIC_TYPES", "")
}

func TestLoadDefaultsAndLists(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RAF_KAFKA_BROKERS", " localhost:9092, localhost:9093 ,,")
	t.Setenv("RAF_PROTO_FILES", " events.proto, models/other.proto ,")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if want := []string{"localhost:9092", "localhost:9093"}; !reflect.DeepEqual(cfg.KafkaBrokers, want) {
		t.Errorf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, want)
	}
	if want := []string{"events.proto", "models/other.proto"}; !reflect.DeepEqual(cfg.ProtoFiles, want) {
		t.Errorf("ProtoFiles = %v, want %v", cfg.ProtoFiles, want)
	}
	if want := []string{"."}; !reflect.DeepEqual(cfg.ProtoImportPaths, want) {
		t.Errorf("ProtoImportPaths = %v, want %v", cfg.ProtoImportPaths, want)
	}
	if len(cfg.TopicTypes) != 0 {
		t.Errorf("TopicTypes = %v, want no defaults", cfg.TopicTypes)
	}
}

func TestLoadImportPaths(t *testing.T) {
	setRequiredEnv(t)
	paths := []string{filepath.Join(t.TempDir(), "contracts"), filepath.Join(t.TempDir(), "shared contracts")}
	t.Setenv("RAF_PROTO_IMPORT_PATHS", " "+strings.Join(paths, " "+string(os.PathListSeparator)+" ")+" ")
	t.Setenv("RAF_HTTP_ADDR", "127.0.0.1:18080")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.ProtoImportPaths, paths) {
		t.Errorf("ProtoImportPaths = %v, want %v", cfg.ProtoImportPaths, paths)
	}
	if cfg.HTTPAddr != "127.0.0.1:18080" {
		t.Errorf("HTTPAddr = %q", cfg.HTTPAddr)
	}
}

func TestLoadTopicTypes(t *testing.T) {
	for _, test := range []struct {
		name    string
		raw     string
		want    map[string]string
		invalid bool
	}{
		{
			name: "not configured",
			raw:  "",
			want: nil,
		},
		{
			name: "empty object",
			raw:  `{}`,
			want: map[string]string{},
		},
		{
			name: "mappings",
			raw:  `{"billing.events":" billing.Event ","metrics.raw":"metrics.Metric"}`,
			want: map[string]string{
				"billing.events": "billing.Event",
				"metrics.raw":    "metrics.Metric",
			},
		},
		{
			name:    "invalid JSON",
			raw:     `{`,
			invalid: true,
		},
		{
			name:    "array",
			raw:     `[]`,
			invalid: true,
		},
		{
			name:    "string",
			raw:     `"billing.Event"`,
			invalid: true,
		},
		{
			name:    "null object",
			raw:     `null`,
			invalid: true,
		},
		{
			name:    "null type",
			raw:     `{"billing.events":null}`,
			invalid: true,
		},
		{
			name:    "numeric type",
			raw:     `{"billing.events":42}`,
			invalid: true,
		},
		{
			name:    "empty type",
			raw:     `{"billing.events":""}`,
			invalid: true,
		},
		{
			name:    "blank type",
			raw:     `{"billing.events":"  "}`,
			invalid: true,
		},
		{
			name:    "empty topic",
			raw:     `{"":"billing.Event"}`,
			invalid: true,
		},
		{
			name:    "blank topic",
			raw:     `{"  ":"billing.Event"}`,
			invalid: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("RAF_TOPIC_TYPES", test.raw)
			cfg, err := config.Load()
			if test.invalid {
				if err == nil || !strings.Contains(err.Error(), "RAF_TOPIC_TYPES") {
					t.Fatalf("expected diagnostic naming RAF_TOPIC_TYPES, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.TopicTypes, test.want) {
				t.Errorf("TopicTypes = %#v, want %#v", cfg.TopicTypes, test.want)
			}
		})
	}
}

func TestLoadRejectsEmptyRequiredValues(t *testing.T) {
	for _, test := range []struct{ name, value string }{
		{
			name:  "RAF_KAFKA_BROKERS",
			value: " , ",
		},
		{
			name:  "RAF_PROTO_FILES",
			value: " , ",
		},
		{
			name:  "RAF_PROTO_IMPORT_PATHS",
			value: " ",
		},
		{
			name:  "RAF_PROTO_IMPORT_PATHS",
			value: "." + string(os.PathListSeparator),
		},
		{
			name:  "RAF_PROTO_IMPORT_PATHS",
			value: string(os.PathListSeparator) + ".",
		},
	} {
		t.Run(test.name+"="+test.value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(test.name, test.value)
			if _, err := config.Load(); err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("expected diagnostic naming %s, got %v", test.name, err)
			}
		})
	}
}
