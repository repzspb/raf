package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	HTTPAddr         string
	KafkaBrokers     []string
	ProtoFiles       []string
	ProtoImportPaths []string
	TopicTypes       map[string]string
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:         getenv("RAF_HTTP_ADDR", ":8080"),
		KafkaBrokers:     split(os.Getenv("RAF_KAFKA_BROKERS"), ","),
		ProtoFiles:       split(os.Getenv("RAF_PROTO_FILES"), ","),
		ProtoImportPaths: filepath.SplitList(getenv("RAF_PROTO_IMPORT_PATHS", ".")),
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("RAF_KAFKA_BROKERS is required")
	}
	if len(cfg.ProtoFiles) == 0 {
		return Config{}, fmt.Errorf("RAF_PROTO_FILES is required")
	}
	for i, path := range cfg.ProtoImportPaths {
		cfg.ProtoImportPaths[i] = strings.TrimSpace(path)
		if cfg.ProtoImportPaths[i] == "" {
			return Config{}, fmt.Errorf("RAF_PROTO_IMPORT_PATHS contains an empty directory")
		}
	}
	if raw := os.Getenv("RAF_TOPIC_TYPES"); raw != "" {
		var mappings map[string]*string
		if err := json.Unmarshal([]byte(raw), &mappings); err != nil || mappings == nil {
			return Config{}, fmt.Errorf("RAF_TOPIC_TYPES must be a JSON object mapping topics to protobuf message names")
		}
		cfg.TopicTypes = make(map[string]string, len(mappings))
		for topic, messageType := range mappings {
			if strings.TrimSpace(topic) == "" || messageType == nil || strings.TrimSpace(*messageType) == "" {
				return Config{}, fmt.Errorf("RAF_TOPIC_TYPES requires non-empty topics and message names")
			}
			cfg.TopicTypes[topic] = strings.TrimSpace(*messageType)
		}
	}
	return cfg, nil
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func split(value, separator string) []string {
	var result []string
	for _, item := range strings.Split(value, separator) {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
