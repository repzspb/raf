package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config содержит настройки запуска приложения и загрузки контрактов.
type Config struct {
	// HTTPAddr — адрес прослушивания HTTP; Load использует :8080 по умолчанию.
	HTTPAddr string
	// KafkaBrokers — начальные адреса брокеров для подключения к Kafka.
	KafkaBrokers []string
	// ProtoFiles — корневые файлы контрактов относительно каталогов поиска.
	ProtoFiles []string
	// ProtoImportPaths — каталоги поиска корневых .proto-файлов и их импортов.
	ProtoImportPaths []string
	// TopicTypes — соответствие топиков полным именам типов, используемых по умолчанию.
	TopicTypes map[string]string
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:         ":8080",
		ProtoImportPaths: []string{"."},
	}
	if path := os.Getenv("RAF_CONFIG_FILE"); path != "" {
		if err := applyFile(&cfg, path); err != nil {
			return Config{}, fmt.Errorf("RAF_CONFIG_FILE %q: %w", path, err)
		}
	}
	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnv заменяет настройки только для непустых переменных окружения.
func applyEnv(cfg *Config) error {
	if raw := os.Getenv("RAF_HTTP_ADDR"); raw != "" {
		cfg.HTTPAddr = raw
	}
	if raw := os.Getenv("RAF_KAFKA_BROKERS"); raw != "" {
		cfg.KafkaBrokers = split(raw, ",")
	}
	if raw := os.Getenv("RAF_PROTO_FILES"); raw != "" {
		cfg.ProtoFiles = split(raw, ",")
	}
	if raw := os.Getenv("RAF_PROTO_IMPORT_PATHS"); raw != "" {
		cfg.ProtoImportPaths = filepath.SplitList(raw)
	}
	for i, path := range cfg.ProtoImportPaths {
		cfg.ProtoImportPaths[i] = strings.TrimSpace(path)
	}
	if raw := os.Getenv("RAF_TOPIC_TYPES"); raw != "" {
		var mappings map[string]*string
		if err := json.Unmarshal([]byte(raw), &mappings); err != nil || mappings == nil {
			return fmt.Errorf("RAF_TOPIC_TYPES must be a JSON object mapping topics to protobuf message names")
		}
		cfg.TopicTypes = make(map[string]string, len(mappings))
		for topic, messageType := range mappings {
			if strings.TrimSpace(topic) == "" || messageType == nil || strings.TrimSpace(*messageType) == "" {
				return fmt.Errorf("RAF_TOPIC_TYPES requires non-empty topics and message names")
			}
			cfg.TopicTypes[topic] = strings.TrimSpace(*messageType)
		}
	}
	return nil
}

// Validate проверяет настройки также при создании приложения без переменных окружения.
func (c Config) Validate() error {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return fmt.Errorf("RAF_HTTP_ADDR / http.address must not be empty")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("RAF_KAFKA_BROKERS / kafka.brokers is required")
	}
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("RAF_KAFKA_BROKERS contains an empty address")
		}
	}
	if len(c.ProtoFiles) == 0 {
		return fmt.Errorf("RAF_PROTO_FILES / protobuf.files is required")
	}
	for _, file := range c.ProtoFiles {
		if strings.TrimSpace(file) == "" {
			return fmt.Errorf("RAF_PROTO_FILES contains an empty file name")
		}
	}
	for _, path := range c.ProtoImportPaths {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("RAF_PROTO_IMPORT_PATHS contains an empty directory")
		}
	}
	for topic, name := range c.TopicTypes {
		if strings.TrimSpace(topic) == "" || strings.TrimSpace(name) == "" {
			return fmt.Errorf("RAF_TOPIC_TYPES requires non-empty topics and message names")
		}
	}
	return nil
}

func split(
	value string,
	separator string,
) []string {
	var result []string
	for _, item := range strings.Split(value, separator) {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
