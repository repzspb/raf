package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

// fileConfig описывает документ YAML; формат файла отделён от настроек приложения.
type fileConfig struct {
	// HTTP содержит настройки HTTP-сервера.
	HTTP httpConfig `yaml:"http"`
	// Kafka содержит начальные адреса брокеров.
	Kafka kafkaConfig `yaml:"kafka"`
	// Protobuf задаёт корневые контракты и каталоги импортов.
	Protobuf protobufConfig `yaml:"protobuf"`
	// Topics связывает имена топиков с настройками raf.
	Topics map[string]topicConfig `yaml:"topics"`
}

// httpConfig задаёт адрес HTTP-сервера в файле.
type httpConfig struct {
	// Address — адрес прослушивания; nil сохраняет значение по умолчанию.
	Address *string `yaml:"address"`
}

// kafkaConfig задаёт параметры подключения к Kafka.
type kafkaConfig struct {
	// Brokers — список адресов host:port.
	Brokers []string `yaml:"brokers"`
}

// protobufConfig объединяет пути ко всем используемым контрактам.
type protobufConfig struct {
	// ImportPaths — каталоги поиска относительно каталога конфигурации.
	ImportPaths []string `yaml:"import_paths"`
	// Files — корневые .proto-файлы относительно каталогов поиска.
	Files []string `yaml:"files"`
}

// topicConfig задаёт контракт одного топика в raf.
type topicConfig struct {
	// Type — полное имя типа сообщения Protobuf.
	Type string `yaml:"type"`
}

func applyFile(
	cfg *Config,
	path string,
) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	file, err := decodeFile(data)
	if err != nil {
		return err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if file.HTTP.Address != nil {
		cfg.HTTPAddr = strings.TrimSpace(*file.HTTP.Address)
	}
	if file.Kafka.Brokers != nil {
		cfg.KafkaBrokers = trimmed(file.Kafka.Brokers)
	}
	if file.Protobuf.Files != nil {
		cfg.ProtoFiles = trimmed(file.Protobuf.Files)
	}
	paths := []string{"."}
	if file.Protobuf.ImportPaths != nil {
		if len(file.Protobuf.ImportPaths) == 0 {
			return fmt.Errorf("protobuf.import_paths must not be empty")
		}
		paths = trimmed(file.Protobuf.ImportPaths)
	}
	cfg.ProtoImportPaths = make([]string, len(paths))
	for i, directory := range paths {
		if directory == "" {
			return fmt.Errorf("protobuf.import_paths contains an empty directory")
		}
		if !filepath.IsAbs(directory) {
			directory = filepath.Join(filepath.Dir(absolute), directory)
		}
		cfg.ProtoImportPaths[i] = filepath.Clean(directory)
	}
	if file.Topics != nil {
		cfg.TopicTypes = make(map[string]string, len(file.Topics))
		for name, topic := range file.Topics {
			cfg.TopicTypes[name] = strings.TrimSpace(topic.Type)
		}
	}
	return nil
}

func decodeFile(data []byte) (fileConfig, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return fileConfig{}, err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return fileConfig{}, fmt.Errorf("configuration must be a YAML mapping")
	}
	if err := validateYAML(&document); err != nil {
		return fileConfig{}, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var file fileConfig
	if err := decoder.Decode(&file); err != nil {
		return fileConfig{}, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return fileConfig{}, err
		}
		return fileConfig{}, fmt.Errorf("configuration must contain exactly one YAML document")
	}
	return file, nil
}

// validateYAML исключает неявные преобразования чисел и null в строковые настройки.
func validateYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode && node.ShortTag() != "!!str" {
		return fmt.Errorf("line %d: configuration values and keys must be strings", node.Line)
	}
	for _, child := range node.Content {
		if err := validateYAML(child); err != nil {
			return err
		}
	}
	return nil
}

func trimmed(values []string) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = strings.TrimSpace(value)
	}
	return result
}
