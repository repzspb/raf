package model

// TopicSummary содержит общие сведения о существующем топике Kafka.
type TopicSummary struct {
	// Name — имя топика.
	Name string
	// Internal указывает на служебный топик Kafka.
	Internal bool
}

// Topic описывает топик и доступные диапазоны его партиций.
type Topic struct {
	// TopicSummary содержит общие сведения из метаданных Kafka.
	TopicSummary
	// Partitions содержит партиции в порядке возрастания номера.
	Partitions []Partition
}

// Partition задаёт доступный полуоткрытый диапазон offset одной партиции.
type Partition struct {
	// ID — номер партиции.
	ID int
	// FirstOffset — нижняя включённая граница чтения.
	FirstOffset int64
	// EndOffset — верхняя исключённая граница; разница границ не равна числу сообщений.
	EndOffset int64
}
