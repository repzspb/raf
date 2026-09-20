package usecase

import (
	"context"

	"github.com/repzspb/raf/internal/message/model"
)

// TopicCatalog предоставляет сведения о существующих топиках брокера.
type TopicCatalog interface {
	// ListTopics возвращает доступные топики без чтения сообщений и диапазонов offset.
	ListTopics(ctx context.Context) ([]model.TopicSummary, error)

	// DescribeTopic возвращает партиции и их текущие границы; отсутствующий топик даёт NotFoundError.
	DescribeTopic(
		ctx context.Context,
		name string,
	) (model.Topic, error)
}

// Publisher записывает бинарные сообщения в брокер.
type Publisher interface {
	// Publish ожидает подтверждения записи и возвращает её позицию.
	// Отмена ctx прекращает ожидание, но не гарантирует отмену записи в брокере.
	Publish(
		context.Context,
		model.Message,
	) (model.Position, error)
}

// Reader читает сообщения для просмотра без изменения позиции потребителей.
type Reader interface {
	// Recent выбирает до limit последних доступных записей из каждой партиции,
	// объединяет их по убыванию времени и ограничивает общий результат значением limit.
	// При равном времени записи упорядочиваются по партиции, затем по убыванию offset.
	// limit должен быть от 1 до 100; новые записи после захвата границ партиций не включаются.
	// При отмене ctx или ошибке чтения частичный результат не возвращается.
	Recent(
		ctx context.Context,
		topic string,
		limit int,
	) ([]model.Message, error)
}

// Codec предоставляет загруженные контракты и преобразует JSON в бинарный формат и обратно.
type Codec interface {
	// Example создаёт пример ProtoJSON для выбранного типа без обращения к брокеру.
	// Отмена ctx прерывает обход контракта.
	Example(
		ctx context.Context,
		name string,
	) ([]byte, error)

	// ValidateType проверяет, что name — полное имя доступного типа сообщения.
	ValidateType(name string) error

	// MessageTypes возвращает отсортированные полные имена типов, включая вложенные
	// и импортированные сообщения, но исключая служебные типы элементов map.
	MessageTypes() []string

	// Encode проверяет JSON по контракту name и возвращает бинарное сообщение.
	// Пустое сообщение представляется срезом нулевой длины, отличным от nil,
	// чтобы при публикации оно не стало tombstone.
	Encode(
		name string,
		jsonValue []byte,
	) ([]byte, error)

	// Decode преобразует бинарное сообщение типа name в JSON.
	// Tombstone обрабатывается сценарием отдельно и не передаётся в Decode.
	Decode(
		name string,
		value []byte,
	) ([]byte, error)
}
