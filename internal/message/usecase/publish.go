package usecase

import (
	"context"

	"github.com/repzspb/raf/internal/message/model"
)

// PublishInput задаёт входные данные сценария публикации.
// Сценарий выбирает контракт и преобразует JSON в бинарное model.Message.
type PublishInput struct {
	// Topic — топик, в который нужно записать сообщение.
	Topic string
	// Type — полное имя типа контракта; пустое значение выбирает настройку топика.
	Type string
	// Key — необязательный бинарный ключ, участвующий в выборе партиции.
	Key []byte
	// Headers — необязательные заголовки Kafka-сообщения, отдельные от его тела.
	Headers []model.Header
	// JSON — тело сообщения в формате ProtoJSON выбранного контракта.
	JSON []byte
}

func (s *Service) Publish(
	ctx context.Context,
	input PublishInput,
) (model.Position, error) {
	name, err := s.messageType(input.Topic, input.Type)
	if err != nil {
		return model.Position{}, err
	}
	value, err := s.codec.Encode(name, input.JSON)
	if err != nil {
		return model.Position{}, &ValidationError{Err: err}
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return s.publisher.Publish(ctx, model.Message{
		Topic:   input.Topic,
		Key:     input.Key,
		Headers: input.Headers,
		Value:   value,
	})
}
