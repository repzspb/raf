package usecase

import (
	"context"
	"fmt"

	"github.com/repzspb/raf/internal/message/model"
)

// InspectInput задаёт параметры просмотра последних сообщений топика.
type InspectInput struct {
	// Topic — топик для чтения из всех его партиций.
	Topic string
	// Type — полное имя типа для декодирования; пустое значение выбирает настройку топика.
	Type string
	// Limit — максимальное число записей в общем результате, от 1 до 100.
	Limit int
}

// InspectedMessage сохраняет исходную запись даже при ошибке декодирования.
type InspectedMessage struct {
	// Message — исходная бинарная запись вместе с позицией и метаданными.
	Message model.Message
	// JSON — декодированное тело; для tombstone или ошибки декодирования не заполняется.
	JSON []byte
	// DecodeError — ошибка декодирования этой записи; остальные записи обрабатываются дальше.
	DecodeError error
}

func (s *Service) Inspect(
	ctx context.Context,
	input InspectInput,
) ([]InspectedMessage, error) {
	name, err := s.messageType(input.Topic, input.Type)
	if err != nil {
		return nil, err
	}
	if input.Limit < 1 || input.Limit > 100 {
		return nil, &ValidationError{Err: fmt.Errorf("limit must be between 1 and 100")}
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	messages, err := s.reader.Recent(ctx, input.Topic, input.Limit)
	if err != nil {
		return nil, err
	}
	result := make([]InspectedMessage, 0, len(messages))
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := InspectedMessage{Message: message}
		if message.Value != nil {
			item.JSON, item.DecodeError = s.codec.Decode(name, message.Value)
		}
		result = append(result, item)
	}
	return result, nil
}
