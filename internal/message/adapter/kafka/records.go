package kafka

import (
	"bytes"
	"errors"
	"io"

	"github.com/repzspb/raf/internal/message/model"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

// Учитываем служебные записи при продвижении offset, но не возвращаем их пользователю.
// Освобождаем буферы всех записей, даже если обработчик завершился с ошибкой.
func visitRecords(
	reader kafkago.RecordReader,
	control bool,
	visit func(
		*kafkago.Record,
		bool,
	) error,
) error {
	if reader == nil {
		return nil
	}
	if stream, ok := reader.(*protocol.RecordStream); ok {
		var result error
		for _, batch := range stream.Records {
			err := visitRecords(batch, control, func(
				r *kafkago.Record,
				control bool,
			) error {
				if result != nil {
					return nil
				}
				return visit(r, control)
			})
			result = errors.Join(result, err)
		}
		return result
	}
	if _, ok := reader.(*protocol.ControlBatch); ok {
		control = true
	}
	var result error
	for {
		record, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			return result
		}
		if err != nil {
			return errors.Join(result, err)
		}
		if result == nil {
			result = visit(record, control)
		}
		if record.Key != nil {
			result = errors.Join(result, record.Key.Close())
		}
		if record.Value != nil {
			result = errors.Join(result, record.Value.Close())
		}
	}
}

// kafka-go не предоставляет последний offset пустого пакета после compaction.
// Такой пакет ещё не означает конец окна: продвигаемся постепенно,
// чтобы не пропустить оставшиеся записи.
func nextAfterEmptyBatch(
	reader kafkago.RecordReader,
	offset int64,
) (int64, bool) {
	stream, ok := reader.(*protocol.RecordStream)
	if !ok || len(stream.Records) == 0 {
		return offset, false
	}
	next := offset + 1
	for _, batch := range stream.Records {
		// positioned позволяет получить начальную позицию пакета без чтения его записей.
		if positioned, ok := batch.(interface {
			// Offset возвращает базовый offset пакета.
			Offset() int64
		}); ok {
			next = max(next, positioned.Offset()+1)
		}
	}
	return next, true
}

// copyRecord копирует данные до освобождения буферов сетевого ответа.
func copyRecord(
	topic string,
	partition int,
	record *kafkago.Record,
) (model.Message, error) {
	key, err := kafkago.ReadAll(record.Key)
	if err != nil {
		return model.Message{}, err
	}
	value, err := kafkago.ReadAll(record.Value)
	if err != nil {
		return model.Message{}, err
	}
	headers := make([]model.Header, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = model.Header{
			Key:   header.Key,
			Value: bytes.Clone(header.Value),
		}
	}
	return model.Message{
		Topic:     topic,
		Partition: partition,
		Offset:    record.Offset,
		Time:      record.Time,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, nil
}
