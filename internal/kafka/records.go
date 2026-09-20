package kafka

import (
	"errors"
	"io"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

// Visit control records too so they advance the fetch offset without appearing
// as user messages. Release every record's buffers, even after a callback error.
func visitRecords(reader kafkago.RecordReader, control bool, visit func(*kafkago.Record, bool) error) error {
	if reader == nil {
		return nil
	}
	if stream, ok := reader.(*protocol.RecordStream); ok {
		var result error
		for _, batch := range stream.Records {
			err := visitRecords(batch, control, func(r *kafkago.Record, control bool) error {
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

// kafka-go does not expose the last offset of an empty compacted batch. An
// actual wire batch therefore differs from an empty fetch response: advance
// conservatively instead of incorrectly declaring the window exhausted.
func nextAfterEmptyBatch(reader kafkago.RecordReader, offset int64) (int64, bool) {
	stream, ok := reader.(*protocol.RecordStream)
	if !ok || len(stream.Records) == 0 {
		return offset, false
	}
	next := offset + 1
	for _, batch := range stream.Records {
		if positioned, ok := batch.(interface{ Offset() int64 }); ok {
			next = max(next, positioned.Offset()+1)
		}
	}
	return next, true
}
