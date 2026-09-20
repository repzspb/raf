package kafka

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/repzspb/raf/internal/protobuf"
	kafkago "github.com/segmentio/kafka-go"
)

// Opt in against a disposable/local broker. Only this test's unique topic is
// created and deleted; existing topics and consumer groups are not touched.
func TestIntegrationKafka(t *testing.T) {
	brokers := os.Getenv("RAF_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set RAF_TEST_KAFKA_BROKERS to run against Kafka")
	}
	c := New(strings.Split(brokers, ","))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	admin := c.reader.(*kafkago.Client)
	topic := fmt.Sprintf("raf-test-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	created, err := admin.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := created.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := admin.DeleteTopics(ctx, &kafkago.DeleteTopicsRequest{Topics: []string{topic}})
		if err != nil {
			t.Errorf("delete test topic %s: %v", topic, err)
			return
		}
		if err := result.Errors[topic]; err != nil {
			t.Errorf("delete test topic %s: %v", topic, err)
		}
	})
	t.Logf("test topic: %s", topic)
	// Topic creation can return before every broker has elected its leaders.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		messages, err := c.Recent(ctx, topic, 10)
		if err == nil {
			if len(messages) != 0 {
				t.Fatalf("new topic contains messages: %+v", messages)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic did not become readable: %v", err)
		case <-ticker.C:
		}
	}
	codec, err := protobuf.New(ctx, []string{"event.proto"}, []string{"../../examples/proto"})
	if err != nil {
		t.Fatal(err)
	}
	baseTime := time.Now().UTC().Truncate(time.Millisecond)
	// Compressed batches force Fetch to return records before the requested
	// offset. Partition 2 remains empty during the initial inspection.
	for partition := 0; partition < 2; partition++ {
		var records []kafkago.Record
		for i := 0; i < 8; i++ {
			body := fmt.Sprintf(`{"id":"p%d-%d","status":"CREATED"}`, partition, i)
			value, err := codec.Encode("example.Event", []byte(body))
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, kafkago.Record{
				Time: baseTime.Add(time.Duration(partition*10+i) * time.Millisecond),
				Key:  kafkago.NewBytes([]byte(body)), Value: kafkago.NewBytes(value),
			})
		}
		response, err := admin.Produce(ctx, &kafkago.ProduceRequest{
			Topic: topic, Partition: partition, RequiredAcks: kafkago.RequireAll,
			Compression: kafkago.Gzip, Records: kafkago.NewRecordReader(records...),
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.Error != nil {
			t.Fatal(response.Error)
		}
	}
	messages, err := c.Recent(ctx, topic, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 3 {
		t.Fatalf("limit=3 returned %d records", len(messages))
	}
	for i, message := range messages {
		if message.Partition != 1 || message.Offset != int64(7-i) {
			t.Fatalf("unexpected latest record %d: %+v", i, message)
		}
		if _, err := codec.Decode("example.Event", message.Value); err != nil {
			t.Fatal(err)
		}
	}
	// Exercise raf's actual synchronous publisher and delivery receipt.
	value, err := codec.Encode("example.Event", []byte(`{"id":"raf-published"}`))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := c.Publish(ctx, topic, "raf-key", []kafkago.Header{{Key: "id", Value: []byte("integration")}}, value)
	if err != nil {
		t.Fatal(err)
	}
	messages, err = c.Recent(ctx, topic, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 17 {
		t.Fatalf("got %d records, want 17", len(messages))
	}
	found := false
	for _, message := range messages {
		if message.Partition == receipt.Partition && message.Offset == receipt.Offset {
			found = true
			if !bytes.Equal(message.Value, value) || string(message.Key) != "raf-key" || len(message.Headers) != 1 || string(message.Headers[0].Value) != "integration" {
				t.Fatalf("publish receipt points to wrong record: %+v", message)
			}
		}
	}
	if !found {
		t.Fatal("published record absent from inspection")
	}
	// Preserve Kafka's distinction between zero bytes and null, plus binary
	// metadata. These are fetched through the real wire decoder.
	response, err := admin.Produce(ctx, &kafkago.ProduceRequest{
		Topic: topic, Partition: 2, RequiredAcks: kafkago.RequireAll,
		Records: kafkago.NewRecordReader(
			kafkago.Record{Time: baseTime, Key: kafkago.NewBytes([]byte("deleted")), Value: nil},
			kafkago.Record{Time: baseTime, Key: kafkago.NewBytes([]byte{}), Value: kafkago.NewBytes([]byte{})},
			kafkago.Record{Time: baseTime, Key: kafkago.NewBytes([]byte{0xff}), Value: kafkago.NewBytes(value), Headers: []kafkago.Header{{Key: "binary", Value: []byte{0xfe}}}},
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	messages, err = c.Recent(ctx, topic, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 20 {
		t.Fatalf("got %d records, want 20", len(messages))
	}
	for _, message := range messages {
		if message.Partition != 2 {
			continue
		}
		switch message.Offset - response.BaseOffset {
		case 0:
			if message.Value != nil {
				t.Fatalf("tombstone became %#v", message.Value)
			}
		case 1:
			if message.Value == nil || len(message.Value) != 0 || message.Key == nil {
				t.Fatalf("empty record became null: %+v", message)
			}
		case 2:
			if !bytes.Equal(message.Key, []byte{0xff}) || len(message.Headers) != 1 || !bytes.Equal(message.Headers[0].Value, []byte{0xfe}) {
				t.Fatalf("binary metadata changed: %+v", message)
			}
		}
	}
}
