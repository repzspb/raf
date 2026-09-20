package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type stubReader struct {
	metadata func(context.Context, *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error)
	offsets  func(context.Context, *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error)
	fetch    func(context.Context, *kafkago.FetchRequest) (*kafkago.FetchResponse, error)
}

func (s stubReader) Metadata(ctx context.Context, r *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
	return s.metadata(ctx, r)
}
func (s stubReader) ListOffsets(ctx context.Context, r *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error) {
	return s.offsets(ctx, r)
}
func (s stubReader) Fetch(ctx context.Context, r *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
	return s.fetch(ctx, r)
}

func fixture(first, last int64, offsets []int64) stubReader {
	return stubReader{
		metadata: func(_ context.Context, r *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
			return &kafkago.MetadataResponse{Topics: []kafkago.Topic{{Name: r.Topics[0], Partitions: []kafkago.Partition{{ID: 0}}}}}, nil
		},
		offsets: func(_ context.Context, r *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error) {
			topics := make(map[string][]kafkago.PartitionOffsets)
			for topic, reqs := range r.Topics {
				topics[topic] = []kafkago.PartitionOffsets{{Partition: reqs[0].Partition, FirstOffset: first, LastOffset: last}}
			}
			return &kafkago.ListOffsetsResponse{Topics: topics}, nil
		},
		fetch: func(_ context.Context, r *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
			var records []kafkago.Record
			for _, offset := range offsets {
				if offset >= r.Offset {
					records = append(records, kafkago.Record{Offset: offset, Time: time.Unix(offset, 0), Value: kafkago.NewBytes([]byte("value"))})
					if len(records) == 2 {
						break
					} // force multiple fetches within a window
				}
			}
			return &kafkago.FetchResponse{HighWatermark: last, Records: kafkago.NewRecordReader(records...)}, nil
		},
	}
}

func TestRecentAvailableRecords(t *testing.T) {
	tests := []struct {
		name        string
		first, last int64
		offsets     []int64
		limit       int
		want        []int64
	}{
		{"empty", 100, 100, nil, 10, nil},
		{"retained offsets start above zero", 100, 105, []int64{100, 101, 102, 103, 104}, 10, []int64{104, 103, 102, 101, 100}},
		{"limit", 100, 105, []int64{100, 101, 102, 103, 104}, 2, []int64{104, 103}},
		{"compacted gaps", 0, 200, []int64{3, 20, 100, 198}, 3, []int64{198, 100, 20}},
		{"entire log compacted", 0, 10000, nil, 10, nil},
		{"new records excluded", 0, 5, []int64{0, 1, 2, 3, 4, 5, 6}, 2, []int64{4, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{reader: fixture(tt.first, tt.last, tt.offsets)}
			messages, err := c.Recent(t.Context(), "events", tt.limit)
			if err != nil {
				t.Fatal(err)
			}
			var got []int64
			for _, m := range messages {
				got = append(got, m.Offset)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("offsets = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecentAcrossPartitions(t *testing.T) {
	s := fixture(0, 3, []int64{0, 1, 2})
	s.metadata = func(context.Context, *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
		return &kafkago.MetadataResponse{Topics: []kafkago.Topic{{Name: "events", Partitions: []kafkago.Partition{{ID: 0}, {ID: 1}, {ID: 2}}}}}, nil
	}
	messages, err := (&Client{reader: s}).Recent(t.Context(), "events", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Partition != 0 || messages[1].Partition != 1 || messages[0].Offset != 2 || messages[1].Offset != 2 {
		t.Fatalf("unexpected global limit/order: %+v", messages)
	}
}

func TestRecentRetentionAdvances(t *testing.T) {
	s := fixture(100, 105, []int64{103, 104})
	baseOffsets, baseFetch := s.offsets, s.fetch
	var moved bool
	s.offsets = func(ctx context.Context, r *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error) {
		result, err := baseOffsets(ctx, r)
		if moved {
			result.Topics["events"][0].FirstOffset = 103
		}
		return result, err
	}
	s.fetch = func(ctx context.Context, r *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
		if r.Offset < 103 {
			moved = true
			return &kafkago.FetchResponse{Error: kafkago.OffsetOutOfRange}, nil
		}
		return baseFetch(ctx, r)
	}
	messages, err := (&Client{reader: s}).Recent(t.Context(), "events", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Offset != 104 || messages[1].Offset != 103 {
		t.Fatalf("unexpected records: %+v", messages)
	}
}

func TestRecentContinuesPastEmptyCompactedBatch(t *testing.T) {
	s := fixture(0, 10, []int64{7, 9})
	baseFetch := s.fetch
	s.fetch = func(ctx context.Context, request *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
		if request.Offset < 7 {
			return &kafkago.FetchResponse{HighWatermark: 10, Records: &protocol.RecordStream{
				Records: []protocol.RecordReader{&protocol.RecordBatch{
					BaseOffset: 0, Records: kafkago.NewRecordReader(),
				}},
			}}, nil
		}
		return baseFetch(ctx, request)
	}
	messages, err := (&Client{reader: s}).Recent(t.Context(), "events", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Offset != 9 || messages[1].Offset != 7 {
		t.Fatalf("empty batch hid later records: %+v", messages)
	}
}

func TestRecentSkipsControlAndEarlierRecords(t *testing.T) {
	s := fixture(0, 4, nil)
	s.fetch = func(_ context.Context, req *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
		if req.Offset >= 4 {
			t.Fatalf("read beyond snapshot: %d", req.Offset)
		}
		return &kafkago.FetchResponse{Records: &protocol.RecordStream{Records: []protocol.RecordReader{
			&protocol.RecordBatch{Records: kafkago.NewRecordReader(
				kafkago.Record{Offset: 0, Value: kafkago.NewBytes([]byte("old"))},
				kafkago.Record{Offset: 2, Value: kafkago.NewBytes([]byte("latest"))},
			)},
			protocol.NewControlBatch(protocol.ControlRecord{Offset: 3}),
		}}}, nil
	}
	messages, err := (&Client{reader: s}).Recent(t.Context(), "events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Offset != 2 || string(messages[0].Value) != "latest" {
		t.Fatalf("unexpected records: %+v", messages)
	}
}

func TestRecentCancellationAtEveryStage(t *testing.T) {
	for _, stage := range []string{"metadata", "offsets", "fetch"} {
		t.Run(stage, func(t *testing.T) {
			s := fixture(0, 1, []int64{0})
			started := make(chan struct{})
			block := func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }
			switch stage {
			case "metadata":
				s.metadata = func(ctx context.Context, _ *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
					return nil, block(ctx)
				}
			case "offsets":
				s.offsets = func(ctx context.Context, _ *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error) {
					return nil, block(ctx)
				}
			case "fetch":
				s.fetch = func(ctx context.Context, _ *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
					return nil, block(ctx)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := (&Client{reader: s}).Recent(ctx, "events", 10); done <- err }()
			<-started
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("read did not stop after cancellation")
			}
		})
	}
}

func TestRecentUnavailableBrokerHonorsDeadline(t *testing.T) {
	c := New([]string{"silent-broker:9092"})
	var mu sync.Mutex
	var peers []net.Conn
	c.transport.Dial = func(context.Context, string, string) (net.Conn, error) {
		conn, peer := net.Pipe()
		mu.Lock()
		peers = append(peers, peer)
		mu.Unlock()
		return conn, nil // accept a connection but never answer the Kafka handshake
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(ctx)
		mu.Lock()
		defer mu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Recent(ctx, "events", 10)
	if err == nil {
		t.Fatal("expected unavailable broker error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("deadline ignored: %s, error: %v", elapsed, err)
	}
}

func TestRecentKafkaErrors(t *testing.T) {
	for _, stage := range []string{"topic", "partition", "offsets", "fetch"} {
		t.Run(stage, func(t *testing.T) {
			s := fixture(0, 1, []int64{0})
			switch stage {
			case "topic":
				s.metadata = func(context.Context, *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
					return &kafkago.MetadataResponse{Topics: []kafkago.Topic{{Name: "events", Error: kafkago.UnknownTopicOrPartition}}}, nil
				}
			case "partition":
				s.metadata = func(context.Context, *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
					return &kafkago.MetadataResponse{Topics: []kafkago.Topic{{Name: "events", Partitions: []kafkago.Partition{{Error: kafkago.UnknownTopicOrPartition}}}}}, nil
				}
			case "offsets":
				s.offsets = func(context.Context, *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error) {
					return nil, kafkago.UnknownTopicOrPartition
				}
			case "fetch":
				s.fetch = func(context.Context, *kafkago.FetchRequest) (*kafkago.FetchResponse, error) {
					return &kafkago.FetchResponse{Error: kafkago.UnknownTopicOrPartition}, nil
				}
			}
			_, err := (&Client{reader: s}).Recent(t.Context(), "events", 10)
			if !errors.Is(err, kafkago.UnknownTopicOrPartition) {
				t.Fatal(fmt.Sprintf("expected Kafka error at %s, got %v", stage, err))
			}
		})
	}
}
