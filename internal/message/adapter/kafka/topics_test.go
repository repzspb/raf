package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/repzspb/raf/internal/message/usecase"
	kafkago "github.com/segmentio/kafka-go"
)

func TestTopicCatalog(t *testing.T) {
	stub := fixture(100, 103, nil)
	stub.metadata = func(
		ctx context.Context,
		request *kafkago.MetadataRequest,
	) (*kafkago.MetadataResponse, error) {
		if len(request.Topics) == 0 {
			return &kafkago.MetadataResponse{Topics: []kafkago.Topic{
				{Name: "z"},
				{
					Name:     "__consumer_offsets",
					Internal: true,
				},
			}}, nil
		}
		return &kafkago.MetadataResponse{Topics: []kafkago.Topic{{
			Name:       request.Topics[0],
			Partitions: []kafkago.Partition{{ID: 2}, {ID: 0}},
		}}}, nil
	}
	client := &Client{reader: stub}
	topics, err := client.ListTopics(t.Context())
	if err != nil || len(topics) != 2 || topics[0].Name != "__consumer_offsets" || !topics[0].Internal {
		t.Fatalf("topics=%+v error=%v", topics, err)
	}
	topic, err := client.DescribeTopic(t.Context(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if topic.Name != "events" || len(topic.Partitions) != 2 {
		t.Fatalf("topic=%+v", topic)
	}
	for i, partition := range topic.Partitions {
		if partition.ID != i*2 || partition.FirstOffset != 100 || partition.EndOffset != 103 {
			t.Fatalf("partition=%+v", partition)
		}
	}
}

func TestDescribeTopicErrors(t *testing.T) {
	for _, stage := range []string{"missing", "metadata", "partition", "offsets"} {
		t.Run(stage, func(t *testing.T) {
			stub := fixture(0, 0, nil)
			switch stage {
			case "missing":
				stub.metadata = func(
					context.Context,
					*kafkago.MetadataRequest,
				) (*kafkago.MetadataResponse, error) {
					return &kafkago.MetadataResponse{}, nil
				}
			case "metadata":
				stub.metadata = func(
					context.Context,
					*kafkago.MetadataRequest,
				) (*kafkago.MetadataResponse, error) {
					return nil, context.DeadlineExceeded
				}
			case "partition":
				stub.metadata = func(
					context.Context,
					*kafkago.MetadataRequest,
				) (*kafkago.MetadataResponse, error) {
					return &kafkago.MetadataResponse{Topics: []kafkago.Topic{{
						Name:       "events",
						Partitions: []kafkago.Partition{{Error: kafkago.RequestTimedOut}},
					}}}, nil
				}
			case "offsets":
				stub.offsets = func(
					context.Context,
					*kafkago.ListOffsetsRequest,
				) (*kafkago.ListOffsetsResponse, error) {
					return nil, context.Canceled
				}
			}
			topic, err := (&Client{reader: stub}).DescribeTopic(t.Context(), "events")
			if err == nil || topic.Partitions != nil {
				t.Fatalf("unexpected partial result: %+v, %v", topic, err)
			}
			var notFound *usecase.NotFoundError
			var timeout *usecase.TimeoutError
			if stage == "missing" && !errors.As(err, &notFound) {
				t.Fatalf("missing classification: %v", err)
			}
			if (stage == "metadata" || stage == "partition") && !errors.As(err, &timeout) {
				t.Fatalf("timeout classification: %v", err)
			}
			if stage == "offsets" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}
