package kafka

import (
	"context"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Client реализует чтение и публикацию сообщений и владеет соединениями с Kafka.
type Client struct {
	// reader выполняет запросы метаданных, границ партиций и пакетов записей.
	reader readClient
	// writer публикует сообщения и передаёт подтверждённые позиции через Completion.
	writer messageWriter
	// transport используется совместно читателем и отправителем.
	transport *kafkago.Transport
	// cancel останавливает фоновое обнаружение брокеров при закрытии клиента.
	cancel context.CancelFunc
	// closeOnce запускает закрытие writer только один раз.
	closeOnce sync.Once
	// closed закрывается после завершения writer.Close и освобождения простаивающих соединений.
	closed chan struct{}
	// closeErr хранит результат writer.Close; читать его можно после закрытия closed.
	closeErr error
}

// messageWriter описывает синхронную запись через kafka-go, используемую адаптером.
type messageWriter interface {
	// WriteMessages отправляет сообщения и ожидает результата записи в пределах ctx.
	// При отмене ожидания уже переданные сообщения могут быть записаны позднее.
	WriteMessages(
		context.Context,
		...kafkago.Message,
	) error

	// Close завершает отправку накопленных сообщений и закрывает writer.
	// У метода нет контекста: ограничением времени ожидания управляет Client.Close.
	Close() error
}

func New(brokers []string) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &kafkago.Transport{
		Context:     ctx,
		DialTimeout: 5 * time.Second,
	}
	client := &Client{
		reader: &kafkago.Client{
			Addr:      kafkago.TCP(brokers...),
			Transport: transport,
			Timeout:   5 * time.Second,
		},
		transport: transport,
		cancel:    cancel,
		closed:    make(chan struct{}),
	}
	client.writer = &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Transport:    transport,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  3,
		BatchSize:    1,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Completion: func(
			messages []kafkago.Message,
			err error,
		) {
			if err != nil {
				return
			}
			for _, message := range messages {
				if receipt, ok := message.WriterData.(chan kafkago.Message); ok {
					receipt <- message
				}
			}
		},
	}
	return client
}

// Close ожидает завершения публикаций до отмены ctx. При отмене останавливает
// обнаружение брокеров и закрывает неиспользуемые соединения. Активные сетевые
// запросы завершаются по собственным таймаутам и не задерживают выход приложения.
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		go func() {
			c.closeErr = c.writer.Close()
			c.cancel()
			c.transport.CloseIdleConnections()
			close(c.closed)
		}()
	})
	select {
	case <-c.closed:
		return c.closeErr
	case <-ctx.Done():
		c.cancel()
		c.transport.CloseIdleConnections()
		return ctx.Err()
	}
}
