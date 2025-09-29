package broker

import (
	"context"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader      *kafka.Reader
	messageChan chan kafka.Message
	topic       string
}

func (kc *KafkaConsumer) CommitOffset(ctx context.Context, msg kafka.Message) error {
	return kc.reader.CommitMessages(ctx, msg)
}

func (kc *KafkaConsumer) GetLastOffset() int64 {
	return kc.reader.Offset()
}

func NewKafkaConsumer(brokers []string, topic string, groupID string, messageChan chan kafka.Message) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &KafkaConsumer{
		reader:      reader,
		messageChan: messageChan,
		topic:       topic,
	}
}

func (kc *KafkaConsumer) StartConsuming(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			err := kc.Close()
			if err != nil {
				slog.Error("Failed to close Kafka reader", err)

				return err
			}

			slog.Info("kafka consumer stopped successfully", slog.String("topic", kc.topic))

			return nil
		default:
			msg, err := kc.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				slog.Error("Failed to fetch Kafka message", slog.String("topic", kc.topic), slog.String("error", err.Error()))

				continue
			}

			// slog.Info("message received", slog.String("topic", kc.topic), slog.String("message", string(msg.Value)))

			kc.messageChan <- msg
		}
	}
}

func (kc *KafkaConsumer) Close() error {
	return kc.reader.Close()
}
