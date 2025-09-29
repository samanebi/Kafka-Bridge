package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"kafak-bridge/internal/model"
	"log/slog"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaProducer{
		writer: writer,
		topic:  topic,
	}
}

func (kp *KafkaProducer) ProduceEvent(ctx context.Context, event *model.Event) error {
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	message := kafka.Message{
		Key:   []byte("kafka-bridge"),
		Value: eventData,
	}

	err = kp.writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (kp *KafkaProducer) GenerateSampleEvents(ctx context.Context, count int) error {
	eventTypes := []string{"purchase", "view", "login", "logout", "signup"}

	for i := 1; i <= count; i++ {
		select {
		case <-ctx.Done():
			err := kp.Close()
			if err != nil {
				slog.Error("failed to close kafka producer", slog.Any("error", err))

				return err
			}

			return ctx.Err()
		default:
			event := &model.Event{
				UserID:    fmt.Sprintf("user_%d", rand.Intn(10000)),
				EventType: eventTypes[rand.Intn(len(eventTypes))],
				Amount:    rand.Float64() * 1000,
				Metadata:  json.RawMessage(fmt.Sprintf(`{"product_id":"p_%d","location":"IR"}`, rand.Intn(100))),
				CreatedAt: time.Now(),
			}

			if err := kp.ProduceEvent(ctx, event); err != nil {
				return err
			}

			time.Sleep(10 * time.Millisecond)
		}
	}

	return nil
}

func (kp *KafkaProducer) Close() error {
	return kp.writer.Close()
}
