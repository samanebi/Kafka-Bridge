// internal/application/services/event_service.go
package services

import (
	"context"
	"fmt"
	"kafak-bridge/internal/model"
	"kafak-bridge/internal/repository"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type EventService struct {
	eventRepo    repository.EventRepository
	consumerRepo repository.ConsumerRepository
}

func NewEventService(eventRepo repository.EventRepository, consumerRepo repository.ConsumerRepository) *EventService {
	return &EventService{
		eventRepo:    eventRepo,
		consumerRepo: consumerRepo,
	}
}

func (s *EventService) ProcessEvent(ctx context.Context, event *model.Event, msg kafka.Message) error {
	event.Sequence = uint64(msg.Offset)
	exists, err := s.eventRepo.Exists(ctx, uint64(msg.Offset))
	if err != nil {
		slog.Error("processing event failed", slog.Any("err", err))

		return fmt.Errorf("checking duplicate: %w", err)
	}

	if exists {
		slog.Debug("skipping duplicate event", slog.Int("partition", msg.Partition), slog.String("topic", msg.Topic), slog.Int64("offset", msg.Offset))

		return nil
	}

	slog.Info("event does not exists", slog.Int("partition", msg.Partition))

	if err := event.Validate(); err != nil {
		return fmt.Errorf("event validation failed: %w", err)
	}

	err = s.eventRepo.WithTransaction(ctx, func(txCtx context.Context) error {
		slog.Debug("creating event into database")
		if err := s.eventRepo.Create(txCtx, event); err != nil {
			slog.Error("Failed to create event", slog.Any("err", err))

			return err
		}

		slog.Debug("event created successfully", slog.Int("partition", msg.Partition), slog.Int64("offset", msg.Offset), slog.String("topic", msg.Topic))

		if err := s.consumerRepo.CommitOffset(txCtx, msg); err != nil {
			return fmt.Errorf("committing offset: %w", err)
		}

		return nil
	})

	return err
}
