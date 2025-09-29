package database

import (
	"context"
	"errors"
	"fmt"
	"kafak-bridge/internal/model"
	"kafak-bridge/internal/repository"
	"log/slog"
	"math"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type EventRepository struct {
	db *gorm.DB
}

func (r *EventRepository) GetBySequence(ctx context.Context, sequence uint64) (*model.Event, error) {
	var event model.Event
	err := r.db.WithContext(ctx).Table("events").Select("*").Where("sequence = ?", sequence).Find(&event).Limit(1).Error
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (r *EventRepository) Exists(ctx context.Context, sequence uint64) (bool, error) {
	err := r.db.WithContext(ctx).Table("events").Select("1").Where("sequence = ?", sequence).First(&model.Event{}).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {

			return false, nil
		}

		return false, err
	}

	return true, nil
}

func NewPostgresRepository(dsn string) (*EventRepository, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.AutoMigrate(&model.Event{}); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return &EventRepository{db: db}, nil
}

func (r *EventRepository) Create(ctx context.Context, event *model.Event) error {
	slog.Info("create event", "event", event)
	result := r.db.WithContext(ctx).Table("events").Create(event)

	return result.Error
}

func (r *EventRepository) List(ctx context.Context, page, size int) (repository.PaginatedResponse, error) {
	var events []model.Event
	var total int64

	offset := (page - 1) * size

	if err := r.db.WithContext(ctx).Table("events").Count(&total).Error; err != nil {
		return repository.PaginatedResponse{}, err
	}

	if err := r.db.WithContext(ctx).Table("events").
		Select("*").
		Order("sequence DESC").
		Offset(offset).
		Limit(size).
		Find(&events).Error; err != nil {
		return repository.PaginatedResponse{}, err
	}

	totalPages := int(math.Ceil(float64(total) / float64(size)))
	hasNext := page < totalPages

	return repository.PaginatedResponse{
		Events:     events,
		Total:      int(total),
		Page:       page,
		Size:       size,
		TotalPages: totalPages,
		HasNext:    hasNext,
	}, nil
}

func (r *EventRepository) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return r.db.WithContext(ctx).Table("events").Transaction(func(tx *gorm.DB) error {
		return fn(ctx)
	})
}
